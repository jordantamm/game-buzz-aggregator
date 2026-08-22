package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jordantamm/game-buzz-aggregator/pkg/embed"
	"github.com/jordantamm/game-buzz-aggregator/pkg/models"
	"github.com/jordantamm/game-buzz-aggregator/pkg/queries"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Deps carries the backends the tool handlers need.
type Deps struct {
	Pool *pgxpool.Pool
	// Embedder may be nil; search_mentions then degrades to keyword-only.
	Embedder *embed.Client
}

// Register adds all GBA tools to the MCP server.
func Register(s *server.MCPServer, deps Deps) {
	s.AddTool(searchMentionsTool(), searchMentionsHandler(deps))
	s.AddTool(getGameBuzzTool(), getGameBuzzHandler(deps.Pool))
	s.AddTool(findSimilarGamesTool(), findSimilarGamesHandler(deps.Pool))
	s.AddTool(listTrendingTool(), listTrendingHandler(deps.Pool))
}

// ── search_mentions ─────────────────────────────────────────────────────────

func searchMentionsTool() mcp.Tool {
	return mcp.NewTool("search_mentions",
		mcp.WithDescription(`Hybrid search over individual Reddit mentions about video games.

Combines two independent retrieval passes and fuses them with reciprocal rank fusion:
  1. Keyword matching (pg_trgm) - finds mentions containing the literal words you search for.
  2. Semantic similarity (pgvector) - finds mentions that mean the same thing in different words.

Because of the semantic pass, you do NOT need to guess the exact wording used by posters.
A query like "people are frustrated by the difficulty" will surface posts complaining about
hard bosses even if they never use the word "frustrated".

Each result reports "matched_by": "keyword", "vector", or "both". Results matched by "both"
are the strongest hits. "score" is the fused rank score (higher is better), not a similarity
percentage - compare results to each other, do not interpret it as an absolute confidence.

Parameters:
  query   (required) - what to search for. Natural language works better than keywords alone.
  game_id (optional) - restrict results to one game slug, e.g. "elden-ring". Use this when you
                       already know the game; omit it to search across all games.
  since   (optional) - RFC3339 timestamp lower bound, e.g. "2026-08-01T00:00:00Z". Omit for all time.
  limit   (optional) - number of results, default 20, max 100.

Examples:
  search_mentions(query="is the dlc worth buying")
  search_mentions(query="performance problems on pc", game_id="cyberpunk-2077", limit=10)
  search_mentions(query="disappointed with the ending", since="2026-08-01T00:00:00Z")

Use get_game_buzz instead if you want counts and sentiment over time rather than individual posts.`),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural-language search string; matched both literally and semantically")),
		mcp.WithString("game_id", mcp.Description("Optional game slug filter, e.g. elden-ring")),
		mcp.WithString("since", mcp.Description("Optional RFC3339 lower bound on mention creation time")),
		mcp.WithNumber("limit", mcp.Description("Max results to return (default 20, max 100)")),
	)
}

func searchMentionsHandler(deps Deps) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx, span := otel.Tracer("mcp-server").Start(ctx, "tool.search_mentions")
		defer span.End()

		query, _ := req.Params.Arguments["query"].(string)
		if query == "" {
			return errorResult("query is required and must be a non-empty string"), nil
		}

		params := queries.HybridSearchParams{
			Query: query,
			Limit: 20,
		}
		if gid, ok := req.Params.Arguments["game_id"].(string); ok {
			params.GameID = gid
		}
		if l, ok := req.Params.Arguments["limit"].(float64); ok && l > 0 {
			params.Limit = minInt(int(l), 100)
		}
		if s, ok := req.Params.Arguments["since"].(string); ok && s != "" {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return errorResult(fmt.Sprintf(
					"since must be an RFC3339 timestamp such as 2026-08-01T00:00:00Z (got %q)", s)), nil
			}
			params.Since = t
		}
		span.SetAttributes(
			attribute.String("tool.query", params.Query),
			attribute.String("tool.game_id", params.GameID),
			attribute.Int("tool.limit", params.Limit),
		)

		hits, err := queries.HybridSearch(ctx, deps.Pool, deps.Embedder, params)
		if err != nil {
			return nil, fmt.Errorf("search_mentions: %w", err)
		}
		return jsonResult(map[string]any{
			"query":        params.Query,
			"result_count": len(hits),
			"results":      hits,
		})
	}
}

// ── get_game_buzz ────────────────────────────────────────────────────────────

func getGameBuzzTool() mcp.Tool {
	return mcp.NewTool("get_game_buzz",
		mcp.WithDescription(`Mention volume and average sentiment for one game over time.

Returns a timeseries of buckets, each with a mention_count and an avg_sentiment in the range
-1.0 (uniformly negative) to +1.0 (uniformly positive). avg_sentiment is null for buckets with
no scored mentions - treat null as "no data", not as neutral.

Use this to answer "is buzz rising or falling" and "did sentiment shift". Use search_mentions
instead if you need the actual text of what people said.

Parameters:
  game_id (required) - game slug, e.g. "elden-ring". Get valid slugs from list_trending.
  window  (required) - how far back to look: "24h", "7d", or "30d".
  bucket  (optional) - "hour" (default) or "day". Use "day" for 7d/30d windows, otherwise the
                       series is long and noisy.

Examples:
  get_game_buzz(game_id="baldurs-gate-3", window="7d", bucket="day")
  get_game_buzz(game_id="elden-ring", window="24h", bucket="hour")`),
		mcp.WithString("game_id", mcp.Required(), mcp.Description("Game slug, e.g. elden-ring")),
		mcp.WithString("window", mcp.Required(), mcp.Description(`Time window: "24h", "7d", or "30d"`)),
		mcp.WithString("bucket", mcp.Description(`Aggregation granularity: "hour" (default) or "day"`)),
	)
}

func getGameBuzzHandler(pool *pgxpool.Pool) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx, span := otel.Tracer("mcp-server").Start(ctx, "tool.get_game_buzz")
		defer span.End()

		gameID, _ := req.Params.Arguments["game_id"].(string)
		if gameID == "" {
			return errorResult("game_id is required; call list_trending to discover valid game slugs"), nil
		}
		window := parseDuration(req.Params.Arguments["window"])
		bucket := "1 hour"
		if b, _ := req.Params.Arguments["bucket"].(string); b == "day" {
			bucket = "1 day"
		}
		span.SetAttributes(
			attribute.String("tool.game_id", gameID),
			attribute.String("tool.bucket", bucket),
		)

		series, err := queries.GameTimeseries(ctx, pool, gameID, window, bucket)
		if err != nil {
			return nil, fmt.Errorf("get_game_buzz: %w", err)
		}
		if len(series) == 0 {
			return jsonResult(map[string]any{
				"game_id": gameID,
				"window":  window.String(),
				"bucket":  bucket,
				"series":  series,
				"note":    "No mentions for this game in the requested window. Try a longer window, or call list_trending to confirm the game_id is correct.",
			})
		}
		return jsonResult(map[string]any{
			"game_id": gameID,
			"window":  window.String(),
			"bucket":  bucket,
			"series":  series,
		})
	}
}

// ── find_similar_games ───────────────────────────────────────────────────────

func findSimilarGamesTool() mcp.Tool {
	return mcp.NewTool("find_similar_games",
		mcp.WithDescription(`Finds games whose community discussion resembles a given game's discussion.

Similarity is computed over the embeddings of what people actually post, not over genre tags or
metadata. Two games score as similar when players talk about them in similar terms - so this
surfaces things like "games people discuss the same way they discuss Hades", which may cross
genre boundaries.

Returns games ordered by ascending "distance" (cosine distance; lower means more similar).

Parameters:
  game_id (required) - the anchor game slug, e.g. "hades".
  limit   (optional) - number of games to return, default 5.

Example: find_similar_games(game_id="hades", limit=5)`),
		mcp.WithString("game_id", mcp.Required(), mcp.Description("Anchor game slug")),
		mcp.WithNumber("limit", mcp.Description("Number of similar games to return (default 5)")),
	)
}

func findSimilarGamesHandler(pool *pgxpool.Pool) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx, span := otel.Tracer("mcp-server").Start(ctx, "tool.find_similar_games")
		defer span.End()

		gameID, _ := req.Params.Arguments["game_id"].(string)
		if gameID == "" {
			return errorResult("game_id is required; call list_trending to discover valid game slugs"), nil
		}
		limit := 5
		if l, ok := req.Params.Arguments["limit"].(float64); ok && l > 0 {
			limit = minInt(int(l), 50)
		}
		span.SetAttributes(attribute.String("tool.game_id", gameID))

		dbRows, err := pool.Query(ctx, `
			WITH anchor AS (
				SELECT AVG(me.embedding) AS avg_embedding
				FROM mention_embeddings me
				JOIN mention_games mg ON mg.mention_id = me.mention_id
				WHERE mg.game_id = $1
			)
			SELECT
				g.id, g.canonical_name,
				AVG(me.embedding <=> anchor.avg_embedding) AS distance
			FROM games g
			JOIN mention_games mg      ON mg.game_id = g.id
			JOIN mention_embeddings me ON me.mention_id = mg.mention_id
			CROSS JOIN anchor
			WHERE g.id != $1 AND anchor.avg_embedding IS NOT NULL
			GROUP BY g.id, g.canonical_name
			ORDER BY distance
			LIMIT $2
		`, gameID, limit)
		if err != nil {
			return nil, fmt.Errorf("find_similar_games: %w", err)
		}
		defer dbRows.Close()

		type result struct {
			GameID        string  `json:"game_id"`
			CanonicalName string  `json:"canonical_name"`
			Distance      float64 `json:"distance"`
		}
		results := []result{}
		for dbRows.Next() {
			var r result
			if err := dbRows.Scan(&r.GameID, &r.CanonicalName, &r.Distance); err != nil {
				return nil, err
			}
			results = append(results, r)
		}
		if err := dbRows.Err(); err != nil {
			return nil, err
		}
		if len(results) == 0 {
			return jsonResult(map[string]any{
				"anchor_game_id": gameID,
				"results":        results,
				"note":           "No similar games found. Either the anchor game has no embedded mentions yet, or no other game does.",
			})
		}
		return jsonResult(map[string]any{"anchor_game_id": gameID, "results": results})
	}
}

// ── list_trending ─────────────────────────────────────────────────────────────

func listTrendingTool() mcp.Tool {
	return mcp.NewTool("list_trending",
		mcp.WithDescription(`Top games ranked by mention volume over a time window.

This is the usual starting point for open-ended questions like "what's hot right now" or
"what should I look at" - it is the only tool that discovers game_id slugs, which the other
tools require as input. Call this first when you do not already know which game you care about.

Each row has game_id, canonical_name, mention_count, and avg_sentiment (-1.0 to +1.0, null when
no mentions in the window carry a sentiment score).

Parameters:
  window (required) - "24h" or "7d".
  limit  (optional) - number of games, default 10.

Example: list_trending(window="24h", limit=10)`),
		mcp.WithString("window", mcp.Required(), mcp.Description(`Time window: "24h" or "7d"`)),
		mcp.WithNumber("limit", mcp.Description("Number of games to return (default 10)")),
	)
}

func listTrendingHandler(pool *pgxpool.Pool) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx, span := otel.Tracer("mcp-server").Start(ctx, "tool.list_trending")
		defer span.End()

		window := parseDuration(req.Params.Arguments["window"])
		limit := 10
		if l, ok := req.Params.Arguments["limit"].(float64); ok && l > 0 {
			limit = minInt(int(l), 100)
		}
		span.SetAttributes(attribute.String("tool.window", window.String()))

		results, err := queries.Trending(ctx, pool, window, limit)
		if err != nil {
			return nil, fmt.Errorf("list_trending: %w", err)
		}
		if results == nil {
			results = []models.Trending{}
		}
		return jsonResult(map[string]any{"window": window.String(), "results": results})
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// errorResult returns a tool-level error. Returning this (rather than a Go
// error) tells the calling model the call failed for a reason it can act on —
// a bad argument — instead of surfacing a transport-level failure. mcp-go
// v0.17.0 has no NewToolResultError helper, hence the explicit construction.
func errorResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{mcp.TextContent{Type: "text", Text: msg}},
		IsError: true,
	}
}

func jsonResult(v any) (*mcp.CallToolResult, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return mcp.NewToolResultText(string(b)), nil
}

func parseDuration(v any) time.Duration {
	switch s, _ := v.(string); s {
	case "7d":
		return 7 * 24 * time.Hour
	case "30d":
		return 30 * 24 * time.Hour
	default:
		return 24 * time.Hour
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
