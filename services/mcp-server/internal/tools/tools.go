package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jordantamm/game-buzz-aggregator/pkg/queries"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"go.opentelemetry.io/otel"
)

// Register adds all GBA tools to the MCP server.
func Register(s *server.MCPServer, pool *pgxpool.Pool) {
	s.AddTool(searchMentionsTool(), searchMentionsHandler(pool))
	s.AddTool(getGameBuzzTool(), getGameBuzzHandler(pool))
	s.AddTool(findSimilarGamesTool(), findSimilarGamesHandler(pool))
	s.AddTool(listTrendingTool(), listTrendingHandler(pool))
}

// ── search_mentions ─────────────────────────────────────────────────────────

func searchMentionsTool() mcp.Tool {
	return mcp.NewTool("search_mentions",
		mcp.WithDescription(`Search game discussion mentions by keyword and optional filters.
Returns up to 'limit' recent mentions matching the query.
Parameters:
  query   (required) - free-text search string, e.g. "elden ring dlc"
  game_id (optional) - filter to a specific game slug, e.g. "elden-ring"
  limit   (optional, default 20, max 100) - number of results to return
Example: search_mentions(query="elden ring", game_id="elden-ring", limit=10)`),
		mcp.WithString("query", mcp.Required(), mcp.Description("Keyword to search for in mention text")),
		mcp.WithString("game_id", mcp.Description("Filter results to a specific game (game slug)")),
		mcp.WithNumber("limit", mcp.Description("Max results to return (default 20, max 100)")),
	)
}

func searchMentionsHandler(pool *pgxpool.Pool) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx, span := otel.Tracer("mcp-server").Start(ctx, "tool.search_mentions")
		defer span.End()

		query, _ := req.Params.Arguments["query"].(string)
		limit := 20
		if l, ok := req.Params.Arguments["limit"].(float64); ok && l > 0 {
			limit = minInt(int(l), 100)
		}
		gameID, _ := req.Params.Arguments["game_id"].(string)

		var (
			rows any
			err  error
		)
		if gameID != "" {
			rows, err = queries.RecentMentionsForGame(ctx, pool, gameID, limit)
		} else {
			rows, err = searchByText(ctx, pool, query, limit)
		}
		if err != nil {
			return nil, fmt.Errorf("search_mentions: %w", err)
		}
		return jsonResult(rows)
	}
}

func searchByText(ctx context.Context, pool *pgxpool.Pool, query string, limit int) (any, error) {
	dbRows, err := pool.Query(ctx, `
		SELECT id, source, text, created_at, sentiment_score
		FROM mentions
		WHERE text ILIKE '%' || $1 || '%'
		ORDER BY created_at DESC
		LIMIT $2
	`, query, limit)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()

	type row struct {
		ID             string    `json:"id"`
		Source         string    `json:"source"`
		Text           string    `json:"text"`
		CreatedAt      time.Time `json:"created_at"`
		SentimentScore *float64  `json:"sentiment_score"`
	}
	var results []row
	for dbRows.Next() {
		var r row
		if err := dbRows.Scan(&r.ID, &r.Source, &r.Text, &r.CreatedAt, &r.SentimentScore); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, dbRows.Err()
}

// ── get_game_buzz ────────────────────────────────────────────────────────────

func getGameBuzzTool() mcp.Tool {
	return mcp.NewTool("get_game_buzz",
		mcp.WithDescription(`Returns mention count and average sentiment timeseries for a specific game.
Parameters:
  game_id (required) - game slug, e.g. "elden-ring"
  window  (required) - time window: "24h", "7d", or "30d"
  bucket  (optional) - aggregation granularity: "hour" (default) or "day"
Example: get_game_buzz(game_id="baldurs-gate-3", window="7d", bucket="day")`),
		mcp.WithString("game_id", mcp.Required(), mcp.Description("Game slug, e.g. elden-ring")),
		mcp.WithString("window", mcp.Required(), mcp.Description(`Time window: "24h", "7d", or "30d"`)),
		mcp.WithString("bucket", mcp.Description(`Aggregation: "hour" (default) or "day"`)),
	)
}

func getGameBuzzHandler(pool *pgxpool.Pool) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ctx, span := otel.Tracer("mcp-server").Start(ctx, "tool.get_game_buzz")
		defer span.End()

		gameID, _ := req.Params.Arguments["game_id"].(string)
		window := parseDuration(req.Params.Arguments["window"])
		bucket := "1 hour"
		if b, _ := req.Params.Arguments["bucket"].(string); b == "day" {
			bucket = "1 day"
		}

		since := time.Now().Add(-window)
		dbRows, err := pool.Query(ctx, `
			SELECT
				time_bucket($1::interval, m.created_at) AS bucket,
				COUNT(*) AS mention_count,
				AVG(m.sentiment_score) AS avg_sentiment
			FROM mentions m
			JOIN mention_games mg ON mg.mention_id = m.id
			WHERE mg.game_id = $2 AND m.created_at >= $3
			GROUP BY bucket
			ORDER BY bucket
		`, bucket, gameID, since)
		if err != nil {
			return nil, fmt.Errorf("get_game_buzz: %w", err)
		}
		defer dbRows.Close()

		type point struct {
			Bucket       time.Time `json:"bucket"`
			MentionCount int       `json:"mention_count"`
			AvgSentiment *float64  `json:"avg_sentiment"`
		}
		var series []point
		for dbRows.Next() {
			var p point
			if err := dbRows.Scan(&p.Bucket, &p.MentionCount, &p.AvgSentiment); err != nil {
				return nil, err
			}
			series = append(series, p)
		}
		return jsonResult(map[string]any{"game_id": gameID, "window": window.String(), "series": series})
	}
}

// ── find_similar_games ───────────────────────────────────────────────────────

func findSimilarGamesTool() mcp.Tool {
	return mcp.NewTool("find_similar_games",
		mcp.WithDescription(`Finds games with similar community discussion patterns using vector similarity.
Parameters:
  game_id (required) - game slug to use as the query anchor, e.g. "hades"
  limit   (optional, default 5) - number of similar games to return
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
		limit := 5
		if l, ok := req.Params.Arguments["limit"].(float64); ok && l > 0 {
			limit = int(l)
		}

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
			JOIN mention_games mg   ON mg.game_id = g.id
			JOIN mention_embeddings me ON me.mention_id = mg.mention_id
			CROSS JOIN anchor
			WHERE g.id != $1
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
		var results []result
		for dbRows.Next() {
			var r result
			if err := dbRows.Scan(&r.GameID, &r.CanonicalName, &r.Distance); err != nil {
				return nil, err
			}
			results = append(results, r)
		}
		return jsonResult(results)
	}
}

// ── list_trending ─────────────────────────────────────────────────────────────

func listTrendingTool() mcp.Tool {
	return mcp.NewTool("list_trending",
		mcp.WithDescription(`Returns the top games ranked by mention volume (hype velocity) for the given window.
Parameters:
  window (required) - time window: "24h" or "7d"
  limit  (optional, default 10) - number of games to return
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
			limit = int(l)
		}

		results, err := queries.Trending(ctx, pool, window, limit)
		if err != nil {
			return nil, fmt.Errorf("list_trending: %w", err)
		}
		return jsonResult(results)
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

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
