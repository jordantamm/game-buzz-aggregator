package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jordantamm/game-buzz-aggregator/pkg/embed"
	"github.com/jordantamm/game-buzz-aggregator/pkg/queries"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// Handler holds the DB pool, embedding client, and logger for all route handlers.
type Handler struct {
	pool     *pgxpool.Pool
	embedder *embed.Client // may be nil; /v1/search then degrades to keyword-only
	log      *zap.Logger
}

func New(pool *pgxpool.Pool, embedder *embed.Client, log *zap.Logger) *Handler {
	return &Handler{pool: pool, embedder: embedder, log: log}
}

// Routes registers all HTTP routes onto r.
func (h *Handler) Routes(r chi.Router) {
	r.Get("/healthz", h.Healthz)
	r.Get("/readyz", h.Readyz)
	r.Get("/v1/trending", h.Trending)
	r.Get("/v1/search", h.Search)
	r.Get("/v1/games/{game_id}", h.GameDetail)
	r.Get("/v1/games/{game_id}/mentions", h.GameMentions)
	r.Get("/v1/mentions/{mention_id}/similar", h.SimilarMentions)
}

func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	if err := h.pool.Ping(r.Context()); err != nil {
		h.log.Warn("readyz: postgres unreachable", zap.Error(err))
		http.Error(w, "postgres unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) Trending(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("api-gateway").Start(r.Context(), "handler.trending")
	defer span.End()

	window := parseDuration(r.URL.Query().Get("window"), 24*time.Hour)
	limit := parseInt(r.URL.Query().Get("limit"), 20)

	results, err := queries.Trending(ctx, h.pool, window, limit)
	if err != nil {
		h.log.Error("trending query failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, results)
}

func (h *Handler) GameMentions(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("api-gateway").Start(r.Context(), "handler.game_mentions")
	defer span.End()

	gameID := chi.URLParam(r, "game_id")
	limit := parseInt(r.URL.Query().Get("limit"), 50)

	results, err := queries.RecentMentionsForGame(ctx, h.pool, gameID, limit)
	if err != nil {
		h.log.Error("game mentions query failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, results)
}

func (h *Handler) SimilarMentions(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("api-gateway").Start(r.Context(), "handler.similar_mentions")
	defer span.End()

	mentionID := chi.URLParam(r, "mention_id")
	limit := parseInt(r.URL.Query().Get("limit"), 10)

	results, err := queries.SimilarMentions(ctx, h.pool, mentionID, limit)
	if err != nil {
		h.log.Error("similar mentions query failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, results)
}

// Search serves the same hybrid (trigram + pgvector, RRF-fused) retrieval that
// the MCP server exposes as search_mentions. Both call pkg/queries so the
// ranking logic has exactly one implementation.
func (h *Handler) Search(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("api-gateway").Start(r.Context(), "handler.search")
	defer span.End()

	q := r.URL.Query().Get("q")
	if q == "" {
		http.Error(w, `query parameter "q" is required`, http.StatusBadRequest)
		return
	}

	params := queries.HybridSearchParams{
		Query:  q,
		GameID: r.URL.Query().Get("game_id"),
		Limit:  parseInt(r.URL.Query().Get("limit"), 20),
	}
	if since := r.URL.Query().Get("since"); since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			http.Error(w, `"since" must be an RFC3339 timestamp`, http.StatusBadRequest)
			return
		}
		params.Since = t
	}

	hits, err := queries.HybridSearch(ctx, h.pool, h.embedder, params)
	if err != nil {
		h.log.Error("hybrid search failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if hits == nil {
		hits = []queries.SearchHit{}
	}
	writeJSON(w, map[string]any{"query": q, "result_count": len(hits), "results": hits})
}

// GameDetail returns a game plus its hourly mention timeseries for the last 7 days.
func (h *Handler) GameDetail(w http.ResponseWriter, r *http.Request) {
	ctx, span := otel.Tracer("api-gateway").Start(r.Context(), "handler.game_detail")
	defer span.End()

	gameID := chi.URLParam(r, "game_id")

	var (
		canonicalName string
		steamAppID    *int
	)
	err := h.pool.QueryRow(ctx,
		`SELECT canonical_name, steam_app_id FROM games WHERE id = $1`, gameID,
	).Scan(&canonicalName, &steamAppID)
	if errors.Is(err, pgx.ErrNoRows) {
		http.Error(w, "game not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.log.Error("game detail query failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	series, err := queries.GameTimeseries(ctx, h.pool, gameID, 7*24*time.Hour, "1 hour")
	if err != nil {
		h.log.Error("game timeseries query failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"game_id":        gameID,
		"canonical_name": canonicalName,
		"steam_app_id":   steamAppID,
		"series":         series,
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func parseDuration(s string, def time.Duration) time.Duration {
	switch s {
	case "7d":
		return 7 * 24 * time.Hour
	case "30d":
		return 30 * 24 * time.Hour
	case "24h":
		return 24 * time.Hour
	default:
		return def
	}
}

func parseInt(s string, def int) int {
	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return n
	}
	return def
}
