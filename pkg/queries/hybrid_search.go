package queries

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jordantamm/game-buzz-aggregator/pkg/embed"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// rrfK is the reciprocal-rank-fusion smoothing constant. 60 is the value from
// Cormack et al. (2009); it damps the influence of the top rank enough that a
// document ranked #1 by one retriever does not automatically outrank a document
// ranked #2 by both.
const rrfK = 60.0

// candidateMultiplier controls how deep each retrieval pass goes relative to
// the requested limit. Fusion needs more candidates than it returns, or the two
// result sets rarely overlap and RRF degenerates into interleaving.
const candidateMultiplier = 4

// SearchHit is one fused hybrid-search result.
type SearchHit struct {
	MentionID      string    `json:"mention_id"`
	Source         string    `json:"source"`
	Text           string    `json:"text"`
	CreatedAt      time.Time `json:"created_at"`
	SentimentScore *float64  `json:"sentiment_score"`
	GameID         *string   `json:"game_id,omitempty"`

	// Score is the fused RRF score. Retrieval provenance is reported so the
	// caller (and any LLM consuming this via MCP) can see whether a hit came
	// from keyword matching, semantic similarity, or both.
	Score       float64 `json:"score"`
	KeywordRank int     `json:"keyword_rank,omitempty"`
	VectorRank  int     `json:"vector_rank,omitempty"`
	MatchedBy   string  `json:"matched_by"` // "keyword", "vector", or "both"
}

// HybridSearchParams configures a hybrid search.
type HybridSearchParams struct {
	Query  string
	GameID string    // optional filter; empty means no filter
	Since  time.Time // zero value means no lower bound
	Limit  int
}

// HybridSearch runs keyword (pg_trgm) and semantic (pgvector HNSW) retrieval as
// two independent passes and fuses them with reciprocal rank fusion.
//
// The two passes are deliberately NOT combined into a single SQL statement with
// both predicates in one WHERE clause. Doing that lets whichever signal matches
// fewer rows — usually trigram, on short or paraphrased queries — starve the
// other's recall, because the intersection is taken before ranking. Running
// them separately keeps each retriever's ordering meaningful over its own full
// candidate pool, and RRF then combines the two orderings by rank rather than
// by raw score (trigram similarity and cosine distance are not comparable
// quantities, so score-level fusion would need a calibration step that rank
// fusion avoids entirely).
//
// If embedder is nil or the embedding call fails, the search degrades to
// keyword-only rather than returning an error: a keyword-only result set is far
// more useful to the caller than no result set.
func HybridSearch(
	ctx context.Context,
	pool *pgxpool.Pool,
	embedder *embed.Client,
	p HybridSearchParams,
) ([]SearchHit, error) {
	ctx, span := otel.Tracer("pkg/queries").Start(ctx, "queries.HybridSearch")
	defer span.End()

	if p.Limit <= 0 {
		p.Limit = 20
	}
	depth := p.Limit * candidateMultiplier
	span.SetAttributes(
		attribute.String("search.query", p.Query),
		attribute.Int("search.limit", p.Limit),
		attribute.Int("search.candidate_depth", depth),
	)

	keywordHits, err := keywordPass(ctx, pool, p, depth)
	if err != nil {
		return nil, fmt.Errorf("keyword pass: %w", err)
	}

	var vectorHits []SearchHit
	switch {
	case embedder == nil:
		span.SetAttributes(attribute.String("search.vector_pass_skipped", "no embedder configured"))
	default:
		vec, embErr := embedder.Embed(ctx, p.Query)
		if embErr != nil {
			// Degrade to keyword-only; record why on the span so the missing
			// vector pass is visible in Jaeger rather than silent.
			span.SetAttributes(attribute.String("search.vector_pass_skipped", embErr.Error()))
			break
		}
		vectorHits, err = vectorPass(ctx, pool, p, vec, depth)
		if err != nil {
			return nil, fmt.Errorf("vector pass: %w", err)
		}
	}

	fused := fuseRRF(keywordHits, vectorHits, p.Limit)
	span.SetAttributes(
		attribute.Int("search.keyword_hits", len(keywordHits)),
		attribute.Int("search.vector_hits", len(vectorHits)),
		attribute.Int("search.fused_hits", len(fused)),
	)
	return fused, nil
}

// keywordPass ranks by trigram word similarity between the query and the
// mention text. word_similarity (rather than plain similarity) is used because
// the query is short and the mention text is long — plain similarity divides by
// the union of both trigram sets and so scores long documents into irrelevance
// no matter how well the query matches a phrase inside them.
func keywordPass(ctx context.Context, pool *pgxpool.Pool, p HybridSearchParams, depth int) ([]SearchHit, error) {
	ctx, span := otel.Tracer("pkg/queries").Start(ctx, "queries.hybridSearch.keywordPass")
	defer span.End()

	const sql = `
		SELECT m.id, m.source, m.text, m.created_at, m.sentiment_score, mg.game_id
		FROM mentions m
		LEFT JOIN mention_games mg ON mg.mention_id = m.id
		WHERE word_similarity($1, m.text) > 0.3
		  AND ($2 = '' OR mg.game_id = $2)
		  AND ($3::timestamptz IS NULL OR m.created_at >= $3)
		ORDER BY word_similarity($1, m.text) DESC, m.created_at DESC
		LIMIT $4
	`
	rows, err := pool.Query(ctx, sql, p.Query, p.GameID, nullableTime(p.Since), depth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSearchHits(rows)
}

// vectorPass ranks by cosine distance against the HNSW index over
// mention_embeddings.
func vectorPass(ctx context.Context, pool *pgxpool.Pool, p HybridSearchParams, vec []float32, depth int) ([]SearchHit, error) {
	ctx, span := otel.Tracer("pkg/queries").Start(ctx, "queries.hybridSearch.vectorPass")
	defer span.End()

	const sql = `
		SELECT m.id, m.source, m.text, m.created_at, m.sentiment_score, mg.game_id
		FROM mention_embeddings me
		JOIN mentions m ON m.id = me.mention_id
		LEFT JOIN mention_games mg ON mg.mention_id = m.id
		WHERE ($2 = '' OR mg.game_id = $2)
		  AND ($3::timestamptz IS NULL OR m.created_at >= $3)
		ORDER BY me.embedding <=> $1::vector
		LIMIT $4
	`
	rows, err := pool.Query(ctx, sql, embed.Vector(vec), p.GameID, nullableTime(p.Since), depth)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSearchHits(rows)
}

// fuseRRF merges two ranked lists by reciprocal rank fusion:
//
//	score(d) = sum over retrievers r of 1 / (k + rank_r(d))
//
// A document found by both retrievers accumulates two terms and so outranks a
// document found by only one at a comparable position — exactly the behaviour
// wanted from hybrid search.
func fuseRRF(keyword, vector []SearchHit, limit int) []SearchHit {
	type acc struct {
		hit         SearchHit
		score       float64
		keywordRank int
		vectorRank  int
	}
	merged := make(map[string]*acc, len(keyword)+len(vector))

	for i, h := range keyword {
		rank := i + 1
		merged[h.MentionID] = &acc{hit: h, score: 1.0 / (rrfK + float64(rank)), keywordRank: rank}
	}
	for i, h := range vector {
		rank := i + 1
		if existing, ok := merged[h.MentionID]; ok {
			existing.score += 1.0 / (rrfK + float64(rank))
			existing.vectorRank = rank
			continue
		}
		merged[h.MentionID] = &acc{hit: h, score: 1.0 / (rrfK + float64(rank)), vectorRank: rank}
	}

	out := make([]SearchHit, 0, len(merged))
	for _, a := range merged {
		hit := a.hit
		hit.Score = a.score
		hit.KeywordRank = a.keywordRank
		hit.VectorRank = a.vectorRank
		switch {
		case a.keywordRank > 0 && a.vectorRank > 0:
			hit.MatchedBy = "both"
		case a.keywordRank > 0:
			hit.MatchedBy = "keyword"
		default:
			hit.MatchedBy = "vector"
		}
		out = append(out, hit)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		// Deterministic tie-break so equal scores do not reorder run to run.
		return out[i].MentionID < out[j].MentionID
	})

	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

func scanSearchHits(rows pgx.Rows) ([]SearchHit, error) {
	var hits []SearchHit
	for rows.Next() {
		var h SearchHit
		if err := rows.Scan(&h.MentionID, &h.Source, &h.Text, &h.CreatedAt, &h.SentimentScore, &h.GameID); err != nil {
			return nil, err
		}
		hits = append(hits, h)
	}
	return hits, rows.Err()
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
