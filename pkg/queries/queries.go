package queries

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jordantamm/game-buzz-aggregator/pkg/models"
)

// Trending returns the top games by mention count in the given window.
func Trending(ctx context.Context, pool *pgxpool.Pool, window time.Duration, limit int) ([]models.Trending, error) {
	since := time.Now().Add(-window)
	rows, err := pool.Query(ctx, `
		SELECT
			g.id,
			g.canonical_name,
			COUNT(DISTINCT mg.mention_id) AS mention_count,
			AVG(m.sentiment_score)        AS avg_sentiment
		FROM games g
		JOIN mention_games mg ON mg.game_id = g.id
		JOIN mentions m       ON m.id = mg.mention_id
		WHERE m.created_at >= $1
		GROUP BY g.id, g.canonical_name
		ORDER BY mention_count DESC
		LIMIT $2
	`, since, limit)
	if err != nil {
		return nil, fmt.Errorf("trending query: %w", err)
	}
	defer rows.Close()

	var results []models.Trending
	for rows.Next() {
		var t models.Trending
		t.WindowStart = since
		if err := rows.Scan(&t.GameID, &t.CanonicalName, &t.MentionCount, &t.AvgSentiment); err != nil {
			return nil, err
		}
		results = append(results, t)
	}
	return results, rows.Err()
}

// SimilarMentions returns mentions with the highest cosine similarity to the given mention's embedding.
func SimilarMentions(ctx context.Context, pool *pgxpool.Pool, mentionID string, limit int) ([]models.MentionRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT m.id, m.source, m.native_id, m.author_hash, m.text, m.created_at, m.ingested_at,
		       m.sentiment_score, m.sentiment_magnitude
		FROM mentions m
		JOIN mention_embeddings me_target ON me_target.mention_id = $1
		JOIN mention_embeddings me        ON me.mention_id = m.id AND me.mention_id != $1
		ORDER BY me.embedding <=> me_target.embedding
		LIMIT $2
	`, mentionID, limit)
	if err != nil {
		return nil, fmt.Errorf("similar mentions query: %w", err)
	}
	defer rows.Close()
	return scanMentions(rows)
}

// RecentMentionsForGame returns recent mentions for a given game.
func RecentMentionsForGame(ctx context.Context, pool *pgxpool.Pool, gameID string, limit int) ([]models.MentionRow, error) {
	rows, err := pool.Query(ctx, `
		SELECT m.id, m.source, m.native_id, m.author_hash, m.text, m.created_at, m.ingested_at,
		       m.sentiment_score, m.sentiment_magnitude
		FROM mentions m
		JOIN mention_games mg ON mg.mention_id = m.id
		WHERE mg.game_id = $1
		ORDER BY m.created_at DESC
		LIMIT $2
	`, gameID, limit)
	if err != nil {
		return nil, fmt.Errorf("recent mentions query: %w", err)
	}
	defer rows.Close()
	return scanMentions(rows)
}

func scanMentions(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]models.MentionRow, error) {
	var results []models.MentionRow
	for rows.Next() {
		var r models.MentionRow
		if err := rows.Scan(
			&r.ID, &r.Source, &r.NativeID, &r.AuthorHash, &r.Text,
			&r.CreatedAt, &r.IngestedAt,
			&r.SentimentScore, &r.SentimentMagnitude,
		); err != nil {
			return nil, err
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

// BuzzPoint is one bucket of a game's mention/sentiment timeseries.
type BuzzPoint struct {
	Bucket       time.Time `json:"bucket"`
	MentionCount int       `json:"mention_count"`
	AvgSentiment *float64  `json:"avg_sentiment"`
}

// GameTimeseries returns bucketed mention counts and average sentiment for a
// game, using TimescaleDB's time_bucket. bucket is a Postgres interval literal
// such as "1 hour" or "1 day".
//
// Shared by api-gateway's /v1/games/{id} and mcp-server's get_game_buzz tool so
// the two surfaces cannot drift apart.
func GameTimeseries(ctx context.Context, pool *pgxpool.Pool, gameID string, window time.Duration, bucket string) ([]BuzzPoint, error) {
	since := time.Now().Add(-window)
	rows, err := pool.Query(ctx, `
		SELECT
			time_bucket($1::interval, m.created_at) AS bucket,
			COUNT(*)                                AS mention_count,
			AVG(m.sentiment_score)                  AS avg_sentiment
		FROM mentions m
		JOIN mention_games mg ON mg.mention_id = m.id
		WHERE mg.game_id = $2 AND m.created_at >= $3
		GROUP BY bucket
		ORDER BY bucket
	`, bucket, gameID, since)
	if err != nil {
		return nil, fmt.Errorf("game timeseries query: %w", err)
	}
	defer rows.Close()

	series := []BuzzPoint{}
	for rows.Next() {
		var p BuzzPoint
		if err := rows.Scan(&p.Bucket, &p.MentionCount, &p.AvgSentiment); err != nil {
			return nil, err
		}
		series = append(series, p)
	}
	return series, rows.Err()
}
