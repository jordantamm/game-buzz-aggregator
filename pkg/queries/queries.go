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
