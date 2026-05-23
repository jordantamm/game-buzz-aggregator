package sink

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	gbav1 "github.com/jordantamm/game-buzz-aggregator/gen/go/gba/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// Sink writes batches of EnrichedMentions to Postgres.
type Sink struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

func New(pool *pgxpool.Pool, log *zap.Logger) *Sink {
	return &Sink{pool: pool, log: log}
}

// WriteBatch writes a batch transactionally. All-or-nothing.
func (s *Sink) WriteBatch(ctx context.Context, batch []*gbav1.EnrichedMention) error {
	ctx, span := otel.Tracer("sink-consumer").Start(ctx, "sink.write_batch")
	defer span.End()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := s.insertMentions(ctx, tx, batch); err != nil {
		return err
	}
	if err := s.insertMentionGames(ctx, tx, batch); err != nil {
		return err
	}
	if err := s.insertEmbeddings(ctx, tx, batch); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	s.log.Info("batch written", zap.Int("count", len(batch)))
	return nil
}

func (s *Sink) insertMentions(ctx context.Context, tx pgx.Tx, batch []*gbav1.EnrichedMention) error {
	rows := make([][]any, 0, len(batch))
	for _, em := range batch {
		m := em.Mention
		if m == nil {
			continue
		}
		createdAt := m.CreatedAt.AsTime()
		rows = append(rows, []any{
			m.MentionId,
			m.Source.String(),
			m.NativeId,
			m.AuthorHash,
			m.Text,
			createdAt,
			time.Now(),
			engagementJSON(m.Engagement),
			m.RawPayloadRef,
			m.ParentId,
			sourceMetaJSON(m.SourceMetadata),
			em.Sentiment.GetScore(),
			em.Sentiment.GetMagnitude(),
			em.Sentiment.GetModelVersion(),
			em.EnricherVersion,
		})
	}

	_, err := tx.CopyFrom(ctx,
		pgx.Identifier{"mentions"},
		[]string{
			"id", "source", "native_id", "author_hash", "text",
			"created_at", "ingested_at", "engagement",
			"raw_payload_ref", "parent_id", "source_metadata",
			"sentiment_score", "sentiment_magnitude", "sentiment_model",
			"enricher_version",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		// CopyFrom doesn't support ON CONFLICT — fall back to upsert loop.
		return s.upsertMentions(ctx, tx, batch)
	}
	return nil
}

func (s *Sink) upsertMentions(ctx context.Context, tx pgx.Tx, batch []*gbav1.EnrichedMention) error {
	for _, em := range batch {
		m := em.Mention
		if m == nil {
			continue
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO mentions
				(id, source, native_id, author_hash, text, created_at, ingested_at,
				 engagement, raw_payload_ref, parent_id, source_metadata,
				 sentiment_score, sentiment_magnitude, sentiment_model, enricher_version)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
			ON CONFLICT (source, native_id) DO UPDATE SET
				sentiment_score     = EXCLUDED.sentiment_score,
				sentiment_magnitude = EXCLUDED.sentiment_magnitude,
				sentiment_model     = EXCLUDED.sentiment_model,
				enricher_version    = EXCLUDED.enricher_version
		`,
			m.MentionId, m.Source.String(), m.NativeId, m.AuthorHash, m.Text,
			m.CreatedAt.AsTime(), time.Now(),
			engagementJSON(m.Engagement), m.RawPayloadRef, m.ParentId, sourceMetaJSON(m.SourceMetadata),
			em.Sentiment.GetScore(), em.Sentiment.GetMagnitude(), em.Sentiment.GetModelVersion(),
			em.EnricherVersion,
		)
		if err != nil {
			return fmt.Errorf("upsert mention %s: %w", m.MentionId, err)
		}
	}
	return nil
}

func (s *Sink) insertMentionGames(ctx context.Context, tx pgx.Tx, batch []*gbav1.EnrichedMention) error {
	for _, em := range batch {
		if em.Mention == nil {
			continue
		}
		for _, gm := range em.GameMatches {
			_, err := tx.Exec(ctx, `
				INSERT INTO mention_games (mention_id, game_id, confidence, method)
				VALUES ($1,$2,$3,$4)
				ON CONFLICT (mention_id, game_id) DO UPDATE SET
					confidence = EXCLUDED.confidence,
					method     = EXCLUDED.method
			`, em.Mention.MentionId, gm.GameId, gm.Confidence, gm.Method)
			if err != nil {
				return fmt.Errorf("upsert mention_game: %w", err)
			}
		}
	}
	return nil
}

func (s *Sink) insertEmbeddings(ctx context.Context, tx pgx.Tx, batch []*gbav1.EnrichedMention) error {
	for _, em := range batch {
		if em.Mention == nil || len(em.Embedding) == 0 {
			continue
		}
		vec := embeddingToText(em.Embedding)
		_, err := tx.Exec(ctx, `
			INSERT INTO mention_embeddings (mention_id, embedding, model_version)
			VALUES ($1, $2::vector, $3)
			ON CONFLICT (mention_id) DO UPDATE SET
				embedding     = EXCLUDED.embedding,
				model_version = EXCLUDED.model_version
		`, em.Mention.MentionId, vec, em.EmbeddingModel)
		if err != nil {
			return fmt.Errorf("upsert embedding: %w", err)
		}
	}
	return nil
}

// embeddingToText converts little-endian float32 bytes to a pgvector literal like "[0.1,0.2,...]".
func embeddingToText(b []byte) string {
	n := len(b) / 4
	out := make([]byte, 0, n*10)
	out = append(out, '[')
	for i := 0; i < n; i++ {
		bits := binary.LittleEndian.Uint32(b[i*4:])
		f := math.Float32frombits(bits)
		out = fmt.Appendf(out, "%g", f)
		if i < n-1 {
			out = append(out, ',')
		}
	}
	out = append(out, ']')
	return string(out)
}

func engagementJSON(e *gbav1.Engagement) string {
	if e == nil {
		return "{}"
	}
	return fmt.Sprintf(`{"score":%d,"reply_count":%d,"view_count":%d}`, e.Score, e.ReplyCount, e.ViewCount)
}

func sourceMetaJSON(m map[string]string) string {
	if len(m) == 0 {
		return "{}"
	}
	b := []byte{'{'}
	i := 0
	for k, v := range m {
		if i > 0 {
			b = append(b, ',')
		}
		b = fmt.Appendf(b, `"%s":"%s"`, k, v)
		i++
	}
	b = append(b, '}')
	return string(b)
}
