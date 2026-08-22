package sink

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	gbav1 "github.com/jordantamm/game-buzz-aggregator/gen/go/gba/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

// WriteBatch writes a batch transactionally. All-or-nothing: if any statement
// fails the whole batch rolls back, the Kafka offset is not committed, and the
// batch is redelivered. The upserts below make that redelivery safe.
func (s *Sink) WriteBatch(ctx context.Context, batch []*gbav1.EnrichedMention) error {
	if len(batch) == 0 {
		return nil
	}

	ctx, span := otel.Tracer("sink-consumer").Start(ctx, "sink.write_batch")
	defer span.End()
	span.SetAttributes(attribute.Int("sink.batch_size", len(batch)))

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	mentions, err := s.upsertMentions(ctx, tx, batch)
	if err != nil {
		return err
	}
	games, err := s.upsertMentionGames(ctx, tx, batch)
	if err != nil {
		return err
	}
	embeddings, err := s.upsertEmbeddings(ctx, tx, batch)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	span.SetAttributes(
		attribute.Int("sink.mentions_written", mentions),
		attribute.Int("sink.mention_games_written", games),
		attribute.Int("sink.embeddings_written", embeddings),
	)
	s.log.Info("batch written",
		zap.Int("events", len(batch)),
		zap.Int("mentions", mentions),
		zap.Int("mention_games", games),
		zap.Int("embeddings", embeddings),
	)
	return nil
}

// upsertMentions writes the mention rows.
//
// pgx.CopyFrom would be faster, but COPY cannot express ON CONFLICT, and
// conflicts are the *normal* case here: mention_id is deterministic from
// (source, native_id), one mention fans out to one event per matched game, and
// redelivery is expected. A batched upsert is the correct tool.
//
// The conflict target must name every column of the unique index. `mentions` is
// a TimescaleDB hypertable partitioned on created_at, and Timescale requires the
// partitioning column to participate in every unique index — so the index is
// (source, native_id, created_at), and inferring on (source, native_id) alone
// would fail with "no unique or exclusion constraint matching the ON CONFLICT
// specification".
func (s *Sink) upsertMentions(ctx context.Context, tx pgx.Tx, batch []*gbav1.EnrichedMention) (int, error) {
	const stmt = `
		INSERT INTO mentions
			(id, source, native_id, author_hash, text, created_at, ingested_at,
			 engagement, raw_payload_ref, parent_id, source_metadata,
			 sentiment_score, sentiment_magnitude, sentiment_model, enricher_version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (source, native_id, created_at) DO UPDATE SET
			sentiment_score     = EXCLUDED.sentiment_score,
			sentiment_magnitude = EXCLUDED.sentiment_magnitude,
			sentiment_model     = EXCLUDED.sentiment_model,
			enricher_version    = EXCLUDED.enricher_version`

	// A mention with N game matches arrives as N separate events carrying the
	// same mention. Writing it once per event would be N-1 redundant upserts.
	seen := make(map[string]struct{}, len(batch))
	queued := 0
	queue := &pgx.Batch{}
	now := time.Now()

	for _, event := range batch {
		mention := event.GetMention()
		if mention == nil {
			continue
		}
		if _, dup := seen[mention.GetMentionId()]; dup {
			continue
		}
		seen[mention.GetMentionId()] = struct{}{}

		engagement, err := engagementJSON(mention.GetEngagement())
		if err != nil {
			return 0, fmt.Errorf("encode engagement for %s: %w", mention.GetMentionId(), err)
		}
		metadata, err := sourceMetaJSON(mention.GetSourceMetadata())
		if err != nil {
			return 0, fmt.Errorf("encode source_metadata for %s: %w", mention.GetMentionId(), err)
		}

		queue.Queue(stmt,
			mention.GetMentionId(),
			mention.GetSource().String(),
			mention.GetNativeId(),
			mention.GetAuthorHash(),
			mention.GetText(),
			mention.GetCreatedAt().AsTime(),
			now,
			engagement,
			nullIfEmpty(mention.GetRawPayloadRef()),
			nullIfEmpty(mention.GetParentId()),
			metadata,
			event.GetSentiment().GetScore(),
			event.GetSentiment().GetMagnitude(),
			nullIfEmpty(event.GetSentiment().GetModelVersion()),
			nullIfEmpty(event.GetEnricherVersion()),
		)
		queued++
	}

	if err := sendBatch(ctx, tx, queue, queued); err != nil {
		return 0, fmt.Errorf("upsert mentions: %w", err)
	}
	return queued, nil
}

func (s *Sink) upsertMentionGames(ctx context.Context, tx pgx.Tx, batch []*gbav1.EnrichedMention) (int, error) {
	const stmt = `
		INSERT INTO mention_games (mention_id, game_id, confidence, method)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (mention_id, game_id) DO UPDATE SET
			confidence = EXCLUDED.confidence,
			method     = EXCLUDED.method`

	queue := &pgx.Batch{}
	queued := 0
	for _, event := range batch {
		mention := event.GetMention()
		if mention == nil {
			continue
		}
		for _, match := range event.GetGameMatches() {
			queue.Queue(stmt, mention.GetMentionId(), match.GetGameId(), match.GetConfidence(), match.GetMethod())
			queued++
		}
	}

	if err := sendBatch(ctx, tx, queue, queued); err != nil {
		return 0, fmt.Errorf("upsert mention_games: %w", err)
	}
	return queued, nil
}

func (s *Sink) upsertEmbeddings(ctx context.Context, tx pgx.Tx, batch []*gbav1.EnrichedMention) (int, error) {
	const stmt = `
		INSERT INTO mention_embeddings (mention_id, embedding, model_version)
		VALUES ($1, $2::vector, $3)
		ON CONFLICT (mention_id) DO UPDATE SET
			embedding     = EXCLUDED.embedding,
			model_version = EXCLUDED.model_version`

	seen := make(map[string]struct{}, len(batch))
	queue := &pgx.Batch{}
	queued := 0

	for _, event := range batch {
		mention := event.GetMention()
		if mention == nil || len(event.GetEmbedding()) == 0 {
			continue
		}
		if _, dup := seen[mention.GetMentionId()]; dup {
			continue
		}
		seen[mention.GetMentionId()] = struct{}{}

		vector, err := embeddingToVectorLiteral(event.GetEmbedding())
		if err != nil {
			// A malformed embedding must not take down the whole batch's
			// mentions. Log and store the mention without its vector; it can be
			// backfilled by re-enriching.
			s.log.Warn("skipping malformed embedding",
				zap.String("mention_id", mention.GetMentionId()), zap.Error(err))
			continue
		}
		queue.Queue(stmt, mention.GetMentionId(), vector, nullIfEmpty(event.GetEmbeddingModel()))
		queued++
	}

	if err := sendBatch(ctx, tx, queue, queued); err != nil {
		return 0, fmt.Errorf("upsert mention_embeddings: %w", err)
	}
	return queued, nil
}

// sendBatch executes a queued pgx.Batch and drains every result.
//
// Draining matters: pgx reports a failed statement only when its result is
// read, so returning early would commit a transaction containing a failed
// statement's error unnoticed.
func sendBatch(ctx context.Context, tx pgx.Tx, queue *pgx.Batch, queued int) error {
	if queued == 0 {
		return nil
	}
	results := tx.SendBatch(ctx, queue)
	defer results.Close()

	for i := 0; i < queued; i++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("statement %d of %d: %w", i+1, queued, err)
		}
	}
	return results.Close()
}

// embeddingToVectorLiteral converts little-endian float32 bytes into a pgvector
// text literal such as "[0.1,0.2]".
func embeddingToVectorLiteral(raw []byte) (string, error) {
	if len(raw)%4 != 0 {
		return "", fmt.Errorf("embedding length %d is not a multiple of 4", len(raw))
	}
	n := len(raw) / 4
	if n != expectedDims {
		return "", fmt.Errorf("embedding has %d dims, expected %d", n, expectedDims)
	}

	out := make([]byte, 0, n*10)
	out = append(out, '[')
	for i := 0; i < n; i++ {
		value := math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return "", fmt.Errorf("embedding contains a non-finite value at index %d", i)
		}
		if i > 0 {
			out = append(out, ',')
		}
		out = fmt.Appendf(out, "%g", value)
	}
	out = append(out, ']')
	return string(out), nil
}

// expectedDims matches the vector(384) column and all-MiniLM-L6-v2.
const expectedDims = 384

func engagementJSON(e *gbav1.Engagement) (string, error) {
	if e == nil {
		return "{}", nil
	}
	b, err := json.Marshal(map[string]int64{
		"score":       e.GetScore(),
		"reply_count": e.GetReplyCount(),
		"view_count":  e.GetViewCount(),
	})
	return string(b), err
}

// sourceMetaJSON encodes source metadata with encoding/json rather than string
// concatenation: values come from an external API (subreddit names, flair) and
// a quote or backslash in one would otherwise produce invalid JSON.
func sourceMetaJSON(m map[string]string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	return string(b), err
}

// nullIfEmpty maps proto's zero-value empty string to SQL NULL, so an absent
// optional field is stored as NULL rather than an empty string.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
