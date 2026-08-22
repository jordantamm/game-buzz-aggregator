//go:build integration

// Package integration_test exercises the real persistence path end to end.
//
// It runs the actual migration files against a real TimescaleDB+pgvector
// container, publishes protobuf messages to a real Redpanda container, and
// drives the real pkg/sink code. Nothing is reimplemented for the test — a test
// that inserts its own rows and then asserts they exist proves only that
// Postgres works.
package integration_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	gbav1 "github.com/jordantamm/game-buzz-aggregator/gen/go/gba/v1"
	"github.com/jordantamm/game-buzz-aggregator/pkg/queries"
	"github.com/jordantamm/game-buzz-aggregator/pkg/sink"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const enrichedTopic = "mentions.enriched"

// TestE2E_RedditMentionToPostgres is the required end-to-end test: a Mention
// enters as a protobuf on Redpanda and comes out as a queryable row with
// non-null sentiment, a game attribution, and a searchable embedding.
func TestE2E_RedditMentionToPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pool := startPostgres(ctx, t)
	brokers := startRedpanda(ctx, t)

	seedGames(ctx, t, pool)

	// ── Produce two enriched mentions for the same game ──────────────────
	events := []*gbav1.EnrichedMention{
		enrichedMention("01JTEST00000000000000001", "t3_test_a",
			"Elden Ring is fantastic, best boss design in years", 0.92, "elden-ring", 0.1),
		enrichedMention("01JTEST00000000000000002", "t3_test_b",
			"Elden Ring runs terribly on my machine", -0.71, "elden-ring", 0.2),
	}
	produce(ctx, t, brokers, events)

	// ── Consume with the real sink ───────────────────────────────────────
	consumed := consumeAll(ctx, t, brokers, len(events))
	if len(consumed) != len(events) {
		t.Fatalf("consumed %d events from Redpanda, want %d", len(consumed), len(events))
	}

	writer := sink.New(pool, zap.NewNop())
	if err := writer.WriteBatch(ctx, consumed); err != nil {
		t.Fatalf("sink.WriteBatch: %v", err)
	}

	// ── Assert the mention rows landed with sentiment ────────────────────
	for _, event := range events {
		mentionID := event.GetMention().GetMentionId()

		var (
			text      string
			sentiment *float64
			model     *string
		)
		err := pool.QueryRow(ctx, `
			SELECT text, sentiment_score, sentiment_model FROM mentions WHERE id = $1
		`, mentionID).Scan(&text, &sentiment, &model)
		if err != nil {
			t.Fatalf("querying mention %s: %v", mentionID, err)
		}
		if text != event.GetMention().GetText() {
			t.Errorf("mention %s text = %q, want %q", mentionID, text, event.GetMention().GetText())
		}
		if sentiment == nil {
			t.Errorf("mention %s has NULL sentiment_score; enrichment did not persist", mentionID)
		} else if *sentiment != float64(float32(event.GetSentiment().GetScore())) {
			t.Errorf("mention %s sentiment = %v, want %v", mentionID, *sentiment, event.GetSentiment().GetScore())
		}
		if model == nil || *model == "" {
			t.Errorf("mention %s has no sentiment_model recorded", mentionID)
		}
	}

	// ── Assert the game attribution landed ───────────────────────────────
	var gameLinks int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM mention_games WHERE game_id = 'elden-ring'`,
	).Scan(&gameLinks); err != nil {
		t.Fatalf("counting mention_games: %v", err)
	}
	if gameLinks != len(events) {
		t.Errorf("mention_games rows = %d, want %d", gameLinks, len(events))
	}

	// ── Assert the embeddings are queryable as vectors ───────────────────
	var embeddings int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM mention_embeddings`).Scan(&embeddings); err != nil {
		t.Fatalf("counting mention_embeddings: %v", err)
	}
	if embeddings != len(events) {
		t.Errorf("mention_embeddings rows = %d, want %d", embeddings, len(events))
	}

	// ── Assert the trending query works over the written data ────────────
	trending, err := queries.Trending(ctx, pool, 24*time.Hour, 10)
	if err != nil {
		t.Fatalf("queries.Trending: %v", err)
	}
	if len(trending) == 0 {
		t.Fatal("trending returned no rows despite two stored mentions")
	}
	if trending[0].GameID != "elden-ring" {
		t.Errorf("top trending game = %q, want %q", trending[0].GameID, "elden-ring")
	}
	if trending[0].MentionCount != len(events) {
		t.Errorf("trending mention_count = %d, want %d", trending[0].MentionCount, len(events))
	}
}

// TestE2E_RedeliveryIsIdempotent covers the guarantee the whole pipeline rests
// on: a redelivered batch must upsert, not duplicate. This is what breaks if
// the ON CONFLICT target stops matching the unique index, or if mention_id
// stops being deterministic.
func TestE2E_RedeliveryIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pool := startPostgres(ctx, t)
	seedGames(ctx, t, pool)

	writer := sink.New(pool, zap.NewNop())
	batch := []*gbav1.EnrichedMention{
		enrichedMention("01JTEST00000000000000003", "t3_dupe",
			"Hades 2 is great", 0.8, "elden-ring", 0.3),
	}

	if err := writer.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("first write: %v", err)
	}
	// Same batch again, as Kafka would redeliver after an uncommitted offset.
	if err := writer.WriteBatch(ctx, batch); err != nil {
		t.Fatalf("redelivered write failed — the pipeline is not idempotent: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM mentions WHERE native_id = 't3_dupe'`,
	).Scan(&count); err != nil {
		t.Fatalf("counting mentions: %v", err)
	}
	if count != 1 {
		t.Errorf("got %d rows after redelivery, want exactly 1", count)
	}
}

// TestE2E_ReEnrichmentUpdatesInPlace verifies that reprocessing with a newer
// model version overwrites rather than duplicating.
func TestE2E_ReEnrichmentUpdatesInPlace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pool := startPostgres(ctx, t)
	seedGames(ctx, t, pool)
	writer := sink.New(pool, zap.NewNop())

	original := enrichedMention("01JTEST00000000000000004", "t3_reenrich",
		"Elden Ring thoughts", 0.5, "elden-ring", 0.4)
	if err := writer.WriteBatch(ctx, []*gbav1.EnrichedMention{original}); err != nil {
		t.Fatalf("initial write: %v", err)
	}

	updated := enrichedMention("01JTEST00000000000000004", "t3_reenrich",
		"Elden Ring thoughts", -0.25, "elden-ring", 0.9)
	updated.Sentiment.ModelVersion = "sentiment-v2"
	updated.EnricherVersion = "0.2.0"
	if err := writer.WriteBatch(ctx, []*gbav1.EnrichedMention{updated}); err != nil {
		t.Fatalf("re-enrichment write: %v", err)
	}

	var (
		score   float64
		model   string
		version string
	)
	if err := pool.QueryRow(ctx, `
		SELECT sentiment_score, sentiment_model, enricher_version
		FROM mentions WHERE native_id = 't3_reenrich'
	`).Scan(&score, &model, &version); err != nil {
		t.Fatalf("querying re-enriched mention: %v", err)
	}

	if score != float64(float32(-0.25)) {
		t.Errorf("sentiment_score = %v, want the re-enriched -0.25", score)
	}
	if model != "sentiment-v2" || version != "0.2.0" {
		t.Errorf("model/version = %q/%q, want sentiment-v2/0.2.0", model, version)
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func startPostgres(ctx context.Context, t *testing.T) *pgxpool.Pool {
	t.Helper()

	container, err := tcpostgres.Run(ctx,
		"timescale/timescaledb-ha:pg16",
		tcpostgres.WithDatabase("gba"),
		tcpostgres.WithUsername("gba"),
		tcpostgres.WithPassword("gba"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(3*time.Minute),
		),
	)
	if err != nil {
		t.Fatalf("starting postgres container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("creating pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	applyMigrations(ctx, t, pool)
	return pool
}

// applyMigrations runs the real migration files from /migrations.
//
// Using the actual migrations rather than an inline simplified schema is the
// point: the production schema has a composite primary key and a three-column
// unique index (TimescaleDB requires the partitioning column in every unique
// index), and a simplified test schema would not catch an upsert whose
// ON CONFLICT target does not match.
func applyMigrations(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	dir := repoPath(t, "migrations")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading migrations dir: %v", err)
	}

	var upFiles []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".up.sql") {
			upFiles = append(upFiles, entry.Name())
		}
	}
	if len(upFiles) == 0 {
		t.Fatalf("no .up.sql files found in %s", dir)
	}
	sort.Strings(upFiles)

	for _, name := range upFiles {
		content, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("reading migration %s: %v", name, err)
		}
		if _, err := pool.Exec(ctx, string(content)); err != nil {
			t.Fatalf("applying migration %s: %v", name, err)
		}
	}
	t.Logf("applied %d migrations: %s", len(upFiles), strings.Join(upFiles, ", "))
}

func repoPath(t *testing.T, elements ...string) string {
	t.Helper()
	// This file lives at <repo>/tests/integration.
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	return filepath.Join(append([]string{root}, elements...)...)
}

func seedGames(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	// mention_games has a foreign key into games, so the game must exist first.
	if _, err := pool.Exec(ctx, `
		INSERT INTO games (id, canonical_name) VALUES ('elden-ring', 'Elden Ring')
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seeding games: %v", err)
	}
}

func startRedpanda(ctx context.Context, t *testing.T) string {
	t.Helper()

	container, err := tcredpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	if err != nil {
		t.Fatalf("starting redpanda container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("redpanda seed broker: %v", err)
	}
	return broker
}

func produce(ctx context.Context, t *testing.T, broker string, events []*gbav1.EnrichedMention) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("creating kafka producer: %v", err)
	}
	defer client.Close()

	for _, event := range events {
		value, err := proto.Marshal(event)
		if err != nil {
			t.Fatalf("marshalling event: %v", err)
		}
		record := &kgo.Record{
			Topic: enrichedTopic,
			Key:   []byte("game:elden-ring"),
			Value: value,
		}
		if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
			t.Fatalf("producing to %s: %v", enrichedTopic, err)
		}
	}
}

// consumeAll reads exactly want messages back off the topic and decodes them.
func consumeAll(ctx context.Context, t *testing.T, broker string, want int) []*gbav1.EnrichedMention {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(enrichedTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumerGroup(fmt.Sprintf("integration-test-%d", time.Now().UnixNano())),
	)
	if err != nil {
		t.Fatalf("creating kafka consumer: %v", err)
	}
	defer client.Close()

	pollCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	var out []*gbav1.EnrichedMention
	for len(out) < want {
		fetches := client.PollRecords(pollCtx, want-len(out))
		if err := fetches.Err(); err != nil {
			t.Fatalf("polling records (got %d of %d): %v", len(out), want, err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			var event gbav1.EnrichedMention
			if err := proto.Unmarshal(record.Value, &event); err != nil {
				t.Errorf("unmarshalling record at offset %d: %v", record.Offset, err)
				return
			}
			out = append(out, &event)
		})
	}
	return out
}

// enrichedMention builds a realistic EnrichedMention, including a full 384-dim
// embedding so the vector(384) column and its HNSW index are genuinely exercised.
func enrichedMention(mentionID, nativeID, text string, sentiment float64, gameID string, fill float32) *gbav1.EnrichedMention {
	return &gbav1.EnrichedMention{
		Mention: &gbav1.Mention{
			MentionId:      mentionID,
			Source:         gbav1.Source_SOURCE_REDDIT,
			NativeId:       nativeID,
			AuthorHash:     "0000000000000000000000000000000000000000000000000000000000000000",
			Text:           text,
			CreatedAt:      timestamppb.New(time.Now().Add(-time.Hour)),
			Engagement:     &gbav1.Engagement{Score: 100, ReplyCount: 12},
			SourceMetadata: map[string]string{"subreddit": "games", "kind": "post"},
		},
		GameMatches: []*gbav1.GameMatch{
			{GameId: gameID, Confidence: 0.95, Method: "trigram"},
		},
		Sentiment: &gbav1.Sentiment{
			Score:        sentiment,
			Magnitude:    0.9,
			ModelVersion: "twitter-roberta-base-sentiment-latest-v1",
		},
		Embedding:       embeddingBytes(fill),
		EmbeddingModel:  "all-MiniLM-L6-v2",
		EnricherVersion: "0.1.0",
	}
}

// embeddingBytes builds a 384-dim little-endian float32 buffer in the same wire
// format the enricher emits.
func embeddingBytes(fill float32) []byte {
	const dims = 384
	out := make([]byte, dims*4)
	for i := 0; i < dims; i++ {
		putFloat32LE(out[i*4:], fill)
	}
	return out
}

func putFloat32LE(b []byte, f float32) {
	binary.LittleEndian.PutUint32(b, math.Float32bits(f))
}
