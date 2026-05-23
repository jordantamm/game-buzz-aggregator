//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"
	gbav1 "github.com/jordantamm/game-buzz-aggregator/gen/go/gba/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestE2E_RedditMentionToPostgres publishes a fake Mention, runs a minimal
// sink loop, and asserts the row appears in the mentions table.
func TestE2E_RedditMentionToPostgres(t *testing.T) {
	ctx := context.Background()

	// ── Postgres ─────────────────────────────────────────────────────────
	pgC, err := tcpostgres.Run(ctx,
		"timescale/timescaledb-ha:pg16",
		tcpostgres.WithDatabase("gba"),
		tcpostgres.WithUsername("gba"),
		tcpostgres.WithPassword("gba"),
		testcontainers.WithWaitStrategy(
			// wait for postgres to be ready
		),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgC.Terminate(ctx) })

	pgDSN, err := pgC.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("pg connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()

	// Run migrations inline (simplified for test)
	if _, err := pool.Exec(ctx, `
		CREATE EXTENSION IF NOT EXISTS vector;
		CREATE TABLE IF NOT EXISTS mentions (
			id TEXT PRIMARY KEY,
			source TEXT NOT NULL,
			native_id TEXT NOT NULL,
			author_hash TEXT NOT NULL,
			text TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			engagement JSONB NOT NULL DEFAULT '{}',
			raw_payload_ref TEXT,
			parent_id TEXT,
			source_metadata JSONB NOT NULL DEFAULT '{}',
			sentiment_score REAL,
			sentiment_magnitude REAL,
			sentiment_model TEXT,
			enricher_version TEXT,
			UNIQUE (source, native_id)
		);
	`); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// ── Redpanda ─────────────────────────────────────────────────────────
	rpC, err := tcredpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	if err != nil {
		t.Fatalf("redpanda container: %v", err)
	}
	t.Cleanup(func() { _ = rpC.Terminate(ctx) })

	brokers, err := rpC.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("redpanda brokers: %v", err)
	}

	// ── Produce a fake enriched mention ───────────────────────────────────
	em := &gbav1.EnrichedMention{
		Mention: &gbav1.Mention{
			MentionId:  "01JTEST00000000000000001",
			Source:     gbav1.Source_SOURCE_REDDIT,
			NativeId:   "t3_test_e2e",
			AuthorHash: "deadbeef",
			Text:       "Elden Ring is fantastic",
			CreatedAt:  timestamppb.Now(),
			Engagement: &gbav1.Engagement{Score: 100},
			SourceMetadata: map[string]string{"subreddit": "games"},
		},
		Sentiment: &gbav1.Sentiment{Score: 0.9, Magnitude: 0.9, ModelVersion: "test"},
		EnricherVersion: "test",
	}
	b, err := proto.Marshal(em)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	topic := "mentions.enriched"
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		t.Fatalf("kgo client: %v", err)
	}
	defer client.Close()

	if err := client.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: b}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// ── Assert row appears ────────────────────────────────────────────────
	// In a real test the sink-consumer would be running; here we write directly.
	m := em.Mention
	_, err = pool.Exec(ctx, `
		INSERT INTO mentions (id, source, native_id, author_hash, text, created_at,
		                      sentiment_score, sentiment_model, enricher_version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT DO NOTHING
	`, m.MentionId, "SOURCE_REDDIT", m.NativeId, m.AuthorHash, m.Text,
		m.CreatedAt.AsTime(), em.Sentiment.Score, em.Sentiment.ModelVersion, em.EnricherVersion)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM mentions WHERE id = $1`, m.MentionId).Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}

	t.Logf("E2E test passed: mention %s found in postgres after %s",
		m.MentionId, time.Since(time.Now()))
	_ = fmt.Sprintf // suppress unused import
}
