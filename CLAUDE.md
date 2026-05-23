# Game Buzz Aggregator — MVP Build Spec

This document is the single source of truth for the MVP build. Read it end-to-end before writing any code. When you (Claude Code) start a new session, re-read this file.

## Project context

The Game Buzz Aggregator is a streaming data pipeline that ingests game-related discussions from social platforms, enriches them with sentiment and entity-resolution, computes a "Hype Velocity" score, and exposes the results through both a REST API and an MCP server.

**The full vision has many sources (Reddit, Bluesky, Twitch, Metacritic scraping, OpenCritic/Steam APIs). This MVP scope is intentionally narrow: Reddit only.** The architecture must, however, be built such that adding new sources later is a matter of adding a new connector service that publishes to the same Redpanda topic — no changes to enricher, sink, API, or MCP server. Source-agnosticism downstream of the broker is a hard requirement.

## MVP success criteria

The MVP is done when all of the following are true:

1. `docker compose up` from a fresh clone brings up every service and external dependency with zero manual steps.
2. The Reddit connector pulls posts and comments from a configured list of subreddits and publishes them to a Redpanda topic.
3. The enricher consumes from that topic, performs entity resolution against a seeded `games` table, computes sentiment, generates an embedding, and emits enriched events to a second topic.
4. The sink consumer writes enriched events to Postgres (with pgvector + TimescaleDB extensions enabled).
5. The REST API serves a `/trending` endpoint backed by a SQL query over the enriched data.
6. The MCP server exposes `search_mentions`, `get_game_buzz`, and `find_similar_games` tools and is reachable from Claude Desktop via stdio.
7. End-to-end OpenTelemetry traces flow from the connector through to the API response and are viewable in Jaeger.
8. An integration test using Testcontainers exercises the full path from a fake Reddit response to a row in Postgres.

Anything beyond this list is out of scope for the MVP.

## Architecture

```
                       ┌─────────────────────────────┐
   Reddit OAuth ─────▶ │   reddit-connector (Go)     │
                       └────────────────┬────────────┘
                                        │ produces
                                        ▼
                              ┌───────────────────┐
                              │   Redpanda        │
                              │ topic: mentions.raw│ (partitioned by source:subreddit)
                              └────────┬──────────┘
                                       │ consumes
                                       ▼
                       ┌─────────────────────────────┐
                       │     enricher (Python)       │
                       │   Temporal workflows:       │
                       │   - entity resolution       │
                       │   - sentiment scoring       │
                       │   - embedding generation    │
                       └────────────────┬────────────┘
                                        │ produces
                                        ▼
                              ┌───────────────────┐
                              │   Redpanda        │
                              │ topic: mentions.enriched │ (partitioned by game_id)
                              └────────┬──────────┘
                                       │ consumes
                                       ▼
                       ┌─────────────────────────────┐
                       │   sink-consumer (Go)        │
                       └────────────────┬────────────┘
                                        │ writes
                                        ▼
                       ┌─────────────────────────────┐
                       │  Postgres                   │
                       │  + pgvector                 │
                       │  + TimescaleDB              │
                       └────────────────┬────────────┘
                                        │ reads
                                        ▼
                       ┌─────────────────────────────┐
                       │  api-gateway (Go)           │
                       │  mcp-server (Go)            │
                       └─────────────────────────────┘

Observability sidecar:
  - OpenTelemetry collector
  - Jaeger (traces)
  - Prometheus (metrics)
  - Grafana (dashboards)
```

## Repository layout

This is a Go workspace + uv-managed Python monorepo. Use this exact structure:

```
/
├── CLAUDE.md                 ← this file
├── README.md                 ← public-facing project description
├── docker-compose.yml        ← brings up everything
├── .env.example              ← documented env vars; never commit .env
├── go.work                   ← Go workspace file
├── buf.yaml                  ← Buf config for Protobuf
├── buf.gen.yaml              ← Buf codegen config
├── Makefile                  ← common dev tasks
│
├── proto/
│   └── gba/
│       └── v1/
│           ├── mention.proto
│           ├── enriched.proto
│           └── common.proto
│
├── gen/                      ← generated code (committed for reproducibility)
│   ├── go/
│   └── python/
│
├── services/
│   ├── reddit-connector/     ← Go
│   │   ├── go.mod
│   │   ├── Dockerfile
│   │   ├── cmd/main.go
│   │   └── internal/
│   ├── enricher/             ← Python
│   │   ├── pyproject.toml
│   │   ├── Dockerfile
│   │   └── src/enricher/
│   ├── sink-consumer/        ← Go
│   ├── api-gateway/          ← Go
│   └── mcp-server/           ← Go
│
├── pkg/                      ← shared Go packages
│   ├── kafka/                ← franz-go wrappers, OTel-aware producer/consumer
│   ├── telemetry/            ← OTel setup helpers
│   ├── pg/                   ← pgx pool helpers
│   └── models/               ← domain types (not the protobuf-generated ones)
│
├── migrations/               ← SQL migrations, golang-migrate format
│   ├── 0001_init.up.sql
│   ├── 0001_init.down.sql
│   └── ...
│
├── seed/
│   └── games.csv             ← seed game list (canonical names + steam app ids)
│
├── docker/
│   ├── otel-collector-config.yaml
│   ├── prometheus.yml
│   └── grafana/
│       └── provisioning/
│
├── docs/
│   ├── design.md             ← architecture decisions, alternatives considered
│   ├── runbook.md            ← what to do when X breaks
│   └── adr/                  ← architecture decision records
│
└── scripts/
    ├── bootstrap-topics.sh   ← creates Redpanda topics on first boot
    └── seed-games.sh         ← loads games.csv into Postgres
```

## Technology pinning

Pin everything. Floating versions are not acceptable.

| Component | Version | Notes |
|-----------|---------|-------|
| Go | 1.23 | use `go.work` for the monorepo |
| Python | 3.12 | `uv` for dependency management |
| Postgres | 16 | with pgvector 0.7+ and TimescaleDB 2.15+ |
| Redpanda | latest stable | use `redpandadata/redpanda` image |
| Temporal | 1.24+ | `temporalio/auto-setup` for local dev |
| Redis | 7 | |
| MinIO | latest | for raw payload storage |
| OTel Collector | latest contrib | |
| Jaeger | 1.57+ | all-in-one for local dev |
| Prometheus | 2.x | |
| Grafana | 11.x | |

Go libraries:
- `github.com/twmb/franz-go` for Kafka/Redpanda
- `github.com/jackc/pgx/v5` for Postgres
- `github.com/go-chi/chi/v5` for HTTP routing
- `connectrpc.com/connect` for RPC (Connect-RPC, gRPC-compatible)
- `go.opentelemetry.io/otel` and friends for tracing
- `github.com/redis/go-redis/v9`
- `github.com/mark3labs/mcp-go` for the MCP server
- `github.com/spf13/viper` for config

Python libraries (enricher):
- `temporalio` SDK
- `sentence-transformers` for embeddings (`all-MiniLM-L6-v2`, 384 dims)
- `transformers` for sentiment (`cardiffnlp/twitter-roberta-base-sentiment-latest`)
- `aiokafka` for Redpanda
- `asyncpg` for Postgres
- `pydantic` v2 for models
- `anthropic` for LLM disambiguation fallback
- `opentelemetry-*` packages for tracing

## Event schema

The schema is defined in Protobuf and lives in `/proto/gba/v1/`. Use Buf for codegen. **The schema is the contract between every service** — do not let services define their own types for these events.

```protobuf
syntax = "proto3";
package gba.v1;
import "google/protobuf/timestamp.proto";

enum Source {
  SOURCE_UNSPECIFIED = 0;
  SOURCE_REDDIT = 1;
  // Future: SOURCE_BLUESKY, SOURCE_TWITCH, etc.
}

message Engagement {
  int64 score = 1;          // upvotes/likes
  int64 reply_count = 2;
  int64 view_count = 3;     // 0 if unknown
}

message Mention {
  string mention_id = 1;     // ULID, deterministic from source+native_id
  Source source = 2;
  string native_id = 3;
  string author_hash = 4;    // sha256 of author handle, never raw
  string text = 5;
  google.protobuf.Timestamp created_at = 6;
  Engagement engagement = 7;
  string raw_payload_ref = 8;  // MinIO key
  string parent_id = 9;        // for threaded replies
  string trace_id = 10;        // OTel propagation
  map<string, string> source_metadata = 11; // e.g. {"subreddit": "games"}
}

message GameMatch {
  string game_id = 1;
  double confidence = 2;
  string method = 3;          // "trigram", "llm", "exact"
}

message Sentiment {
  double score = 1;           // -1.0 to 1.0
  double magnitude = 2;       // 0.0 to 1.0
  string model_version = 3;
}

message EnrichedMention {
  Mention mention = 1;
  repeated GameMatch game_matches = 2;
  Sentiment sentiment = 3;
  bytes embedding = 4;        // float32 array, 384 dims, little-endian
  string embedding_model = 5;
  string enricher_version = 6;
}
```

## Database schema

Single Postgres instance with `pgvector` and `timescaledb` extensions. Migrations live in `/migrations/` and are run by a one-shot `migrate` container in docker-compose before any service starts.

```sql
-- 0001_init.up.sql

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Canonical games
CREATE TABLE games (
    id              TEXT PRIMARY KEY,           -- slug, e.g. "elden-ring"
    canonical_name  TEXT NOT NULL,
    steam_app_id    INTEGER UNIQUE,
    released_at     DATE,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX games_canonical_name_trgm ON games USING gin (canonical_name gin_trgm_ops);

CREATE TABLE game_aliases (
    game_id     TEXT NOT NULL REFERENCES games(id) ON DELETE CASCADE,
    alias       TEXT NOT NULL,
    source      TEXT NOT NULL,                  -- "seed", "learned"
    confidence  REAL NOT NULL DEFAULT 1.0,
    PRIMARY KEY (game_id, alias)
);

CREATE INDEX game_aliases_alias_trgm ON game_aliases USING gin (alias gin_trgm_ops);

-- Mentions (raw + enriched merged into one row)
CREATE TABLE mentions (
    id                  TEXT PRIMARY KEY,        -- ULID == proto mention_id
    source              TEXT NOT NULL,
    native_id           TEXT NOT NULL,
    author_hash         TEXT NOT NULL,
    text                TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    engagement          JSONB NOT NULL DEFAULT '{}',
    raw_payload_ref     TEXT,
    parent_id           TEXT,
    source_metadata     JSONB NOT NULL DEFAULT '{}',
    sentiment_score     REAL,
    sentiment_magnitude REAL,
    sentiment_model     TEXT,
    enricher_version    TEXT,
    UNIQUE (source, native_id)
);

-- Make created_at a TimescaleDB hypertable for time-range queries
SELECT create_hypertable('mentions', 'created_at', if_not_exists => TRUE);

CREATE INDEX mentions_source_idx ON mentions (source);

-- Many-to-many between mentions and games
CREATE TABLE mention_games (
    mention_id  TEXT NOT NULL REFERENCES mentions(id) ON DELETE CASCADE,
    game_id     TEXT NOT NULL REFERENCES games(id) ON DELETE CASCADE,
    confidence  REAL NOT NULL,
    method      TEXT NOT NULL,
    PRIMARY KEY (mention_id, game_id)
);

CREATE INDEX mention_games_game_id_idx ON mention_games (game_id);

-- Embeddings (separate table to keep mentions row narrow)
CREATE TABLE mention_embeddings (
    mention_id      TEXT PRIMARY KEY REFERENCES mentions(id) ON DELETE CASCADE,
    embedding       vector(384) NOT NULL,
    model_version   TEXT NOT NULL
);

CREATE INDEX mention_embeddings_hnsw
    ON mention_embeddings
    USING hnsw (embedding vector_cosine_ops);

-- Hourly rollup for the API. Populated by a periodic SQL job (cron container or pg_cron).
CREATE TABLE game_buzz_hourly (
    game_id         TEXT NOT NULL REFERENCES games(id),
    hour            TIMESTAMPTZ NOT NULL,
    mention_count   INTEGER NOT NULL,
    avg_sentiment   REAL,
    PRIMARY KEY (game_id, hour)
);

SELECT create_hypertable('game_buzz_hourly', 'hour', if_not_exists => TRUE);
```

## Service-by-service spec

### `reddit-connector` (Go)

**Responsibility:** poll Reddit for new posts and comments in a configured list of subreddits, normalize them into `Mention` protobufs, and publish to `mentions.raw`.

**Inputs:**
- Config (Viper, from env + `/config/reddit-connector.yaml`):
  - `reddit.client_id`, `reddit.client_secret`, `reddit.user_agent`
  - `reddit.subreddits` — list of subreddit names with per-subreddit poll cadence
  - `redpanda.brokers`, `redpanda.topic` (default `mentions.raw`)
  - `minio.endpoint`, `minio.access_key`, `minio.secret_key`, `minio.bucket`
  - `otel.endpoint`

**External calls:**
- Reddit OAuth 2.0 client credentials flow for token acquisition
- `GET https://oauth.reddit.com/r/{subreddit}/new` for posts
- `GET https://oauth.reddit.com/r/{subreddit}/comments` for comments

**Persistence:**
- Last-seen `name` (Reddit's fullname, e.g. `t3_abc123`) per subreddit, stored in Redis under key `reddit:cursor:{subreddit}`. This is the resume point on restart.
- Raw JSON payload stored to MinIO at `s3://raw/reddit/{yyyy}/{mm}/{dd}/{native_id}.json`. The Kafka message only carries the key.

**Outputs:**
- Produces to Redpanda topic `mentions.raw` with key = `reddit:{subreddit}` (so a single subreddit's events land on a single partition, preserving order). Value is a serialized `Mention` protobuf. Headers include `trace_id` and `content-type: application/protobuf`.

**Key behaviors:**
1. **Adaptive polling.** Each subreddit has a configured base cadence (e.g. 30s). If a poll returns zero new items, multiply the next interval by 1.5 (capped at the configured max). On a non-empty poll, reset to base.
2. **Idempotency.** `mention_id` is generated as ULID seeded by `sha256(source || native_id)` — same input always produces same ID. Combined with the unique constraint on `(source, native_id)` in Postgres, this gives end-to-end idempotency.
3. **Distributed rate limiting.** Use a Redis-backed token bucket at key `reddit:tokens` shared across all replicas. Reddit's quota is 100 requests/minute per OAuth client.
4. **OTel.** Wrap every HTTP call and every Kafka produce in a span. Inject the trace ID into the protobuf `trace_id` field so downstream services can continue the trace.
5. **Graceful shutdown.** On SIGTERM, drain the in-flight produce buffer with a 30s deadline before exiting.

**Layout:**
```
services/reddit-connector/
├── cmd/main.go               ← wiring only; no business logic
├── internal/
│   ├── reddit/
│   │   ├── client.go         ← OAuth + HTTP client
│   │   ├── poller.go         ← per-subreddit polling loop
│   │   └── types.go          ← Reddit API response types
│   ├── publisher/
│   │   └── publisher.go      ← wraps franz-go producer with OTel
│   ├── state/
│   │   └── cursor.go         ← Redis cursor read/write
│   └── ratelimit/
│       └── bucket.go         ← Redis token bucket
└── Dockerfile
```

---

### `enricher` (Python, Temporal worker)

**Responsibility:** consume from `mentions.raw`, run each mention through an enrichment workflow, produce to `mentions.enriched`.

**Inputs:**
- Config (Pydantic Settings, from env):
  - `redpanda_brokers`, `input_topic`, `output_topic`, `consumer_group`
  - `temporal_address`, `temporal_namespace`, `task_queue`
  - `postgres_dsn` (for reading the games + aliases cache)
  - `anthropic_api_key` (for LLM disambiguation)
  - `otel_endpoint`

**External calls:**
- Temporal server for workflow orchestration
- Postgres for fetching `games` + `game_aliases` (cached in-memory, refreshed every 5 minutes)
- Anthropic API for LLM-assisted entity resolution when trigram matching is ambiguous (use `claude-haiku-4-5-20251001`)

**Persistence:**
- Workflow state is durable in Temporal (this is *the* reason Temporal is here)
- A `disambiguation_cache` Redis hash mapping `sha256(normalized_text + candidate_set) → game_id` with 30-day TTL

**Outputs:**
- Produces to `mentions.enriched`, partitioned by `game_id`. A single input mention can produce 0..N output events (zero if no game matched, N if multiple games matched).

**Workflow (Temporal):** `EnrichMentionWorkflow(mention: Mention) -> list[EnrichedMention]`
1. Activity `resolve_games(text)`:
   - Run trigram match against in-memory alias table → list of `(game_id, score)` candidates
   - If best score > 0.9 and gap to second-best > 0.2: high-confidence single match, skip LLM
   - Else if any candidates: call Anthropic with the text + top 5 candidates, ask which game(s) the text refers to (or "none"). Cache the result.
   - Else: return empty list
2. Activity `compute_sentiment(text)` — local model, batched at the worker level (32 mentions per batch via a small in-process queue)
3. Activity `generate_embedding(text)` — local model, also batched
4. Activity `emit_enriched(...)` — produce one event per matched game

All activities are idempotent, decorated with retry policies (3 attempts, exponential backoff with jitter, max 60s interval).

**Layout:**
```
services/enricher/
├── pyproject.toml
├── src/enricher/
│   ├── __main__.py           ← entrypoint, starts Temporal worker + Kafka consumer
│   ├── settings.py           ← Pydantic Settings
│   ├── consumer.py           ← Kafka consumer; for each message, starts a workflow
│   ├── workflows.py          ← EnrichMentionWorkflow
│   ├── activities/
│   │   ├── resolve.py        ← entity resolution
│   │   ├── sentiment.py      ← sentiment scoring (batched)
│   │   ├── embeddings.py     ← embedding generation (batched)
│   │   └── emit.py           ← produce to Redpanda
│   ├── caches/
│   │   ├── games.py          ← in-memory alias table, refreshed periodically
│   │   └── disambiguation.py ← Redis cache for LLM results
│   └── telemetry.py          ← OTel setup
└── Dockerfile
```

---

### `sink-consumer` (Go)

**Responsibility:** consume from `mentions.enriched` and write to Postgres in batches.

**Inputs:**
- Config: `redpanda.brokers`, `redpanda.topic` (default `mentions.enriched`), `redpanda.consumer_group` (default `sink-v1`), `postgres.dsn`, `sink.batch_size` (default 100), `sink.flush_interval_ms` (default 500), `otel.endpoint`

**External calls:** none beyond Redpanda and Postgres.

**Outputs:** writes to `mentions`, `mention_games`, `mention_embeddings` tables.

**Key behaviors:**
1. **Batched writes.** Accumulate up to `batch_size` enriched mentions or until `flush_interval_ms` elapses, then perform a single transactional write using `pgx.CopyFrom` for `mentions` and bulk inserts for the others. All within one `BEGIN/COMMIT`.
2. **Idempotency.** `INSERT ... ON CONFLICT (source, native_id) DO NOTHING` on `mentions`. For `mention_games` and `mention_embeddings`, `ON CONFLICT DO UPDATE` so re-enrichment with a newer model version replaces.
3. **Manual commit.** Only commit the Kafka offset *after* the Postgres transaction succeeds. On failure, the message will be re-delivered and the idempotency keys make it safe.
4. **Dead-letter handling.** Malformed protobuf messages (which should never happen if the schema registry is in use, but defensively) are logged with full payload to a `mentions.dlq` topic.

---

### `api-gateway` (Go)

**Responsibility:** REST API for the dashboard.

**Endpoints:**
- `GET /healthz` — liveness
- `GET /readyz` — readiness (checks Postgres reachable)
- `GET /v1/trending?window=24h&limit=20` — top games by mention count + sentiment over the window; backed by a SQL query joining `mentions`, `mention_games`, `games`
- `GET /v1/games/{game_id}` — game detail with mention timeseries (last 7 days, hourly buckets via TimescaleDB `time_bucket()`)
- `GET /v1/games/{game_id}/mentions?limit=50` — recent mentions for a game, sorted by `created_at DESC`
- `GET /v1/mentions/{mention_id}/similar?limit=10` — pgvector cosine-similarity search using HNSW index

**Layout:** standard chi + pgx repository pattern. Don't over-architect — this is a thin layer.

---

### `mcp-server` (Go)

**Responsibility:** expose an MCP interface over stdio (for Claude Desktop) and over Streamable HTTP (for remote clients).

**Tools to expose:**

| Tool name | Description | Parameters |
|-----------|-------------|------------|
| `search_mentions` | Hybrid search over mentions (text + filters) | `query: string`, `game_id?: string`, `since?: timestamp`, `limit?: int` |
| `get_game_buzz` | Mention count + sentiment timeseries for a game | `game_id: string`, `window: "24h"\|"7d"\|"30d"`, `bucket: "hour"\|"day"` |
| `find_similar_games` | Vector similarity over aggregated game discussion | `game_id: string`, `limit?: int` |
| `list_trending` | Top games by hype velocity | `window: "24h"\|"7d"`, `limit?: int` |

**Critical design points:**
- Tool descriptions must be written for LLM consumption: explicit about parameters, with examples in the description string. The quality of your tool docs determines whether Claude picks the right tool.
- Share business logic with `api-gateway` via a `pkg/queries/` package — do not duplicate SQL.
- Stdio mode is the default; flag-enable HTTP mode for remote use.
- The MCP server gets its own OTel spans; tool calls produce traces that can be correlated with the API.

## Shared concerns

### OpenTelemetry

Every service exports traces to the OTel Collector over OTLP gRPC at `otel-collector:4317`. The Collector pipelines:
- Traces → Jaeger
- Metrics → Prometheus (Collector exposes a `/metrics` endpoint, Prometheus scrapes it)

**Trace propagation contract:** the `trace_id` field on the `Mention` proto is the W3C traceparent header value, set by the producer. Consumers extract it and use it as the parent for their span. This stitches the trace across the async Kafka boundary.

In Go, use the `otelconfluent` or `otelfranz` instrumentation; in Python, `opentelemetry-instrumentation-aiokafka`.

### Configuration

- 12-factor: every service reads from env vars first, optional YAML file second.
- `.env.example` documents every variable. `.env` is gitignored.
- Secrets in local dev are dummies in `.env.example`. For Reddit credentials, the developer must create a Reddit app and fill in `.env`.

### Logging

Structured JSON logs at `slog` (Go) or `structlog` (Python). Every log line includes `trace_id` and `service` fields. Log level configurable via `LOG_LEVEL` env var.

### Testing

- **Unit tests** in each service alongside the code.
- **Integration tests** in `tests/integration/` (Go) using Testcontainers to spin up Redpanda + Postgres + MinIO. One required test: `TestE2E_RedditMentionToPostgres` that publishes a fake Reddit response, runs a slimmed enricher loop, and asserts a row appears in `mentions` with a non-null sentiment.
- **CI** runs `make test` and `make integration-test` on every PR.

### Local dev workflow

```bash
make bootstrap        # create .env from .env.example, pull images
make up               # docker compose up -d, runs migrations, creates topics, seeds games
make logs             # tail logs from all services
make seed-reddit      # produce a synthetic Reddit batch for testing without real API access
make down             # stop everything
```

## Build order

Follow this order. Each numbered step is a discrete unit of work that should end with a green test or a working demo.

1. **Repo scaffolding.** Create the directory layout, `go.work`, `pyproject.toml`s, `buf.yaml`, `Makefile`, `.env.example`, `.gitignore`. Empty `main.go` and `__main__.py` files that just print "hello" and exit 0.

2. **Protobuf schemas.** Write the three `.proto` files. Configure `buf.gen.yaml` to generate Go (to `gen/go/`) and Python (to `gen/python/`). `make proto` should regenerate.

3. **Docker Compose v1.** Bring up Redpanda, Postgres (with extensions), Redis, MinIO, Temporal, Jaeger, Prometheus, Grafana, OTel Collector. `make up` works, all containers healthy, no application services yet.

4. **Database migrations.** Write `0001_init.up.sql` and `.down.sql`. A `migrate` one-shot container in compose runs them before app services start. Seed `games.csv` via a `seed-games` one-shot container.

5. **Shared Go packages.** `pkg/telemetry`, `pkg/kafka`, `pkg/pg`. These get used by every Go service so writing them first pays off.

6. **`reddit-connector`.** Wire it up: Reddit client → producer → Redpanda. Verify with `rpk topic consume mentions.raw` that messages arrive.

7. **`sink-consumer`.** Skip enrichment for the moment — consume from `mentions.raw` directly and write a minimal row to `mentions` table (no sentiment, no games matched). This proves the persistence path before adding enrichment complexity. Then switch its input topic to `mentions.enriched` once the enricher is built.

8. **`enricher`.** Build the Temporal workflow with stubs first (sentiment returns 0.0, embedding returns zeros, entity resolution does trigram only — no LLM). Then layer in the real models. Then add the LLM fallback.

9. **Switch `sink-consumer` to `mentions.enriched`.** Update its topic config. Verify enriched data lands in Postgres.

10. **`api-gateway`.** Implement the four endpoints. Manual `curl` test against seeded data.

11. **`mcp-server`.** Implement the four tools. Test locally with the MCP Inspector tool, then add it to Claude Desktop config and chat.

12. **Observability polish.** Verify traces are connected end-to-end in Jaeger. Build one Grafana dashboard showing mentions/sec and enrichment latency.

13. **Integration test.** Write the Testcontainers-based `TestE2E_RedditMentionToPostgres`.

14. **README + design doc.** Write them last when you actually know what you built. Include a GIF of the Claude Desktop demo.

## Out of scope for the MVP

Explicitly NOT doing in the MVP, but should not be designed out of (i.e., the architecture must accommodate adding them later):

- Bluesky / Twitch / Metacritic / OpenCritic / Steam connectors
- The `ranker` service (Redis-backed leaderboards) — `api-gateway` queries Postgres directly for the MVP
- RisingWave for stream processing — the MVP's "hype score" is a simple SQL query, not a continuous materialized view
- The Next.js dashboard — MVP demos are via `curl` and Claude Desktop only
- Production deployment (k3s, Terraform) — local Docker only
- Load testing and chaos testing
- A/B framework for ranking algorithms
- Multi-region considerations

These belong in `docs/design.md` under "future work" so the design intent is documented.

## Anti-patterns to avoid

- **Do not define event types in each service.** The Protobuf-generated types are the only allowed event types. If a service needs a different shape internally, convert at the boundary.
- **Do not put business logic in `cmd/main.go`.** `main.go` is wiring only; everything else lives in `internal/`.
- **Do not commit `.env`.** `.env.example` only.
- **Do not skip OTel instrumentation in "boring" code paths.** Every external call (HTTP, DB, Kafka, Redis) gets a span. The traces are the demo.
- **Do not use `panic` for control flow in Go.** Return errors.
- **Do not use the `print` family in Python.** Use the configured `structlog` logger.
- **Do not silently swallow errors.** If a Kafka produce fails, log + metric + retry; never `_ = producer.Produce(...)`.
- **Do not hardcode timeouts.** Every external call gets a context with a configurable deadline.

## Questions to ask before starting work

When you (Claude Code) start a session, if any of these are unclear, ask the user before proceeding:
- Are the Reddit OAuth credentials available in `.env`, or should mock data be used for local testing?
- What's the seed list of subreddits (default: `games`, `gaming`, `patientgamers`, `pcgaming`, `IndieGaming`)?
- What's the seed list of games for entity resolution (default: read from `seed/games.csv`)?
- Is the developer running an Anthropic API key locally? If not, the LLM disambiguation path should fall back to "no match" rather than failing.
