# Design Document — Game Buzz Aggregator

> Living document. Update as decisions evolve. Each major decision gets an ADR in `docs/adr/`.

## Goals

1. Ingest game discussions from social platforms in near-real-time (seconds, not hours).
2. Resolve free-text mentions to canonical game identities with calibrated confidence.
3. Compute and serve trending/buzz signals via both a REST API and an MCP server.
4. Architect such that adding sources is a connector-only change — downstream services are source-agnostic.

## Non-goals

- Long-term archival storage (>90 days). Hot data only.
- Real-time alerting / notifications.
- User-facing accounts, auth, personalization.
- A polished consumer UI.

## Key decisions

### Redpanda over Kafka

Kafka API-compatible, single binary, easier ops footprint. Migration path is trivial if scale ever justifies it. Alternative considered: NATS JetStream (simpler) — rejected because the broader Kafka ecosystem (Schema Registry, Connect, console UIs) is too valuable to give up.

### Temporal for enrichment, not for ingestion

Ingestion is a tight poll loop where workflow durability adds little. Enrichment, with retries against external APIs (Reddit, Anthropic) and multi-step orchestration, is exactly Temporal's sweet spot. We use the right tool for each part of the pipeline.

### Postgres + pgvector + TimescaleDB in one instance

Operational simplicity dominates at our scale. A single Postgres with three extensions covers OLTP, time-series, and vector search. We accept the upper-bound limits (HNSW slow above ~50M vectors; TimescaleDB compression tradeoffs) because we won't approach them. Alternatives considered: Qdrant or Weaviate for vectors (rejected: extra deployment, no transactional joins with relational data); ClickHouse for time-series (rejected: scale mismatch).

### Source-agnostic enrichment

Every connector produces a `Mention` proto with no source-specific shape leaking into downstream services. Source-specific context goes into `source_metadata`, a typed map that's deliberately not used for routing or business logic. This means adding Bluesky/Twitch later is a connector-only change.

### Two-topic topology with re-partitioning at enrichment

`mentions.raw` is partitioned by `source:subreddit`. `mentions.enriched` is partitioned by `game_id`. The enricher re-partitions because game_id isn't known at ingest time and per-game ordering is what downstream consumers actually need.

### MCP server alongside REST, not instead

REST serves the dashboard (where MCP semantics would be wrong). MCP serves the agentic demo. Both share business logic via `pkg/queries/` — no duplication.

## Future work (post-MVP)

- Bluesky Jetstream connector
- Twitch IRC connector for chat + Helix API for viewership signals
- Metacritic scraper (Playwright + Browserless)
- OpenCritic + Steam score pollers
- RisingWave for continuous Hype Velocity materialization
- `ranker` service with Redis-backed leaderboards
- Next.js dashboard
- A/B framework for ranking algorithms
- Production deployment on k3s + Hetzner

## Known tradeoffs and limitations

- LLM disambiguation introduces tail latency (~500ms p99) when trigram matching is ambiguous. Mitigated by aggressive caching.
- Enrichment is per-mention, not batched at the workflow level. Activities within the workflow are batched (32 mentions per inference call) but Temporal workflow overhead per mention is non-trivial. Would revisit if throughput becomes a constraint.
- Single-region by design. Geographic redundancy is explicitly out of scope.
