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

### Hybrid search with reciprocal rank fusion, not a single SQL predicate

`search_mentions` runs `pg_trgm` keyword matching and pgvector HNSW similarity as two independent retrieval passes, then fuses them with reciprocal rank fusion rather than combining both predicates in one SQL `WHERE` clause. Combining predicates lets whichever signal has fewer matches (usually trigram, on short or paraphrased queries) starve the other's recall. Running them independently and fusing by rank keeps each retrieval method's ordering meaningful regardless of how sparse or dense its result set is. Rejected: a cross-encoder reranker (Cohere Rerank / BGE) — adds a network hop and a paid dependency for a marginal quality gain at MVP data volumes; RRF is free and stateless. Revisit if `search_mentions` result quality becomes a demo weak point.

### Structured LLM output via Instructor, not free-text parsing

The `resolve_games` disambiguation call is the one LLM call whose output is trusted (a `game_id` gets written to Postgres). Wrapping it with `instructor` against a Pydantic schema means the SDK retries automatically on schema-invalid output instead of the enricher trying to regex a game ID out of prose. `game_id` is further validated against the candidate set post-call — the schema guarantees shape, not truthfulness, so a belt-and-suspenders check against known-valid IDs still runs. Alternative considered: raw Anthropic tool-use with a manual JSON-schema tool definition — functionally similar, but Instructor's automatic retry-on-validation-failure loop is worth the dependency.

### promptfoo eval gate on the disambiguation prompt, not manual spot-checks

Any change to the `resolve_games` prompt template, the candidate-selection logic, or the underlying model can silently regress disambiguation accuracy — and unlike most bugs, it fails quietly (wrong game gets a mention instead of an error). A promptfoo suite over a fixed set of labeled ambiguous mentions runs in CI on every touching PR, asserting accuracy, zero-hallucination, and confidence-calibration thresholds. This is a regression gate, not exploratory prompt testing — it exists to catch silent drift, not to design the prompt.

### LangGraph agent as an MCP client, not a Temporal workflow

`analyst-agent` and the enricher's Temporal workflow solve different problems and deliberately use different orchestration tools. Temporal orchestrates a **fixed, durable DAG** with retries — the enricher always runs the same four activities in the same order, and durability (surviving worker crashes mid-mention) matters more than flexibility. The analyst agent orchestrates an **open-ended reasoning loop** — which tools to call, how many times, and when to stop depend on the question and on intermediate results, and any individual run failing is not a data-loss event (the user just re-asks). LangGraph's graph-of-nodes-with-conditional-edges model fits branching tool choice and self-correction; Temporal's model does not. Using Temporal for the agent, or LangGraph for the enricher, would each be the wrong tool forced into the wrong job. `langchain-mcp-adapters` discovers `mcp-server`'s tools at startup rather than hand-declaring them, so the tool surface stays a single source of truth (the MCP server) instead of drifting between two definitions.

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
- Cross-encoder reranking (Cohere Rerank / BGE) on top of RRF for `search_mentions`, if fusion-based ranking proves insufficient
- Multi-turn conversation memory for `analyst-agent` (currently single-question, stateless per call)

## Known tradeoffs and limitations

- LLM disambiguation introduces tail latency (~500ms p99) when trigram matching is ambiguous. Mitigated by aggressive caching.
- Enrichment is per-mention, not batched at the workflow level. Activities within the workflow are batched (32 mentions per inference call) but Temporal workflow overhead per mention is non-trivial. Would revisit if throughput becomes a constraint.
- Single-region by design. Geographic redundancy is explicitly out of scope.
- `analyst-agent`'s tool-call cap (6 per question) trades completeness for bounded latency and cost; a question genuinely requiring more chained calls returns a partial, lower-confidence answer rather than continuing.
