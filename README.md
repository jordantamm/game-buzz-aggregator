# Game Buzz Aggregator

A real-time, event-driven pipeline that tracks how much people are talking about video games on
Reddit, how they feel about them, and why — then exposes the whole thing to LLM agents as tools.

Reddit posts stream in over Kafka (Redpanda), Temporal-orchestrated Python workers resolve which
game each post is about and score its sentiment, embeddings land in Postgres with pgvector and
TimescaleDB, and a Go MCP server serves hybrid keyword-plus-semantic search to Claude Desktop or
to the built-in LangGraph analyst agent.

```
Reddit ──▶ reddit-connector ──▶ ┌──────────────┐ ──▶ enricher ──▶ ┌──────────────────┐
            (Go)                │ mentions.raw │     (Python,     │ mentions.enriched│
                                └──────────────┘      Temporal)   └──────────────────┘
                                                                            │
                                                                            ▼
                                                                     sink-consumer (Go)
                                                                            │
                                                                            ▼
                                                              ┌──────────────────────────┐
                                                              │ Postgres                 │
                                                              │ + pgvector + TimescaleDB │
                                                              └──────────────────────────┘
                                                                     │            │
                                                          api-gateway (Go)   mcp-server (Go)
                                                                                  │
                                                                                  ▼
                                                                    analyst-agent (LangGraph)
                                                                    Claude Desktop
```

Everything downstream of `mentions.raw` is source-agnostic. Adding Bluesky or Twitch means writing
one connector that publishes the same `Mention` protobuf — the enricher, sink, API, and MCP server
do not change.

---

## What's interesting here

### Hybrid search that actually fuses two retrievers

`search_mentions` runs a `pg_trgm` keyword pass and a pgvector HNSW similarity pass as **two
independent queries**, then merges them with reciprocal rank fusion
([`pkg/queries/hybrid_search.go`](pkg/queries/hybrid_search.go)):

```
score(doc) = Σ  1 / (60 + rank_in_retriever)
```

The tempting alternative — one SQL query with both predicates in the `WHERE` clause — is worse,
because the intersection is taken *before* ranking. Whichever signal matches fewer rows (usually
trigram, on short or paraphrased queries) starves the other's recall. Running them separately keeps
each retriever's ordering meaningful over its own candidate pool. Rank fusion also sidesteps the
fact that trigram similarity and cosine distance aren't comparable quantities, so score-level
fusion would need a calibration step that RRF doesn't.

Every result reports `matched_by: keyword | vector | both`, so a consuming model can see *why*
something surfaced.

Query embeddings come from the enricher's `/embed` endpoint rather than a second copy of the model
in Go — a query embedded by a different model isn't comparable to the stored vectors. If the
embedder is unreachable, search degrades to keyword-only instead of failing.

### LLM output that can't corrupt the database

`resolve_games` is the only model call whose output is written to Postgres, so it's constrained in
three layers ([`activities/resolve.py`](services/enricher/src/enricher/activities/resolve.py)):

1. **Instructor + Pydantic.** The call is forced into a `DisambiguationResult` schema and retried
   automatically on schema-invalid output. Nothing downstream parses prose.
2. **Post-hoc candidate validation.** The schema guarantees *shape*, not *truthfulness* — a
   hallucinated-but-well-formed slug passes Pydantic. Every returned `game_id` is checked against
   the candidate set actually sent, because `game_id` is a foreign key into `games`.
3. **promptfoo regression gate in CI.** A prompt regression here doesn't raise an error; it
   silently misattributes mentions and every downstream aggregate inherits the corruption. The
   [eval suite](evals/) runs on any PR touching the prompt and enforces exact-match rate, **zero**
   hallucinated IDs, and confidence calibration.

The eval imports the *production* prompt builder rather than keeping its own copy, and CI fails if
`system_prompt.txt` has drifted from the module. A suite testing a stale prompt passes while
production regresses — the worst failure mode a gate can have.

The trigram pre-filter is deliberately conservative: short aliases like `er` are capped below the
high-confidence threshold so they always route to LLM adjudication, and alias matching is on word
boundaries (`"controller"` does not match the game *Control*).

### Two orchestrators, on purpose

The enricher uses **Temporal**; the analyst agent uses **LangGraph**. They solve different problems:

|                | enricher (Temporal)                   | analyst-agent (LangGraph)                 |
| -------------- | ------------------------------------- | ----------------------------------------- |
| Shape          | Fixed durable DAG, same 4 steps       | Open-ended reasoning loop, branching       |
| Failure means  | Data loss — must resume mid-mention   | User re-asks                               |
| Needs          | Durability, retries, replay           | Conditional edges, self-correction         |

Using Temporal for the agent, or LangGraph for the enricher, would each be the wrong tool forced
into the wrong job.

The agent's `reflect` node is what earns the graph: a single-shot agent that calls `get_game_buzz`
for a quiet game returns "no data" and stops. This one detects the empty result and retries with a
wider window or a different tool, up to a hard 6-call budget — after which it answers from what it
has, with `confidence` lowered to say so. Citations naming a `mention_id` that never appeared in a
tool result are dropped before the answer is returned.

Tools are **discovered** from the MCP server at startup via `langchain-mcp-adapters`, never
hand-declared, so the Go server stays the single source of truth for the tool surface.

### End-to-end tracing across the async boundary

The trace ID is carried two ways: as W3C `traceparent` Kafka headers (transport-level) and in the
`Mention.trace_id` protobuf field (survives topic replay). A single Jaeger trace spans
`reddit-connector → Redpanda → enricher → Redpanda → sink-consumer → Postgres`, and agent spans
correlate with the MCP tool calls they triggered.

---

## Quick start

**Prerequisites:** Docker Desktop (8GB+ RAM allocated), `make`. Go 1.23 / Python 3.12 only if you
want to run tests outside containers.

```bash
git clone https://github.com/jordantamm/game-buzz-aggregator
cd game-buzz-aggregator

make bootstrap      # creates .env, pulls images
# edit .env — see "Credentials" below
make up             # builds and starts everything
make logs
```

First boot downloads ~500MB of ML models, so the enricher stays unhealthy for a few minutes. Its
`/readyz` only passes once both models are loaded, so nothing queries an embedder that would 503.

### Credentials

| Variable | Needed for | If missing |
| --- | --- | --- |
| `REDDIT_CLIENT_ID` / `REDDIT_CLIENT_SECRET` | Live Reddit ingestion | Use `make seed-reddit` for synthetic data |
| `ANTHROPIC_API_KEY` | LLM disambiguation, analyst agent | Ambiguous mentions resolve to "no match"; the agent refuses to start |

Create a Reddit app at <https://www.reddit.com/prefs/apps> (type: **script**).

### No credentials? Still works

```bash
make seed-reddit    # runs the connector against synthetic posts
make trending
```

---

## Trying it

```bash
# REST
curl "http://localhost:8080/v1/trending?window=24h&limit=10"
curl "http://localhost:8080/v1/search?q=people+frustrated+by+the+difficulty"
curl "http://localhost:8080/v1/games/elden-ring"

# The analyst agent, chaining tool calls
make ask Q="what's trending and why?"

curl -X POST http://localhost:8100/ask \
  -H 'Content-Type: application/json' \
  -d '{"question": "is sentiment on Baldurs Gate 3 improving or declining?"}'
```

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "game-buzz": {
      "command": "docker",
      "args": ["compose", "-f", "/absolute/path/to/docker-compose.yml", "run", "--rm", "-T", "mcp-server"],
      "env": { "POSTGRES_DSN": "postgres://gba:gba_dev_password@postgres:5432/gba?sslmode=disable" }
    }
  }
}
```

The server speaks stdio by default and Streamable HTTP with `--http` (which is how it runs in
Compose, on port 8085).

---

## MCP tools

| Tool | What it does |
| --- | --- |
| `list_trending` | Top games by mention volume. The only tool that discovers `game_id` slugs, so it's usually the entry point. |
| `search_mentions` | Hybrid keyword + semantic search over individual posts, RRF-fused. |
| `get_game_buzz` | Mention count and sentiment timeseries for one game (TimescaleDB `time_bucket`). |
| `find_similar_games` | Games whose *discussion* embeddings are similar — crosses genre boundaries. |

Tool descriptions are written for model consumption: explicit parameters, worked examples, and
cross-references telling the model when to reach for a different tool instead.

---

## Services

| Service | Language | Role |
| --- | --- | --- |
| `reddit-connector` | Go | Adaptive polling, Redis token-bucket rate limiting, MinIO raw archival, produces `mentions.raw` |
| `enricher` | Python | Temporal worker: entity resolution, sentiment, embeddings. Also serves `/embed` |
| `sink-consumer` | Go | Batched transactional upserts into Postgres |
| `api-gateway` | Go | REST API (chi + pgx) |
| `mcp-server` | Go | MCP over stdio and Streamable HTTP |
| `analyst-agent` | Python | LangGraph plan → call → reflect → synthesize loop |

Shared Go packages live in `pkg/` — notably `pkg/queries`, which both `api-gateway` and
`mcp-server` call so the SQL has exactly one implementation.

---

## Development

```bash
make test              # Go + Python unit tests
make integration-test  # Testcontainers end-to-end
make eval              # promptfoo disambiguation suite (needs ANTHROPIC_API_KEY)
make proto             # regenerate protobuf code
make psql              # shell into the database
```

CI runs build/vet/test for every Go module and both Python services on each PR. The eval gate runs
only on PRs touching the prompt, since each run costs tokens.

### Idempotency

`mention_id` is a ULID derived deterministically from `sha256(source | native_id)` — **both** the
timestamp prefix and the entropy come from the hash, so the same post always produces the same ID.
Combined with upserts in the sink and Temporal workflow IDs keyed on `mention_id`, reprocessing is
safe at every layer.

---

## Notable design decisions

Full reasoning and rejected alternatives are in [`docs/design.md`](docs/design.md). In brief:

- **Protobuf as the contract.** Services never define their own event types; they convert at the boundary.
- **`mentions` as a TimescaleDB hypertable.** Timescale requires the partitioning column in every unique index, so the PK is composite `(id, created_at)` and upserts must infer on `(source, native_id, created_at)`.
- **Sentiment via `twitter-roberta-base-sentiment`**, not a general-purpose model — Reddit comments are short, informal, emoji-bearing social text.
- **Redis-cached disambiguation** keyed by `(normalized_text, candidate_set)`, not text alone: the same text can have a different correct answer once the games table changes.
- **Batched upserts, not `COPY`,** in the sink. `COPY` can't express `ON CONFLICT`, and conflicts are the normal case in an idempotent pipeline.

## Out of scope (deliberately)

Bluesky/Twitch/Metacritic connectors, a Redis-backed ranker service, RisingWave materialized views,
the Next.js dashboard, and Kubernetes deployment. The architecture accommodates all of them; see
"Future work" in the design doc.
