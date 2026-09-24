# reddit-connector

Polls Reddit for new posts and comments, converts them to `Mention` protobufs, and publishes them to Redpanda topic `mentions.raw`. Everything downstream (enricher, sink, API) reads from that topic — this service is the only thing that ever talks to Reddit.

---

## Files

```
services/reddit-connector/
├── cmd/main.go                     ← wiring only: reads config, connects Redis + Kafka, starts pollers
├── internal/
│   ├── reddit/
│   │   ├── client.go               ← Reddit OAuth + HTTP: gets a token, calls /r/{sub}/new
│   │   ├── poller.go               ← one goroutine per subreddit; adaptive polling loop
│   │   ├── mock.go                 ← MockClient: returns 3 hardcoded posts, no Reddit creds needed
│   │   └── types.go                ← Go structs that match Reddit's JSON response shape
│   ├── publisher/
│   │   └── publisher.go            ← proto-marshal + Kafka produce
│   ├── state/
│   │   └── cursor.go               ← Redis: reads/writes the "after" cursor per subreddit
│   └── ratelimit/
│       └── bucket.go               ← Redis token bucket: 100 req/min across all replicas
└── Dockerfile
```

---

## API calls that come IN (what it receives)

### 1. Reddit OAuth token
```
POST https://www.reddit.com/api/v1/access_token
Authorization: Basic {clientID}:{clientSecret}
Body: grant_type=client_credentials
```
Returns a bearer token valid for ~60 minutes. The client caches it and refreshes automatically.

### 2. Reddit listing (per subreddit, per poll cycle)
```
GET https://oauth.reddit.com/r/{subreddit}/new?limit=100&after={cursor}
Authorization: Bearer {token}
User-Agent: {configured user agent}
```
Returns up to 100 posts/comments sorted newest-first. The `after` param is Reddit's pagination cursor (a "fullname" like `t3_abc123`). On first run, `after` is empty and you get the latest 100.

---

## Data that goes OUT

One Redpanda message per post/comment, published to topic **`mentions.raw`**:

| Field | Value |
|-------|-------|
| Key | `reddit:{subreddit}` (e.g. `reddit:gaming`) |
| Value | Serialized `Mention` protobuf (see `/proto/gba/v1/mention.proto`) |

The `Mention` contains: a deterministic ULID for the ID, author as a SHA-256 hash (never raw), the post title/body as `text`, upvote score, reply count, timestamp, and `subreddit` in `source_metadata`.

---

## State it touches

| Storage | Key | What it stores |
|---------|-----|----------------|
| Redis | `reddit:cursor:{subreddit}` | Pagination cursor so it picks up where it left off after a restart |
| Redis | `reddit:tokens` | Token bucket state — shared across replicas to enforce 100 req/min total |

---

## Key behaviors

- **One goroutine per subreddit.** Each subreddit is polled independently on its own loop.
- **Adaptive backoff.** If a poll returns 0 new items, the next wait is multiplied by 1.5 (capped at `REDDIT_POLL_MAX_INTERVAL_SECONDS`). A non-empty poll resets to the base interval.
- **Idempotent IDs.** The mention ID is a ULID seeded by `sha256("reddit" + native_id)`. The same Reddit post always produces the same ID, so republishing is safe.
- **Graceful shutdown.** Catches SIGTERM/SIGINT, cancels all pollers, and exits cleanly.

---

## Testing in isolation (mock mode)

You do **not** need real Reddit credentials. Pass `--mock` and the service uses `MockClient` instead of the real HTTP client. It returns 32 hardcoded posts on every poll: 10 about Elden Ring, 5 about Baldur's Gate 3, and 1 for each other game in `seed/games.csv`.

### Minimal infrastructure needed

The connector only requires Redpanda and Redis. You don't need Postgres, Temporal, or any other service.

**Step 1 — start just the dependencies:**
```powershell
docker compose up -d redpanda redis bootstrap-topics
```
Wait ~15 seconds for Redpanda to become healthy.

**Step 2 — run the connector in mock mode:**

Option A — run the binary directly (if Go is installed):
```powershell
cd services/reddit-connector
go run ./cmd --mock
```

Option B — run via Docker (builds from the workspace root):
```powershell
docker compose run --rm `
  -e MOCK=true `
  reddit-connector /reddit-connector --mock
```

**Step 3 — watch messages arrive in the topic:**
```powershell
docker compose exec redpanda rpk topic consume mentions.raw --num 5
```

You should see JSON-ish output (the raw bytes are protobuf, so the values are binary, but the key field will show `reddit:games` etc.) Every ~30 seconds the mock posts will be re-published (the cursor never advances in mock mode, so the same 3 posts loop).

**Step 4 — check the Redpanda Console UI (optional):**

Open `http://localhost:8090` in a browser. You'll see the `mentions.raw` topic with growing message counts.

---

## Environment variables

| Variable | Default | Notes |
|----------|---------|-------|
| `REDDIT_CLIENT_ID` | — | Required in real mode; unused in mock |
| `REDDIT_CLIENT_SECRET` | — | Required in real mode; unused in mock |
| `REDDIT_USER_AGENT` | — | Required in real mode; unused in mock |
| `REDDIT_SUBREDDITS` | `games,gaming,patientgamers,pcgaming,IndieGaming` | Comma-separated |
| `REDDIT_POLL_INTERVAL_SECONDS` | `120` | Base polling interval |
| `REDDIT_POLL_MAX_INTERVAL_SECONDS` | `600` | Backoff ceiling |
| `REDPANDA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `TOPIC_MENTIONS_RAW` | `mentions.raw` | Topic to produce to |
| `REDIS_ADDR` | `localhost:6379` | |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `otel-collector:4317` | Traces; service starts fine if unreachable |

---

## What the logs look like

On startup (mock mode):
```
{"level":"info","msg":"reddit-connector started","subreddits":["games","gaming",...],"mock":true}
```

Each poll cycle produces a span in Jaeger (if OTel is running) and a log line if there's an error. Silent on success by design — watch the topic consumer output to confirm data is flowing.
