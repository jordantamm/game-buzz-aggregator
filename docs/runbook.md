# Runbook

## Health checks

| Service | How to check |
|---------|--------------|
| Redpanda | `docker compose exec redpanda rpk cluster health` |
| Postgres | `docker compose exec postgres pg_isready -U gba` |
| Temporal | `curl localhost:8233` (UI) or `docker compose logs temporal` |
| API | `curl localhost:8080/healthz` |

## Common issues

### `reddit-connector` is logging 401s

The OAuth token has expired or the credentials in `.env` are wrong. Verify with `curl -X POST -d "grant_type=client_credentials" -u "$REDDIT_CLIENT_ID:$REDDIT_CLIENT_SECRET" https://www.reddit.com/api/v1/access_token`.

### `enricher` workflows stuck in retry

Check Temporal UI at `http://localhost:8233`. Common causes: Anthropic API key missing (the fallback should return empty, but verify), Postgres unreachable from the enricher.

### Postgres extensions missing

Verify with `\dx` in psql. The `timescale/timescaledb-ha` image should include all three. If pgvector is missing, the image variant might be wrong — pin to `timescale/timescaledb-ha:pg16` exactly.

### Traces not appearing in Jaeger

Check otel-collector logs: `docker compose logs otel-collector`. Verify each service has `OTEL_SERVICE_NAME` and `OTEL_EXPORTER_OTLP_ENDPOINT` set. Trace propagation requires the producer to set the `trace_id` field in the protobuf — consumers parse it and inject as parent context.

### Kafka consumer lag growing

Check `rpk group describe sink-v1` (or whichever group). If the enricher is falling behind, scale it up (`docker compose up -d --scale enricher=3`). If the sink is falling behind, check Postgres CPU and increase `SINK_BATCH_SIZE`.

## Reset procedures

### Wipe all data and start fresh

```bash
make clean
make up
```

### Replay from a specific offset

```bash
docker compose exec redpanda rpk group seek sink-v1 --to start --topics mentions.enriched
```
