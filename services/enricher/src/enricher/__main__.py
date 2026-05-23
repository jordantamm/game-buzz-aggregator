import asyncio
import structlog
from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker

from enricher.activities.embeddings import generate_embedding
from enricher.activities.emit import emit_enriched
from enricher.activities.resolve import resolve_games
from enricher.activities.sentiment import compute_sentiment
from enricher.caches.disambiguation import DisambiguationCache
from enricher.caches.games import GameCache
from enricher.consumer import run_consumer
from enricher.settings import settings
from enricher.telemetry import init_telemetry
from enricher.workflows import EnrichMentionWorkflow

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        structlog.stdlib.NAME_TO_LEVEL.get(settings.log_level.upper(), 20)
    )
)

log = structlog.get_logger()


async def main() -> None:
    init_telemetry(settings.otel_service_name, settings.otel_exporter_otlp_endpoint)

    import asyncpg
    conn = await asyncpg.connect(settings.postgres_dsn)
    game_cache = GameCache(settings.postgres_dsn)
    await game_cache.ensure_fresh(conn)
    await conn.close()

    disambiguation_cache = DisambiguationCache(f"redis://{settings.redis_addr}")

    temporal_client = await TemporalClient.connect(settings.temporal_address)

    worker = Worker(
        temporal_client,
        task_queue=settings.task_queue,
        workflows=[EnrichMentionWorkflow],
        activities=[resolve_games, compute_sentiment, generate_embedding, emit_enriched],
    )

    log.info("enricher starting", task_queue=settings.task_queue)
    await asyncio.gather(
        worker.run(),
        run_consumer(temporal_client, game_cache, disambiguation_cache),
    )


if __name__ == "__main__":
    asyncio.run(main())
