import asyncio
import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from temporalio.client import Client as TemporalClient

from enricher.caches.disambiguation import DisambiguationCache
from enricher.caches.games import GameCache
from enricher.settings import settings
from enricher.workflows import EnrichMentionWorkflow

log = structlog.get_logger()


async def run_consumer(
    temporal_client: TemporalClient,
    game_cache: GameCache,
    disambiguation_cache: DisambiguationCache,
) -> None:
    consumer = AIOKafkaConsumer(
        settings.input_topic,
        bootstrap_servers=settings.redpanda_brokers,
        group_id=settings.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=settings.redpanda_brokers)

    await consumer.start()
    await producer.start()
    log.info("kafka consumer started", topic=settings.input_topic)

    try:
        async for msg in consumer:
            try:
                await temporal_client.start_workflow(
                    EnrichMentionWorkflow.run,
                    args=[msg.value, game_cache, disambiguation_cache, producer],
                    id=f"enrich-{msg.partition}-{msg.offset}",
                    task_queue=settings.task_queue,
                )
                await consumer.commit()
            except Exception as e:
                log.error("failed to start workflow", error=str(e), offset=msg.offset)
    finally:
        await consumer.stop()
        await producer.stop()
