"""Kafka consumer: one mention in, one Temporal workflow started.

Offsets are committed only after the workflow has been durably started. Once
Temporal accepts the workflow, the mention is guaranteed to be processed even if
this consumer dies immediately afterwards — so the commit point is exactly the
handoff point.
"""

from __future__ import annotations

import asyncio
import base64

import structlog
from aiokafka import AIOKafkaConsumer
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from temporalio.client import Client as TemporalClient

from enricher.activities.protoio import decode_mention
from enricher.settings import settings
from enricher.workflows import EnrichMentionWorkflow

log = structlog.get_logger()
tracer = trace.get_tracer("enricher.consumer")


async def run_consumer(temporal_client: TemporalClient, stop_event: asyncio.Event) -> None:
    consumer = AIOKafkaConsumer(
        settings.input_topic,
        bootstrap_servers=settings.redpanda_brokers,
        group_id=settings.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    log.info(
        "kafka consumer started",
        topic=settings.input_topic,
        group=settings.consumer_group,
    )

    try:
        async for msg in consumer:
            if stop_event.is_set():
                break
            await _handle_message(temporal_client, consumer, msg)
    finally:
        await consumer.stop()
        log.info("kafka consumer stopped")


async def _handle_message(temporal_client: TemporalClient, consumer, msg) -> None:
    with tracer.start_as_current_span("consume.mentions.raw", kind=SpanKind.CONSUMER) as span:
        try:
            mention = decode_mention(base64.b64encode(msg.value).decode("ascii"))
        except Exception as exc:  # noqa: BLE001
            # A message we cannot even parse will never parse. Committing past
            # it is correct — retrying forever would wedge the partition.
            log.error(
                "undecodable mention; skipping",
                error=str(exc),
                partition=msg.partition,
                offset=msg.offset,
            )
            span.record_exception(exc)
            await consumer.commit()
            return

        span.set_attribute("mention.id", mention.mention_id)
        span.set_attribute("kafka.partition", msg.partition)
        span.set_attribute("kafka.offset", msg.offset)

        try:
            # The workflow ID is the mention ID, which is deterministic from
            # (source, native_id). Temporal rejects a duplicate workflow ID, so
            # a redelivered mention is deduplicated by the orchestrator itself
            # rather than being enriched twice.
            await temporal_client.start_workflow(
                EnrichMentionWorkflow.run,
                args=[base64.b64encode(msg.value).decode("ascii"), mention.mention_id],
                id=f"enrich-{mention.mention_id}",
                task_queue=settings.task_queue,
            )
        except Exception as exc:  # noqa: BLE001
            if _is_already_started(exc):
                log.debug("workflow already started; skipping", mention_id=mention.mention_id)
                await consumer.commit()
                return
            # Do not commit: leave the offset so the message is redelivered.
            log.error(
                "failed to start workflow",
                error=str(exc),
                mention_id=mention.mention_id,
                offset=msg.offset,
            )
            span.record_exception(exc)
            return

        await consumer.commit()


def _is_already_started(exc: Exception) -> bool:
    """True if Temporal rejected the start because the workflow ID already exists."""
    from temporalio.service import RPCError

    if isinstance(exc, RPCError):
        return "already" in str(exc).lower() and "started" in str(exc).lower()
    return type(exc).__name__ == "WorkflowAlreadyStartedError"
