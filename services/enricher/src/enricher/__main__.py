"""Enricher entrypoint.

Runs three things in one process:
  1. A Temporal worker executing the enrichment activities.
  2. A Kafka consumer turning `mentions.raw` messages into workflow starts.
  3. An HTTP server exposing the embedding model to the Go services.

They share a process so there is one copy of the model weights in memory —
the models are the dominant cost here, not the concurrency.
"""

from __future__ import annotations

import asyncio
import logging
import signal

import structlog
import uvicorn
from temporalio.client import Client as TemporalClient
from temporalio.worker import Worker

from enricher import runtime
from enricher.activities.embeddings import generate_embedding
from enricher.activities.embeddings import warm_up as warm_up_embeddings
from enricher.activities.emit import emit_enriched
from enricher.activities.protoio import extract_text
from enricher.activities.resolve import resolve_games
from enricher.activities.sentiment import compute_sentiment
from enricher.activities.sentiment import warm_up as warm_up_sentiment
from enricher.consumer import run_consumer
from enricher.embed_server import app as embed_app
from enricher.settings import settings
from enricher.telemetry import init_telemetry
from enricher.workflows import EnrichMentionWorkflow


def _configure_logging() -> None:
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=True,
    )


log = structlog.get_logger()


async def main() -> None:
    _configure_logging()
    init_telemetry(settings.otel_service_name, settings.otel_exporter_otlp_endpoint)

    # Load both models before accepting any work. Otherwise the first mention
    # pays the download/load cost inside an activity timeout and looks flaky.
    log.info("warming up models")
    await asyncio.gather(
        asyncio.to_thread(warm_up_sentiment),
        asyncio.to_thread(warm_up_embeddings),
    )
    log.info("models ready")

    await runtime.start()

    temporal_client = await TemporalClient.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )

    worker = Worker(
        temporal_client,
        task_queue=settings.task_queue,
        workflows=[EnrichMentionWorkflow],
        activities=[
            extract_text,
            resolve_games,
            compute_sentiment,
            generate_embedding,
            emit_enriched,
        ],
        max_concurrent_activities=settings.max_concurrent_activities,
    )

    embed_config = uvicorn.Config(
        embed_app,
        host="0.0.0.0",
        port=settings.embed_server_port,
        log_config=None,
        access_log=False,
    )
    embed_server = uvicorn.Server(embed_config)
    # uvicorn installs its own SIGTERM handler otherwise, which would race the
    # shutdown sequence below.
    embed_server.install_signal_handlers = lambda: None

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    log.info(
        "enricher started",
        task_queue=settings.task_queue,
        embed_port=settings.embed_server_port,
    )

    tasks = [
        asyncio.create_task(worker.run(), name="temporal-worker"),
        asyncio.create_task(run_consumer(temporal_client, stop_event), name="kafka-consumer"),
        asyncio.create_task(embed_server.serve(), name="embed-server"),
        asyncio.create_task(stop_event.wait(), name="shutdown-signal"),
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    # Surface a crash in any component instead of exiting silently.
    for task in done:
        if task.get_name() != "shutdown-signal" and task.exception() is not None:
            log.error("component failed", component=task.get_name(), error=str(task.exception()))

    log.info("shutting down")
    stop_event.set()
    embed_server.should_exit = True
    await worker.shutdown()

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    await runtime.stop()
    log.info("shutdown complete")


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows event loops do not support add_signal_handler.
            signal.signal(sig, lambda *_: stop_event.set())


if __name__ == "__main__":
    asyncio.run(main())
