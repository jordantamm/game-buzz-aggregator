"""Process-local singletons shared by Temporal activities.

Activities run in the worker process, so they can hold references to live
objects — Kafka producers, Redis clients, the games cache. Workflows cannot:
their arguments and results are persisted to Temporal's event history and must
be JSON-serializable. This module is how an activity reaches a live object
without it ever crossing a workflow boundary.

`init()` is called once at worker startup, before the worker begins polling.
"""

from __future__ import annotations

import asyncio

import asyncpg
import structlog
from aiokafka import AIOKafkaProducer

from enricher.caches.disambiguation import DisambiguationCache
from enricher.caches.games import GameCache
from enricher.settings import settings

log = structlog.get_logger()


class Runtime:
    """Holds the worker's long-lived clients."""

    def __init__(self) -> None:
        self.game_cache: GameCache | None = None
        self.disambiguation_cache: DisambiguationCache | None = None
        self.producer: AIOKafkaProducer | None = None
        self._pg_pool: asyncpg.Pool | None = None
        self._refresh_task: asyncio.Task | None = None

    async def start(self) -> None:
        self._pg_pool = await asyncpg.create_pool(
            settings.postgres_dsn, min_size=1, max_size=4
        )

        self.game_cache = GameCache(settings.postgres_dsn)
        async with self._pg_pool.acquire() as conn:
            await self.game_cache.refresh(conn)
        if settings.resolution_mode in ("vector", "hybrid"):
            await self.game_cache.build_vector_index()

        self.disambiguation_cache = DisambiguationCache(f"redis://{settings.redis_addr}")

        self.producer = AIOKafkaProducer(bootstrap_servers=settings.redpanda_brokers)
        await self.producer.start()

        self._refresh_task = asyncio.create_task(self._refresh_loop())
        log.info("runtime started", brokers=settings.redpanda_brokers)

    async def stop(self) -> None:
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
        if self.producer is not None:
            await self.producer.stop()
        if self._pg_pool is not None:
            await self._pg_pool.close()
        log.info("runtime stopped")

    async def _refresh_loop(self) -> None:
        """Periodically reload the games/alias table from Postgres.

        New games and learned aliases are picked up without a restart. Failures
        are logged and retried rather than killing the worker — a stale cache
        still resolves the games it already knows about.
        """
        while True:
            await asyncio.sleep(settings.games_cache_refresh_seconds)
            try:
                assert self._pg_pool is not None and self.game_cache is not None
                async with self._pg_pool.acquire() as conn:
                    await self.game_cache.refresh(conn)
                if settings.resolution_mode in ("vector", "hybrid"):
                    await self.game_cache.build_vector_index()
            except Exception as exc:  # noqa: BLE001 - loop must survive any failure
                log.warning("games cache refresh failed", error=str(exc))


_runtime = Runtime()


def get() -> Runtime:
    """Return the process runtime. Raises if the worker has not started it."""
    if _runtime.game_cache is None:
        raise RuntimeError("runtime.start() has not been called")
    return _runtime


async def start() -> Runtime:
    await _runtime.start()
    return _runtime


async def stop() -> None:
    await _runtime.stop()
