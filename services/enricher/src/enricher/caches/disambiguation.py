import hashlib
import json

import redis.asyncio as redis
import structlog

log = structlog.get_logger()

TTL_SECONDS = 30 * 24 * 3600  # 30 days


class DisambiguationCache:
    """Redis cache for LLM disambiguation results to avoid redundant API calls."""

    def __init__(self, redis_url: str):
        self._redis = redis.from_url(redis_url)

    def _key(self, text: str, candidates: list[str]) -> str:
        payload = text + "|" + ",".join(sorted(candidates))
        return "disambiguation:" + hashlib.sha256(payload.encode()).hexdigest()

    async def get(self, text: str, candidates: list[str]) -> list[str] | None:
        key = self._key(text, candidates)
        val = await self._redis.get(key)
        if val is None:
            return None
        return json.loads(val)

    async def set(self, text: str, candidates: list[str], result: list[str]) -> None:
        key = self._key(text, candidates)
        await self._redis.setex(key, TTL_SECONDS, json.dumps(result))
