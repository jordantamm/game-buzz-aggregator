"""Redis cache for LLM disambiguation results.

Keyed by (normalized text, candidate set) rather than by text alone: the same
post text can produce a different correct answer if the games table has changed
underneath it, so the candidate set is part of the identity of the question.

Beyond saving API spend, this makes resolution deterministic for repeated text —
crossposts and copypasta resolve identically every time instead of drifting with
model sampling.
"""

from __future__ import annotations

import hashlib
import json
import re

import redis.asyncio as redis
import structlog

log = structlog.get_logger()

TTL_SECONDS = 30 * 24 * 3600  # 30 days
KEY_PREFIX = "disambiguation:"

_WHITESPACE = re.compile(r"\s+")


def _normalize(text: str) -> str:
    """Collapse whitespace and case so trivial reformatting still hits cache."""
    return _WHITESPACE.sub(" ", text.strip().lower())


class DisambiguationCache:
    def __init__(self, redis_url: str):
        self._redis = redis.from_url(redis_url, decode_responses=True)

    def _key(self, text: str, candidate_ids: list[str]) -> str:
        payload = _normalize(text) + "|" + ",".join(sorted(candidate_ids))
        return KEY_PREFIX + hashlib.sha256(payload.encode()).hexdigest()

    async def get(self, text: str, candidate_ids: list[str]) -> list[dict] | None:
        """Return cached GameMatch dicts, or None on a miss.

        A cache failure must not fail enrichment — Redis being down should cost
        money (a redundant LLM call), not correctness.
        """
        try:
            raw = await self._redis.get(self._key(text, candidate_ids))
        except Exception as exc:  # noqa: BLE001
            log.warning("disambiguation cache read failed", error=str(exc))
            return None

        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.warning("discarding corrupt disambiguation cache entry")
            return None

    async def set(self, text: str, candidate_ids: list[str], matches: list[dict]) -> None:
        try:
            await self._redis.setex(
                self._key(text, candidate_ids), TTL_SECONDS, json.dumps(matches)
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("disambiguation cache write failed", error=str(exc))

    async def close(self) -> None:
        await self._redis.aclose()
