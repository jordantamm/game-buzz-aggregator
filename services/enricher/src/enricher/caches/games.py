import asyncio
import time
from dataclasses import dataclass, field

import asyncpg
import structlog

log = structlog.get_logger()

REFRESH_INTERVAL = 300  # seconds


@dataclass
class GameEntry:
    game_id: str
    canonical_name: str
    aliases: list[str] = field(default_factory=list)


class GameCache:
    """In-memory alias → game_id table, refreshed from Postgres every 5 minutes."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._entries: list[GameEntry] = []
        self._alias_index: dict[str, str] = {}  # alias.lower() → game_id
        self._last_refresh = 0.0
        self._lock = asyncio.Lock()

    async def _refresh(self, conn: asyncpg.Connection) -> None:
        rows = await conn.fetch("""
            SELECT g.id, g.canonical_name, array_agg(ga.alias) AS aliases
            FROM games g
            LEFT JOIN game_aliases ga ON ga.game_id = g.id
            GROUP BY g.id, g.canonical_name
        """)
        entries = []
        alias_index: dict[str, str] = {}
        for row in rows:
            aliases = [a for a in (row["aliases"] or []) if a]
            e = GameEntry(game_id=row["id"], canonical_name=row["canonical_name"], aliases=aliases)
            entries.append(e)
            alias_index[row["canonical_name"].lower()] = row["id"]
            for alias in aliases:
                alias_index[alias.lower()] = row["id"]
        self._entries = entries
        self._alias_index = alias_index
        self._last_refresh = time.monotonic()
        log.info("game_cache refreshed", count=len(entries))

    async def ensure_fresh(self, conn: asyncpg.Connection) -> None:
        async with self._lock:
            if time.monotonic() - self._last_refresh > REFRESH_INTERVAL:
                await self._refresh(conn)

    def lookup(self, text: str) -> list[tuple[str, float]]:
        """Trigram-style substring scan. Returns (game_id, score) pairs."""
        text_lower = text.lower()
        results: list[tuple[str, float]] = []

        for alias, game_id in self._alias_index.items():
            if alias in text_lower:
                # Score by alias length (longer = more specific = higher confidence)
                score = min(1.0, len(alias) / 20.0 + 0.5)
                results.append((game_id, score))

        # Deduplicate: keep highest score per game_id
        best: dict[str, float] = {}
        for game_id, score in results:
            if score > best.get(game_id, 0.0):
                best[game_id] = score

        return sorted(best.items(), key=lambda x: x[1], reverse=True)

    def get_candidates(self, game_ids: list[str]) -> list[GameEntry]:
        id_set = set(game_ids)
        return [e for e in self._entries if e.game_id in id_set]
