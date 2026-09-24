"""In-memory games/alias table used by the entity-resolution activity.

Kept in memory because it is small (hundreds of rows), read on every single
mention, and changes rarely. It is refreshed from Postgres on an interval by
`enricher.runtime`, not on a cache miss — an unknown alias is a legitimate
"no match", not a signal to hit the database.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field

import asyncpg
import structlog

log = structlog.get_logger()


@dataclass
class GameEntry:
    game_id: str
    canonical_name: str
    aliases: list[str] = field(default_factory=list)
    # Free-text blurb (genre, setting, franchise) from games.metadata->>'description'.
    # The richer this is, the better the vector index separates similar titles.
    description: str = ""

    def profile(self) -> str:
        """Text that represents this game in embedding space."""
        parts = [self.canonical_name]
        if self.aliases:
            parts.append("also known as " + ", ".join(self.aliases[:6]))
        if self.description:
            parts.append(self.description)
        return ". ".join(parts)


# Aliases short enough to collide with ordinary English are only accepted as
# exact standalone tokens, and never score high enough to skip LLM adjudication.
AMBIGUOUS_ALIAS_MAX_LEN = 8


class GameCache:
    """Alias to game_id lookup with a crude token-boundary trigram score."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._entries: list[GameEntry] = []
        # alias (lowercased) -> game_id
        self._alias_index: dict[str, str] = {}
        # alias -> precompiled word-boundary pattern
        self._alias_patterns: dict[str, re.Pattern[str]] = {}
        self._last_refresh = 0.0
        # Vector index: row i of _vectors is the embedding of _entries[i].profile().
        self._vectors = None  # numpy (n, dims) float32, L2-normalized
        self._vector_ids: list[str] = []
        self._profile_vectors: dict[str, object] = {}  # profile text -> vector

    async def refresh(self, conn: asyncpg.Connection) -> None:
        rows = await conn.fetch(
            """
            SELECT g.id, g.canonical_name, g.metadata, array_agg(ga.alias) AS aliases
            FROM games g
            LEFT JOIN game_aliases ga ON ga.game_id = g.id
            GROUP BY g.id, g.canonical_name, g.metadata
            """
        )

        entries: list[GameEntry] = []
        alias_index: dict[str, str] = {}
        alias_patterns: dict[str, re.Pattern[str]] = {}

        for row in rows:
            aliases = [a for a in (row["aliases"] or []) if a]
            metadata = row["metadata"] or {}
            if isinstance(metadata, str):  # asyncpg returns JSONB as str by default
                metadata = json.loads(metadata)
            entries.append(
                GameEntry(
                    game_id=row["id"],
                    canonical_name=row["canonical_name"],
                    aliases=aliases,
                    description=str(metadata.get("description", "")),
                )
            )
            for name in [row["canonical_name"], *aliases]:
                key = name.lower().strip()
                if not key:
                    continue
                alias_index[key] = row["id"]
                alias_patterns[key] = re.compile(
                    r"(?<!\w)" + re.escape(key) + r"(?!\w)", re.IGNORECASE
                )

        self._entries = entries
        self._alias_index = alias_index
        self._alias_patterns = alias_patterns
        self._last_refresh = time.monotonic()
        log.info("games cache refreshed", games=len(entries), aliases=len(alias_index))

    async def build_vector_index(self) -> None:
        """Embed every game profile. Only profiles not seen before are encoded,
        so the periodic refresh is nearly free when nothing changed."""
        import numpy as np

        from enricher.activities.embeddings import encode_texts

        entries = self._entries
        profiles = [e.profile() for e in entries]
        missing = [p for p in dict.fromkeys(profiles) if p not in self._profile_vectors]
        if missing:
            encoded = await asyncio.to_thread(encode_texts, missing)
            for text, vec in zip(missing, encoded):
                self._profile_vectors[text] = vec
        live = set(profiles)
        self._profile_vectors = {k: v for k, v in self._profile_vectors.items() if k in live}

        self._vector_ids = [e.game_id for e in entries]
        self._vectors = (
            np.stack([self._profile_vectors[p] for p in profiles]) if profiles else None
        )
        log.info("game vector index built", games=len(entries), newly_encoded=len(missing))

    def vector_lookup(
        self, query_vec, top_k: int, min_sim: float
    ) -> list[tuple[str, float]]:
        """Nearest games to a normalized query vector, by cosine similarity."""
        if self._vectors is None or not len(self._vector_ids):
            return []
        sims = self._vectors @ query_vec  # both L2-normalized -> cosine
        order = sims.argsort()[::-1][:top_k]
        return [
            (self._vector_ids[i], float(sims[i])) for i in order if sims[i] >= min_sim
        ]

    @property
    def has_vector_index(self) -> bool:
        return self._vectors is not None

    def lookup(self, text: str) -> list[tuple[str, float]]:
        """Return (game_id, score) candidates, best first.

        Matching is on token boundaries, not raw substrings: a plain
        `"inside" in text` check fires on "insidergaming", "outside", and
        "inside the studio", which is how naive alias matching produces its
        worst false positives.

        The score is a heuristic specificity proxy — longer, more distinctive
        alias strings are less likely to be coincidental. Short aliases are
        deliberately capped below the high-confidence threshold so they always
        route to LLM adjudication rather than being trusted outright.
        """
        results: dict[str, float] = {}

        for alias, pattern in self._alias_patterns.items():
            if not pattern.search(text):
                continue

            game_id = self._alias_index[alias]
            # Longer aliases are more specific; saturates at 1.0 around 20 chars.
            score = min(1.0, 0.5 + len(alias) / 20.0)
            if len(alias) <= AMBIGUOUS_ALIAS_MAX_LEN:
                # Force short/common aliases through disambiguation.
                score = min(score, 0.75)

            if score > results.get(game_id, 0.0):
                results[game_id] = score

        return sorted(results.items(), key=lambda kv: (-kv[1], kv[0]))

    def get_candidates(self, game_ids: list[str]) -> list[GameEntry]:
        wanted = set(game_ids)
        return [e for e in self._entries if e.game_id in wanted]

    def get(self, game_id: str) -> GameEntry | None:
        for entry in self._entries:
            if entry.game_id == game_id:
                return entry
        return None

    @property
    def size(self) -> int:
        return len(self._entries)

    @property
    def alias_count(self) -> int:
        """How many alias strings are indexed, canonical names included.

        Logged alongside a no-match drop: a suspiciously low count is the
        signature of a seeding failure rather than genuinely unmatched text.
        """
        return len(self._alias_index)
