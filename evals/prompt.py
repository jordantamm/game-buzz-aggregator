"""Prompt builder for the promptfoo suite.

This deliberately imports the *production* prompt builder from the enricher
rather than restating the template. An eval that keeps its own copy of the
prompt stops testing production the moment someone edits one and not the other,
and it will keep passing while it does — the worst failure mode a gate can have.

If the enricher package is not importable (no dependencies installed), this
raises rather than falling back to a copy. A skipped eval must look like a
broken eval, not a passing one.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ENRICHER_SRC = Path(__file__).resolve().parents[1] / "services" / "enricher" / "src"
if str(_ENRICHER_SRC) not in sys.path:
    sys.path.insert(0, str(_ENRICHER_SRC))

from enricher.activities.resolve import build_disambiguation_prompt  # noqa: E402
from enricher.caches.games import GameEntry  # noqa: E402


def build_prompt(context: dict) -> str:
    """Render the user turn for one eval case.

    promptfoo passes {"vars": {...}}; the system prompt is supplied separately
    via the provider config so it matches how production sends it.
    """
    variables = context["vars"]
    candidates = [
        GameEntry(
            game_id=c["id"],
            canonical_name=c["name"],
            aliases=list(c.get("aliases") or []),
        )
        for c in variables["candidates"]
    ]
    return build_disambiguation_prompt(variables["text"], candidates)
