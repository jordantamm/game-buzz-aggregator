"""Tests for the LLM disambiguation contract.

The point of these is that the Instructor schema and the post-hoc candidate
check are two *different* guarantees. The schema guarantees the model returned
well-formed data; only the candidate check guarantees it returned a game that
actually exists. A hallucinated-but-well-formed slug passes the first and must
fail the second.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from enricher.caches.games import GameEntry
from enricher.schemas import DisambiguationResult, GameMatch, LLMGameMatch
from enricher.activities.resolve import build_disambiguation_prompt


# ── Schema enforcement ───────────────────────────────────────────────────────


def test_empty_matches_is_valid():
    """'None of these games' must be expressible, or the model will guess."""
    result = DisambiguationResult(matches=[])
    assert result.matches == []


def test_confidence_out_of_range_rejected():
    with pytest.raises(ValidationError):
        LLMGameMatch(game_id="elden-ring", confidence=1.4, rationale="too confident")
    with pytest.raises(ValidationError):
        LLMGameMatch(game_id="elden-ring", confidence=-0.1, rationale="negative")


def test_rationale_is_required():
    with pytest.raises(ValidationError):
        LLMGameMatch(game_id="elden-ring", confidence=0.9)


def test_duplicate_game_ids_rejected():
    with pytest.raises(ValidationError):
        DisambiguationResult(
            matches=[
                LLMGameMatch(game_id="elden-ring", confidence=0.9, rationale="a"),
                LLMGameMatch(game_id="elden-ring", confidence=0.7, rationale="b"),
            ]
        )


def test_invalid_method_rejected():
    with pytest.raises(ValidationError):
        GameMatch(game_id="elden-ring", confidence=0.9, method="vibes")


def test_valid_methods_accepted():
    for method in ("exact", "trigram", "llm"):
        assert GameMatch(game_id="g", confidence=0.5, method=method).method == method


# ── Prompt construction ──────────────────────────────────────────────────────


def test_prompt_lists_every_candidate_id_verbatim():
    candidates = [
        GameEntry("elden-ring", "Elden Ring", ["eldenring"]),
        GameEntry("control", "Control", []),
    ]
    prompt = build_disambiguation_prompt("some post text", candidates)

    for candidate in candidates:
        assert candidate.game_id in prompt
        assert candidate.canonical_name in prompt


def test_prompt_includes_post_text():
    prompt = build_disambiguation_prompt("my controller broke", [GameEntry("control", "Control")])
    assert "my controller broke" in prompt


def test_prompt_is_stable_for_same_input():
    """The eval suite pins on this prompt; nondeterminism would make it useless."""
    candidates = [GameEntry("control", "Control", ["the control game"])]
    assert build_disambiguation_prompt("text", candidates) == build_disambiguation_prompt(
        "text", candidates
    )
