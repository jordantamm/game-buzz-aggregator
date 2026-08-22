"""Alias matching tests.

These are the false positives that quietly poison a mention database, so they
get explicit coverage: a substring hit inside a longer word is not a mention.
"""

from __future__ import annotations

import pytest

from enricher.caches.games import AMBIGUOUS_ALIAS_MAX_LEN, GameCache, GameEntry


@pytest.fixture
def cache() -> GameCache:
    c = GameCache("postgresql://unused")
    entries = [
        GameEntry("elden-ring", "Elden Ring", ["eldenring", "er", "elden"]),
        GameEntry("control", "Control", []),
        GameEntry("baldurs-gate-3", "Baldur's Gate 3", ["bg3", "baldurs gate 3"]),
    ]
    # Populate the index the same way refresh() does, without touching Postgres.
    c._entries = entries
    for entry in entries:
        for name in [entry.canonical_name, *entry.aliases]:
            key = name.lower().strip()
            c._alias_index[key] = entry.game_id
            import re

            c._alias_patterns[key] = re.compile(
                r"(?<!\w)" + re.escape(key) + r"(?!\w)", re.IGNORECASE
            )
    return c


def test_matches_alias_on_word_boundary(cache: GameCache):
    results = dict(cache.lookup("Just finished Elden Ring, what a game"))
    assert "elden-ring" in results


def test_does_not_match_substring_inside_longer_word(cache: GameCache):
    # "control" appears inside "controller" — the classic naive-substring bug.
    results = dict(cache.lookup("my controller keeps disconnecting"))
    assert "control" not in results


def test_matches_standalone_common_word(cache: GameCache):
    results = dict(cache.lookup("Control is a great game"))
    assert "control" in results


def test_short_alias_is_capped_below_high_confidence(cache: GameCache):
    # "er" is a real alias but far too generic to trust without adjudication.
    results = dict(cache.lookup("er was fantastic"))
    assert results["elden-ring"] <= 0.75, (
        "short aliases must stay below the high-confidence threshold so they "
        "route to LLM disambiguation"
    )
    assert len("er") <= AMBIGUOUS_ALIAS_MAX_LEN


def test_case_insensitive(cache: GameCache):
    assert "baldurs-gate-3" in dict(cache.lookup("BG3 is incredible"))


def test_apostrophes_and_punctuation_survive_escaping(cache: GameCache):
    assert "baldurs-gate-3" in dict(cache.lookup("playing Baldur's Gate 3 tonight"))


def test_no_match_returns_empty(cache: GameCache):
    assert cache.lookup("thoughts on the new patch notes?") == []


def test_results_sorted_by_score_descending(cache: GameCache):
    results = cache.lookup("Elden Ring and Control are both great")
    scores = [score for _, score in results]
    assert scores == sorted(scores, reverse=True)


def test_dedupes_to_best_score_per_game(cache: GameCache):
    # Both "Elden Ring" and "elden" match; only one entry should come back.
    results = cache.lookup("elden and Elden Ring")
    ids = [game_id for game_id, _ in results]
    assert ids.count("elden-ring") == 1
