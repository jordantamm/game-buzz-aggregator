"""Vector candidate retrieval and merging, without loading the real model."""

from __future__ import annotations

import numpy as np

from enricher.activities import resolve
from enricher.caches.games import GameCache, GameEntry


def _cache() -> GameCache:
    c = GameCache("postgresql://unused")
    c._entries = [GameEntry("a", "A"), GameEntry("b", "B"), GameEntry("c", "C")]
    c._vector_ids = ["a", "b", "c"]
    c._vectors = np.array([[1, 0], [0.8, 0.6], [0, 1]], dtype="float32")
    return c


def test_vector_lookup_ranks_by_cosine_and_applies_floor():
    hits = _cache().vector_lookup(np.array([1, 0], dtype="float32"), top_k=3, min_sim=0.5)
    assert [g for g, _ in hits] == ["a", "b"]  # "c" is orthogonal, below floor


def test_merge_puts_alias_hits_first_and_dedupes():
    merged = resolve._merge_candidates([("x", 0.7)], [("y", 0.6), ("x", 0.4)])
    assert merged == [("x", 0.7), ("y", 0.6)]


def test_merge_caps_at_max_candidates():
    vec = [(f"g{i}", 0.5) for i in range(20)]
    assert len(resolve._merge_candidates([], vec)) == resolve.MAX_CANDIDATES


def test_accept_vector_requires_similarity_and_gap(monkeypatch):
    log = resolve.log
    assert resolve._accept_vector([("a", 0.8), ("b", 0.3)], log)[0]["method"] == "vector"
    assert resolve._accept_vector([("a", 0.8), ("b", 0.79)], log) == []  # too close
    assert resolve._accept_vector([("a", 0.2)], log) == []  # too weak
    assert resolve._accept_vector([], log) == []
