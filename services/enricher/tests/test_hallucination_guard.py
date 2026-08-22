"""The post-hoc candidate check in `_llm_disambiguate`.

game_id is a foreign key into `games`. A hallucinated slug that happens to be a
valid string would pass Instructor's schema validation and then fail at INSERT
time — or worse, collide with a real game. It has to be filtered before it
leaves the activity.
"""

from __future__ import annotations

import pytest

from enricher.activities import resolve
from enricher.caches.games import GameEntry
from enricher.schemas import DisambiguationResult, LLMGameMatch

CANDIDATES = [
    GameEntry("elden-ring", "Elden Ring", ["eldenring"]),
    GameEntry("control", "Control", []),
]


class _FakeMessages:
    def __init__(self, result: DisambiguationResult):
        self._result = result

    async def create(self, **_kwargs) -> DisambiguationResult:
        return self._result


class _FakeClient:
    def __init__(self, result: DisambiguationResult):
        self.messages = _FakeMessages(result)


@pytest.fixture
def with_api_key(monkeypatch):
    monkeypatch.setattr(resolve.settings, "anthropic_api_key", "test-key")


def _patch_client(monkeypatch, result: DisambiguationResult) -> None:
    monkeypatch.setattr(resolve, "_get_instructor_client", lambda: _FakeClient(result))


async def test_hallucinated_game_id_is_discarded(monkeypatch, with_api_key):
    _patch_client(
        monkeypatch,
        DisambiguationResult(
            matches=[
                LLMGameMatch(game_id="elden-ring", confidence=0.95, rationale="named directly"),
                LLMGameMatch(
                    game_id="skyrim",  # never offered as a candidate
                    confidence=0.9,
                    rationale="invented",
                ),
            ]
        ),
    )

    matches = await resolve._llm_disambiguate("some text", CANDIDATES)

    assert [m.game_id for m in matches] == ["elden-ring"]


async def test_valid_matches_pass_through_with_llm_method(monkeypatch, with_api_key):
    _patch_client(
        monkeypatch,
        DisambiguationResult(
            matches=[LLMGameMatch(game_id="control", confidence=0.8, rationale="clearly the game")]
        ),
    )

    matches = await resolve._llm_disambiguate("text", CANDIDATES)

    assert len(matches) == 1
    assert matches[0].game_id == "control"
    assert matches[0].method == "llm"
    assert matches[0].confidence == pytest.approx(0.8)


async def test_empty_result_is_respected(monkeypatch, with_api_key):
    """An empty answer must survive as empty, not be back-filled with a guess."""
    _patch_client(monkeypatch, DisambiguationResult(matches=[]))
    assert await resolve._llm_disambiguate("unrelated text", CANDIDATES) == []


async def test_no_api_key_returns_no_match(monkeypatch):
    monkeypatch.setattr(resolve.settings, "anthropic_api_key", "")
    assert await resolve._llm_disambiguate("text", CANDIDATES) == []


async def test_llm_exception_degrades_to_no_match(monkeypatch, with_api_key):
    """A model outage must drop the mention, never crash enrichment."""

    class _Boom:
        class messages:
            @staticmethod
            async def create(**_kwargs):
                raise RuntimeError("anthropic is down")

    monkeypatch.setattr(resolve, "_get_instructor_client", lambda: _Boom())
    assert await resolve._llm_disambiguate("text", CANDIDATES) == []
