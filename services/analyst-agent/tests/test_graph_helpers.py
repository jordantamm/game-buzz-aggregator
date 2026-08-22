"""Tests for the agent's deterministic logic.

The planning and synthesis steps need a model, so they are exercised in the
demo rather than here. The two pieces that are pure logic — deciding whether a
tool result was empty, and dropping fabricated citations — are also the two that
silently degrade the answer when they break, so they get real coverage.
"""

from __future__ import annotations

from analyst_agent.graph import _drop_unsupported_citations, _looks_empty
from analyst_agent.schemas import AnalystAnswer, MentionRef, ToolCallRecord


# ── _looks_empty ─────────────────────────────────────────────────────────────


def test_empty_results_array_detected():
    assert _looks_empty('{"query": "x", "result_count": 0, "results": []}')


def test_empty_series_detected():
    assert _looks_empty('{"game_id": "hades", "window": "24h", "series": []}')


def test_compact_json_without_spaces_detected():
    # The MCP server pretty-prints today, but a formatting change must not
    # silently turn the reflect loop off.
    assert _looks_empty('{"results":[]}')
    assert _looks_empty('{"series":[]}')


def test_tool_failure_treated_as_empty():
    assert _looks_empty("Tool call failed: connection refused")


def test_explicit_no_data_note_detected():
    assert _looks_empty('{"note": "No mentions for this game in the requested window."}')


def test_blank_output_is_empty():
    assert _looks_empty("")
    assert _looks_empty("   \n  ")


def test_populated_results_not_empty():
    payload = '{"result_count": 2, "results": [{"mention_id": "01H1"}, {"mention_id": "01H2"}]}'
    assert not _looks_empty(payload)


def test_populated_series_not_empty():
    assert not _looks_empty('{"series": [{"bucket": "2026-08-01T00:00:00Z", "mention_count": 5}]}')


# ── _drop_unsupported_citations ──────────────────────────────────────────────


def _records(*summaries: str) -> list[ToolCallRecord]:
    return [ToolCallRecord(tool="search_mentions", result_summary=s) for s in summaries]


def test_fabricated_citation_is_dropped():
    answer = AnalystAnswer(
        summary="People like it.",
        citations=[
            MentionRef(mention_id="01HREAL", excerpt="genuinely great"),
            MentionRef(mention_id="01HFAKE", excerpt="never appeared in any result"),
        ],
        confidence=0.9,
    )
    cleaned = _drop_unsupported_citations(answer, _records('{"mention_id": "01HREAL"}'))

    assert [c.mention_id for c in cleaned.citations] == ["01HREAL"]


def test_all_supported_citations_kept():
    answer = AnalystAnswer(
        summary="s",
        citations=[
            MentionRef(mention_id="01HA", excerpt="a"),
            MentionRef(mention_id="01HB", excerpt="b"),
        ],
        confidence=0.7,
    )
    cleaned = _drop_unsupported_citations(answer, _records('"01HA" and "01HB" both here'))

    assert len(cleaned.citations) == 2


def test_summary_and_confidence_untouched_when_dropping():
    answer = AnalystAnswer(
        summary="original summary",
        citations=[MentionRef(mention_id="01HFAKE", excerpt="x")],
        confidence=0.65,
    )
    cleaned = _drop_unsupported_citations(answer, _records("nothing matching"))

    assert cleaned.citations == []
    assert cleaned.summary == "original summary"
    assert cleaned.confidence == 0.65


def test_no_citations_is_a_noop():
    answer = AnalystAnswer(summary="s", citations=[], confidence=0.2)
    assert _drop_unsupported_citations(answer, _records("anything")).citations == []
