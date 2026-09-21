"""Entity resolution: map free-text mention content to canonical game IDs.

Two-stage design. Stage one is a cheap local trigram/alias scan. Stage two — an
LLM adjudication — runs only when stage one is ambiguous, because it is the
expensive path and the one that can corrupt data if it misfires.

The LLM call is the only place in this pipeline where a model's output is
written to Postgres, so it is constrained three ways:

1. `instructor` forces the response into the `DisambiguationResult` Pydantic
   schema, retrying automatically on schema-invalid output. Nothing downstream
   ever parses prose.
2. Every returned `game_id` is checked against the candidate set that was sent.
   The schema guarantees shape, not truthfulness — a well-formed hallucinated
   slug would still pass validation, so it is rejected here.
3. Results are cached in Redis keyed by (normalized text, candidate set), so a
   repeated post costs nothing and stays consistent.

The prompt built by `build_disambiguation_prompt` is also the artifact under
test in evals/promptfooconfig.yaml — keep the two in sync when editing.
"""

from __future__ import annotations

import structlog
from temporalio import activity

from enricher import runtime
from enricher.caches.games import GameEntry
from enricher.schemas import DisambiguationResult, GameMatch
from enricher.settings import settings

log = structlog.get_logger()

# Above this trigram score, with this much daylight to the runner-up, the match
# is unambiguous and the LLM is skipped entirely.
HIGH_CONFIDENCE_SCORE = 0.9
HIGH_CONFIDENCE_GAP = 0.2

# How many candidates to show the model. More than this and the prompt starts
# hurting precision without improving recall.
MAX_CANDIDATES = 5

SYSTEM_PROMPT = """You are an entity resolver for video game discussions on Reddit.

You are given a post and a list of candidate games whose names or aliases appear
somewhere in the post's text. Your job is to decide which candidates, if any, the
post is ACTUALLY DISCUSSING.

Rules:
- A game is only a match if the post is about that game, or meaningfully discusses
  it. A passing comparison ("it's like Dark Souls but easier") IS a mention of the
  compared game.
- A coincidental word collision is NOT a match. Common words that happen to be game
  titles ("Control", "Journey", "Limbo", "Inside", "Prey") are only matches when the
  surrounding context is clearly about the game.
- A post may discuss multiple games. Return all of them.
- A post may discuss none of the candidates. Returning an empty list is correct and
  expected in that case. Never pick the closest candidate just to avoid an empty
  answer.
- Only ever return a game_id that appears verbatim in the candidate list.
- Set confidence below 0.5 when the post only mentions the game in passing."""


def build_disambiguation_prompt(text: str, candidates: list[GameEntry]) -> str:
    """Render the user-turn prompt. Shared with the promptfoo eval suite."""
    lines = []
    for candidate in candidates:
        alias_hint = ""
        if candidate.aliases:
            alias_hint = f" (also known as: {', '.join(candidate.aliases[:4])})"
        lines.append(f"- {candidate.game_id}: {candidate.canonical_name}{alias_hint}")
    candidate_block = "\n".join(lines)

    return (
        f"Candidate games:\n{candidate_block}\n\n"
        f"Reddit post:\n\"\"\"\n{text}\n\"\"\"\n\n"
        "Which candidate games is this post actually discussing?"
    )


# Log previews are truncated: mention text is unbounded, and a log pipeline is
# not the place to carry a full Reddit post.
PREVIEW_CHARS = 120


def _preview(text: str) -> str:
    collapsed = " ".join(text.split())
    if len(collapsed) <= PREVIEW_CHARS:
        return collapsed
    return collapsed[:PREVIEW_CHARS] + "..."


@activity.defn
async def resolve_games(text: str, mention_id: str = "") -> list[dict]:
    """Resolve mention text to canonical games.

    Returns a list of GameMatch dicts (JSON-serializable for Temporal).
    An empty list means no game matched, and the mention is dropped.

    Every exit path logs either a `mention matched` or a `mention dropped`
    event carrying the reason. A drop is a normal, non-error outcome here, so
    nothing else in the pipeline records it — without these lines a mention
    vanishing is indistinguishable from one that never arrived.
    """
    rt = runtime.get()
    assert rt.game_cache is not None and rt.disambiguation_cache is not None

    bound = log.bind(mention_id=mention_id, text_preview=_preview(text))

    candidates = rt.game_cache.lookup(text)

    # Stage one found nothing. The LLM is never consulted here: it only ever
    # chooses *among* candidates, so no candidates means no question to ask.
    if not candidates:
        bound.info(
            "mention dropped",
            reason="no_alias_match",
            stage="alias_scan",
            candidate_count=0,
            games_in_cache=rt.game_cache.size,
            aliases_in_cache=rt.game_cache.alias_count,
        )
        return []

    best_id, best_score = candidates[0]
    runner_up = candidates[1][1] if len(candidates) > 1 else 0.0
    gap = best_score - runner_up
    scored = [{"game_id": g, "score": round(s, 3)} for g, s in candidates[:MAX_CANDIDATES]]

    # Unambiguous: one strong match, clearly ahead of anything else.
    if best_score >= HIGH_CONFIDENCE_SCORE and gap > HIGH_CONFIDENCE_GAP:
        bound.info(
            "mention matched",
            game_id=best_id,
            confidence=round(best_score, 3),
            method="trigram",
            stage="alias_scan",
            runner_up_score=round(runner_up, 3),
            gap=round(gap, 3),
            candidates=scored,
        )
        return [GameMatch(game_id=best_id, confidence=best_score, method="trigram").model_dump()]

    # Ambiguous. Record which of the two thresholds sent this to adjudication —
    # "score too low" and "runner-up too close" are different tuning problems.
    ambiguity = (
        "best_score_below_threshold"
        if best_score < HIGH_CONFIDENCE_SCORE
        else "gap_to_runner_up_too_small"
    )
    top_ids = [game_id for game_id, _ in candidates[:MAX_CANDIDATES]]
    bound.info(
        "ambiguous alias match; routing to llm adjudication",
        stage="alias_scan",
        ambiguity=ambiguity,
        best_game_id=best_id,
        best_score=round(best_score, 3),
        score_threshold=HIGH_CONFIDENCE_SCORE,
        gap=round(gap, 3),
        required_gap=HIGH_CONFIDENCE_GAP,
        candidates=scored,
    )

    cached = await rt.disambiguation_cache.get(text, top_ids)
    if cached is not None:
        if cached:
            bound.info(
                "mention matched",
                stage="disambiguation_cache",
                method="llm",
                cache_hit=True,
                game_ids=[m["game_id"] for m in cached],
                matches=cached,
            )
        else:
            bound.info(
                "mention dropped",
                reason="cached_no_match",
                stage="disambiguation_cache",
                cache_hit=True,
                candidates=scored,
            )
        return cached

    entries = rt.game_cache.get_candidates(top_ids)
    matches = await _llm_disambiguate(text, entries, bound)
    payload = [m.model_dump() for m in matches]

    # A drop here is logged inside _llm_disambiguate, where the reason is known.
    if payload:
        bound.info(
            "mention matched",
            stage="llm",
            method="llm",
            game_ids=[m["game_id"] for m in payload],
            matches=payload,
        )

    await rt.disambiguation_cache.set(text, top_ids, payload)
    return payload


async def _llm_disambiguate(
    text: str, candidates: list[GameEntry], bound=None
) -> list[GameMatch]:
    """Ask the model which candidates the post is about, with a typed response.

    Returns an empty list on any failure. Dropping an ambiguous mention is the
    safe direction to fail: a missing row is recoverable by reprocessing, a row
    attributed to the wrong game silently corrupts every downstream aggregate.
    """
    bound = bound or log
    candidate_ids = [c.game_id for c in candidates]

    if not settings.anthropic_api_key:
        # Documented degradation: without a key, ambiguous mentions resolve to
        # nothing rather than guessing. See CLAUDE.md.
        bound.info(
            "mention dropped",
            reason="llm_disabled_no_api_key",
            stage="llm",
            candidates=candidate_ids,
            hint="set ANTHROPIC_API_KEY to enable adjudication of ambiguous matches",
        )
        return []

    valid_ids = {c.game_id for c in candidates}

    try:
        client = _get_instructor_client()
        result: DisambiguationResult = await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": build_disambiguation_prompt(text, candidates)}],
            response_model=DisambiguationResult,
            max_retries=settings.llm_max_retries,
        )
    except Exception as exc:  # noqa: BLE001 - never let a model failure kill enrichment
        bound.warning(
            "mention dropped",
            reason="llm_call_failed",
            stage="llm",
            error=str(exc),
            candidates=candidate_ids,
        )
        return []

    matches: list[GameMatch] = []
    rejected: list[str] = []
    for match in result.matches:
        # Schema validation guarantees shape, not truthfulness. A hallucinated
        # but well-formed slug would pass Pydantic; it must not reach Postgres,
        # where game_id is a foreign key into `games`.
        if match.game_id not in valid_ids:
            rejected.append(match.game_id)
            bound.warning(
                "llm returned game_id outside candidate set; discarding",
                reason="hallucinated_game_id",
                stage="llm",
                game_id=match.game_id,
                rationale=match.rationale,
                candidates=sorted(valid_ids),
            )
            continue
        bound.debug(
            "llm adjudicated match",
            game_id=match.game_id,
            confidence=round(match.confidence, 3),
            rationale=match.rationale,
        )
        matches.append(
            GameMatch(game_id=match.game_id, confidence=match.confidence, method="llm")
        )

    if not matches:
        # Separate the two empty outcomes: the model declining to match (the
        # designed behaviour for a coincidental word collision) and the model
        # naming only IDs that failed validation (a prompt/eval regression).
        bound.info(
            "mention dropped",
            reason="llm_ids_all_rejected" if rejected else "llm_returned_no_match",
            stage="llm",
            candidates=candidate_ids,
            rejected_ids=rejected or None,
        )

    return matches


_client = None


def _get_instructor_client():
    """Lazily build the Instructor-patched Anthropic client.

    Built on first use rather than at import so the module stays importable
    (and unit-testable) without an API key present.
    """
    global _client
    if _client is None:
        import anthropic
        import instructor

        _client = instructor.from_anthropic(
            anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key),
            mode=instructor.Mode.ANTHROPIC_TOOLS,
        )
    return _client
