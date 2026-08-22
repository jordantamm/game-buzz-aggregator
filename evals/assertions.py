"""Custom promptfoo assertions for the disambiguation suite.

Each returns promptfoo's GradingResult shape: {"pass": bool, "score": float,
"reason": str}. Reasons are written to be readable in CI output — when this gate
fails, the person reading it is usually not the person who wrote the prompt.
"""

from __future__ import annotations

import json
from typing import Any


def _extract_matches(output: Any) -> tuple[list[dict], str | None]:
    """Pull the `matches` array out of whatever shape promptfoo hands back.

    The Anthropic provider may surface a forced tool call as a dict, as a list
    of content blocks, or as a JSON string depending on version and config, so
    all three are handled rather than pinning to one and breaking on upgrade.

    Returns (matches, error). error is non-None if nothing could be extracted.
    """
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            return [], f"output was not JSON and not a tool call: {output[:200]!r}"

    # A list of Anthropic content blocks.
    if isinstance(output, list):
        for block in output:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                output = block.get("input", {})
                break
        else:
            if len(output) == 1 and isinstance(output[0], dict):
                output = output[0]
            else:
                return [], f"no tool_use block in output: {str(output)[:200]!r}"

    if not isinstance(output, dict):
        return [], f"unexpected output type {type(output).__name__}"

    # Some provider versions nest the tool input one level down.
    if "matches" not in output:
        for key in ("input", "arguments", "tool_input"):
            nested = output.get(key)
            if isinstance(nested, dict) and "matches" in nested:
                output = nested
                break

    matches = output.get("matches")
    if matches is None:
        return [], f"response had no 'matches' field: {str(output)[:200]!r}"
    if not isinstance(matches, list):
        return [], f"'matches' was {type(matches).__name__}, expected a list"

    return matches, None


def assert_no_hallucinated_ids(output: Any, context: dict) -> dict:
    """Every returned game_id must come from the candidate set that was sent.

    This is the highest-weighted assertion. game_id is a foreign key into the
    `games` table — an invented slug is either a failed INSERT or, if it happens
    to collide with a real game, a silent misattribution.
    """
    matches, error = _extract_matches(output)
    if error:
        return {"pass": False, "score": 0.0, "reason": error}

    allowed = set(context["vars"]["candidate_ids"])
    returned = [m.get("game_id") for m in matches]
    hallucinated = [gid for gid in returned if gid not in allowed]

    if hallucinated:
        return {
            "pass": False,
            "score": 0.0,
            "reason": (
                f"HALLUCINATION: returned {hallucinated} which were not offered as "
                f"candidates (candidates were {sorted(allowed)})"
            ),
        }
    return {"pass": True, "score": 1.0, "reason": "all game_ids came from the candidate set"}


def assert_exact_match(output: Any, context: dict) -> dict:
    """The returned game_id set must equal the labeled set exactly.

    No partial credit. An extra game is a false attribution that pollutes that
    game's aggregates; a missing game silently loses a mention. Both are wrong
    in ways that matter downstream, so both fail.
    """
    matches, error = _extract_matches(output)
    if error:
        return {"pass": False, "score": 0.0, "reason": error}

    expected = set(context["vars"]["expected_game_ids"])
    actual = {m.get("game_id") for m in matches}

    if actual == expected:
        detail = "correctly returned no match" if not expected else f"matched {sorted(expected)}"
        return {"pass": True, "score": 1.0, "reason": detail}

    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    parts = []
    if missing:
        parts.append(f"missed {missing}")
    if extra:
        parts.append(f"falsely attributed {extra}")
    return {
        "pass": False,
        "score": 0.0,
        "reason": f"expected {sorted(expected) or 'no match'}; " + " and ".join(parts),
    }


def assert_schema_valid(output: Any, context: dict) -> dict:
    """Validate against the same Pydantic model production uses.

    Instructor enforces this at runtime; asserting it here catches a schema
    change that the tool definition in promptfooconfig.yaml did not track.
    """
    matches, error = _extract_matches(output)
    if error:
        return {"pass": False, "score": 0.0, "reason": error}

    try:
        import sys
        from pathlib import Path

        src = Path(__file__).resolve().parents[1] / "services" / "enricher" / "src"
        if str(src) not in sys.path:
            sys.path.insert(0, str(src))
        from enricher.schemas import DisambiguationResult

        DisambiguationResult.model_validate({"matches": matches})
    except Exception as exc:  # noqa: BLE001
        return {"pass": False, "score": 0.0, "reason": f"schema validation failed: {exc}"}

    return {"pass": True, "score": 1.0, "reason": "output validates against DisambiguationResult"}


def assert_confidence_calibration(output: Any, context: dict) -> dict:
    """Sanity-check confidence on a single case.

    Full calibration (mean confidence on wrong answers below mean confidence on
    right answers) is a suite-level property computed by
    scripts/check_calibration.py over the results file, not something a
    per-case assertion can see. This only catches the degenerate case of a model
    reporting maximum confidence on an answer it got wrong.
    """
    matches, error = _extract_matches(output)
    if error:
        return {"pass": False, "score": 0.0, "reason": error}

    expected = set(context["vars"]["expected_game_ids"])
    actual = {m.get("game_id") for m in matches}
    if actual == expected:
        return {"pass": True, "score": 1.0, "reason": "correct answer; calibration not applicable"}

    overconfident = [
        m for m in matches if isinstance(m.get("confidence"), (int, float)) and m["confidence"] >= 0.95
    ]
    if overconfident:
        return {
            "pass": False,
            "score": 0.0,
            "reason": (
                "wrong answer reported with >=0.95 confidence: "
                f"{[(m.get('game_id'), m.get('confidence')) for m in overconfident]}"
            ),
        }
    return {"pass": True, "score": 1.0, "reason": "wrong, but not overconfident"}
