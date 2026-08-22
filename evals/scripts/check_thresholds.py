"""Merge gate for the disambiguation eval.

Reads promptfoo's results JSON and enforces three thresholds. Exits non-zero on
any breach, which is what fails the CI job.

Run as: python evals/scripts/check_thresholds.py evals/results/latest.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from statistics import mean

# ── Gates ────────────────────────────────────────────────────────────────────

# Exact-match rate across all cases. Set below current measured performance on
# purpose: a gate pinned at 1.0 fails on ordinary model nondeterminism and gets
# switched off within a week. Raise as the prompt improves.
MIN_EXACT_MATCH_RATE = 0.80

# Absolute. A game_id outside the candidate set is a foreign-key violation
# against `games` — there is no acceptable non-zero rate.
MAX_HALLUCINATIONS = 0

# Confidence must carry information: the model should be less sure when it is
# wrong. If it is not, `confidence` is noise and downstream consumers should
# stop weighting by it.
REQUIRE_CALIBRATION = True


def load_results(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    # promptfoo nests results differently across versions.
    if isinstance(data, dict):
        results = data.get("results")
        if isinstance(results, dict):
            return results.get("results", [])
        if isinstance(results, list):
            return results
    raise ValueError(f"unrecognized promptfoo results format in {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("results", type=Path, nargs="?", default=Path("evals/results/latest.json"))
    args = parser.parse_args()

    if not args.results.exists():
        print(f"FAIL: no results at {args.results}. Did `promptfoo eval` run?", file=sys.stderr)
        return 1

    results = load_results(args.results)
    if not results:
        print("FAIL: results file contained zero cases", file=sys.stderr)
        return 1

    total = len(results)
    exact_matches = 0
    hallucinations: list[str] = []
    confidence_when_right: list[float] = []
    confidence_when_wrong: list[float] = []

    for result in results:
        description = result.get("description") or result.get("testCase", {}).get("description", "?")
        reasons = {
            component.get("reason", "")
            for component in _assertion_components(result)
        }

        matched = any("matched" in r or "correctly returned no match" in r for r in reasons)
        if matched:
            exact_matches += 1
        if any("HALLUCINATION" in r for r in reasons):
            hallucinations.append(description)

        for confidence in _confidences(result):
            (confidence_when_right if matched else confidence_when_wrong).append(confidence)

    exact_match_rate = exact_matches / total

    print(f"cases:            {total}")
    print(f"exact matches:    {exact_matches} ({exact_match_rate:.1%})")
    print(f"hallucinations:   {len(hallucinations)}")

    failures: list[str] = []

    if exact_match_rate < MIN_EXACT_MATCH_RATE:
        failures.append(
            f"exact-match rate {exact_match_rate:.1%} is below the {MIN_EXACT_MATCH_RATE:.0%} gate"
        )

    if len(hallucinations) > MAX_HALLUCINATIONS:
        failures.append(
            f"{len(hallucinations)} case(s) returned a game_id outside the candidate set: "
            + ", ".join(hallucinations[:5])
        )

    if REQUIRE_CALIBRATION and confidence_when_right and confidence_when_wrong:
        right, wrong = mean(confidence_when_right), mean(confidence_when_wrong)
        print(f"mean confidence:  {right:.3f} when right / {wrong:.3f} when wrong")
        if wrong >= right:
            failures.append(
                f"confidence is not calibrated: mean {wrong:.3f} on wrong answers is not below "
                f"mean {right:.3f} on right answers"
            )
    elif REQUIRE_CALIBRATION:
        print("mean confidence:  calibration not computable (no wrong answers, or no scores)")

    if failures:
        print("\nEVAL GATE FAILED", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print("\nEVAL GATE PASSED")
    return 0


def _assertion_components(result: dict) -> list[dict]:
    grading = result.get("gradingResult") or {}
    return grading.get("componentResults") or []


def _confidences(result: dict) -> list[float]:
    """Pull confidence values out of the model's tool call, tolerating shapes."""
    output = result.get("response", {}).get("output", result.get("output"))
    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            return []
    if isinstance(output, list):
        for block in output:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                output = block.get("input", {})
                break
    if not isinstance(output, dict):
        return []
    matches = output.get("matches")
    if not isinstance(matches, list):
        return []
    return [
        float(m["confidence"])
        for m in matches
        if isinstance(m, dict) and isinstance(m.get("confidence"), (int, float))
    ]


if __name__ == "__main__":
    raise SystemExit(main())
