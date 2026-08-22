"""Turn evals/cases/*.yaml into promptfoo test cases.

Cases live in plain YAML so that adding one is a data edit, not a code edit —
the intent is that mislabeled production mentions get pasted in as they are
found. Assertions come from defaultTest in promptfooconfig.yaml; this only
supplies vars and the expected label.
"""

from __future__ import annotations

from pathlib import Path

import yaml

CASES_DIR = Path(__file__).resolve().parent / "cases"


def generate_tests() -> list[dict]:
    tests: list[dict] = []

    for path in sorted(CASES_DIR.glob("*.yaml")):
        cases = yaml.safe_load(path.read_text(encoding="utf-8")) or []
        for index, case in enumerate(cases):
            _validate(case, path, index)
            expected = list(case["expected"])
            label = ",".join(expected) if expected else "none"
            tests.append(
                {
                    "description": f"{path.stem}[{index}] -> {label}",
                    "vars": {
                        "text": case["text"],
                        "candidates": case["candidates"],
                        # Assertions read the label back out of vars.
                        "expected_game_ids": expected,
                        "candidate_ids": [c["id"] for c in case["candidates"]],
                    },
                }
            )

    if not tests:
        raise RuntimeError(f"no eval cases found under {CASES_DIR}")
    return tests


def _validate(case: dict, path: Path, index: int) -> None:
    """Fail loudly on a malformed case.

    A case with a typo'd key would otherwise silently become a test that asserts
    nothing, quietly shrinking the gate.
    """
    where = f"{path.name}[{index}]"
    for key in ("text", "candidates", "expected"):
        if key not in case:
            raise ValueError(f"{where}: missing required key {key!r}")

    candidate_ids = {c["id"] for c in case["candidates"]}
    unknown = set(case["expected"]) - candidate_ids
    if unknown:
        raise ValueError(
            f"{where}: expected game_ids {sorted(unknown)} are not in the candidate set "
            f"{sorted(candidate_ids)} — the label is unreachable, so this case can never pass"
        )
