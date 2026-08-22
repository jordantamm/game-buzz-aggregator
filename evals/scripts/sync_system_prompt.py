"""Write evals/system_prompt.txt from the enricher's production SYSTEM_PROMPT.

promptfoo's provider config can reference a file but cannot call Python, so the
system prompt has to exist as a file. Generating it from the module — rather
than maintaining a second copy — is what keeps the eval honest: a suite testing
a stale copy of the prompt passes while production regresses.

`make eval` runs this first. CI runs it with --check, which fails if the
committed file has drifted from the module.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "enricher" / "src"))

from enricher.activities.resolve import SYSTEM_PROMPT  # noqa: E402

TARGET = ROOT / "evals" / "system_prompt.txt"

HEADER = (
    "# GENERATED FILE - DO NOT EDIT.\n"
    "# Source: services/enricher/src/enricher/activities/resolve.py (SYSTEM_PROMPT)\n"
    "# Regenerate: make eval  (or python evals/scripts/sync_system_prompt.py)\n"
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the committed file differs from the module.",
    )
    args = parser.parse_args()

    # The header is a comment for humans reading the repo; it must not be sent
    # to the model, so it is stripped when comparing and never written into the
    # prompt body. Keep the two files separate instead.
    content = SYSTEM_PROMPT.rstrip() + "\n"

    if args.check:
        if not TARGET.exists():
            print(f"FAIL: {TARGET.relative_to(ROOT)} does not exist. Run: make eval", file=sys.stderr)
            return 1
        current = TARGET.read_text(encoding="utf-8")
        if current != content:
            print(
                f"FAIL: {TARGET.relative_to(ROOT)} is out of sync with resolve.SYSTEM_PROMPT.\n"
                "The eval suite would test a stale prompt. Run:\n"
                "  python evals/scripts/sync_system_prompt.py",
                file=sys.stderr,
            )
            return 1
        print("system_prompt.txt is in sync")
        return 0

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(content, encoding="utf-8")
    print(f"wrote {TARGET.relative_to(ROOT)} ({len(content)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
