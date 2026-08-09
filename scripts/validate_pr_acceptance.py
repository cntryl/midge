#!/usr/bin/env python3
"""Require explicit, checked acceptance evidence in a pull-request body."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


REQUIRED_HEADINGS = ("Linked issues", "Acceptance audit", "Verification")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("body", type=Path)
    args = parser.parse_args()
    body = args.body.read_text(encoding="utf-8")
    errors: list[str] = []

    for heading in REQUIRED_HEADINGS:
        if not re.search(rf"(?im)^##+\s+{re.escape(heading)}\s*$", body):
            errors.append(f"missing heading: {heading}")
    if re.search(r"(?m)^\s*- \[ \]", body):
        errors.append("unchecked acceptance or review item remains")
    criteria = re.findall(r"(?im)^\s*- \[x\]\s+Criterion:\s*(.+)$", body)
    if not criteria:
        errors.append("add at least one checked `Criterion:` item")
    if not re.search(r"(?im)^\s+Evidence:\s*\S", body):
        errors.append("acceptance audit needs indented `Evidence:`")
    if not re.search(r"(?im)^\s+Production entry point:\s*\S", body):
        errors.append("acceptance audit needs indented `Production entry point:`")
    if not re.search(r"(?im)^\s+Resolution:\s*\S", body):
        errors.append("acceptance audit needs indented `Resolution:`")
    if not re.search(r"(?im)^\s*(Closes|Fixes)\s+#\d+\b", body) and not re.search(
        r"(?im)^\s*No issue:\s*\S", body
    ):
        errors.append("link an issue with `Closes #N` or explain `No issue:`")

    if errors:
        print("PR acceptance contract failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
