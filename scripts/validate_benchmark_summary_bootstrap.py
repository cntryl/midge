#!/usr/bin/env python3
"""Accept the benchmark summarizer's initial no-baseline report only."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()

    try:
        manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
        summary = manifest["comparison_summary"]
        baseline_available = summary["baseline_available"]
        critical = summary["critical"]
        new = summary["new"]
        missing = summary["missing"]
        if not isinstance(baseline_available, bool):
            raise ValueError("baseline_available must be a boolean")
        if not all(isinstance(value, int) and value >= 0 for value in (critical, new, missing)):
            raise ValueError("critical, new, and missing must be non-negative integers")
    except (KeyError, json.JSONDecodeError, OSError, TypeError, ValueError) as error:
        print(f"invalid benchmark summary: {error}", file=sys.stderr)
        return 2

    if baseline_available or critical != 0 or missing != 0 or new == 0:
        print(
            "benchmark summary failure is not an initial no-baseline report: "
            f"baseline_available={baseline_available} critical={critical} "
            f"new={new} missing={missing}",
            file=sys.stderr,
        )
        return 1

    print(f"Accepted initial benchmark report with {new} new measurements and no baseline.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
