#!/usr/bin/env python3
"""Compare one benchmark metric across base and candidate stress reports."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path


def metric(path: Path, benchmark: str, field: str) -> float:
    report = json.loads(path.read_text(encoding="utf-8"))
    matches = [row for row in report["summaries"] if row["name"] == benchmark]
    if len(matches) != 1:
        raise ValueError(f"{path}: expected exactly one {benchmark!r} summary")
    value = matches[0]["stats"][field]
    if not isinstance(value, (int, float)) or value <= 0:
        raise ValueError(f"{path}: {benchmark}.{field} must be positive")
    return float(value)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", action="append", required=True, type=Path)
    parser.add_argument("--candidate", action="append", required=True, type=Path)
    parser.add_argument("--benchmark", required=True)
    parser.add_argument("--metric", default="median")
    parser.add_argument("--max-regression", required=True, type=float)
    args = parser.parse_args()

    if not 0 < args.max_regression < 1:
        parser.error("--max-regression must be between zero and one")
    if len(args.base) < 3 or len(args.candidate) < 3:
        parser.error("at least three base and candidate reports are required")

    try:
        base = statistics.median(
            metric(path, args.benchmark, args.metric) for path in args.base
        )
        candidate = statistics.median(
            metric(path, args.benchmark, args.metric) for path in args.candidate
        )
    except (KeyError, json.JSONDecodeError, OSError, ValueError) as error:
        print(f"invalid benchmark evidence: {error}", file=sys.stderr)
        return 2

    change = candidate / base - 1
    print(f"base median: {base:.2f}")
    print(f"candidate median: {candidate:.2f}")
    print(f"change: {change:+.2%}")
    if change < -args.max_regression - 1e-12:
        print(
            f"regression exceeds allowed {args.max_regression:.0%}", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
