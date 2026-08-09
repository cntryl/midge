#!/usr/bin/env python3
"""Validate and summarize a bounded cargo-mutants pilot report."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def load_report(path: Path) -> dict:
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"invalid cargo-mutants report: {error}") from error
    if not isinstance(report, dict):
        raise SystemExit("invalid cargo-mutants report: root must be an object")
    return report


def count(report: dict, key: str) -> int:
    value = report.get(key)
    if not isinstance(value, int) or value < 0:
        raise SystemExit(f"invalid cargo-mutants report: {key} must be nonnegative")
    return value


def mutation_names(report: dict, summary: str) -> list[str]:
    names = []
    for outcome in report.get("outcomes", []):
        if not isinstance(outcome, dict) or outcome.get("summary") != summary:
            continue
        scenario = outcome.get("scenario")
        if isinstance(scenario, dict):
            name = scenario.get("Mutant", {}).get("name")
        else:
            name = None
        if isinstance(name, str):
            names.append(name)
    return names


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: mutation_report.py OUTCOMES_JSON")
    report = load_report(Path(sys.argv[1]))
    total = count(report, "total_mutants")
    caught = count(report, "caught")
    missed = count(report, "missed")
    timeout = count(report, "timeout")
    unviable = count(report, "unviable")
    if total < 1:
        raise SystemExit("mutation pilot produced no mutants")
    if caught + missed + timeout < 1:
        raise SystemExit("mutation pilot produced no viable mutant outcome")
    baseline_ok = any(
        outcome.get("scenario") == "Baseline" and outcome.get("summary") == "Success"
        for outcome in report.get("outcomes", [])
        if isinstance(outcome, dict)
    )
    if not baseline_ok:
        raise SystemExit("mutation pilot baseline did not succeed")

    print("## Mutation pilot")
    print()
    print(f"- Tested: {total}")
    print(f"- Caught: {caught}")
    print(f"- Survived: {missed}")
    print(f"- Timed out: {timeout}")
    print(f"- Unviable: {unviable}")
    survivors = mutation_names(report, "MissedMutant")
    if survivors:
        print("- Survivor triage required:")
        for survivor in survivors:
            print(f"  - `{survivor}`")


if __name__ == "__main__":
    main()
