#!/usr/bin/env python3
"""Validate documented and automated benchmark targets against Cargo metadata."""

from __future__ import annotations

import fnmatch
import json
from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
BENCH_COMMAND = re.compile(r"cargo\s+bench\s+--bench\s+['\"`]?([A-Za-z0-9_.*-]+)")


def cargo_benchmark_names() -> set[str]:
    completed = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    metadata = json.loads(completed.stdout)
    manifest = str((ROOT / "Cargo.toml").resolve())
    package = next(
        package
        for package in metadata["packages"]
        if str(Path(package["manifest_path"]).resolve()) == manifest
    )
    return {
        target["name"]
        for target in package["targets"]
        if "bench" in target["kind"]
    }


def command_targets(path: Path) -> list[str]:
    return BENCH_COMMAND.findall(path.read_text(encoding="utf-8"))


def validate() -> list[str]:
    registered = cargo_benchmark_names()
    failures: list[str] = []

    documentation = [
        ROOT / "docs/development/benchmarks.md",
        ROOT / "docs/development/performance-targets.md",
        ROOT / "docs/operations/performance-tuning.md",
    ]
    for path in documentation:
        document = path.read_text(encoding="utf-8")
        for target in BENCH_COMMAND.findall(document):
            if target not in registered:
                failures.append(
                    f"{path.relative_to(ROOT)} advertises unregistered benchmark {target}"
                )
        if "--save-baseline" in document or "--baseline" in document:
            failures.append(
                f"{path.relative_to(ROOT)} advertises obsolete Criterion baseline flags"
            )

    workflow = ROOT / ".github/workflows/bench.yml"
    patterns = command_targets(workflow)
    for target in sorted(registered):
        if not any(fnmatch.fnmatchcase(target, pattern) for pattern in patterns):
            failures.append(f"benchmark workflow does not execute {target}")

    benchmark_docs = documentation[0].read_text(encoding="utf-8")
    workflow_text = workflow.read_text(encoding="utf-8")
    if "workflow_dispatch:" not in workflow_text:
        failures.append("benchmark workflow has no documented manual trigger")
    if "pull_request:" in workflow_text:
        failures.append("benchmark docs must be updated before enabling a PR trigger")
    if not re.search(r"not a\s+pull-request performance gate", benchmark_docs):
        failures.append("benchmark docs do not state the current non-PR-gate contract")

    return failures


def main() -> int:
    failures = validate()
    if failures:
        for failure in failures:
            print(f"error: {failure}", file=sys.stderr)
        return 1

    print("benchmark contract matches Cargo metadata and workflow coverage")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
