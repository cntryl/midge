#!/usr/bin/env python3
"""Run Rust tests one-by-one with timeouts for hang visibility.

Why:
- `cargo test` can appear to "run forever" when a single test hangs.
- Rust's built-in harness doesn't reliably print per-test progress unless the
  test completes and reports `ok`/`FAILED`.

This script:
- lists tests in a suite (or all integration test suites)
- runs them one at a time with `--exact` + `--nocapture`
- enforces a per-test timeout and reports the last test started

Examples:
  python scripts/test_watchdog.py --suite column_families --timeout 30
  python scripts/test_watchdog.py --timeout 60 --pattern cloud
  python scripts/test_watchdog.py --all --timeout 45

Exit codes:
- 0: all executed tests passed within timeout
- 2: a test failed
- 124: a test timed out
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


RE_TEST_LIST_LINE = re.compile(r"^(?P<name>.+):\s+test\s*$")


@dataclass(frozen=True)
class TestRef:
    suite: str
    name: str


def repo_root() -> Path:
    # scripts/test_watchdog.py -> scripts -> repo root
    return Path(__file__).resolve().parent.parent


def run(cmd: list[str], *, timeout_s: float | None = None) -> subprocess.CompletedProcess:
    # Keep output streaming to the console for visibility.
    # On Windows, `timeout` in subprocess.run is reliable enough for our purpose.
    return subprocess.run(
        cmd,
        cwd=str(repo_root()),
        env=os.environ.copy(),
        text=True,
        timeout=timeout_s,
        check=False,
    )


def list_suites_from_tests_dir() -> list[str]:
    tests_dir = repo_root() / "tests"
    suites: list[str] = []
    for p in sorted(tests_dir.glob("*.rs")):
        suites.append(p.stem)
    return suites


def list_tests_for_suite(suite: str) -> list[str]:
    # `-- --list` prints names like:
    #   foo: test
    #   module::bar: test
    proc = subprocess.run(
        ["cargo", "test", "--test", suite, "--", "--list"],
        cwd=str(repo_root()),
        env=os.environ.copy(),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )

    if proc.returncode != 0:
        # Surface cargo errors directly.
        sys.stdout.write(proc.stdout)
        raise RuntimeError(f"Failed to list tests for suite '{suite}' (exit {proc.returncode})")

    out: list[str] = []
    for line in proc.stdout.splitlines():
        m = RE_TEST_LIST_LINE.match(line.strip())
        if m:
            out.append(m.group("name"))
    return out


def run_single_test(t: TestRef, timeout_s: float) -> int:
    start = time.time()
    print(f"\n=== RUN {t.suite}::{t.name} (timeout {timeout_s:.0f}s) ===", flush=True)

    cmd = [
        "cargo",
        "test",
        "--test",
        t.suite,
        t.name,
        "--",
        "--exact",
        "--nocapture",
        "--test-threads=1",
    ]

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(repo_root()),
            env=os.environ.copy(),
            text=True,
            timeout=timeout_s,
            check=False,
        )
        elapsed = time.time() - start
        print(f"=== DONE {t.suite}::{t.name} exit={proc.returncode} elapsed={elapsed:.2f}s ===")
        return proc.returncode
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        print(f"=== TIMEOUT {t.suite}::{t.name} elapsed={elapsed:.2f}s ===")
        return 124


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--suite", help="Integration test suite (tests/<suite>.rs)")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Run all integration test suites under tests/*.rs",
    )
    ap.add_argument(
        "--pattern",
        default=None,
        help="Only run tests whose full name contains this substring",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Per-test timeout (seconds)",
    )
    ap.add_argument(
        "--continue-on-fail",
        action="store_true",
        help="Continue running remaining tests after a failure",
    )

    args = ap.parse_args()

    if not args.suite and not args.all:
        ap.error("Provide --suite <name> or --all")

    suites = [args.suite] if args.suite else list_suites_from_tests_dir()

    tests: list[TestRef] = []
    for suite in suites:
        for name in list_tests_for_suite(suite):
            full = f"{suite}::{name}"
            if args.pattern and args.pattern not in full:
                continue
            tests.append(TestRef(suite=suite, name=name))

    if not tests:
        print("No matching tests found.")
        return 0

    print(f"Will run {len(tests)} tests across {len(set(t.suite for t in tests))} suite(s).")

    any_fail = False
    for t in tests:
        rc = run_single_test(t, args.timeout)
        if rc == 124:
            return 124
        if rc != 0:
            any_fail = True
            if not args.continue_on_fail:
                return 2

    return 2 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
