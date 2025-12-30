#!/usr/bin/env python3
"""Test Validation Utility (Python)

Ported from `testutils/validate_tests.rs` so contributors can run the validator
without compiling the Rust helper. Usage:

  python scripts/validate_tests.py --summary
  python scripts/validate_tests.py --file src/wal/wal_helpers.rs
"""

from __future__ import annotations

import argparse
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

# ----- Data structures -----

@dataclass
class TestResult:
    test_name: str
    file: str
    line: int
    line_count: int
    issues: List[str]

    def is_compliant(self) -> bool:
        return len(self.issues) == 0

# ----- Core analysis logic -----

def test_single_test(file: str, test_name: str, line_num: int, lines: List[str]) -> TestResult:
    test_start = line_num - 1  # convert to 0-based index

    # Find test end by tracking brace depth (simple heuristic similar to Rust version)
    test_end = len(lines) - 1
    brace_depth = 0
    found_open_brace = False

    for i in range(test_start, len(lines)):
        line = lines[i]
        for ch in line:
            if ch == "{":
                brace_depth += 1
                found_open_brace = True
            elif ch == "}":
                brace_depth -= 1
                if found_open_brace and brace_depth == 0:
                    test_end = i
                    break
        if found_open_brace and brace_depth == 0:
            break

    test_body = "\n".join(lines[test_start : test_end + 1])
    test_lines = test_end - test_start + 1

    issues: List[str] = []

    # Check 1: Naming convention
    if not test_name.startswith("should_"):
        issues.append("NAMING: Does not start with 'should_'")

    # Check 2: AAA structure (only for tests >5 lines)
    if test_lines > 5:
        arrange_re = re.compile(r"//\s*Arrange")
        act_re = re.compile(r"//\s*Act")
        assert_re = re.compile(r"//\s*Assert")
        combined_re = re.compile(r"//\s*(Arrange|Act|Assert)\s*[+&]")

        has_arrange = bool(arrange_re.search(test_body))
        has_act = bool(act_re.search(test_body))
        has_assert = bool(assert_re.search(test_body))
        has_combined = bool(combined_re.search(test_body))

        if not has_arrange:
            issues.append("AAA: Missing '// Arrange' comment")
        if not has_act:
            issues.append("AAA: Missing '// Act' comment")
        if not has_assert:
            issues.append("AAA: Missing '// Assert' comment")
        if has_combined:
            issues.append("AAA: Has combined AAA comment (e.g., '// Arrange + Act')")

    # Check 3: Multiple Act sections (indicates multi-behavior)
    act_count_re = re.compile(r"^\s*//\s*Act(\s|$)", re.MULTILINE)
    # Count Act comments while being mindful of string literals (simple heuristic)
    act_count = 0
    in_string = False

    for line in test_body.splitlines():
        trimmed = line.strip()
        quote_count = trimmed.count('"')
        if quote_count % 2 == 1:
            in_string = not in_string
        if not in_string and act_count_re.match(line):
            act_count += 1

    if act_count > 1:
        issues.append(f"MULTI-BEHAVIOR: Has {act_count} '// Act' sections")

    # Check 4: 'and' in action part of name
    if "_and_" in test_name:
        parts = test_name.split("_given_")
        if len(parts) > 1:
            action_part = parts[0]
        else:
            parts2 = test_name.split("_when_")
            action_part = parts2[0] if len(parts2) > 1 else test_name

        if "_and_" in action_part:
            allowed_patterns = [
                "with_id_and_name",
                "point_writes_and_range_deletes",
                "memtable_and_sst",
                "large_keys_and_values",
            ]
            is_allowed = any(pat in test_name for pat in allowed_patterns)
            if not is_allowed:
                issues.append(
                    "MULTI-BEHAVIOR: Test name contains '_and_' in action (may test multiple behaviors)"
                )

    return TestResult(test_name=test_name, file=file, line=line_num, line_count=test_lines, issues=issues)


def find_tests_in_file(file_path: Path) -> List[TestResult]:
    try:
        content = file_path.read_text()
    except Exception:
        return []

    lines = content.splitlines()
    results: List[TestResult] = []

    test_attr_re = re.compile(r"^\s*#\[test\]\s*$")
    fn_name_re = re.compile(r"^\s*fn\s+(\w+)")

    for i in range(len(lines)):
        if test_attr_re.match(lines[i]) and i + 1 < len(lines):
            m = fn_name_re.match(lines[i + 1])
            if m:
                test_name = m.group(1)
                line_num = i + 2
                result = test_single_test(str(file_path), test_name, line_num, lines)
                results.append(result)

    return results


def find_all_rust_files(dir_path: Path) -> List[Path]:
    rust_files: List[Path] = []
    if not dir_path.exists():
        return rust_files
    for root, dirs, files in os.walk(dir_path):
        # skip target and hidden dirs
        dirs[:] = [d for d in dirs if not d.startswith(".") and d != "target"]
        for f in files:
            if f.endswith(".rs"):
                rust_files.append(Path(root) / f)
    return rust_files


def get_all_test_results() -> List[TestResult]:
    all_results: List[TestResult] = []
    for d in ("src", "tests"):
        p = Path(d)
        if p.exists():
            files = find_all_rust_files(p)
            for f in files:
                all_results.extend(find_tests_in_file(f))
    return all_results

# ----- Printing / CLI -----

CSI_YELLOW = "\x1b[33m"
CSI_GREEN = "\x1b[32m"
CSI_CYAN = "\x1b[36m"
CSI_RED = "\x1b[31m"
CSI_RESET = "\x1b[0m"


def print_summary() -> None:
    print(f"{CSI_CYAN}Scanning all tests for guideline violations...{CSI_RESET}\n")

    all_results = get_all_test_results()
    total_count = len(all_results)
    compliant_count = sum(1 for r in all_results if r.is_compliant())
    non_compliant = [r for r in all_results if not r.is_compliant()]

    print(f"{CSI_YELLOW}Summary:{CSI_RESET}")
    print(f"  Total tests: {total_count}")
    pct = (compliant_count / total_count * 100.0) if total_count > 0 else 0.0
    print(f"  {CSI_GREEN}Compliant: {compliant_count} ({pct:.1f}%) {CSI_RESET}")
    npct = (len(non_compliant) / total_count * 100.0) if total_count > 0 else 0.0
    print(f"  {CSI_RED}Non-compliant: {len(non_compliant)} ({npct:.1f}%) {CSI_RESET}\n")

    # Group by issue type
    naming_issues = sum(1 for r in non_compliant if any(i.startswith("NAMING:") for i in r.issues))
    aaa_issues = sum(1 for r in non_compliant if any(i.startswith("AAA:") for i in r.issues))
    multi_issues = sum(1 for r in non_compliant if any(i.startswith("MULTI-BEHAVIOR:") for i in r.issues))

    print(f"{CSI_YELLOW}Issue breakdown:{CSI_RESET}")
    print(f"  Naming violations: {naming_issues}")
    print(f"  AAA structure violations: {aaa_issues}")
    print(f"  Multi-behavior violations: {multi_issues}\n")

    if non_compliant:
        print(f"{CSI_YELLOW}Sample of non-compliant tests (first 20):{CSI_RESET}")
        for r in non_compliant[:20]:
            print(f"  {CSI_RED}{r.file}::{r.test_name}  (line {r.line}){CSI_RESET}")
            for issue in r.issues:
                print(f"    {CSI_YELLOW}- {issue}{CSI_RESET}")
        multi_violations = [r for r in non_compliant if any(i.startswith("MULTI-BEHAVIOR:") for i in r.issues)]
        if multi_violations:
            print(f"\n{CSI_YELLOW}All Multi-behavior violations:{CSI_RESET}")
            for r in multi_violations:
                print(f"  {CSI_RED}{r.file}::{r.test_name}  (line {r.line}){CSI_RESET}")
                for issue in r.issues:
                    if issue.startswith("MULTI-BEHAVIOR:"):
                        print(f"    {CSI_YELLOW}- {issue}{CSI_RESET}")


def print_file_results(file_path: Path) -> None:
    print(f"{CSI_CYAN}Checking tests in: {file_path}{CSI_RESET}\n")
    results = find_tests_in_file(file_path)
    if not results:
        print(f"{CSI_YELLOW}No tests found in file{CSI_RESET}")
        return
    compliant = sum(1 for r in results if r.is_compliant())
    total = len(results)
    pct = (compliant / total * 100.0) if total > 0 else 0.0
    print(f"{CSI_YELLOW}Results: {compliant}/{total} compliant ({pct:.1f}%) {CSI_RESET}\n")

    for r in results:
        if r.is_compliant():
            print(f"{CSI_GREEN}[OK] {r.test_name} (line {r.line}){CSI_RESET}")
        else:
            print(f"{CSI_RED}[!!] {r.test_name} (line {r.line}){CSI_RESET}")
            for issue in r.issues:
                print(f"    {CSI_YELLOW}- {issue}{CSI_RESET}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate test naming and structure rules")
    p.add_argument("--summary", "-s", action="store_true", help="Show summary for repository")
    p.add_argument("--file", "-f", type=Path, help="Check a single file")
    p.add_argument("--json", "-j", type=Path, help="Dump results to JSON file")
    return p.parse_args()


import json


def results_to_dict(results: list) -> list:
    out = []
    for r in results:
        out.append({
            "file": r.file,
            "test": r.test_name,
            "line": r.line,
            "line_count": r.line_count,
            "issues": r.issues,
        })
    return out


def main() -> None:
    args = parse_args()

    if args.summary:
        print_summary()
        if args.json:
            all_results = get_all_test_results()
            with open(args.json, "w", encoding="utf-8") as fh:
                json.dump(results_to_dict(all_results), fh, indent=2)
            print(f"Wrote JSON report to {args.json}")
    elif args.file:
        print_file_results(args.file)
        if args.json:
            results = find_tests_in_file(args.file)
            with open(args.json, "w", encoding="utf-8") as fh:
                json.dump(results_to_dict(results), fh, indent=2)
            print(f"Wrote JSON report to {args.json}")
    else:
        print("Test Validation Helper")
        print("======================")
        print()
        print("Usage:")
        print("  python scripts/validate_tests.py --summary                    # Show summary of all tests")
        print("  python scripts/validate_tests.py --file src/backup.rs         # Check specific file")


if __name__ == "__main__":
    main()
