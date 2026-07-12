#!/usr/bin/env python3
"""Check Rust production-module size without counting test-only items.

The checker intentionally uses a small source scanner rather than a Rust AST
dependency. It only needs to identify ``#[cfg(test)]`` items and their brace
balanced bodies, which keeps the repository qualification gate available in a
fresh checkout before Rust dependencies have been built.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys


WARN_LINES = 1_200
NEW_MODULE_LINES = 1_600

# The core migration wave has no remaining oversize legacy exceptions. A
# future migration exception must be added deliberately here, with a narrow
# responsibility-based reason in the review.
LEGACY_OVERSIZE_MODULES = frozenset()

# These files are intentionally allowed to remain above the warning threshold
# until their format/provider ownership can be split without inventing a
# cross-provider abstraction. They still appear in the report.
TEMPORARY_ALLOWLIST = frozenset(
    {
        "src/metadata/journal.rs",
        "src/sst/types.rs",
        "src/storage/cloud/mod.rs",
        "src/storage/providers/azure.rs",
        "src/storage/providers/gcs.rs",
        "src/storage/providers/s3.rs",
        "src/wal/encoding.rs",
    }
)


@dataclass(frozen=True)
class ModuleReport:
    path: str
    production_lines: int


def _code_without_strings_and_comments(line: str, in_block_comment: bool) -> tuple[str, bool]:
    """Return enough line text for brace/semicolon tracking."""

    output: list[str] = []
    index = 0
    quote: str | None = None
    escaped = False

    while index < len(line):
        if in_block_comment:
            end = line.find("*/", index)
            if end < 0:
                return "".join(output), True
            index = end + 2
            in_block_comment = False
            continue

        if quote is not None:
            character = line[index]
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            index += 1
            continue

        if line.startswith("//", index):
            break
        if line.startswith("/*", index):
            in_block_comment = True
            index += 2
            continue
        character = line[index]
        if character in {'"', "'"}:
            quote = character
        else:
            output.append(character)
        index += 1

    return "".join(output), in_block_comment


def production_line_count(source: str) -> int:
    """Count non-empty lines that are not inside a cfg(test) item."""

    lines = source.splitlines()
    count = 0
    skip = False
    item_started = False
    brace_depth = 0
    in_block_comment = False

    for line in lines:
        code, in_block_comment = _code_without_strings_and_comments(line, in_block_comment)
        stripped = code.strip()

        if not skip and "#[cfg(test)]" in stripped:
            skip = True
            item_started = False
            brace_depth = 0
            continue

        if skip:
            if not item_started:
                item_started = True

            brace_depth += code.count("{") - code.count("}")
            if ";" in code and brace_depth <= 0:
                skip = False
            elif brace_depth < 0 or (brace_depth == 0 and "{" in code):
                skip = False
            elif brace_depth == 0 and "}" in code:
                skip = False
            continue

        if stripped:
            count += 1

    return count


def source_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in (root / "src").rglob("*.rs")
        if path.name != "tests.rs"
    )


def reports(root: Path) -> list[ModuleReport]:
    return [
        ModuleReport(path=path.relative_to(root).as_posix(), production_lines=production_line_count(path.read_text()))
        for path in source_files(root)
    ]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    oversized = [report for report in reports(root) if report.production_lines > WARN_LINES]

    if oversized:
        print(f"Production modules above {WARN_LINES} lines:")
        for report in oversized:
            marker = " [temporary allowlist]" if report.path in TEMPORARY_ALLOWLIST else ""
            print(f"  {report.production_lines:4d} {report.path}{marker}")
    else:
        print(f"All production modules are at or below {WARN_LINES} lines.")

    hard_failures = [
        report
        for report in oversized
        if report.production_lines > NEW_MODULE_LINES
        and report.path not in LEGACY_OVERSIZE_MODULES
        and report.path not in TEMPORARY_ALLOWLIST
    ]
    if hard_failures:
        print(
            f"New or unlisted production modules may not exceed {NEW_MODULE_LINES} lines:",
            file=sys.stderr,
        )
        for report in hard_failures:
            print(f"  {report.production_lines:4d} {report.path}", file=sys.stderr)
        print(
            "Split the module by responsibility or add a narrowly justified migration exception.",
            file=sys.stderr,
        )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
