#!/usr/bin/env python3
"""Report source files reached only by inline unit tests."""

from __future__ import annotations

import json
import re
import sys
from functools import lru_cache
from pathlib import Path

CFG_TEST_ATTRIBUTE = (
    r"#\s*\[\s*cfg\s*\(\s*"
    r"(?:test|all\s*\([^)]*\btest\b[^)]*\))"
    r"\s*\)\s*\]"
)


def _mask_non_code(source: str) -> str:
    """Preserve Rust code/newlines while hiding comments and string contents."""
    masked = list(source)
    index = 0
    block_comment_depth = 0
    while index < len(source):
        if block_comment_depth:
            if source.startswith("/*", index):
                masked[index : index + 2] = "  "
                block_comment_depth += 1
                index += 2
            elif source.startswith("*/", index):
                masked[index : index + 2] = "  "
                block_comment_depth -= 1
                index += 2
            else:
                if source[index] != "\n":
                    masked[index] = " "
                index += 1
            continue

        if source.startswith("//", index):
            line_end = source.find("\n", index)
            if line_end == -1:
                line_end = len(source)
            masked[index:line_end] = " " * (line_end - index)
            index = line_end
            continue
        if source.startswith("/*", index):
            masked[index : index + 2] = "  "
            block_comment_depth = 1
            index += 2
            continue

        raw_match = re.match(r"(?:br|r)(?P<hashes>#{0,255})\"", source[index:])
        if raw_match:
            terminator = '"' + raw_match.group("hashes")
            end = source.find(terminator, index + raw_match.end())
            end = len(source) if end == -1 else end + len(terminator)
            for position in range(index, end):
                if source[position] != "\n":
                    masked[position] = " "
            index = end
            continue

        quote_index = index + 1 if source.startswith('b"', index) else index
        if quote_index < len(source) and source[quote_index] == '"':
            end = quote_index + 1
            escaped = False
            while end < len(source):
                character = source[end]
                if character == '"' and not escaped:
                    end += 1
                    break
                escaped = character == "\\" and not escaped
                if character != "\\":
                    escaped = False
                end += 1
            for position in range(index, end):
                if source[position] != "\n":
                    masked[position] = " "
            index = end
            continue

        if (
            source[index] == "'"
            and index + 2 < len(source)
            and (source[index + 2] == "'" or source[index + 1] == "\\")
        ):
            end = index + 2
            escaped = False
            while end < len(source):
                character = source[end]
                if character == "'" and not escaped:
                    end += 1
                    break
                escaped = character == "\\" and not escaped
                if character != "\\":
                    escaped = False
                end += 1
            masked[index:end] = " " * (end - index)
            index = end
            continue
        index += 1
    return "".join(masked)


def _source_path(filename: str) -> Path | None:
    path = Path(filename)
    if path.is_file():
        return path
    normalized = filename.replace("\\", "/")
    marker = "/src/"
    if marker in normalized:
        candidate = Path.cwd() / "src" / normalized.split(marker, 1)[1]
        if candidate.is_file():
            return candidate
    return None


def _is_cfg_test_external_module(path: Path) -> bool:
    module_name = path.stem
    parents = [path.parent.with_suffix(".rs"), path.parent / "mod.rs"]
    declaration = re.compile(
        CFG_TEST_ATTRIBUTE + r"\s*(?:#\s*\[[^\]]+\]\s*)*"
        r"(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+"
        + re.escape(module_name)
        + r"\s*;"
    )
    return any(
        parent.is_file()
        and declaration.search(_mask_non_code(parent.read_text(encoding="utf-8")))
        for parent in parents
    )


def _item_head(code: str, start: int) -> int:
    position = start
    while True:
        while position < len(code) and code[position].isspace():
            position += 1
        if not code.startswith("#[", position):
            return position
        attribute_end = code.find("]", position + 2)
        if attribute_end == -1:
            return position
        position = attribute_end + 1


def _comma_or_semicolon_item_end(code: str, start: int) -> int | None:
    parentheses = 0
    brackets = 0
    braces = 0
    for position in range(start, len(code)):
        character = code[position]
        if character == "(":
            parentheses += 1
        elif character == ")":
            parentheses = max(0, parentheses - 1)
        elif character == "[":
            brackets += 1
        elif character == "]":
            brackets = max(0, brackets - 1)
        elif character == "{":
            braces += 1
        elif character == "}":
            if braces == 0:
                return None
            braces -= 1
        elif character in ",;" and parentheses == brackets == braces == 0:
            return position
    return None


@lru_cache(maxsize=None)
def cfg_test_line_ranges(filename: str) -> tuple[tuple[int, int], ...]:
    """Return inclusive line ranges for inline items guarded by `#[cfg(test)]`."""
    path = _source_path(filename)
    if path is None:
        return ()
    source = path.read_text(encoding="utf-8")
    if _is_cfg_test_external_module(path):
        return ((1, source.count("\n") + 1),)
    code = _mask_non_code(source)
    attribute = re.compile(CFG_TEST_ATTRIBUTE)
    ranges: list[tuple[int, int]] = []
    for match in attribute.finditer(code):
        start_line = code.count("\n", 0, match.start()) + 1
        item_start = _item_head(code, match.end())
        braced_item = re.match(
            r"(?:pub(?:\s*\([^)]*\))?\s+)?"
            r"(?:(?:async|unsafe|const)\s+)*"
            r"(?:extern\s+(?:\"[^\"]*\"\s+)?)?"
            r"(?:fn|mod|impl|struct|enum|union|trait)\b",
            code[item_start:],
        )
        if braced_item is None:
            item_end = _comma_or_semicolon_item_end(code, item_start)
            end_position = match.end() if item_end is None else item_end
            end_line = code.count("\n", 0, end_position) + 1
            ranges.append((start_line, end_line))
            continue

        open_brace = code.find("{", item_start)
        semicolon = code.find(";", item_start)
        if semicolon != -1 and (open_brace == -1 or semicolon < open_brace):
            end_line = code.count("\n", 0, semicolon) + 1
            ranges.append((start_line, end_line))
            continue
        if open_brace == -1:
            ranges.append((start_line, start_line))
            continue
        depth = 0
        close_brace = None
        for position in range(open_brace, len(code)):
            if code[position] == "{":
                depth += 1
            elif code[position] == "}":
                depth -= 1
                if depth == 0:
                    close_brace = position
                    break
        end_position = len(code) - 1 if close_brace is None else close_brace
        end_line = code.count("\n", 0, end_position) + 1
        ranges.append((start_line, end_line))
    return tuple(ranges)


def _excluded(filename: str, line: int) -> bool:
    return any(start <= line <= end for start, end in cfg_test_line_ranges(filename))


def covered_files(report_path: str) -> dict[str, int]:
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    covered_lines: dict[str, set[int]] = {}
    for datum in report.get("data", []):
        for entry in datum.get("files", []):
            filename = entry.get("filename", "")
            segments = entry.get("segments", [])
            lines = covered_lines.setdefault(filename, set())
            for segment_index, segment in enumerate(segments):
                if (
                    len(segment) < 6
                    or int(segment[2]) <= 0
                    or not bool(segment[3])
                    or bool(segment[5])
                ):
                    continue
                start_line = int(segment[0])
                if segment_index + 1 < len(segments):
                    next_line = int(segments[segment_index + 1][0])
                    next_column = int(segments[segment_index + 1][1])
                    end_line = next_line if next_column > 1 else next_line - 1
                    end_line = max(start_line, end_line)
                else:
                    end_line = start_line
                lines.update(
                    line
                    for line in range(start_line, end_line + 1)
                    if not _excluded(filename, line)
                )
    return {filename: len(lines) for filename, lines in covered_lines.items()}


def covered_function_regions(
    report_path: str,
) -> dict[tuple[str, tuple[tuple[int, int, int, int, int], ...]], int]:
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    result: dict[tuple[str, tuple[tuple[int, int, int, int, int], ...]], int] = {}
    for datum in report.get("data", []):
        for function in datum.get("functions", []):
            filenames = function.get("filenames", [])
            if not filenames:
                continue
            by_filename: dict[str, list[list[int]]] = {}
            for region in function.get("regions", []):
                if len(region) < 8:
                    continue
                file_id = int(region[5])
                if file_id < 0 or file_id >= len(filenames):
                    continue
                by_filename.setdefault(filenames[file_id], []).append(region)
            for filename, regions in by_filename.items():
                regions = [
                    region
                    for region in regions
                    if not _excluded(filename, int(region[0]))
                ]
                geometry = tuple(
                    sorted(
                        {
                            (
                                int(region[0]),
                                int(region[1]),
                                int(region[2]),
                                int(region[3]),
                                int(region[7]),
                            )
                            for region in regions
                        }
                    )
                )
                if not geometry:
                    continue
                key = (filename, geometry)
                count = max(int(region[4]) for region in regions)
                result[key] = max(result.get(key, 0), count)
    return result


def monitored(path: str) -> bool:
    roots = ("src/sst/", "src/compaction/", "src/wal/", "src/lease/", "src/metadata/", "src/runtime/")
    normalized = path.replace("\\", "/")
    return any(root in normalized for root in roots)


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: coverage_tier_diff.py UNIT.json INTEGRATION.json", file=sys.stderr)
        return 2
    unit = covered_files(sys.argv[1])
    integration = covered_files(sys.argv[2])
    islands = sorted(
        path for path, count in unit.items()
        if count > 0 and monitored(path) and integration.get(path, 0) == 0
    )
    print("# Unit-only coverage islands\n")
    print("Informational only. Triage each item as wire, remove, or explicitly test-only.\n")
    for path in islands:
        print(f"- `{path}` ({unit[path]} unit-covered lines)")
    if not islands:
        print("No unit-only source files detected in the monitored subsystems.")
    unit_functions = covered_function_regions(sys.argv[1])
    integration_functions = covered_function_regions(sys.argv[2])
    function_islands = sorted(
        key for key, count in unit_functions.items()
        if count > 0 and monitored(key[0]) and integration_functions.get(key, 0) == 0
    )
    print("\n## Unit-only functions\n")
    for (path, geometry) in function_islands:
        line, column, _, _, _ = geometry[0]
        print(f"- `{path}:{line}:{column}` (source-region identity)")
    if not function_islands:
        print("No unit-only functions detected in the monitored subsystems.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
