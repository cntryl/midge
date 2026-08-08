#!/usr/bin/env python3
"""Report source files reached only by inline unit tests."""

import json
import sys
from pathlib import Path


def covered_files(report_path: str) -> dict[str, int]:
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    result: dict[str, int] = {}
    for datum in report.get("data", []):
        for entry in datum.get("files", []):
            filename = entry.get("filename", "")
            count = entry.get("summary", {}).get("lines", {}).get("covered", 0)
            result[filename] = max(result.get(filename, 0), int(count))
    return result


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
