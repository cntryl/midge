#!/usr/bin/env python3
"""Summarize local stress and Criterion benchmark output.

The script is intentionally tolerant of evolving JSON schemas: it walks known
`target/` output locations, extracts common throughput/latency/diagnostic
fields when present, and leaves missing values blank.
"""

from __future__ import annotations

import argparse
import json
import signal
from pathlib import Path
from typing import Any


FIELDS = (
    "source",
    "name",
    "ops_s",
    "mib_s",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "wal_appends",
    "wal_fsyncs",
    "avg_wal_append_ms",
    "avg_wal_sync_ms",
    "write_stalls",
    "write_stalls_cloud",
    "cache_hit_ratio_ppm",
    "cloud_uploads_failed",
    "pending_cloud_uploads_end",
    "hybrid_usage_percent",
    "hybrid_pending_evictions",
    "avg_ssts_per_read",
    "avg_blocks_per_read",
)

try:
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
except (AttributeError, ValueError):
    pass


def load_json(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def as_number(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def find_number(data: Any, names: tuple[str, ...]) -> float | None:
    if isinstance(data, dict):
        for name in names:
            number = as_number(data.get(name))
            if number is not None:
                return number
        for value in data.values():
            found = find_number(value, names)
            if found is not None:
                return found
    elif isinstance(data, list):
        for value in data:
            found = find_number(value, names)
            if found is not None:
                return found
    return None


def benchmark_name_from_criterion_estimates(path: Path, target: Path) -> str:
    parts = path.relative_to(target).parts
    if len(parts) <= 2:
        return path.parent.name
    return "/".join(parts[1:-1])


def stress_name(path: Path, target: Path) -> str:
    return path.parent.relative_to(target / "stress").as_posix()


def metric_ms(data: Any, ms_names: tuple[str, ...], us_names: tuple[str, ...]) -> float | None:
    value_ms = find_number(data, ms_names)
    if value_ms is not None:
        return value_ms
    value_us = find_number(data, us_names)
    if value_us is not None:
        return value_us / 1000.0
    return None


def throughput_from_result(result: dict[str, Any], field: str) -> float | None:
    duration_ns = as_number(result.get("duration"))
    numerator = as_number(result.get(field))
    if duration_ns is None or duration_ns <= 0 or numerator is None:
        return None
    per_second = numerator / (duration_ns / 1_000_000_000.0)
    if field == "bytes":
        return per_second / (1024.0 * 1024.0)
    return per_second


def summarize_stress_result(
    result: dict[str, Any],
    suite_name: str,
) -> dict[str, Any]:
    tags = result.get("tags") if isinstance(result.get("tags"), dict) else {}
    combined = {**result, **tags}

    return {
        "source": "stress",
        "name": str(result.get("name") or suite_name),
        "ops_s": find_number(combined, ("ops_s", "ops_per_sec", "throughput_ops_s"))
        or throughput_from_result(result, "elements"),
        "mib_s": find_number(combined, ("mib_s", "mb_s", "throughput_mib_s"))
        or throughput_from_result(result, "bytes"),
        "p50_ms": metric_ms(combined, ("p50_ms", "latency_p50_ms"), ("p50_us", "latency_p50_us")),
        "p95_ms": metric_ms(combined, ("p95_ms", "latency_p95_ms"), ("p95_us", "latency_p95_us")),
        "p99_ms": metric_ms(combined, ("p99_ms", "latency_p99_ms"), ("p99_us", "latency_p99_us")),
        "max_ms": metric_ms(combined, ("max_ms", "latency_max_ms"), ("max_us", "latency_max_us")),
        "wal_appends": find_number(combined, ("wal_appends", "wal_append_count")),
        "wal_fsyncs": find_number(combined, ("wal_fsyncs", "fsync_count")),
        "avg_wal_append_ms": metric_ms(
            combined, ("avg_wal_append_ms",), ("avg_wal_append_us",)
        ),
        "avg_wal_sync_ms": metric_ms(
            combined, ("avg_wal_sync_ms", "avg_sync_ms"), ("avg_wal_sync_us", "avg_sync_us")
        ),
        "write_stalls": find_number(combined, ("write_stalls", "write_stall_count")),
        "write_stalls_cloud": find_number(combined, ("write_stalls_cloud",)),
        "cache_hit_ratio_ppm": find_number(combined, ("cache_hit_ratio_ppm",)),
        "cloud_uploads_failed": find_number(
            combined,
            ("cloud_async_wal_uploads_failed", "cloud_uploads_failed"),
        ),
        "pending_cloud_uploads_end": find_number(
            combined, ("pending_cloud_uploads_end", "pending_cloud_uploads")
        ),
        "hybrid_usage_percent": find_number(combined, ("hybrid_usage_percent",)),
        "hybrid_pending_evictions": find_number(
            combined, ("hybrid_pending_evictions",)
        ),
        "avg_ssts_per_read": find_number(combined, ("avg_ssts_per_read",)),
        "avg_blocks_per_read": find_number(combined, ("avg_blocks_per_read",)),
    }


def summarize_stress(path: Path, target: Path) -> list[dict[str, Any]]:
    data = load_json(path)
    if data is None:
        return []

    suite_name = str(data.get("suite") or stress_name(path, target)) if isinstance(data, dict) else stress_name(path, target)
    results = data.get("results") if isinstance(data, dict) else None
    if isinstance(results, list):
        return [
            summarize_stress_result(result, suite_name)
            for result in results
            if isinstance(result, dict)
        ]

    if isinstance(data, dict):
        return [summarize_stress_result(data, suite_name)]
    return []


def summarize_criterion(path: Path, target: Path) -> dict[str, Any] | None:
    data = load_json(path)
    if not isinstance(data, dict):
        return None

    mean = data.get("mean")
    if not isinstance(mean, dict):
        return None
    ns_per_iter = mean.get("point_estimate")
    if not isinstance(ns_per_iter, (int, float)) or ns_per_iter <= 0:
        return None

    return {
        "source": "criterion",
        "name": benchmark_name_from_criterion_estimates(path, target),
        "ops_s": 1_000_000_000.0 / float(ns_per_iter),
    }


def collect_rows(target: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted((target / "stress").glob("*/latest.json")):
        rows.extend(summarize_stress(path, target))

    criterion_root = target / "criterion"
    for path in sorted(criterion_root.glob("**/estimates.json")):
        row = summarize_criterion(path, target)
        if row is not None:
            rows.append(row)

    return rows


def format_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if value >= 100:
            return f"{value:.1f}"
        if value >= 1:
            return f"{value:.3f}"
        return f"{value:.6f}"
    return str(value)


def print_markdown(rows: list[dict[str, Any]]) -> None:
    print("| " + " | ".join(FIELDS) + " |")
    print("| " + " | ".join("---" for _ in FIELDS) + " |")
    for row in rows:
        print("| " + " | ".join(format_value(row.get(field)) for field in FIELDS) + " |")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default="target", type=Path)
    parser.add_argument("--json", action="store_true", help="emit JSON instead of Markdown")
    args = parser.parse_args()

    rows = collect_rows(args.target)
    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        print_markdown(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
