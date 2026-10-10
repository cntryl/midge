#!/usr/bin/env python3
"""Repeat the complete native watchdog fixture under reproducible CPU load.

Example:
  CARGO_TARGET_DIR=/tmp/midge-closeout-target-20261010 python3 \
    tools/qualify_watchdog_contention.py --workers 4 --repeats 3 \
    --evidence-dir /tmp/midge-watchdog-contention
"""
import argparse
import json
import multiprocessing
import os
from pathlib import Path
import platform
import subprocess
import time


def consume_cpu(stop):
    value = 1
    while not stop.is_set():
        for _ in range(10_000):
            value = (value * 1_664_525 + 1_013_904_223) & 0xFFFFFFFF


def collect_receipts(directory):
    held_prefixes = ("rejected-clients-", "held-recovery-", "held-inventory-", "held-final-flush-")
    held = []
    unexpected = []
    publication = []
    for path in directory.rglob("child-stdout.json"):
        if path.stat().st_size == 0:
            continue
        data = json.loads(path.read_text())
        for spec in data.get("benchmark_specs", []):
            meta = spec.get("metadata", {})
            if meta.get("failure_kind") != "no_progress_timeout":
                continue
            record = {"receipt": str(path.relative_to(directory)), "completed_units": meta.get("progress_completed_units")}
            if path.parent.name.startswith(held_prefixes) and record["completed_units"] == "0":
                held.append(record)
            else:
                unexpected.append(record)
    for path in directory.rglob("publication-observations.json"):
        data = json.loads(path.read_text())
        if data.get("worker_released"):
            publication.append({"path": str(path.relative_to(directory)), **data})
    return held, unexpected, publication


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.workers < 1 or args.repeats < 1:
        parser.error("workers and repeats must be positive")
    evidence = args.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    command = ["cargo", "test", "--locked", "--all-features", "--test", "stress_workload_watchdog"]
    with (evidence / "build.log").open("w") as log:
        subprocess.run(command + ["--no-run"], check=True, stdout=log, stderr=subprocess.STDOUT)
    summary = {
        "source": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "cpu_workers": args.workers,
        "command": command + ["--", "--test-threads=1"],
        "runs": [],
    }
    stop = multiprocessing.Event()
    workers = [multiprocessing.Process(target=consume_cpu, args=(stop,)) for _ in range(args.workers)]
    try:
        for worker in workers:
            worker.start()
        for index in range(args.repeats):
            directory = evidence / f"run-{index + 1:02}"
            directory.mkdir()
            env = dict(os.environ, MIDGE_WATCHDOG_EVIDENCE_DIR=str(directory))
            started = time.monotonic()
            with (directory / "full-harness.log").open("w") as log:
                result = subprocess.run(summary["command"], env=env, stdout=log,
                                        stderr=subprocess.STDOUT, timeout=180)
            held, unexpected, publication = collect_receipts(directory)
            record = {"run": index + 1, "exit_code": result.returncode,
                      "elapsed_secs": time.monotonic() - started,
                      "held_no_progress": held, "unexpected_no_progress": unexpected,
                      "completed_publications": publication}
            summary["runs"].append(record)
            (evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(f"run {index + 1}: exit={result.returncode}, held={len(held)}, unexpected={len(unexpected)}", flush=True)
            if result.returncode or unexpected or len(held) < 4:
                raise SystemExit("watchdog contention qualification failed; retained receipts identify the control")
    finally:
        stop.set()
        for worker in workers:
            if worker.pid is not None:
                worker.join(timeout=5)
                if worker.is_alive():
                    worker.terminate()
                    worker.join(timeout=5)
    print(evidence / "summary.json", flush=True)


if __name__ == "__main__":
    main()
