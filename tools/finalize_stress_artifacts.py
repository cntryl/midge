#!/usr/bin/env python3
"""Finalize isolated workload artifacts from the native receipt after process exit."""
import argparse
import json
import os
from pathlib import Path
import re
import tempfile

RUN_STEM = re.compile(r"\d+-(\d+)-\d+\Z")
COUNTERS = (
    "attempted_transactions", "acknowledged_transactions", "acknowledged_rows",
    "logical_operations", "resource_limit_responses", "write_stall_responses",
    "saturation_backoff_ms",
)


def read_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def atomic_json(path, value):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", suffix=".tmp", delete=False) as output:
            temporary = Path(output.name)
            json.dump(value, output, indent=2)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def selected_workload(command_line):
    if not isinstance(command_line, list) or not all(isinstance(x, str) for x in command_line):
        return None
    selections = [command_line[i + 1] for i, value in enumerate(command_line[:-1])
                  if value in ("--workload", "--filter")]
    selections.extend(value.split("=", 1)[1] for value in command_line
                      if value.startswith(("--workload=", "--filter=")))
    return selections[0] if len(selections) == 1 else None


def valid_receipt(receipt):
    if not isinstance(receipt, dict) or not isinstance(receipt.get("metadata", {}), dict):
        return False
    if "reporter_errors" in receipt.get("metadata", {}) and not isinstance(receipt["metadata"]["reporter_errors"], str):
        return False
    for key in ("benchmark_specs", "summaries", "samples"):
        rows = receipt.get(key)
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            return False
        for row in rows:
            metadata = row.get("metadata", {})
            if not isinstance(metadata, dict):
                return False
            if any(field in metadata and not isinstance(metadata[field], str)
                   for field in ("benchmark_error", "failure_kind")):
                return False
            if key == "summaries":
                correctness = row.get("correctness", {})
                if not isinstance(correctness, dict):
                    return False
                if "passed" in correctness and type(correctness["passed"]) is not bool:
                    return False
            if key == "samples":
                counters = row.get("counters", {})
                if not isinstance(counters, dict) or not all(type(x) is int and x >= 0 for x in counters.values()):
                    return False
    return True


def matching_receipts(root, workload, suite, sha):
    receipts = []
    for path in sorted(root.rglob("*.json")):
        stem = RUN_STEM.fullmatch(path.stem)
        if stem is None:  # latest.json is an alias, never another receipt.
            continue
        receipt = read_json(path)
        if not valid_receipt(receipt):
            continue
        environment = receipt.get("environment", {})
        if (isinstance(environment, dict)
                and receipt.get("schema_version") == "cntryl-stress.v2"
                and receipt.get("suite") == suite
                and receipt.get("started_at") == path.stem
                and environment.get("git_commit") == sha
                and selected_workload(environment.get("command_line", [])) == workload):
            receipts.append((int(stem[1]), path, receipt))
    return receipts


def receipt_failure(receipt):
    rows = receipt.get("benchmark_specs", []) + receipt.get("summaries", [])
    metadata = [row.get("metadata", {}) for row in rows]
    messages = sorted({item["benchmark_error"] for item in metadata if item.get("benchmark_error")})
    kinds = sorted({item["failure_kind"] for item in metadata if item.get("failure_kind")})
    correctness_failed = any(row.get("correctness", {}).get("passed") is False
                             for row in receipt.get("summaries", []))
    counters_failed = any(any(sample.get("counters", {}).get(key, 0) > 0
                              for key in ("failures", "timeouts", "duplicates", "dropped", "validation_errors"))
                          for sample in receipt.get("samples", []))
    reporter_error = receipt.get("metadata", {}).get("reporter_errors")
    return {"failed": bool(messages or kinds or correctness_failed or counters_failed or reporter_error),
            "failure_kind": kinds[0] if len(kinds) == 1 else None, "benchmark_errors": messages,
            "correctness_failed": correctness_failed or counters_failed, "reporter_error": reporter_error}


def partial_stage(path, status):
    stage = status.get("stage_index")
    if status.get("phase") != "workload" or type(stage) is not int:
        return None
    snapshots, unreadable, clients = [], [], set()
    root = path.parent / "client-snapshots" / f"stage-{stage:02}"
    for snapshot_path in sorted(root.glob("client-*.json")):
        snapshot = read_json(snapshot_path)
        if not isinstance(snapshot, dict):
            unreadable.append(str(snapshot_path))
            continue
        if (snapshot.get("process_id") != status["process_id"]
                or snapshot.get("stage_index") != stage or snapshot.get("phase") != "workload"):
            continue
        client = snapshot.get("client_index")
        if (type(client) is not int or client in clients
                or not all(type(snapshot.get(key)) is int and snapshot[key] >= 0 for key in COUNTERS)):
            unreadable.append(str(snapshot_path))
            continue
        clients.add(client)
        snapshots.append((snapshot_path, snapshot))
    return {"stage_index": stage, "snapshot_count": len(snapshots),
            "counters": ({key: sum(snapshot[key] for _, snapshot in snapshots) for key in COUNTERS}
                         if snapshots else None),
            "snapshot_paths": [str(path) for path, _ in snapshots], "unreadable_snapshots": unreadable}


def finalize(stress_root, artifact_root, workload, suite, sha, outcome):
    receipts = matching_receipts(stress_root, workload, suite, sha)
    finalized, consistency_errors = [], []
    for path in sorted(artifact_root.rglob("workload-status.json")):
        status = read_json(path)
        if (not isinstance(status, dict) or status.get("benchmark_workload") != workload
                or status.get("git_commit") != sha or type(status.get("process_id")) is not int):
            continue
        matches = [item for item in receipts if item[0] == status.get("process_id")]
        selection = "matched" if len(matches) == 1 else "missing" if not matches else "ambiguous"
        evidence = {"step_outcome": outcome, "receipt_selection": selection, "sha": sha,
                    "benchmark_workload": workload, "failure_kind": None}
        failed = outcome != "success"
        native = None
        if len(matches) == 1:
            _, receipt_path, receipt = matches[0]
            native = receipt_failure(receipt)
            evidence.update(native)
            evidence.update(source="cntryl-stress", receipt_path=str(receipt_path),
                            receipt_run_id=receipt["started_at"])
            failed = failed or native["failed"]
        else:
            evidence["source"] = "github-actions"
        if outcome == "success" and (selection != "matched" or status.get("status") != "passed" or failed):
            consistency_errors.append(str(path))
            failed = True
            evidence["artifact_consistency_error"] = True
        status.setdefault("status_before_finalization", status.get("status"))
        if failed:
            status["status"] = "failed"
        evidence["failed"] = failed
        evidence["failure_source"] = (
            "cntryl-stress" if native is not None and native["failed"] else
            "github-actions" if outcome != "success" else
            "artifact-finalizer" if failed else None
        )
        status["finalization"] = evidence
        partial = partial_stage(path, status)
        if partial is not None:
            status["partial_stage_counters"] = partial
        atomic_json(path, status)
        finalized.append(str(path))
    if outcome == "success" and not finalized:
        consistency_errors.append("no matching workload status")
    summary = {"finalized": finalized, "consistency_errors": consistency_errors,
               "matching_receipts": len(receipts), "step_outcome": outcome}
    if not finalized:
        summary.update(note="No matching workload status; workload cause unknown", failure_kind=None)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stress-root", type=Path, default=Path("target/stress"))
    parser.add_argument("--artifact-root", type=Path, default=Path("target/midge-stress"))
    parser.add_argument("--workload", required=True)
    parser.add_argument("--suite", required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--outcome", required=True, choices=("success", "failure", "cancelled", "skipped"))
    args = parser.parse_args()
    summary = finalize(args.stress_root, args.artifact_root, args.workload, args.suite, args.sha, args.outcome)
    print(json.dumps(summary, indent=2))
    return bool(summary["consistency_errors"])


if __name__ == "__main__":
    raise SystemExit(main())
