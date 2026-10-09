#!/usr/bin/env python3
"""Source-bound, same-host diagnosis; never replaces checkpoint acceptance."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import signal
import shutil
import subprocess
import time
import zipfile

CELLS = {
    "A": ("checkpoint_local_256x1mib_1cf", 256, 1024, 1),
    "B": ("checkpoint_local_512x256kib_16cf", 512, 256, 16),
    "C": ("checkpoint_local_1024x64kib_1cf", 1024, 64, 1),
}
BINARY = "tier4_system_checkpoint_write_amplification"
BENCHMARK_ENV = ("STRESS_PROFILE", "STRESS_SAMPLES", "STRESS_WARMUP_SAMPLES",
                 "STRESS_COOLDOWN_SAMPLES", "STRESS_CONFIRM_REGRESSIONS",
                 "STRESS_TIMEOUT_SECS", "STRESS_NO_PROGRESS_TIMEOUT_SECS",
                 "STRESS_GIT_SHA", "MIDGE_CHECKPOINT_REPEAT", "STRESS_RUN_ID",
                 "STRESS_OUTPUT_DIR", "MIDGE_STRESS_ARTIFACT_DIR", "TMPDIR",
                 "PATH", "GITHUB_SHA")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checked(command, cwd=None):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)


def plan():
    # Balance both source order and the first/last positions in each block.
    return [(block, role) for block in range(1, 4)
            for role in (("baseline", "candidate", "candidate", "baseline")
                         if block % 2 else
                         ("candidate", "baseline", "baseline", "candidate"))]


def verify_source(source, sha, manifest):
    if checked(["git", "rev-parse", "HEAD"], source) != sha:
        raise ValueError("actual checkout differs from declared immutable source")
    if checked(["git", "status", "--porcelain"], source):
        raise ValueError("source checkout is dirty")
    if (manifest["sha"] != sha
            or manifest["tree"] != checked(["git", "rev-parse", "HEAD^{tree}"], source)
            or manifest["cargo_lock_sha256"] != digest(source / "Cargo.lock")
            or manifest["profile"] != "bench/release"
            or manifest["binary"] != BINARY):
        raise ValueError("build/source/lock identity mismatch")


def download_build(repository, run, sha, source, root):
    root.mkdir()
    api = f"repos/{repository}/actions"
    run_data = json.loads(checked(["gh", "api", f"{api}/runs/{run}/attempts/1"]))
    save(root / "run.json", run_data)
    if (run_data["id"] != run or run_data["head_sha"] != sha
            or run_data["run_attempt"] != 1 or run_data["status"] != "completed"
            or run_data["conclusion"] != "success"):
        raise ValueError("origin run is not a completed successful source match")
    pages = json.loads(checked(["gh", "api", "--paginate", "--slurp",
                               f"{api}/runs/{run}/attempts/1/jobs?per_page=100"]))
    jobs = [job for page in pages for job in page["jobs"]]
    save(root / "jobs.json", pages)
    builds = [job for job in jobs if job["name"] == "checkpoint-build"]
    if len(builds) != 1 or builds[0]["conclusion"] != "success":
        raise ValueError("missing successful originating build job")
    pages = json.loads(checked(["gh", "api", "--paginate", "--slurp",
                               f"{api}/runs/{run}/artifacts?per_page=100"]))
    save(root / "artifacts.json", pages)
    name = f"checkpoint-build-{sha}-{run}-a1"
    matches = [a for page in pages for a in page["artifacts"] if a["name"] == name]
    if len(matches) != 1 or matches[0]["expired"]:
        raise ValueError("missing exact original build artifact")
    artifact = matches[0]
    archive = root / "provider.zip"
    with archive.open("wb") as output:
        subprocess.run(["gh", "api", f"{api}/artifacts/{artifact['id']}/zip"],
                       stdout=output, check=True)
    if artifact["digest"] != "sha256:" + digest(archive):
        raise ValueError("original provider archive digest mismatch")
    with zipfile.ZipFile(archive) as zipped:
        for name in zipped.namelist():
            path = Path(name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("invalid archive member path")
        zipped.extractall(root / "files")
    manifests = list((root / "files").rglob("build-manifest.json"))
    if len(manifests) != 1:
        raise ValueError("ambiguous build manifest")
    manifest = json.loads(manifests[0].read_text())
    if manifest["run_id"] != str(run) or manifest["run_attempt"] != "1":
        raise ValueError("build manifest has another run identity")
    verify_source(source, sha, manifest)
    binary = manifests[0].parent / BINARY
    if digest(binary) != manifest["executable_sha256"]:
        raise ValueError("actual release executable differs from build receipt")
    binary.chmod(0o755)
    return {"sha": sha, "source": source, "binary": binary, "manifest": manifest,
            "run_id": run, "artifact_id": artifact["id"], "archive_sha256": digest(archive)}


def native_result(root, sha, cell="A"):
    workload, cycles, rows, families = CELLS[cell]
    warmup = math.ceil(cycles / 10)
    paths = list((root / "midge").glob("*/workload-status.json"))
    if len(paths) != 1:
        raise ValueError("missing unique actual engine receipt")
    directory = paths[0].parent
    status = json.loads(paths[0].read_text())
    if not (status["git_commit"] == sha and status["status"] == "passed"
            and status["phase"] == "complete" and status["terminal_error"] is None
            and status["verified"] and status["reopened_verified"]
            and status["cell"] == cell and status["benchmark_workload"] == workload
            and status["completed_cycles"] == status["total_cycles"] == cycles
            and status["warmup_cycles"] == warmup
            and status["rows_per_cycle"] == rows and status["families"] == families
            and status["acknowledged_rows"] == cycles * rows):
        raise ValueError("source/shape/ACK/reopen proof failed")
    window = json.loads((directory / "accounting-window.json").read_text())
    observations = json.loads((directory / "ingestion-observations.json").read_text())
    latencies = sorted(observations["flush_latencies_ns"])
    if len(latencies) != status["measured_cycles"] or len(latencies) != cycles - warmup:
        raise ValueError("public flush distribution does not cover fixed measured cycles")
    if not window["gate"]["valid"]:
        raise ValueError("native accounting gate is invalid")
    before = json.loads((directory / "runtime-before.json").read_text())
    after = json.loads((directory / "runtime-after.json").read_text())
    return {"measured_elapsed_ns": window["measured_elapsed_ns"],
            "flush_p95_ns": latencies[math.ceil(len(latencies) * .95) - 1],
            "completed_compactions": window["completed_compactions"],
            "runtime_before": before, "runtime_after": after,
            "checkpoint_gate": window["gate"], "verified": True}


def cpu_command(command, root, environment):
    # Only the sampler needs privilege. Git, Engine and its receipts retain
    # the original runner identity and exact benchmark environment.
    owner = pwd.getpwuid(os.getuid()).pw_name
    child = ["runuser", "-u", owner, "--", "env"]
    child.extend(f"{key}={environment[key]}" for key in BENCHMARK_ENV if key in environment)
    child.extend(command)
    return ["sudo", "-n", "perf", "record", "-F", "99", "-g", "--call-graph",
            "dwarf", "-o", str(root / "perf.data"), "--"] + child


def render_cpu_profile(root, result):
    # The data file now belongs to the runner; analysis uses that same identity.
    with (root / "perf-report.txt").open("wb") as output:
        completed = subprocess.run(["perf", "report", "--stdio", "--no-children",
                                    "-i", str(root / "perf.data")],
                                   stdout=output, stderr=subprocess.STDOUT)
    result["profile_report_returncode"] = completed.returncode
    if completed.returncode != 0:
        result.update(valid=False, error="CPU profile analysis failed; raw data retained")


def trial(build, block, root, instrument=None, cell="A"):
    root.mkdir()
    for directory in ("native", "midge"):
        (root / directory).mkdir()
    memory_root = Path("/dev/shm") / f"midge-pair-{os.environ['GITHUB_RUN_ID']}-{root.name}"
    memory_root.mkdir()
    for directory in ("native", "midge"):
        (memory_root / directory).mkdir()
    verify_source(build["source"], build["sha"], build["manifest"])
    if digest(build["binary"]) != build["manifest"]["executable_sha256"]:
        raise ValueError("executable changed between trials")
    environment = os.environ.copy()
    environment.update(STRESS_PROFILE="smoke", STRESS_SAMPLES="1", STRESS_WARMUP_SAMPLES="0",
                       STRESS_COOLDOWN_SAMPLES="0", STRESS_CONFIRM_REGRESSIONS="0",
                       STRESS_TIMEOUT_SECS="900", STRESS_NO_PROGRESS_TIMEOUT_SECS="60",
                       STRESS_GIT_SHA=build["sha"], MIDGE_CHECKPOINT_REPEAT=str(block),
                       STRESS_RUN_ID=root.name, STRESS_OUTPUT_DIR=str(memory_root / "native"),
                       MIDGE_STRESS_ARTIFACT_DIR=str(memory_root / "midge"))
    # GITHUB_SHA remains the real diagnostic workflow head. STRESS_GIT_SHA
    # declares the separately verified, actual immutable benchmark checkout.
    command = ["/usr/bin/time", "-v", "-o", str(root / "process-time.txt"),
               str(build["binary"]), "--workload", CELLS[cell][0], "--bench"]
    if instrument == "sync":
        command = ["strace", "-f", "-c", "-e", "trace=fsync,fdatasync",
                   "-o", str(root / "sync-summary.txt")] + command
    elif instrument == "cpu":
        command = cpu_command(command, root, environment)
    subprocess.run(["sync"], check=True)
    result = {"sha": build["sha"], "binary_sha256": digest(build["binary"]),
              "instrument": instrument, "valid": False, "directory": str(root)}
    start = time.monotonic()
    with (root / "workload.log").open("wb") as log:
        process = subprocess.Popen(command, cwd=build["source"], env=environment,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            result["returncode"] = process.wait(timeout=900)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            result.update(returncode=process.returncode, error="original 900-second outer limit")
    result["process_wall_seconds"] = time.monotonic() - start
    if instrument == "cpu" and (root / "perf.data").exists():
        # perf's restrictive root-owned output otherwise prevents upload of
        # every timing row. Restore ownership only on this trial's exact file.
        subprocess.run(["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}",
                        str(root / "perf.data")], check=True)
    # Companion receipts use tmpfs during the benchmark, as in the original
    # workflow. Copy only after exit and release this trial's owned duplicate.
    for directory in ("native", "midge"):
        shutil.copytree(memory_root / directory, root / directory, dirs_exist_ok=True)
    shutil.rmtree(memory_root)
    try:
        result.update(native_result(root, build["sha"], cell))
        if result["returncode"] != 0:
            raise ValueError("benchmark/instrument process failed")
        result["valid"] = True
    except (ValueError, KeyError, OSError, json.JSONDecodeError) as error:
        result["error"] = str(error)
    if instrument == "cpu" and result["valid"]:
        render_cpu_profile(root, result)
    save(root / "trial.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--baseline-run", type=int, required=True)
    parser.add_argument("--candidate-run", type=int, required=True)
    parser.add_argument("--baseline-sha", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--candidate-source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cells", nargs="+", choices=CELLS, default=["A"])
    parser.add_argument("--host-repeat", type=int, choices=(1, 2, 3), default=1)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir()
    report = {"schema_version": "midge-checkpoint-pair-diagnostic.v1", "accepted": False,
              "workflow_sha": os.environ.get("GITHUB_SHA"), "cells": args.cells,
              "host_repeat": args.host_repeat,
              "original_workload_limits_unchanged": True, "complete": False,
              "planned_uninstrumented_trials": 12 * len(args.cells), "trials": [], "profiles": []}
    save(args.output / "report.json", report)
    builds = {}
    try:
        for role in ("baseline", "candidate"):
            builds[role] = download_build(args.repository, getattr(args, role + "_run"),
                                         getattr(args, role + "_sha"),
                                         getattr(args, role + "_source").resolve(), args.output / role)
        if builds["baseline"]["manifest"]["toolchain"] != builds["candidate"]["manifest"]["toolchain"]:
            raise ValueError("sources were built with different toolchains")
        toolchain = builds["baseline"]["manifest"]["toolchain"]
        subprocess.run(["rustup", "toolchain", "install", toolchain, "--profile", "minimal"], check=True)
        subprocess.run(["rustup", "default", toolchain], check=True)
        # Rotate cell order across hosts without changing any native workload.
        offset = (args.host_repeat - 1) % len(args.cells)
        ordered_cells = args.cells[offset:] + args.cells[:offset]
        for cell in ordered_cells:
            for index, (block, role) in enumerate(plan(), 1):
                item = trial(builds[role], block,
                             args.output / f"timing-{cell}-{index:02}-{role}", cell=cell)
                item.update(cell=cell, block=block, role=role, index=index)
                report["trials"].append(item)
                save(args.output / "report.json", report)
                print(json.dumps({key: item.get(key) for key in
                                  ("cell", "index", "role", "valid", "measured_elapsed_ns", "flush_p95_ns",
                                   "completed_compactions", "error")}), flush=True)
        report["complete"] = (len(report["trials"]) == report["planned_uninstrumented_trials"]
                              and all(t["valid"] for t in report["trials"]))
        save(args.output / "report.json", report)
        # Instrumentation begins only after the uninstrumented comparison.
        for cell in ordered_cells:
            for instrument in ("sync", "cpu"):
                for role in ("baseline", "candidate"):
                    try:
                        item = trial(builds[role], 1,
                                     args.output / f"profile-{cell}-{instrument}-{role}", instrument, cell)
                    except (ValueError, OSError, subprocess.SubprocessError) as error:
                        item = {"valid": False, "instrument": instrument, "error": str(error)}
                    item.update(role=role, cell=cell)
                    report["profiles"].append(item)
                    save(args.output / "report.json", report)
                    print(json.dumps({"cell": cell, "profile": instrument, "role": role,
                                      "valid": item["valid"], "error": item.get("error")}), flush=True)
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
        report["complete"] = False
        save(args.output / "report.json", report)
        raise
    if not report["complete"]:
        raise SystemExit("paired diagnostic incomplete; all adverse rows retained")


if __name__ == "__main__":
    main()
