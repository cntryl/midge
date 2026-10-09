#!/usr/bin/env python3
"""Finite native Engine-open experiment. Preserve every planned outcome."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import select
import selectors
import shutil
import subprocess
import time

VARIANTS = ("baseline", "key_index", "single_reader", "timers_off")


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def run_plain(command, directory):
    with (directory / "stdout.log").open("wb") as out, (directory / "stderr.log").open("wb") as err:
        try:
            result = subprocess.run(command, stdout=out, stderr=err, timeout=300, check=False,
                                    env={**os.environ, "RUST_BACKTRACE": "1"})
            return {"exit_code": result.returncode, "error": None}
        except subprocess.TimeoutExpired:
            return {"exit_code": None, "error": "fixed 300-second process bound exceeded"}


def parse_perf_ack(payload):
    # perf writes sizeof(EVLIST_CTL_CMD_ACK_TAG), including its C terminator.
    if payload not in (b"ack\n", b"ack\n\x00"):
        raise ValueError(f"unexpected sampler acknowledgement {payload!r}")
    return "ack"


def parse_perf_report(report_text):
    lost = re.findall(r"^# Total Lost Samples: (\d+)\s*$", report_text, re.MULTILINE)
    samples = re.findall(r"^# Samples: ([0-9.]+)([KMG]?) of event '[^']+'\s*$", report_text, re.MULTILINE)
    return {
        "lost_samples": int(lost[0]) if len(lost) == 1 else None,
        "samples": int(float(samples[0][0])*{"":1,"K":1000,"M":1000000,"G":1000000000}[samples[0][1]]) if len(samples) == 1 else None,
    }


def run_cpu(command, directory):
    control, ack = directory / "control.fifo", directory / "ack.fifo"
    os.mkfifo(control, 0o600)
    os.mkfifo(ack, 0o600)
    ctl_fd = os.open(control, os.O_RDWR | os.O_NONBLOCK)
    ack_fd = os.open(ack, os.O_RDWR | os.O_NONBLOCK)
    perf_command = ["sudo", "-n", "perf", "record", "-F", "999", "-g", "--call-graph", "dwarf",
                    "--no-buildid", "--no-buildid-cache",
                    "--delay=-1", f"--control=fifo:{control},{ack}", "-o", str(directory / "perf.data"),
                    "--", "runuser", "-u", os.environ.get("USER", "runner"), "--", "env",
                    "MIDGE_RECOVERY_CPU_CONTROL=stdio", "RUST_BACKTRACE=1", *command]
    receipt = {"command": perf_command, "boundaries": [], "exit_code": None, "error": None}
    write(directory / "sampler.json", receipt)
    process = None
    with (directory / "stdout.log").open("wb") as out, (directory / "stderr.log").open("wb") as err:
        try:
            process = subprocess.Popen(perf_command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=err, start_new_session=True)
            selector = selectors.DefaultSelector()
            selector.register(process.stdout, selectors.EVENT_READ)
            pending = b""
            expires = time.monotonic() + 300
            while selector.get_map():
                if time.monotonic() >= expires:
                    raise TimeoutError("fixed 300-second process bound exceeded")
                for key, _ in selector.select(0.1):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    out.write(chunk)
                    out.flush()
                    pending += chunk
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        if not line.startswith(b"MIDGE_PROFILE_BOUNDARY "):
                            continue
                        action = line.split()[-1].decode()
                        expected = "enable" if not receipt["boundaries"] else "disable"
                        if action != expected or len(receipt["boundaries"]) >= 2:
                            raise ValueError("unexpected native sampling boundary")
                        started = time.monotonic_ns()
                        os.write(ctl_fd, (action + "\n").encode())
                        if not select.select([ack_fd], [], [], 5)[0]:
                            raise TimeoutError("sampler did not acknowledge within five seconds")
                        raw_reply = os.read(ack_fd, 4096)
                        reply = parse_perf_ack(raw_reply)
                        receipt["boundaries"].append({"action": action, "ack": reply, "ack_raw_hex": raw_reply.hex(), "elapsed_ns": time.monotonic_ns()-started})
                        write(directory / "sampler.json", receipt)
                        process.stdin.write(b"CONTINUE\n")
                        process.stdin.flush()
            receipt["exit_code"] = process.wait(timeout=5)
            if [row["action"] for row in receipt["boundaries"]] != ["enable", "disable"]:
                raise ValueError("both sampling boundaries required")
        except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired) as error:
            receipt["error"] = str(error)
            if process is not None and process.poll() is None:
                subprocess.run(["sudo", "-n", "kill", "-TERM", "--", f"-{process.pid}"], check=False, stdout=out, stderr=err)
                try:
                    receipt["exit_code"] = process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    subprocess.run(["sudo", "-n", "kill", "-KILL", "--", f"-{process.pid}"], check=False, stdout=out, stderr=err)
                    receipt["exit_code"] = process.wait(timeout=5)
        finally:
            os.close(ctl_fd)
            os.close(ack_fd)
            control.unlink()
            ack.unlink()
    data = directory / "perf.data"
    if data.exists():
        subprocess.run(["sudo", "-n", "chown", f"{os.getuid()}:{os.getgid()}", str(data)], check=True)
        report_command = ["perf", "report", "--stdio", "--no-children", "--no-inline", "-i", str(data)]
        receipt["report_command"] = report_command
        receipt["report_exit_code"] = None
        with (directory / "perf-report.txt").open("wb") as out, (directory / "perf-report-error.log").open("wb") as err:
            try:
                report = subprocess.run(report_command, stdout=out, stderr=err, timeout=60, check=False,
                                        env={**os.environ, "DEBUGINFOD_URLS": ""})
                receipt["report_exit_code"] = report.returncode
            except subprocess.TimeoutExpired:
                receipt["report_error"] = "fixed 60-second report bound exceeded"
        report_text = (directory / "perf-report.txt").read_text()
        receipt.update(parse_perf_report(report_text))
        receipt["data_sha256"] = hashlib.sha256(data.read_bytes()).hexdigest()
    write(directory / "sampler.json", receipt)
    return receipt


def validate_process(outcome, directory):
    if outcome["exit_code"] != 0 or outcome["error"]:
        raise ValueError("native process or sampling control failed")
    # A caught worker-thread panic can leave both exit status and JSON successful.
    # Require the retained stderr too, including after the sampled open boundary.
    if b"panicked at" in (directory / "stderr.log").read_bytes():
        raise ValueError("native process stderr contains Rust panic; retained native failure")


def validate_native(path, source, binary, fixture, variant):
    receipt = json.loads(path.read_text())
    if not receipt["complete"] or not receipt["source_clean"] or receipt["source_sha"] != source or receipt["binary_sha256"] != binary:
        raise ValueError("incomplete or mismatched immutable-source receipt")
    result = receipt["result"]
    if result["fixture_sha256"] != fixture or result["variant"] != variant:
        raise ValueError("trial input or mode differs")
    if len(result["verification"]) != 6 or not all(row["passed"] and row["value_mismatches"] == 0 and row["expected_rows"] == row["actual_rows"] for row in result["verification"]) or result["shutdowns"] != 2:
        raise ValueError("six exact checks and two shutdowns required")
    if (result["memory_bytes"], result["local_storage_bytes"], result["memtable_bytes"], result["open_deadline_seconds"]) != (134217728, 1073741824, 262144, 30):
        raise ValueError("resource/deadline policy differs")
    if result["runtime"]["wal_cloud_durable_seq"] != result["committed_wal_frontier"] or result["runtime"]["current_sequence"] < result["committed_wal_frontier"] or result["committed_wal_frontier"] > result["frontier"] or result["runtime"]["salvage_mode_opens"] != 0:
        raise ValueError("durable WAL frontier or salvage policy differs")
    events = result["native_open_events"]
    facts = [event["attribution"] for event in events if "attribution" in event]
    coverage = [event for event in events if event.get("phase") == "coverage"]
    if len(facts) != 1 or len(coverage) != 1:
        raise ValueError("one native coverage owner required")
    fact = facts[0]
    if fact["variant"] != variant or fact["budget_final"] != 0 or fact["budget_peak"] > fact["budget_limit"] or fact["peak_readers"] > fact["reader_limit"] or fact["reader_limit"] > 4:
        raise ValueError("coverage resource/variant proof differs")
    for name in ("point_probes", "exact_hits", "checkpoint_releases", "index_builds"):
        if fact[name] <= (1 if name == "index_builds" else 0):
            raise ValueError(f"fixture did not exercise {name}")
    if coverage[0]["reader_evictions"] == 0 or (variant != "key_index" and fact["predicate_rejections"] == 0):
        raise ValueError("fixture did not exercise reader churn and predicate rejection")
    reads = fact["remote_ranges"]
    if not fact["remote_observer_attached"] or reads["attempts"] == 0 or reads["attempts"] != reads["completed"] or reads["failures"] != 0:
        raise ValueError("remote range receipts incomplete")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("target/recovery-attribution"))
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    build = ["cargo", "build", "--release", "--example", "recovery_attribution", "--features", "internal-testing,failpoints", "--message-format=json-render-diagnostics"]
    environment = {**os.environ, "CARGO_PROFILE_RELEASE_DEBUG": "1", "CARGO_INCREMENTAL": "0"}
    with (root / "cargo.jsonl").open("wb") as out, (root / "build.log").open("wb") as err:
        subprocess.run(build, env=environment, stdout=out, stderr=err, check=True)
    executable = None
    for line in (root / "cargo.jsonl").read_text().splitlines():
        record = json.loads(line)
        if record.get("reason") == "compiler-artifact" and record.get("target", {}).get("name") == "recovery_attribution" and record.get("executable"):
            executable = record["executable"]
    if executable is None:
        raise ValueError("missing Cargo executable receipt")
    # Record against the archived, immutable executable so skipped perf build-id
    # postprocessing cannot accidentally resolve a later rebuild at that pathname.
    archived_executable = root / "binary" / "recovery_attribution"
    archived_executable.parent.mkdir()
    shutil.copy2(executable, archived_executable)
    executable = str(archived_executable)
    binary = hashlib.sha256(Path(executable).read_bytes()).hexdigest()
    fixture = root / "fixture"
    seed = root / "seed"
    seed.mkdir()
    outcome = run_plain([executable, "seed", str(fixture), str(seed / "native.json")], seed)
    write(seed / "process.json", outcome)
    validate_process(outcome, seed)
    fixture_sha = json.loads((fixture / "fixture.json").read_text())["inventory"]["sha256"]
    manifest = {"source_sha": source, "binary_sha256": binary, "fixture_sha256": fixture_sha, "command": build,
                "binary_path": "binary/recovery_attribution",
                "profile_release_debug": 1, "planned_plain": 12, "planned_cpu": 4, "trials": [], "complete": False}
    write(root / "campaign.json", manifest)
    planned = [(variant, repeat, False) for repeat in range(1, 4) for variant in VARIANTS[repeat-1:]+VARIANTS[:repeat-1]]
    planned += [(variant, 1, True) for variant in VARIANTS]
    failures = []
    for variant, repeat, sampled in planned:
        name = f"{variant}-r{repeat}-{'cpu' if sampled else 'plain'}"
        directory = root / name
        directory.mkdir()
        command = [executable, "trial", str(fixture), str(directory / "database"), variant, str(directory / "native.json")]
        outcome = run_cpu(command, directory) if sampled else run_plain(command, directory)
        row = {"name": name, "variant": variant, "repeat": repeat, "sampled": sampled, "process": outcome, "accepted": False}
        try:
            validate_process(outcome, directory)
            result = validate_native(directory / "native.json", source, binary, fixture_sha, variant)
            if sampled and (outcome.get("report_exit_code") != 0 or outcome.get("lost_samples") != 0 or not outcome.get("samples")):
                raise ValueError("native profile report failed")
            row["open_ns"] = result["open_ns"]
            row["target_met"] = result["target_met"]
            row["accepted"] = True
        except (OSError, KeyError, TypeError, ValueError) as error:
            row["error"] = str(error)
            failures.append(name)
        manifest["trials"].append(row)
        write(root / "campaign.json", manifest)
        write(directory / "process.json", outcome)
        print(json.dumps(row), flush=True)
    manifest["complete"] = not failures
    manifest["failures"] = failures
    write(root / "campaign.json", manifest)
    if failures:
        raise SystemExit("planned trials failed: " + ", ".join(failures))


if __name__ == "__main__":
    main()
