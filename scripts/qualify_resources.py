#!/usr/bin/env python3
"""Linux qualification with a finite ext4 database and a cgroup-limited child.

Run as root after building the qualification test binary and starting Sqrzl.
The fixture controller, Docker client, sampler and evidence are outside the
engine container's cgroup and outside its database filesystem.
"""
from __future__ import annotations

import argparse
import errno
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

TEST = "operational::should_recover_cloud_backlog_after_complete_local_disk_loss"


def run(*args: str, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def save(path: Path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def inspect(container: str):
    return json.loads(run("docker", "inspect", container, capture_output=True).stdout)[0]


def read_number(path: Path) -> int:
    return int(path.read_text().strip())


def child() -> int:
    binary = Path(sys.argv[2]).resolve()
    config = Path(os.environ["MIDGE_OPERATIONAL_CHILD_CONFIG"]).resolve()
    campaign = json.loads(config.read_text())
    artifacts = Path(campaign["artifacts"])
    phase = os.environ["MIDGE_OPERATIONAL_CHILD_PHASE"]
    mount = Path(os.environ["MIDGE_RESOURCE_MOUNT"])
    filler = mount / "deliberate-exhaustion.bin"
    limit = int(os.environ["MIDGE_RESOURCE_PROCESS_BYTES"])
    image = os.environ["MIDGE_RESOURCE_IMAGE"]
    label = os.environ["MIDGE_RESOURCE_RUN_ID"]
    command = ["docker", "create", "--label", f"midge.resource-run={label}",
               "--network", "host", "--memory", str(limit), "--memory-swap", str(limit),
               "--pids-limit", "256", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
               "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,size=16777216"]
    for path, readonly in [(binary, True), (config.parent, True), (artifacts, False), (mount, False)]:
        command += ["--mount", f"type=bind,source={path},target={path}" + (",readonly" if readonly else "")]
    for key in ["MIDGE_OPERATIONAL_CHILD_CONFIG", "MIDGE_OPERATIONAL_CHILD_PHASE", "MIDGE_QUALIFICATION_REVISION"]:
        if key in os.environ:
            command += ["--env", f"{key}={os.environ[key]}"]
    ready = artifacts / f"{phase}-runner-ready"
    ready.unlink(missing_ok=True)
    command += ["--env", "MIDGE_RESOURCE_COLLECTOR_EXTERNAL=1"]
    command += [image, "/bin/sh", "-c", 'while [ ! -e "$1" ]; do sleep 0.02; done; shift; exec "$@"', "midge-child", str(ready), str(binary), *sys.argv[3:]]
    container = run(*command, capture_output=True).stdout.strip()
    peak_memory = 0
    peak_files = 0
    minimum_available = os.statvfs(mount).f_bavail * os.statvfs(mount).f_frsize
    cgroup = None
    sampled_limit = None
    killed = False
    started = time.monotonic()
    deadline = started + max(1, campaign["profile"]["timeout_seconds"] - 15)
    process = None
    try:
        if phase == "disk-exhausted":
            # Fill real allocated blocks outside the database directory. This is
            # independent of engine admission and must produce kernel ENOSPC.
            with filler.open("wb", buffering=0) as output:
                block = bytes(1024 * 1024)
                try:
                    while True:
                        output.write(block)
                except OSError as error:
                    if error.errno != errno.ENOSPC:
                        raise
            os.sync()
        process = subprocess.Popen(["docker", "start", "--attach", container])
        while process.poll() is None:
            if cgroup is None:
                pid = inspect(container)["State"]["Pid"]
                if pid:
                    entries = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
                    relative = next(line[3:] for line in entries if line.startswith("0::"))
                    cgroup = Path("/sys/fs/cgroup") / relative.lstrip("/")
                    sampled_limit = read_number(cgroup / "memory.max")
                    assert sampled_limit == limit, "Docker did not install the requested memory limit"
                    assert read_number(cgroup / "memory.swap.max") == 0, "swap must not hide memory exhaustion"
                    ready.touch()
            if cgroup is not None:
                try:
                    peak_memory = max(peak_memory, read_number(cgroup / "memory.peak"))
                except (FileNotFoundError, OSError):
                    pass  # The kernel removes the cgroup when the child exits.
            files = 0
            for path in Path(campaign["cache"]).rglob("*"):
                try:
                    if path.is_file():
                        files += path.stat().st_size
                except FileNotFoundError:
                    pass
            peak_files = max(peak_files, files)
            stat = os.statvfs(mount)
            minimum_available = min(minimum_available, stat.f_bavail * stat.f_frsize)
            if phase == "terminated" and (artifacts / "terminate-at-checkpoint").exists() and not killed:
                run("docker", "kill", "--signal", "KILL", container, stdout=subprocess.DEVNULL)
                killed = True
            if time.monotonic() > deadline:
                run("docker", "kill", container, stdout=subprocess.DEVNULL)
                raise TimeoutError(f"{phase} exceeded its external resource watchdog")
            time.sleep(0.02)
        state = inspect(container)
        code = state["State"]["ExitCode"]
        report_path = artifacts / f"{phase}.json"
        if report_path.exists():
            report = json.loads(report_path.read_text())
            assert report["file_bytes_observed_externally"]
            report["peak_local_file_bytes"] = peak_files
            save(report_path, report)
        save(artifacts / f"{phase}-resources.json", {
            "engine_pool_bytes": campaign["profile"]["memory_bytes"],
            "process_cgroup_limit_bytes": limit,
            "observed_cgroup_memory_peak_bytes": peak_memory,
            "observed_peak_above_engine_pool_bytes": max(0, peak_memory - campaign["profile"]["memory_bytes"]),
            "engine_local_admission_bytes": campaign["profile"]["local_bytes"],
            "minimum_filesystem_available_bytes": minimum_available,
            "memory_limit_readback_bytes": sampled_limit,
            "oom_killed": state["State"]["OOMKilled"],
            "deliberate_sigkill": killed,
            "exit_code": code,
            "elapsed_seconds": time.monotonic() - started,
            "collector_outside_child_limits": True,
            "observed_peak_local_file_bytes": peak_files,
        })
        assert not state["State"]["OOMKilled"], f"{phase} exceeded its process memory limit"
        assert sampled_limit == limit and peak_memory > 0, "missing cgroup enforcement evidence"
        if phase == "terminated":
            assert killed and code == 137, "expected an external SIGKILL at the durable checkpoint"
        if phase in ("verified", "restored") and code == 0:
            report = json.loads((artifacts / f"{phase}.json").read_text())
            assert report["verification_complete"]
            metrics = report["runtime_metrics"]
            assert metrics["local_storage"]["usage"]["reservations"] == 0, "storage reservation leak after quiescence"
            assert metrics["pinned_ssts"] == 0, "reader pin leak after verification"
        return code
    finally:
        subprocess.run(["docker", "rm", "--force", container], stdout=subprocess.DEVNULL, check=False)
        if process is not None:
            process.wait(timeout=10)
        filler.unlink(missing_ok=True)
        os.sync()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--disk-bytes", type=int, default=64 * 1024**2)
    parser.add_argument("--engine-bytes", type=int, default=64 * 1024**2)
    parser.add_argument("--process-bytes", type=int, default=384 * 1024**2)
    parser.add_argument("--wal-bytes", type=int, default=128 * 1024**2)
    parser.add_argument("--value-bytes", type=int, default=8192)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--image", default="ubuntu:26.04")
    args = parser.parse_args()
    if sys.platform != "linux" or os.geteuid() != 0:
        parser.error("requires a Linux host and root for the isolated loop filesystem and cgroup observations")
    if min(args.disk_bytes, args.engine_bytes, args.process_bytes, args.wal_bytes, args.value_bytes, args.timeout) <= 0:
        parser.error("all limits must be positive")
    if args.process_bytes <= args.engine_bytes:
        parser.error("process limit must explicitly include overhead above the engine pools")
    artifacts = args.artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    run("docker", "pull", args.image)
    image = json.loads(run("docker", "image", "inspect", args.image, capture_output=True).stdout)[0]["Id"]
    run_id = str(uuid.uuid4())
    with tempfile.TemporaryDirectory(prefix="midge-resources-") as temporary:
        root = Path(temporary)
        disk = root / "database.ext4"
        mount = root / "database"
        mount.mkdir()
        with disk.open("wb") as output:
            os.posix_fallocate(output.fileno(), 0, args.disk_bytes)
        run("mkfs.ext4", "-q", "-F", "-m", "0", str(disk))
        run("mount", "-o", "loop,nodev,nosuid", str(disk), str(mount))
        try:
            stat = os.statvfs(mount)
            usable = stat.f_bavail * stat.f_frsize
            headroom = max(4 * 1024**2, usable // 20)
            admission = usable - headroom
            assert admission > 0 and args.wal_bytes > admission
            save(artifacts / "resource-contract.json", {
                "disk_image_bytes": args.disk_bytes,
                "filesystem_initial_usable_bytes": usable,
                "filesystem_headroom_bytes": headroom,
                "engine_local_admission_bytes": admission,
                "engine_pool_bytes": args.engine_bytes,
                "process_cgroup_limit_bytes": args.process_bytes,
                "image_id": image,
                "revision": os.environ.get("MIDGE_QUALIFICATION_REVISION"),
            })
            environment = os.environ | {
                "MIDGE_QUALIFICATION_LOCAL_BYTES": str(admission),
                "MIDGE_QUALIFICATION_WAL_BYTES": str(args.wal_bytes),
                "MIDGE_QUALIFICATION_MEMORY_BYTES": str(args.engine_bytes),
                "MIDGE_QUALIFICATION_VALUE_BYTES": str(args.value_bytes),
                "MIDGE_QUALIFICATION_TIMEOUT_SECONDS": str(args.timeout),
                "MIDGE_QUALIFICATION_ARTIFACT_DIR": str(artifacts),
                "MIDGE_QUALIFICATION_CACHE_DIR": str(mount),
                "MIDGE_QUALIFICATION_CHILD_RUNNER": str(Path(__file__).resolve()),
                "MIDGE_RESOURCE_MOUNT": str(mount),
                "MIDGE_RESOURCE_PROCESS_BYTES": str(args.process_bytes),
                "MIDGE_RESOURCE_IMAGE": image,
                "MIDGE_RESOURCE_RUN_ID": run_id,
            }
            with (artifacts / "controller.log").open("w") as log:
                run(str(args.binary.resolve()), "--exact", TEST, "--ignored", "--nocapture", env=environment, stdout=log, stderr=subprocess.STDOUT)
        finally:
            remaining = run("docker", "ps", "-aq", "--filter", f"label=midge.resource-run={run_id}", capture_output=True).stdout.split()
            if remaining:
                run("docker", "rm", "--force", *remaining)
            run("umount", str(mount))
    return 0


if __name__ == "__main__":
    sys.exit(child() if sys.argv[1:2] == ["--child"] else main())
