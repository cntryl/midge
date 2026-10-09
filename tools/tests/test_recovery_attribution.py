#!/usr/bin/env python3
"""Artifact acceptance must reject partial correctness and bound/source drift."""
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import unittest
from unittest.mock import patch
import recovery_attribution
from recovery_attribution import parse_perf_ack, parse_perf_report, validate_native

FIXTURES = Path(__file__).parent / "fixtures" / "recovery_attribution"


def native_receipt(variant="baseline"):
    checks = [{"passed": True, "value_mismatches": 0, "expected_rows": 8192, "actual_rows": 8192} for _ in range(6)]
    fact = {"variant": variant, "budget_final": 0, "budget_peak": 1024, "budget_limit": 4096,
            "peak_readers": 4, "reader_limit": 4, "point_probes": 16384, "exact_hits": 8192,
            "checkpoint_releases": 16, "index_builds": 17, "predicate_rejections": 8192,
            "remote_observer_attached": True, "remote_ranges": {"attempts": 40, "completed": 40, "failures": 0}}
    result = {"fixture_sha256": "fixture", "variant": variant, "verification": checks, "shutdowns": 2,
              "memory_bytes": 134217728, "local_storage_bytes": 1073741824, "memtable_bytes": 262144,
              "frontier": 100, "committed_wal_frontier": 98, "runtime": {"wal_cloud_durable_seq": 98, "current_sequence": 120, "salvage_mode_opens": 0}, "open_deadline_seconds": 30, "native_open_events": [{"attribution": fact}, {"phase": "coverage", "reader_evictions": 20}]}
    return {"complete": True, "source_clean": True, "source_sha": "source", "binary_sha256": "binary", "result": result}


def run_fake_campaign(root, panic_trials=(), seed_panic=False, null_trials=()):
    """Exercise real campaign acceptance without native execution or perf access."""
    executable = root / "executable"
    executable.write_bytes(b"immutable executable")
    binary = hashlib.sha256(executable.read_bytes()).hexdigest()
    output = root / "campaign"

    def build(command, **kwargs):
        artifact = {"reason": "compiler-artifact", "target": {"name": "recovery_attribution"}, "executable": str(executable)}
        kwargs["stdout"].write(json.dumps(artifact).encode() + b"\n")

    def process(command, directory):
        (directory / "stdout.log").write_bytes(b"")
        sampled = directory.name.endswith("-cpu")
        panic = seed_panic if command[1] == "seed" else directory.name in panic_trials
        stderr = (FIXTURES / ("timers_off-r1-cpu.stderr.txt" if sampled else "baseline-r3-plain.stderr.txt")).read_bytes() if panic else b""
        (directory / "stderr.log").write_bytes(stderr)
        if command[1] == "seed":
            fixture = Path(command[2])
            fixture.mkdir()
            (fixture / "fixture.json").write_text(json.dumps({"inventory": {"sha256": "fixture"}}))
            (directory / "native.json").write_text(json.dumps({"complete": True}))
        else:
            receipt = native_receipt(command[4])
            receipt["binary_sha256"] = binary
            receipt["result"].update(open_ns=2000000000, target_met=True)
            if directory.name in null_trials:
                receipt["result"] = None
            (directory / "native.json").write_text(json.dumps(receipt))
        outcome = {"exit_code": 0, "error": None}
        if sampled:
            outcome.update(report_exit_code=0, lost_samples=0, samples=2000)
            (directory / "perf.data").write_bytes(b"retained native profile")
            (directory / "sampler.json").write_text(json.dumps(outcome))
        return outcome

    with patch.object(sys, "argv", ["recovery_attribution.py", "--output", str(output)]), \
         patch("recovery_attribution.subprocess.check_output", return_value="source\n"), \
         patch("recovery_attribution.subprocess.run", side_effect=build), \
         patch("recovery_attribution.run_plain", side_effect=process), \
         patch("recovery_attribution.run_cpu", side_effect=process), \
         patch("builtins.print"):
        recovery_attribution.main()


class AcceptanceTests(unittest.TestCase):
    def test_should_enable_backtrace_when_plain_native_environment_disables_it(self):
        # Arrange: seed and plain trials share the same native subprocess path.
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"RUST_BACKTRACE": "0"}):
            root = Path(directory)
            command = [sys.executable, "-c", "import os; print(os.environ['RUST_BACKTRACE'])"]
            # Act
            outcome = recovery_attribution.run_plain(command, root)
            # Assert
            self.assertEqual(outcome, {"exit_code": 0, "error": None})
            self.assertEqual((root / "stdout.log").read_text(), "1\n")

    def test_should_enable_native_backtrace_when_profiler_drops_privileges(self):
        # Arrange: no perf privilege is needed to inspect its retained launch receipt.
        with tempfile.TemporaryDirectory() as directory, \
             patch("recovery_attribution.subprocess.Popen", side_effect=OSError("not launched")):
            root = Path(directory)
            command = ["immutable-native-executable", "trial"]
            # Act
            receipt = recovery_attribution.run_cpu(command, root)
            # Assert: runuser's inner environment must set the native setting.
            inner_environment = receipt["command"][receipt["command"].index("env") + 1:]
            self.assertEqual(inner_environment, ["MIDGE_RECOVERY_CPU_CONTROL=stdio", "RUST_BACKTRACE=1", *command])

    def test_should_fail_campaign_and_retain_all_trials_when_zero_status_stderr_contains_runtime_panic(self):
        # Arrange: exact plain/CPU stderr payloads from hosted run 37986974698.
        failures = {"baseline-r3-plain", "key_index-r3-plain", "single_reader-r2-plain",
                    "single_reader-r3-plain", "timers_off-r1-cpu"}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Act: every native process returns zero and emits complete correctness receipts.
            with self.assertRaisesRegex(SystemExit, "planned trials failed"):
                run_fake_campaign(root, panic_trials=failures)
            campaign = root / "campaign"
            manifest = json.loads((campaign / "campaign.json").read_text())
            # Assert: no replacement, early stop, missing outcome or successful campaign.
            self.assertFalse(manifest["complete"])
            self.assertEqual(len(manifest["trials"]), 16)
            self.assertEqual(set(manifest["failures"]), failures)
            for trial in manifest["trials"]:
                self.assertEqual(trial["accepted"], trial["name"] not in failures)
                retained = campaign / trial["name"]
                self.assertEqual(json.loads((retained / "process.json").read_text())["exit_code"], 0)
                self.assertTrue(json.loads((retained / "native.json").read_text())["complete"])
                self.assertTrue((retained / "stderr.log").exists())
                if trial["name"] in failures:
                    self.assertIn("panic", trial["error"])
                    self.assertIn(b"user-provided comparison function", (retained / "stderr.log").read_bytes())
                if trial["sampled"]:
                    self.assertTrue((retained / "perf.data").exists())
                    self.assertTrue((retained / "sampler.json").exists())

    def test_should_reject_fixture_when_zero_status_seed_stderr_contains_runtime_panic(self):
        # Arrange: fixture construction has the same process-wide no-panic requirement.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Act / Assert
            with self.assertRaisesRegex(ValueError, "panic"):
                run_fake_campaign(root, seed_panic=True)
            seed = root / "campaign" / "seed"
            self.assertEqual(json.loads((seed / "process.json").read_text())["exit_code"], 0)
            self.assertTrue(json.loads((seed / "native.json").read_text())["complete"])
            self.assertIn(b"panicked at", (seed / "stderr.log").read_bytes())

    def test_should_reject_process_when_retained_stderr_is_missing(self):
        # Arrange: successful status alone cannot establish the no-panic requirement.
        with tempfile.TemporaryDirectory() as directory:
            # Act / Assert
            with self.assertRaises(OSError):
                recovery_attribution.validate_process({"exit_code": 0, "error": None}, Path(directory))

    def test_should_fail_campaign_when_native_result_is_null(self):
        # Arrange: an explicit null attribution cannot qualify failed evidence.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Act / Assert
            with self.assertRaisesRegex(SystemExit, "planned trials failed"):
                run_fake_campaign(root, null_trials={"baseline-r1-plain"})
            manifest = json.loads((root / "campaign" / "campaign.json").read_text())
            self.assertFalse(manifest["complete"])
            self.assertEqual(len(manifest["trials"]), 16)
            self.assertEqual(manifest["failures"], ["baseline-r1-plain"])

    def test_should_read_event_samples_when_lost_sample_header_precedes_them(self):
        # Arrange: exact header order retained in the hosted baseline profile.
        header = "# Total Lost Samples: 0\n#\n# Samples: 2K of event 'cycles:P'\n"
        # Act / Assert
        self.assertEqual(parse_perf_report(header), {"samples": 2000, "lost_samples": 0})
        self.assertEqual(parse_perf_report("# Total Lost Samples: 0\n"), {"samples": None, "lost_samples": 0})
        self.assertIsNone(parse_perf_report(header + "# Samples: 1K of event 'other'\n")["samples"])
        self.assertEqual(parse_perf_report(header.replace("Lost Samples: 0", "Lost Samples: 3"))["lost_samples"], 3)

    def test_should_accept_native_packet_when_perf_ack_has_c_terminator(self):
        # Arrange: packet retained in all four original failed sampler controls.
        packet = b"ack\n\x00"
        # Act / Assert
        self.assertEqual(parse_perf_ack(packet), "ack")
        for malformed in (b"ack", b"bad\n\x00", b"ack\n\x00\x00", b"ack\nother"):
            with self.assertRaises(ValueError):
                parse_perf_ack(malformed)

    def test_should_reject_false_acceptance_when_native_evidence_is_partial(self):
        # Arrange: one internally consistent receipt and independent bad mutations.
        receipt = native_receipt()
        result = receipt["result"]
        mutations = [
            lambda r: r.update(complete=False), lambda r: r.update(source_sha="other"),
            lambda r: r["result"].update(fixture_sha256="other"),
            lambda r: r["result"]["verification"][0].update(value_mismatches=1),
            lambda r: r["result"]["runtime"].update(wal_cloud_durable_seq=97),
            lambda r: r["result"].update(shutdowns=1), lambda r: r["result"].update(memory_bytes=268435456),
            lambda r: r["result"]["native_open_events"][0]["attribution"].update(budget_final=1),
            lambda r: r["result"]["native_open_events"][0]["attribution"].update(peak_readers=5),
            lambda r: r["result"]["native_open_events"][0]["attribution"].update(index_builds=1),
            lambda r: r["result"]["native_open_events"][0]["attribution"]["remote_ranges"].update(completed=39),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"native.json"
            path.write_text(json.dumps(receipt))
            # Act / Assert
            self.assertEqual(validate_native(path,"source","binary","fixture","baseline"),result)
            for mutate in mutations:
                bad=copy.deepcopy(receipt);mutate(bad);path.write_text(json.dumps(bad))
                with self.assertRaises(ValueError):
                    validate_native(path,"source","binary","fixture","baseline")


if __name__ == "__main__":
    unittest.main()
