import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import profile_checkpoint_pair as pair


class CheckpointPairTests(unittest.TestCase):
    def test_should_keep_benchmark_unprivileged_when_cpu_sampler_requires_root(self):
        # Arrange
        environment = {key: "original-" + key for key in pair.BENCHMARK_ENV}
        command = ["/usr/bin/time", "-v", "/actual/sealed/executable", "--bench"]
        # Act
        profiled = pair.cpu_command(command, Path("/owned/trial"), environment)
        # Assert
        self.assertEqual(profiled[:4], ["sudo", "-n", "perf", "record"])
        child = profiled[profiled.index("--") + 1:]
        self.assertEqual(child[0:2], ["runuser", "-u"])
        self.assertEqual(child[3:5], ["--", "env"])
        self.assertEqual(child[-len(command):], command)
        for key, value in environment.items():
            self.assertIn(key + "=" + value, child)

    def test_should_balance_source_and_position_when_planning_original_trials(self):
        # Arrange
        expected_first = ["baseline", "candidate", "candidate", "baseline"]
        # Act
        planned = pair.plan()
        # Assert
        self.assertEqual(len(planned), 12)
        self.assertEqual([role for _, role in planned[:4]], expected_first)
        self.assertEqual(sum(role == "baseline" for _, role in planned), 6)
        for block in range(1, 4):
            roles = [role for repeat, role in planned if repeat == block]
            self.assertEqual(roles.count("baseline"), 2)
            self.assertEqual(roles.count("candidate"), 2)
        self.assertEqual(planned[0][1], planned[3][1])
        self.assertNotEqual(planned[3][1], planned[4][1])

    def test_should_reject_declared_source_when_actual_checkout_differs(self):
        # Arrange
        manifest = {"sha": "b" * 40}
        # Act / Assert
        with patch.object(pair, "checked", return_value="a" * 40):
            with self.assertRaisesRegex(ValueError, "actual checkout"):
                pair.verify_source(Path("unused"), "b" * 40, manifest)

    def test_should_reject_failed_ack_or_reopen_when_native_receipt_is_incomplete(self):
        # Arrange
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / "midge" / "actual-attempt"
            directory.mkdir(parents=True)
            status = {"git_commit": "a" * 40, "status": "passed", "phase": "complete",
                      "terminal_error": None, "verified": True, "reopened_verified": False,
                      "completed_cycles": 256, "total_cycles": 256,
                      "rows_per_cycle": 1024, "families": 1, "acknowledged_rows": 262144}
            (directory / "workload-status.json").write_text(json.dumps(status))
            # Act / Assert: a green child exit alone is never row validation.
            with self.assertRaisesRegex(ValueError, "ACK/reopen proof"):
                pair.native_result(root, "a" * 40)

    def test_should_reject_lock_mismatch_when_checkout_hash_matches(self):
        # Arrange
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            (source / "Cargo.lock").write_text("actual pinned dependencies")
            manifest = {"sha": "a" * 40, "tree": "c" * 40,
                        "cargo_lock_sha256": "0" * 64,
                        "profile": "bench/release", "binary": pair.BINARY}
            # Act / Assert
            with patch.object(pair, "checked", side_effect=["a" * 40, "", "c" * 40]):
                with self.assertRaisesRegex(ValueError, "lock identity"):
                    pair.verify_source(source, "a" * 40, manifest)


if __name__ == "__main__":
    unittest.main()
