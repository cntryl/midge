import copy
import json
from pathlib import Path
import tempfile
import unittest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from finalize_stress_artifacts import COUNTERS, finalize, selected_workload

SHA = "64380962bd532cd4f9e6fe01756120ac372259f4"
SUITE = "tier5-stress-workloads"
WORKLOAD = "tier5_write_pressure_azure"
STEM = "01791116544414606578-0000007318-00000000000000000000"
PID = 7318


class FinalizationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.stress = self.root / "stress"
        self.artifacts = self.root / "artifacts"
        self.status_path = self.artifacts / "workload" / "workload-status.json"
        self.receipt_path = self.stress / SUITE / f"{STEM}.json"
        self.status = {"benchmark_workload": WORKLOAD, "process_id": PID, "git_commit": SHA,
                       "status": "running", "phase": "shutdown", "stage_index": None,
                       "stage": "shutdown-before-recovery", "acknowledged_transactions": 100}
        self.receipt = {"schema_version": "cntryl-stress.v2", "suite": SUITE, "started_at": STEM,
                        "environment": {"git_commit": SHA, "command_line": ["bin", "--workload", WORKLOAD]},
                        "benchmark_specs": [{"name": "custom diagnostic stage", "metadata": {"trust_class": "diagnostic"}}],
                        "summaries": [{"correctness": {"passed": True}, "metadata": {"trust_class": "diagnostic"}}],
                        "samples": [{"counters": {"failures": 0}}]}
        self.save()

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def save(self):
        self.write(self.status_path, self.status)
        self.write(self.receipt_path, self.receipt)
        self.write(self.receipt_path.with_name("latest.json"), self.receipt)

    def run_finalizer(self, outcome="failure", workload=WORKLOAD):
        result = finalize(self.stress, self.artifacts, workload, SUITE, SHA, outcome)
        return result, json.loads(self.status_path.read_text())

    def timeout(self):
        self.receipt["benchmark_specs"][0]["metadata"].update(
            benchmark_error="made no progress", failure_kind="no_progress_timeout")
        self.receipt["samples"][0]["counters"]["failures"] = 1
        self.receipt["summaries"][0]["correctness"]["passed"] = False
        self.save()

    def test_should_finalize_typed_timeout_and_exclude_latest_alias(self):
        self.timeout()
        result, status = self.run_finalizer()
        self.assertEqual(result["matching_receipts"], 1)
        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["finalization"]["failure_kind"], "no_progress_timeout")
        self.assertEqual(status["finalization"]["source"], "cntryl-stress")
        self.assertEqual(status["phase"], "shutdown")
        self.assertNotIn("partial_stage_counters", status)
        self.assertEqual(result["consistency_errors"], [])

    def test_should_accept_custom_named_diagnostic_success_rows(self):
        self.status.update(status="passed", phase="complete")
        self.save()
        result, status = self.run_finalizer("success")
        self.assertEqual(status["status"], "passed")
        self.assertFalse(status["finalization"]["failed"])
        self.assertEqual(result["consistency_errors"], [])

    def test_should_preserve_panic_without_guessing_typed_cause(self):
        self.receipt["benchmark_specs"][0]["metadata"]["benchmark_error"] = 'panicked: Fenced("unhealthy")'
        self.save()
        _, status = self.run_finalizer()
        self.assertEqual(status["status"], "failed")
        self.assertIsNone(status["finalization"]["failure_kind"])
        self.assertIn("Fenced", status["finalization"]["benchmark_errors"][0])

    def test_should_keep_partial_client_totals_separate_from_completed_stages(self):
        self.status.update(phase="workload", stage_index=2, stage="4-clients")
        self.timeout()
        snapshot_root = self.status_path.parent / "client-snapshots" / "stage-02"
        for client, acknowledged in enumerate((3, 7)):
            snapshot = dict.fromkeys(COUNTERS, 0)
            snapshot.update(phase="workload", stage_index=2, client_index=client, process_id=PID,
                            workload="sqrzl-write-pressure", attempted_transactions=acknowledged + 2,
                            acknowledged_transactions=acknowledged, acknowledged_rows=acknowledged * 32,
                            logical_operations=acknowledged * 32, write_stall_responses=2)
            self.write(snapshot_root / f"client-{client:02}.json", snapshot)
        stale = copy.deepcopy(snapshot)
        stale.update(client_index=9, process_id=PID + 1)
        self.write(snapshot_root / "client-09.json", stale)
        _, status = self.run_finalizer()
        self.assertEqual(status["acknowledged_transactions"], 100)
        partial = status["partial_stage_counters"]
        self.assertEqual(partial["snapshot_count"], 2)
        self.assertEqual(partial["counters"]["acknowledged_transactions"], 10)
        self.assertEqual(partial["counters"]["acknowledged_rows"], 320)
        self.assertEqual(partial["counters"]["write_stall_responses"], 4)

    def test_should_ignore_wrong_revision_workload_process_and_suite(self):
        for field in ("sha", "workload", "pid", "suite"):
            with self.subTest(field=field):
                receipt = copy.deepcopy(self.receipt)
                if field == "sha":
                    receipt["environment"]["git_commit"] = "0" * 40
                elif field == "workload":
                    receipt["environment"]["command_line"][2] = "tier5_write_pressure_s3"
                elif field == "suite":
                    receipt["suite"] = "tier6-mixed-workload-soak"
                else:
                    self.status["process_id"] = PID + 1
                    self.write(self.status_path, self.status)
                self.write(self.receipt_path, receipt)
                _, status = self.run_finalizer()
                self.assertEqual(status["finalization"]["source"], "github-actions")
                self.assertEqual(status["finalization"]["receipt_selection"], "missing")
                self.assertIsNone(status["finalization"]["failure_kind"])
                self.assertEqual(status["status"], "failed")
                self.status["process_id"] = PID
                self.save()

    def test_should_reject_success_without_matching_receipt(self):
        self.status["status"] = "passed"
        self.save()
        self.receipt_path.unlink()
        result, status = self.run_finalizer("success")
        self.assertEqual(status["status"], "failed")
        self.assertTrue(result["consistency_errors"])
        self.assertIsNone(status["finalization"]["failure_kind"])

    def test_should_reject_success_with_unfinished_status(self):
        result, status = self.run_finalizer("success")
        self.assertEqual(status["status"], "failed")
        self.assertTrue(result["consistency_errors"])

    def test_should_not_select_multiple_cli_filters(self):
        self.assertEqual(selected_workload(["bin", "--workload", "alpha", "--bench"]), "alpha")
        self.assertIsNone(selected_workload(["bin", "--workload", "alpha", "--filter", "beta"]))

    def test_should_report_ambiguous_receipts_without_guessing_cause(self):
        self.timeout()
        other = copy.deepcopy(self.receipt)
        other["started_at"] = other["started_at"].replace("01791116544414606578", "01791116544414606579")
        self.write(self.receipt_path.with_name(f"{other['started_at']}.json"), other)
        _, status = self.run_finalizer()
        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["finalization"]["receipt_selection"], "ambiguous")
        self.assertEqual(status["finalization"]["failure_source"], "github-actions")
        self.assertIsNone(status["finalization"]["failure_kind"])

    def test_should_report_unreadable_snapshot_without_inventing_zero_acknowledgments(self):
        self.status.update(phase="workload", stage_index=0)
        self.save()
        path = self.status_path.parent / "client-snapshots" / "stage-00" / "client-00.json"
        path.parent.mkdir(parents=True)
        path.write_text('{"attempted_transactions":')
        _, status = self.run_finalizer()
        partial = status["partial_stage_counters"]
        self.assertEqual(partial["snapshot_count"], 0)
        self.assertIsNone(partial["counters"])
        self.assertEqual(partial["unreadable_snapshots"], [str(path)])

    def test_should_collect_missing_status_failure_as_unknown_cause(self):
        self.status_path.unlink()
        result = finalize(self.stress, self.artifacts, WORKLOAD, SUITE, SHA, "failure")
        self.assertEqual(result["finalized"], [])
        self.assertEqual(result["consistency_errors"], [])
        self.assertIsNone(result["failure_kind"])

    def test_should_ignore_stale_status_despite_reused_process_id(self):
        self.timeout()
        self.status["git_commit"] = "0" * 40
        self.write(self.status_path, self.status)
        result, status = self.run_finalizer()
        self.assertEqual(result["finalized"], [])
        self.assertEqual(status["status"], "running")
        self.assertNotIn("finalization", status)

    def test_should_fall_back_for_malformed_nested_receipt_shapes(self):
        mutations = (
            lambda r: r.update(benchmark_specs={}),
            lambda r: r.update(samples=[None]),
            lambda r: r["benchmark_specs"][0].update(metadata=[]),
            lambda r: r["summaries"][0].update(correctness=[]),
            lambda r: r["samples"][0].update(counters={"failures": "1"}),
            lambda r: r["benchmark_specs"][0]["metadata"].update(failure_kind=[]),
            lambda r: r.update(metadata=[]),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                receipt = copy.deepcopy(self.receipt)
                mutate(receipt)
                self.write(self.receipt_path, receipt)
                _, status = self.run_finalizer()
                self.assertEqual(status["status"], "failed")
                self.assertEqual(status["finalization"]["source"], "github-actions")
                self.assertIsNone(status["finalization"]["failure_kind"])

    def test_should_fail_collection_for_native_failure_after_successful_step(self):
        self.status["status"] = "passed"
        self.timeout()
        result, status = self.run_finalizer("success")
        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["finalization"]["failure_source"], "cntryl-stress")
        self.assertTrue(result["consistency_errors"])


if __name__ == "__main__":
    unittest.main()
