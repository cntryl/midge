#!/usr/bin/env python3
"""Artifact acceptance must reject partial correctness and bound/source drift."""
import copy
import json
from pathlib import Path
import tempfile
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import unittest
from recovery_attribution import validate_native


class AcceptanceTests(unittest.TestCase):
    def test_should_reject_false_acceptance_when_native_evidence_is_partial(self):
        # Arrange: one internally consistent receipt and independent bad mutations.
        checks = [{"passed": True, "value_mismatches": 0, "expected_rows": 8192, "actual_rows": 8192} for _ in range(6)]
        fact = {"variant": "baseline", "budget_final": 0, "budget_peak": 1024, "budget_limit": 4096,
                "peak_readers": 4, "reader_limit": 4, "point_probes": 16384, "exact_hits": 8192,
                "checkpoint_releases": 16, "index_builds": 17, "predicate_rejections": 8192,
                "remote_observer_attached": True, "remote_ranges": {"attempts": 40, "completed": 40, "failures": 0}}
        result = {"fixture_sha256": "fixture", "variant": "baseline", "verification": checks, "shutdowns": 2,
                  "memory_bytes": 134217728, "local_storage_bytes": 1073741824, "memtable_bytes": 262144,
                  "frontier": 100, "committed_wal_frontier": 98, "runtime": {"wal_cloud_durable_seq": 98, "current_sequence": 120, "salvage_mode_opens": 0}, "open_deadline_seconds": 30, "native_open_events": [{"attribution": fact}, {"phase": "coverage", "reader_evictions": 20}]}
        receipt = {"complete": True, "source_clean": True, "source_sha": "source", "binary_sha256": "binary", "result": result}
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
