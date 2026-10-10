"""Contract checks for the evidence runner, without invoking Cargo."""

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / 'run-wal-redesign-regressions.py'
SPEC = importlib.util.spec_from_file_location('wal_regressions', SCRIPT)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.registry = self.root / 'docs/development/wal-redesign-regressions-808.json'
        self.registry.parent.mkdir(parents=True)
        self.case = dict(id='fixture', target='lib', test='should_fixture',
                         baseline='red', failure_marker='specific regression assertion')
        self.registry.write_text(json.dumps(dict(regressions=[self.case])))
        self.source = dict(source_sha='source-a', source_changes_sha256='fingerprint',
                           clean_source=True)
        self.output = self.root / 'receipt'

    def invoke(self, log, exit_code=0, baseline=False, source=None, platform='Darwin'):
        def cargo(command, **kwargs):
            kwargs['stdout'].write(log)
            return subprocess.CompletedProcess(command, exit_code)

        argv = ['run-wal-redesign-regressions.py', '--output', str(self.output)]
        if baseline:
            argv.append('--baseline')
        identities = source if source is not None else [self.source] * 2
        with mock.patch.object(RUNNER, '__file__', self.root / 'tools/runner.py'), \
                mock.patch.object(sys, 'argv', argv), \
                mock.patch.object(RUNNER, 'source_identity', side_effect=identities), \
                mock.patch.object(RUNNER.platform, 'system', return_value=platform), \
                mock.patch.object(RUNNER.subprocess, 'run', side_effect=cargo) as run:
            status = RUNNER.main()
        return status, json.loads((self.output / 'receipt.json').read_text()), run.call_count

    def test_accepts_exactly_one_green_test_on_stable_clean_source(self):
        status, receipt, calls = self.invoke('test result: ok. 1 passed; 0 failed; 0 ignored;')
        self.assertEqual(status, 0)
        self.assertEqual(calls, 1)
        self.assertTrue(receipt['source_stable'])
        self.assertTrue(receipt['results'][0]['matched'])

    def test_requires_specific_regression_assertion_for_baseline_red(self):
        status, receipt, _ = self.invoke(
            'specific regression assertion\ntest result: FAILED. 0 passed; 1 failed; 0 ignored;',
            exit_code=101, baseline=True)
        self.assertEqual(status, 0)
        self.assertTrue(receipt['results'][0]['matched'])

    def test_rejects_compilation_or_setup_failure_as_red_evidence(self):
        for log in ['error: compile failed',
                    'setup failed\ntest result: FAILED. 0 passed; 1 failed; 0 ignored;']:
            with self.subTest(log=log):
                status, receipt, _ = self.invoke(log, exit_code=101, baseline=True)
                self.assertEqual(status, 1)
                self.assertFalse(receipt['results'][0]['matched'])

    def test_rejects_selector_that_executes_multiple_tests(self):
        status, receipt, _ = self.invoke('test result: ok. 2 passed; 0 failed; 0 ignored;')
        self.assertEqual(status, 1)
        self.assertFalse(receipt['results'][0]['executed_exactly_one'])

    def test_refuses_acceptance_from_dirty_source_before_running_cargo(self):
        dirty = dict(self.source, clean_source=False)
        status, receipt, calls = self.invoke('', source=[dirty])
        self.assertEqual(status, 2)
        self.assertEqual(calls, 0)
        self.assertEqual(receipt['results'][0]['status'], 'dirty-acceptance-source')

    def test_invalidates_receipt_when_head_or_source_changes_during_test(self):
        for changed in [dict(self.source, source_sha='source-b'),
                        dict(self.source, source_changes_sha256='changed', clean_source=False)]:
            with self.subTest(changed=changed):
                status, receipt, calls = self.invoke(
                    'test result: ok. 1 passed; 0 failed; 0 ignored;',
                    source=[self.source, changed])
                self.assertEqual(status, 2)
                self.assertEqual(calls, 1)
                self.assertFalse(receipt['source_stable'])

    def test_records_unsupported_last_case_in_incomplete_receipt(self):
        self.case['platforms'] = ['darwin', 'linux']
        self.registry.write_text(json.dumps(dict(regressions=[self.case])))
        status, receipt, calls = self.invoke('', platform='Windows')
        self.assertEqual(status, 1)
        self.assertEqual(calls, 0)
        self.assertEqual(receipt['results'][0]['status'], 'unsupported-platform')


if __name__ == '__main__':
    unittest.main()
