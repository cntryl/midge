"""Adversarial contract checks for #808 receipts without running Cargo."""

import importlib.util
import json
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / 'run-wal-redesign-regressions.py'
SPEC = importlib.util.spec_from_file_location('wal_regressions', SCRIPT)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
GREEN = 'test fixture::should_fixture ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;'
RED = ('---- fixture::should_fixture stdout ----\n'
       "thread 'fixture::should_fixture' panicked at fixture.rs:1:1:\n"
       'specific regression assertion\n\nfailures:\n    fixture::should_fixture\n'
       'test result: FAILED. 0 passed; 1 failed; 0 ignored;')


class ReceiptTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / 'repo'
        self.registry = self.root / RUNNER.REGISTRY
        self.registry.parent.mkdir(parents=True)
        self.case = dict(id='fixture', target='lib', test='should_fixture', source='fixture.rs',
                         baseline='red', failure_marker='specific regression assertion')
        self.write_cases([self.case])
        self.source = dict(source_sha='source-a', source_changes_sha256='fingerprint', clean_source=True)
        self.output = Path(self.directory.name) / 'receipt'

    def write_cases(self, cases):
        self.registry.write_text(json.dumps(dict(schema_version=1, regressions=cases)))

    def invoke(self, log=GREEN, exit_code=0, baseline=False, source=None,
               platform='Darwin', timed_out=False, with_sqrzl=False, available=True):
        def cargo(command, root, output, timeout):
            self.assertGreater(timeout, 0)
            self.assertIn('--include-ignored', command)
            output.write(log)
            return exit_code, timed_out

        argv = ['runner.py', '--output', str(self.output)]
        if baseline:
            argv.append('--baseline')
        if with_sqrzl:
            argv.append('--with-sqrzl')
        identities = list(source if source is not None else [self.source])

        def identity(_):
            return identities.pop(0) if len(identities) > 1 else identities[0]

        with mock.patch.object(RUNNER, '__file__', self.root / 'tools/runner.py'), \
                mock.patch.object(sys, 'argv', argv), \
                mock.patch.object(RUNNER, 'source_identity', side_effect=identity), \
                mock.patch.object(RUNNER.platform, 'system', return_value=platform), \
                mock.patch.object(RUNNER, 'sqrzl_available', return_value=available), \
                mock.patch.object(RUNNER, 'run_test', side_effect=cargo) as run:
            status = RUNNER.main()
        return status, json.loads((self.output / 'receipt.json').read_text()), run.call_count

    def test_accepts_exactly_one_green_test_on_stable_clean_source(self):
        status, receipt, calls = self.invoke()
        self.assertEqual(status, 0)
        self.assertEqual(calls, 1)
        self.assertTrue(receipt['acceptance_passed'])
        self.assertTrue(receipt['source_stable'])

    def test_requires_selected_test_assertion_for_baseline_red(self):
        status, receipt, _ = self.invoke(RED, exit_code=101, baseline=True)
        self.assertEqual(status, 0)
        self.assertTrue(receipt['results'][0]['matched'])
        self.assertFalse(receipt['acceptance_passed'])

    def test_rejects_compilation_setup_warning_or_wrong_test_as_red(self):
        for log in ['error: compile failed',
                    RED.replace('specific regression assertion', 'setup failed'),
                    'warning: specific regression assertion\n' + RED.replace('specific regression assertion', 'setup failed'),
                    RED.replace('fixture::should_fixture', 'other::should_other')]:
            with self.subTest(log=log):
                status, receipt, _ = self.invoke(log, exit_code=101, baseline=True)
                self.assertEqual(status, 1)
                self.assertFalse(receipt['results'][0]['matched'])

    def test_rejects_selector_that_executes_multiple_or_zero_tests(self):
        for log in ['test result: ok. 2 passed; 0 failed; 0 ignored;',
                    'test result: ok. 0 passed; 0 failed; 0 ignored;', GREEN + '\n' + GREEN]:
            with self.subTest(log=log):
                status, receipt, _ = self.invoke(log)
                self.assertEqual(status, 1)
                self.assertFalse(receipt['results'][0]['executed_exactly_one'])

    def test_rejects_a_different_green_test_with_an_overlapping_selector(self):
        status, receipt, _ = self.invoke(GREEN.replace('should_fixture ...', 'should_fixture_control ...'))
        self.assertEqual(status, 1)
        self.assertFalse(receipt['acceptance_passed'])

    def test_interrupted_run_cannot_leave_a_passing_acceptance_receipt(self):
        self.case['baseline'] = 'green'
        self.write_cases([self.case, dict(self.case, test='should_second')])
        with self.assertRaises(KeyboardInterrupt):
            with mock.patch.object(RUNNER, 'classify', side_effect=[(True, True, False), KeyboardInterrupt()]):
                self.invoke()
        receipt = json.loads((self.output / 'receipt.json').read_text())
        self.assertFalse(receipt['acceptance_passed'])
        self.assertFalse(receipt['run_complete'])

    def test_interruption_of_first_case_invalidates_an_old_receipt(self):
        self.output.mkdir()
        (self.output / 'receipt.json').write_text(json.dumps(dict(acceptance_passed=True)))
        with mock.patch.object(RUNNER, 'classify', side_effect=KeyboardInterrupt()):
            with self.assertRaises(KeyboardInterrupt):
                self.invoke()
        receipt = json.loads((self.output / 'receipt.json').read_text())
        self.assertFalse(receipt['acceptance_passed'])
        self.assertFalse(receipt['run_complete'])

    def test_refuses_acceptance_from_dirty_source_before_running_cargo(self):
        status, receipt, calls = self.invoke(source=[dict(self.source, clean_source=False)])
        self.assertEqual(status, 2)
        self.assertEqual(calls, 0)
        self.assertFalse(receipt['acceptance_passed'])

    def test_invalidates_receipt_when_head_or_source_changes_during_test(self):
        for changed in [dict(self.source, source_sha='source-b'),
                        dict(self.source, source_changes_sha256='changed', clean_source=False)]:
            with self.subTest(changed=changed):
                status, receipt, calls = self.invoke(source=[self.source, self.source, changed])
                self.assertEqual(status, 2)
                self.assertEqual(calls, 1)
                self.assertFalse(receipt['source_stable'])
                self.assertFalse(receipt['acceptance_passed'])

    def test_invalidates_receipt_before_next_case_if_source_changed(self):
        status, receipt, calls = self.invoke(source=[self.source, dict(self.source, source_sha='b')])
        self.assertEqual(status, 2)
        self.assertEqual(calls, 0)
        self.assertFalse(receipt['source_stable'])

    def test_baseline_skips_inapplicable_cases_without_claiming_acceptance(self):
        self.case['platforms'] = ['darwin']
        portable = dict(self.case, test='should_portable', platforms=[])
        self.write_cases([portable, self.case])
        status, receipt, calls = self.invoke(RED.replace('should_fixture', 'should_portable'),
                                             exit_code=101, baseline=True, platform='Linux')
        self.assertEqual(status, 0)
        self.assertEqual(calls, 1)
        self.assertFalse(receipt['coverage_complete'])
        self.assertFalse(receipt['acceptance_passed'])
        self.assertEqual(receipt['results'][-1]['status'], 'unsupported-platform')

    def test_acceptance_rejects_unsupported_platform(self):
        self.case['platforms'] = ['darwin']
        self.write_cases([self.case])
        status, receipt, calls = self.invoke(platform='Windows')
        self.assertEqual(status, 1)
        self.assertEqual(calls, 0)
        self.assertFalse(receipt['acceptance_passed'])

    def test_baseline_rejects_an_entirely_skipped_registry(self):
        self.case['platforms'] = ['darwin']
        self.write_cases([self.case])
        status, _, _ = self.invoke(baseline=True, platform='Linux')
        self.assertEqual(status, 1)

    def test_sqrzl_is_explicit_and_unavailable_provider_cannot_count_as_red(self):
        self.case['requires_sqrzl'] = True
        self.write_cases([self.case])
        status, receipt, calls = self.invoke(RED, exit_code=101, baseline=True,
                                             with_sqrzl=True, available=False)
        self.assertEqual(status, 2)
        self.assertEqual(calls, 0)
        self.assertEqual(receipt['results'][0]['status'], 'sqrzl-unavailable')
        status, receipt, calls = self.invoke(RED, exit_code=101, baseline=True, with_sqrzl=True)
        self.assertEqual(status, 0)
        self.assertEqual(calls, 1)
        self.assertTrue(receipt['coverage_complete'])

    def test_acceptance_requires_sqrzl_selection(self):
        self.case['requires_sqrzl'] = True
        self.write_cases([self.case])
        status, receipt, calls = self.invoke()
        self.assertEqual(status, 1)
        self.assertEqual(calls, 0)
        self.assertEqual(receipt['results'][0]['status'], 'sqrzl-not-selected')

    def test_timeout_cannot_count_as_specific_red(self):
        status, receipt, _ = self.invoke(RED, exit_code=101, baseline=True, timed_out=True)
        self.assertEqual(status, 1)
        self.assertTrue(receipt['results'][0]['timed_out'])
        self.assertFalse(receipt['results'][0]['matched'])

    def test_empty_registry_or_missing_marker_is_rejected_before_cargo(self):
        for cases in [[], [dict(self.case, failure_marker='')], [dict(self.case, failure_marker=' ')]]:
            with self.subTest(cases=cases):
                self.write_cases(cases)
                with self.assertRaises(ValueError):
                    self.invoke(baseline=True)

    def test_output_inside_checkout_is_rejected_before_cargo(self):
        self.output = self.root / 'target/receipt'
        with self.assertRaises(SystemExit) as error:
            self.invoke()
        self.assertEqual(error.exception.code, 2)
        self.assertFalse(self.output.exists())

    def test_rejects_duplicate_selectors_and_unsafe_log_names(self):
        for cases in [[self.case, self.case], [dict(self.case, id='../escape')]]:
            with self.subTest(cases=cases):
                self.write_cases(cases)
                with self.assertRaises(ValueError):
                    self.invoke()

    def test_deadline_terminates_a_real_process(self):
        with tempfile.TemporaryFile(mode='w+') as output:
            started = time.monotonic()
            code, timed_out = RUNNER.run_test([sys.executable, '-c', 'import time; time.sleep(30)'],
                                              self.root, output, 0.1)
        self.assertTrue(timed_out)
        self.assertNotEqual(code, 0)
        self.assertLess(time.monotonic() - started, 10)


    def test_deadline_terminates_test_children_before_their_delayed_effect(self):
        effect = self.root / 'late-child-effect'
        child = f"import time; from pathlib import Path; time.sleep(2); Path({str(effect)!r}).write_text('landed')"
        parent = (f"import subprocess,sys,time; subprocess.Popen([sys.executable, '-c', {child!r}]); "
                  "print('child dispatched', flush=True); time.sleep(30)")
        with tempfile.TemporaryFile(mode='w+') as output:
            _, timed_out = RUNNER.run_test([sys.executable, '-c', parent], self.root, output, 0.5)
            output.seek(0)
            self.assertIn('child dispatched', output.read())
        self.assertTrue(timed_out)
        time.sleep(2.1)
        self.assertFalse(effect.exists())


if __name__ == '__main__':
    unittest.main()
