#!/usr/bin/env python3
"""Run #808 regressions, including ignored cases; RED baseline is not acceptance."""

import argparse
import hashlib
import json
import math
import os
import platform
import re
import signal
import socket
import subprocess
from pathlib import Path


REGISTRY = 'docs/development/wal-redesign-regressions-808.json'


def source_identity(root):
    source_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    diff = subprocess.check_output(['git', 'diff', 'HEAD', '--'], cwd=root)
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'], cwd=root, text=True).splitlines()
    source_changes = hashlib.sha256(diff)
    for name in sorted(untracked):
        source_changes.update(name.encode())
        source_changes.update((root / name).read_bytes())
    return dict(source_sha=source_sha, source_changes_sha256=source_changes.hexdigest(), clean_source=not diff and not untracked)


def load_cases(root):
    registry = json.loads((root / REGISTRY).read_text())
    if registry.get('schema_version') != 1:
        raise ValueError('unsupported registry schema')
    cases = registry['regressions'] + registry.get('supplemental_regressions', [])
    if not cases:
        raise ValueError('empty regression registry')
    selectors = set()
    for case in cases:
        for field in ('id', 'target', 'test', 'source'):
            if not isinstance(case.get(field), str) or not case[field].strip():
                raise ValueError(f'missing {field}')
        if not re.fullmatch(r'[a-z0-9-]+', case['id']):
            raise ValueError('unsafe regression id')
        if case.get('baseline') not in ('red', 'green'):
            raise ValueError('missing RED/GREEN baseline')
        if case['baseline'] == 'red' and not case.get('failure_marker', '').strip():
            raise ValueError('RED baseline requires a nonempty failure_marker')
        selector = (case['target'], case['test'])
        if selector in selectors:
            raise ValueError('duplicate test selector')
        selectors.add(selector)
    return cases


def classify(log, exit_code, case):
    summaries = re.findall(r'^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log, re.MULTILINE)
    executed = len(summaries) == 1 and sum(map(int, summaries[0][:2])) == 1 and summaries[0][2] == '0'
    passed_names = re.findall(r'^test (\S+) \.\.\. ok$', log, re.MULTILINE)
    selected_pass = any(name == case['test'] or name.endswith('::' + case['test']) for name in passed_names)
    green = executed and selected_pass and exit_code == 0 and summaries[0][0] == '1'
    # Compiler output and setup failures must never stand in for the selected
    # test's specific assertion. Only inspect its libtest failure block.
    blocks = re.findall(r'^---- (\S+) stdout ----\n(.*?)(?=^---- |^failures:\s*$|^test result:|\Z)', log, re.MULTILINE | re.DOTALL)
    marker = case.get('failure_marker', '')
    selected_failure = any((name == case['test'] or name.endswith('::' + case['test'])) and marker in body
                           for name, body in blocks)
    red = executed and exit_code != 0 and summaries[0][1] == '1' and bool(marker.strip()) and selected_failure
    return executed, green, red


def run_test(command, root, output, timeout):
    """Bound the entire Cargo/test process tree, including crash-test children."""
    options = dict(cwd=root, stdout=output, stderr=subprocess.STDOUT)
    if os.name == 'nt':
        options['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        options['start_new_session'] = True
    with subprocess.Popen(command, **options) as process:
        try:
            return process.wait(timeout=timeout), False
        except subprocess.TimeoutExpired:
            if os.name == 'nt':
                subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
                process.kill()
            else:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait()
            return process.returncode, True


def sqrzl_available():
    if not os.environ.get('SQRZL_SECRET_ACCESS_KEY'):
        return False
    try:
        with socket.create_connection(('127.0.0.1', 9000), timeout=1):
            return True
    except OSError:
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', action='store_true', help='check recorded RED/GREEN states for applicable cases; never an acceptance receipt')
    parser.add_argument('--with-sqrzl', action='store_true', help='require Sqrzl at 127.0.0.1:9000 and SQRZL_SECRET_ACCESS_KEY')
    parser.add_argument('--timeout-seconds', type=float, default=300, help='per-case Cargo and test process-tree deadline (default: 300)')
    parser.add_argument('--output', required=True, type=Path, help='receipt directory outside the checkout')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    if output == root or root in output.parents:
        parser.error('--output must be outside the checkout (including target)')
    if not math.isfinite(args.timeout_seconds) or args.timeout_seconds <= 0:
        parser.error('--timeout-seconds must be finite and positive')
    cases = load_cases(root)
    source = source_identity(root)
    output.mkdir(parents=True, exist_ok=True)
    results = []
    accepted = True
    complete = True
    executed_count = 0
    run_complete = False

    def write_receipt(stable=True):
        receipt = dict(schema_version=2, **source, source_stable=stable,
                       baseline_mode=args.baseline, coverage_complete=complete, run_complete=run_complete,
                       acceptance_passed=run_complete and not args.baseline and accepted and complete and executed_count == len(cases) and stable,
                       results=results)
        (output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')

    # Invalidate a previous receipt before even the first test can block.
    write_receipt()
    if not args.baseline and not source['clean_source']:
        accepted = complete = False
        results.append(dict(status='dirty-acceptance-source'))
        write_receipt()
        return 2
    for index, case in enumerate(cases):
        if source_identity(root) != source:
            accepted = False
            write_receipt(False)
            return 2
        platforms = case.get('platforms', [case['platform']] if case.get('platform') else [])
        skip = None
        if platforms and platform.system().lower() not in platforms:
            skip = 'unsupported-platform'
        elif case.get('requires_sqrzl') and not args.with_sqrzl:
            skip = 'sqrzl-not-selected'
        if skip:
            complete = False
            accepted &= args.baseline
            results.append(dict(**case, status=skip))
            write_receipt()
            continue
        if case.get('requires_sqrzl') and not sqrzl_available():
            accepted = complete = False
            results.append(dict(**case, status='sqrzl-unavailable'))
            write_receipt()
            return 2
        target = ['--lib'] if case['target'] == 'lib' else ['--test', case['target']]
        command = ['cargo', 'test', '--locked', '--all-features'] + target + [case['test'], '--', '--include-ignored', '--test-threads=1']
        log = output / f'{index:02d}-{case["id"]}.log'
        with log.open('w') as stream:
            exit_code, timed_out = run_test(command, root, stream, args.timeout_seconds)
        executed, green, red = classify(log.read_text(errors='replace'), exit_code, case)
        expected = case['baseline'] if args.baseline else 'green'
        matches = not timed_out and (green if expected == 'green' else red)
        accepted &= matches
        executed_count += int(executed)
        results.append(dict(**case, command=command, exit_code=exit_code, timed_out=timed_out,
                            executed_exactly_one=executed, expected=expected, matched=matches, log=log.name))
        stable = source_identity(root) == source
        write_receipt(stable)
        if not stable:
            return 2
    run_complete = True
    accepted &= executed_count > 0
    stable = source_identity(root) == source
    write_receipt(stable)
    return 0 if accepted and stable else 1


if __name__ == '__main__':
    raise SystemExit(main())
