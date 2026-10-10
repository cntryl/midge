#!/usr/bin/env python3
"""Run every #808 audit regression, including temporarily ignored cases."""

import argparse
import hashlib
import json
import platform
import re
import subprocess
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', action='store_true', help='require recorded RED/GREEN classification instead of all GREEN')
    parser.add_argument('--output', required=True, type=Path, help='directory for exact commands, receipts, and source identity')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    registry = json.loads((root / 'docs/development/wal-redesign-regressions-808.json').read_text())
    source_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    diff = subprocess.check_output(['git', 'diff', 'HEAD', '--'], cwd=root)
    untracked = subprocess.check_output(['git', 'ls-files', '--others', '--exclude-standard'], cwd=root, text=True).splitlines()
    source_changes = hashlib.sha256(diff)
    for name in sorted(untracked):
        source_changes.update(name.encode())
        source_changes.update((root / name).read_bytes())
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    accepted = True
    for index, case in enumerate(registry['regressions']):
        if case.get('platform') and case['platform'] != platform.system().lower():
            results.append(dict(**case, status='unsupported-platform'))
            # An all-GREEN acceptance receipt cannot silently omit a scenario.
            accepted = False
            continue
        target = ['--lib'] if case['target'] == 'lib' else ['--test', case['target']]
        command = ['cargo', 'test', '--locked', '--all-features'] + target + [case['test'], '--', '--include-ignored', '--test-threads=1']
        log = args.output / f'{index:02d}-{case["id"]}.log'
        with log.open('w') as output:
            result = subprocess.run(command, cwd=root, stdout=output, stderr=subprocess.STDOUT)
        text = log.read_text()
        summary = re.search(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', text)
        executed = summary is not None and int(summary[1]) + int(summary[2]) == 1 and int(summary[3]) == 0
        green = result.returncode == 0 and executed and int(summary[1]) == 1
        red = result.returncode != 0 and executed and int(summary[2]) == 1 and case.get('failure_marker', '') in text
        expected = case['baseline'] if args.baseline else 'green'
        matches = green if expected == 'green' else red
        accepted &= matches
        results.append(dict(**case, command=command, exit_code=result.returncode, executed_exactly_one=executed, expected=expected, matched=matches, log=log.name))
        receipt = dict(schema_version=1, source_sha=source_sha, source_changes_sha256=source_changes.hexdigest(), clean_source=not diff and not untracked, baseline_mode=args.baseline, results=results)
        (args.output / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    return 0 if accepted else 1


if __name__ == '__main__':
    raise SystemExit(main())
