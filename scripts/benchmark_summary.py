#!/usr/bin/env python3
"""
Produce comprehensive CSV and Markdown summaries of Criterion benchmarks and stress tests.

- Criterion: extracts mean, CI, std_dev, relative_stddev from target/criterion
- Stress: extracts duration, throughput (elements/ns), scenario tags from target/stress

Stress output path: run stress bench binaries first, e.g.:
  cargo bench --bench tier3_system_kv
  cargo bench --bench tier4_integration_kv
  ...
Then cntryl-stress writes results under target/stress/<bench_name>/ (e.g. latest.json).
This script expects target/stress/<suite_dir>/latest.json per suite.

Flags high variance when relative_stddev > 0.10 (10%).
Also writes human-friendly mean_us and mean_ms columns assuming nanoseconds.
"""
from pathlib import Path
import json
import csv

CRITERION_ROOT = Path(__file__).resolve().parents[1] / 'target' / 'criterion'
STRESS_ROOT = Path(__file__).resolve().parents[1] / 'target' / 'stress'
TARGET_ROOT = Path(__file__).resolve().parents[1] / 'target'
OUT_CSV = CRITERION_ROOT / 'benchmark_summary.csv'
OUT_MD = TARGET_ROOT / 'bench_summary.md'
STRESS_CSV = STRESS_ROOT / 'stress_summary.csv'

# ============================================================================
# CRITERION BENCHMARKS
# ============================================================================
entries = []
if not CRITERION_ROOT.exists():
    pass  # skip criterion section
else:
    for p in CRITERION_ROOT.rglob('new/estimates.json'):
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text())
        except Exception as e:
            print(f"skipping {p} (read error): {e}")
            continue
        # Determine benchmark id as path relative to ROOT, omit trailing '/new/estimates.json'
        benchmark = str(p.relative_to(CRITERION_ROOT).parent.parent)
        mean = data.get('mean', {}).get('point_estimate')
        ci = data.get('mean', {}).get('confidence_interval', {})
        ci_lower = ci.get('lower_bound')
        ci_upper = ci.get('upper_bound')
        stddev = data.get('std_dev', {}).get('point_estimate')
        # fallback: some Criterion variants place std_dev under 'std_dev' or in same level
        if mean is None:
            continue
        rel_stddev = None
        if stddev is not None and mean != 0:
            rel_stddev = stddev / mean
        high_variance = False
        if rel_stddev is not None:
            high_variance = rel_stddev > 0.10
        # Skip legacy/stale Criterion entries (e.g. old "schedule_system_scan_and_fire" 222ms row)
        if 'schedule_system_scan_and_fire' in benchmark:
            continue
        # assume raw numbers are nanoseconds and provide converted columns
        mean_us = mean / 1e3
        mean_ms = mean / 1e6
        entries.append({
            'benchmark': benchmark,
            'mean': mean,
            'mean_ci_lower': ci_lower,
            'mean_ci_upper': ci_upper,
            'std_dev': stddev,
            'rel_stddev': rel_stddev,
            'high_variance': high_variance,
            'mean_us': mean_us,
            'mean_ms': mean_ms,
            'file': str(p)
        })

# Write Criterion CSV (skip if criterion dir missing; create parent so write never errors)
if CRITERION_ROOT.exists():
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['benchmark','mean','mean_ci_lower','mean_ci_upper','std_dev','rel_stddev','high_variance','mean_us(assume_ns)','mean_ms(assume_ns)','file'])
        for e in sorted(entries, key=lambda x: x['mean']):
            writer.writerow([
                e['benchmark'],
                f"{e['mean']:.6f}" if isinstance(e['mean'], float) else e['mean'],
                f"{e['mean_ci_lower']:.6f}" if isinstance(e['mean_ci_lower'], float) else e['mean_ci_lower'],
                f"{e['mean_ci_upper']:.6f}" if isinstance(e['mean_ci_upper'], float) else e['mean_ci_upper'],
                f"{e['std_dev']:.6f}" if isinstance(e['std_dev'], float) else e['std_dev'],
                f"{e['rel_stddev']:.6f}" if isinstance(e['rel_stddev'], float) else e['rel_stddev'],
                str(e['high_variance']),
                f"{e['mean_us']:.6f}",
                f"{e['mean_ms']:.6f}",
                e['file']
            ])

# Derive suite (first path component) for each Criterion entry for per-suite grouping
# Path can use / or \ depending on OS
for e in entries:
    parts = e['benchmark'].replace('\\', '/').split('/')
    e['suite'] = parts[0] if parts else 'other'

# Write a small Markdown summary: top 10 fastest and slowest, and high-variance list
sorted_by_mean = sorted(entries, key=lambda x: x['mean'])
fastest = sorted_by_mean[:10]
slowest = sorted_by_mean[-10:][::-1]
high_var = [e for e in entries if e['high_variance']]

# Group Criterion entries by suite (tier) for per-suite summaries
criterion_suites = {}
for e in entries:
    suite_name = e['suite']
    if suite_name not in criterion_suites:
        criterion_suites[suite_name] = []
    criterion_suites[suite_name].append(e)
# Sort suite names: tier1_* first, then tier2_*, then rest alphabetically
def suite_sort_key(name):
    if name.startswith('tier1_'):
        return (0, name)
    if name.startswith('tier2_'):
        return (1, name)
    return (2, name)
criterion_suite_order = sorted(criterion_suites.keys(), key=suite_sort_key)

# ============================================================================
# STRESS TESTS
# ============================================================================
stress_entries = []
if STRESS_ROOT.exists():
    for suite_dir in sorted(STRESS_ROOT.glob('*/')):
        if not suite_dir.is_dir():
            continue
        latest_json = suite_dir / 'latest.json'
        if not latest_json.exists():
            continue
        try:
            data = json.loads(latest_json.read_text())
        except Exception as e:
            print(f"skipping {latest_json} (read error): {e}")
            continue

        suite = data.get('suite', suite_dir.name)
        results = data.get('results', [])

        for result in results:
            name = result.get('name', '')
            duration = result.get('duration')
            elements = result.get('elements')
            all_runs = result.get('all_runs', [duration] if duration else [])
            tags = result.get('tags', {})
            scenario = tags.get('scenario', 'unknown')

            if duration is None or elements is None or elements == 0:
                continue

            # Compute statistics
            throughput_ops_per_ns = elements / duration if duration > 0 else 0
            throughput_ops_per_us = throughput_ops_per_ns * 1e3
            throughput_ops_per_ms = throughput_ops_per_ns * 1e6
            throughput_ops_per_s = throughput_ops_per_ns * 1e9

            duration_us = duration / 1e3
            duration_ms = duration / 1e6
            per_op_ns = duration / elements if elements > 0 else 0
            per_op_us = per_op_ns / 1e3

            # Variance across runs
            if len(all_runs) > 1:
                avg_run = sum(all_runs) / len(all_runs)
                variance = sum((x - avg_run) ** 2 for x in all_runs) / len(all_runs)
                stddev = variance ** 0.5
                rel_stddev_runs = stddev / avg_run if avg_run > 0 else 0
            else:
                stddev = 0
                rel_stddev_runs = 0

            stress_entries.append({
                'suite': suite,
                'name': name,
                'scenario': scenario,
                'layer': tags.get('layer'),  # tier4: direct/tcp/websocket/multiclient
                'duration_ns': duration,
                'duration_us': duration_us,
                'duration_ms': duration_ms,
                'elements': elements,
                'per_op_ns': per_op_ns,
                'per_op_us': per_op_us,
                'throughput_ops_per_ns': throughput_ops_per_ns,
                'throughput_ops_per_us': throughput_ops_per_us,
                'throughput_ops_per_ms': throughput_ops_per_ms,
                'throughput_ops_per_s': throughput_ops_per_s,
                'num_runs': len(all_runs),
                'stddev_runs': stddev,
                'rel_stddev_runs': rel_stddev_runs,
                'file': str(latest_json)
            })

# Write Stress CSV (skip if stress dir missing)
if STRESS_ROOT.exists():
    STRESS_CSV.parent.mkdir(parents=True, exist_ok=True)
    with STRESS_CSV.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['suite', 'name', 'scenario', 'duration_ms', 'elements', 'per_op_us', 'throughput_ops_per_s', 'runs', 'stddev_runs_ns', 'rel_stddev_runs', 'file'])
        for e in sorted(stress_entries, key=lambda x: x['throughput_ops_per_s'], reverse=True):
            writer.writerow([
                e['suite'],
                e['name'],
                e['scenario'],
                f"{e['duration_ms']:.2f}",
                e['elements'],
                f"{e['per_op_us']:.6f}",
                f"{e['throughput_ops_per_s']:.2f}",
                e['num_runs'],
                f"{e['stddev_runs']:.2f}",
                f"{e['rel_stddev_runs']:.6f}" if e['rel_stddev_runs'] else "NA",
                e['file']
            ])

# Write unified Markdown summary with both Criterion and Stress tests
OUT_MD.parent.mkdir(parents=True, exist_ok=True)
with OUT_MD.open('w', encoding='utf-8') as f:
    f.write('# Benchmark & Stress Test Summary\n\n')
    f.write('Generated from Criterion benchmarks and stress tests.\n\n')
    
    # ========== CRITERION SECTION ==========
    f.write('# Criterion Benchmarks\n\n')
    f.write('Note: mean_us / mean_ms assume raw numbers are in nanoseconds.\n\n')
    f.write('## Top 10 fastest (by mean)\n\n')
    f.write('| rank | benchmark | mean | mean_ms | mean_us | std_dev | rel_stddev |\n')
    f.write('|---:|---|---:|---:|---:|---:|---:|\n')
    for i, e in enumerate(fastest, 1):
        rel = f"{e['rel_stddev']:.6f}" if e['rel_stddev'] is not None else "NA"
        std = f"{e['std_dev']:.6f}" if isinstance(e['std_dev'], float) else (str(e['std_dev']) if e['std_dev'] is not None else "NA")
        f.write(f"| {i} | {e['benchmark']} | {e['mean']:.6f} | {e['mean_ms']:.6f} | {e['mean_us']:.6f} | {std} | {rel} |\n")
    f.write('\n## Top 10 slowest (by mean)\n\n')
    f.write('| rank | benchmark | mean | mean_ms | mean_us | std_dev | rel_stddev |\n')
    f.write('|---:|---|---:|---:|---:|---:|---:|\n')
    for i, e in enumerate(slowest, 1):
        rel = f"{e['rel_stddev']:.6f}" if e['rel_stddev'] is not None else "NA"
        std = f"{e['std_dev']:.6f}" if isinstance(e['std_dev'], float) else (str(e['std_dev']) if e['std_dev'] is not None else "NA")
        f.write(f"| {i} | {e['benchmark']} | {e['mean']:.6f} | {e['mean_ms']:.6f} | {e['mean_us']:.6f} | {std} | {rel} |\n")

    f.write('\n## High variance benchmarks (rel_stddev > 0.10)\n\n')
    if not high_var:
        f.write('None detected.\n')
    else:
        f.write('| benchmark | mean | std_dev | rel_stddev |\n')
        f.write('|---|---:|---:|---:|\n')
        for e in sorted(high_var, key=lambda x: x['rel_stddev'], reverse=True):
            f.write(f"| {e['benchmark']} | {e['mean']:.6f} | {e['std_dev'] or 'NA'} | {e['rel_stddev']:.6f} |\n")

    # ========== CRITERION PER-SUITE (ALL TIERS) ==========
    f.write('\n## Per-Suite Results (Criterion)\n\n')
    for suite_name in criterion_suite_order:
        suite_entries = criterion_suites[suite_name]
        # Sort by mean (fastest first) within suite
        suite_entries_sorted = sorted(suite_entries, key=lambda x: x['mean'])
        total_mean_ns = sum(e['mean'] for e in suite_entries_sorted)
        count = len(suite_entries_sorted)
        avg_ns = total_mean_ns / count if count else 0
        f.write(f'### {suite_name}\n\n')
        f.write(f'**Benchmarks**: {count} | **Avg mean**: {avg_ns/1e3:.3f} µs (total {total_mean_ns/1e6:.2f} ms)\n\n')
        f.write('| benchmark | mean_ns | mean_us | mean_ms | std_dev | rel_stddev |\n')
        f.write('|---:|---:|---:|---:|---:|---:|\n')
        for e in suite_entries_sorted:
            norm = e['benchmark'].replace('\\', '/')
            bench_short = norm.split('/', 1)[-1] if '/' in norm else e['benchmark']
            rel = f"{e['rel_stddev']:.4f}" if e['rel_stddev'] is not None else "NA"
            std = f"{e['std_dev']:.4f}" if isinstance(e['std_dev'], float) else (str(e['std_dev']) if e['std_dev'] is not None else "NA")
            f.write(f"| {bench_short} | {e['mean']:.2f} | {e['mean_us']:.4f} | {e['mean_ms']:.6f} | {std} | {rel} |\n")
        f.write('\n')

    # ========== STRESS TEST SECTION ==========
    f.write('\n# Stress Tests\n\n')
    if stress_entries:
        f.write('Ordered by throughput (highest first).\n\n')
        f.write('## Per-Suite Results (Stress)\n\n')
        
        # Group by suite
        suites = {}
        for e in stress_entries:
            suite_name = e['suite']
            if suite_name not in suites:
                suites[suite_name] = []
            suites[suite_name].append(e)
        
        for suite_name in sorted(suites.keys()):
            suite_tests = suites[suite_name]
            total_duration = sum(e['duration_ns'] for e in suite_tests)
            total_elements = sum(e['elements'] for e in suite_tests)
            total_throughput = total_elements / total_duration if total_duration > 0 else 0
            
            f.write(f'### {suite_name}\n\n')
            # Duration: never show 0.00ms; use ns or µs when < 1ms
            def fmt_duration(e):
                ns = e['duration_ns']
                if ns >= 1e6:
                    return f'{ns/1e6:.2f}ms'
                if ns >= 1e3:
                    return f'{ns/1e3:.2f}µs'
                return f'{ns:.0f}ns'
            total_dur_str = fmt_duration({'duration_ns': total_duration})
            f.write(f'**Total**: {total_elements} ops in {total_dur_str} = {total_throughput*1e9:.0f} ops/sec\n\n')
            # Table: include layer (transport) for tier4 when present
            has_layer = any(e.get('layer') for e in suite_tests)
            if has_layer:
                f.write('| scenario | layer | ops | duration | per_op_us | ops/sec |\n')
                f.write('|---|---|---|---:|---:|---:|\n')
                for e in sorted(suite_tests, key=lambda x: x['throughput_ops_per_s'], reverse=True):
                    layer = e.get('layer') or '—'
                    dur_str = fmt_duration(e)
                    f.write(f"| {e['scenario']} | {layer} | {e['elements']} | {dur_str} | {e['per_op_us']:.3f} | {e['throughput_ops_per_s']:.0f} |\n")
            else:
                f.write('| scenario | ops | duration | per_op_us | ops/sec |\n')
                f.write('|---|---:|---:|---:|---:|\n')
                for e in sorted(suite_tests, key=lambda x: x['throughput_ops_per_s'], reverse=True):
                    dur_str = fmt_duration(e)
                    f.write(f"| {e['scenario']} | {e['elements']} | {dur_str} | {e['per_op_us']:.3f} | {e['throughput_ops_per_s']:.0f} |\n")
            f.write('\n')
    else:
        f.write('No stress test results found.\n')

if CRITERION_ROOT.exists():
    print(f"Wrote {OUT_CSV} (criterion) with {len(entries)} entries.")
if STRESS_ROOT.exists():
    print(f"Wrote {STRESS_CSV} (stress) with {len(stress_entries)} entries.")
print(f"Wrote {OUT_MD} (unified summary).")
