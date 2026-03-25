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
import statistics
from collections import Counter, defaultdict

CRITERION_ROOT = Path(__file__).resolve().parents[1] / 'target' / 'criterion'
STRESS_ROOT = Path(__file__).resolve().parents[1] / 'target' / 'stress'
TARGET_ROOT = Path(__file__).resolve().parents[1] / 'target'
OUT_CSV = CRITERION_ROOT / 'benchmark_summary.csv'
OUT_MD = TARGET_ROOT / 'bench_summary.md'
STRESS_CSV = STRESS_ROOT / 'stress_summary.csv'


def load_csv_rows(path: Path):
    if not path.exists():
        return []

    with path.open('r', newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))


def parse_float(value):
    if value in (None, '', 'NA'):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile(values, fraction):
    if not values:
        return None

    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]

    lower_value = ordered[lower]
    upper_value = ordered[upper]
    return lower_value + (upper_value - lower_value) * (position - lower)


def variance_band(rel_stddev):
    if rel_stddev is None:
        return 'unknown'
    if rel_stddev <= 0.05:
        return 'stable'
    if rel_stddev <= 0.10:
        return 'acceptable'
    if rel_stddev <= 0.20:
        return 'noisy'
    return 'untrustworthy'


def latency_bucket(mean_ns):
    if mean_ns < 10_000:
        return '<10us'
    if mean_ns < 100_000:
        return '10-100us'
    if mean_ns < 1_000_000:
        return '100us-1ms'
    return '>1ms'


def format_delta(value):
    if value is None:
        return 'NA'
    sign = '+' if value >= 0 else ''
    return f'{sign}{value:.1f}%'


def summarize_deltas(changes, lower_is_better=True, threshold=0.05):
    improved = 0
    regressed = 0
    unchanged = 0
    new = 0
    missing = 0
    movers = []

    for item in changes:
        delta = item.get('delta_pct')
        if delta is None:
            new += 1
            continue
        if item.get('baseline_only'):
            missing += 1
            continue

        movers.append(item)
        if lower_is_better:
            if delta <= -threshold:
                improved += 1
            elif delta >= threshold:
                regressed += 1
            else:
                unchanged += 1
        else:
            if delta >= threshold:
                improved += 1
            elif delta <= -threshold:
                regressed += 1
            else:
                unchanged += 1

    return {
        'improved': improved,
        'regressed': regressed,
        'unchanged': unchanged,
        'new': new,
        'missing': missing,
        'movers': movers,
    }

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

previous_criterion_rows = load_csv_rows(OUT_CSV)
previous_stress_rows = load_csv_rows(STRESS_CSV)

criterion_by_benchmark = {entry['benchmark']: entry for entry in entries}
criterion_means = [entry['mean'] for entry in entries]
criterion_variance_bands = Counter(variance_band(entry['rel_stddev']) for entry in entries)
criterion_latency_bands = Counter(latency_bucket(entry['mean']) for entry in entries)
criterion_suite_groups = defaultdict(list)
for entry in entries:
    criterion_suite_groups[entry['suite']].append(entry)

criterion_comparisons = []
for benchmark, entry in criterion_by_benchmark.items():
    baseline = next((row for row in previous_criterion_rows if row.get('benchmark') == benchmark), None)
    baseline_mean = parse_float(baseline.get('mean')) if baseline else None
    delta_pct = None
    if baseline_mean and baseline_mean > 0:
        delta_pct = ((entry['mean'] - baseline_mean) / baseline_mean) * 100.0
    criterion_comparisons.append({
        'benchmark': benchmark,
        'mean': entry['mean'],
        'baseline_mean': baseline_mean,
        'delta_pct': delta_pct,
        'variance_band': variance_band(entry['rel_stddev']),
        'rel_stddev': entry['rel_stddev'],
        'suite': entry['suite'],
    })

criterion_missing = [
    row for row in previous_criterion_rows
    if row.get('benchmark') not in criterion_by_benchmark
]

criterion_delta_summary = summarize_deltas(criterion_comparisons, lower_is_better=True)
criterion_delta_summary['missing'] = len(criterion_missing)
criterion_delta_summary['tracked'] = len(criterion_comparisons)
criterion_delta_summary['baseline_total'] = len(previous_criterion_rows)
criterion_delta_summary['median'] = statistics.median(criterion_means) if criterion_means else None
criterion_delta_summary['p90'] = percentile(criterion_means, 0.90)
criterion_delta_summary['fastest'] = min(entries, key=lambda x: x['mean']) if entries else None
criterion_delta_summary['slowest'] = max(entries, key=lambda x: x['mean']) if entries else None
criterion_delta_summary['noisiest'] = sorted(entries, key=lambda x: x['rel_stddev'] or -1, reverse=True)[:5]
criterion_delta_summary['slowest_top'] = sorted(entries, key=lambda x: x['mean'], reverse=True)[:5]
criterion_comparison_map = {item['benchmark']: item for item in criterion_comparisons}

stress_by_key = {
    (entry['suite'], entry['name'], entry['scenario']): entry
    for entry in stress_entries
}
previous_stress_by_name = defaultdict(list)
for row in previous_stress_rows:
    previous_stress_by_name[row.get('name')].append(row)
stress_throughputs = [entry['throughput_ops_per_s'] for entry in stress_entries]
stress_variance_bands = Counter(variance_band(entry['rel_stddev_runs']) for entry in stress_entries)
stress_suite_groups = defaultdict(list)
stress_layer_groups = defaultdict(list)
for entry in stress_entries:
    stress_suite_groups[entry['suite']].append(entry)
    if entry.get('layer'):
        stress_layer_groups[entry['layer']].append(entry)

stress_comparisons = []
for key, entry in stress_by_key.items():
    baseline = next(
        (
            row for row in previous_stress_rows
            if row.get('suite') == entry['suite']
            and row.get('name') == entry['name']
            and row.get('scenario') == entry['scenario']
        ),
        None,
    )
    if baseline is None:
        matches = previous_stress_by_name.get(entry['name'], [])
        baseline = matches[0] if matches else None
    baseline_throughput = parse_float(baseline.get('throughput_ops_per_s')) if baseline else None
    delta_pct = None
    if baseline_throughput and baseline_throughput > 0:
        delta_pct = ((entry['throughput_ops_per_s'] - baseline_throughput) / baseline_throughput) * 100.0
    stress_comparisons.append({
        'suite': entry['suite'],
        'name': entry['name'],
        'scenario': entry['scenario'],
        'throughput_ops_per_s': entry['throughput_ops_per_s'],
        'baseline_throughput_ops_per_s': baseline_throughput,
        'delta_pct': delta_pct,
        'variance_band': variance_band(entry['rel_stddev_runs']),
        'rel_stddev_runs': entry['rel_stddev_runs'],
        'layer': entry.get('layer'),
    })

stress_missing = [
    row for row in previous_stress_rows
    if (row.get('suite'), row.get('name'), row.get('scenario')) not in stress_by_key
]

stress_delta_summary = summarize_deltas(stress_comparisons, lower_is_better=False)
stress_delta_summary['missing'] = len(stress_missing)
stress_delta_summary['tracked'] = len(stress_comparisons)
stress_delta_summary['baseline_total'] = len(previous_stress_rows)
stress_delta_summary['median'] = statistics.median(stress_throughputs) if stress_throughputs else None
stress_delta_summary['p90'] = percentile(stress_throughputs, 0.90)
stress_delta_summary['best'] = max(stress_entries, key=lambda x: x['throughput_ops_per_s']) if stress_entries else None
stress_delta_summary['worst'] = min(stress_entries, key=lambda x: x['throughput_ops_per_s']) if stress_entries else None
stress_delta_summary['noisiest'] = sorted(stress_entries, key=lambda x: x['rel_stddev_runs'] or -1, reverse=True)[:5]
stress_delta_summary['best_layer'] = None
if stress_layer_groups:
    stress_delta_summary['best_layer'] = max(
        ((layer, sum(item['throughput_ops_per_s'] for item in items) / len(items)) for layer, items in stress_layer_groups.items()),
        key=lambda pair: pair[1],
    )
stress_comparison_map = {
    (item['suite'], item['name'], item['scenario']): item for item in stress_comparisons
}

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

def mean_or_none(values):
    return statistics.mean(values) if values else None


def fmt_ns(value):
    if value is None:
        return 'NA'
    return f'{value:.0f}'


def fmt_us(value):
    if value is None:
        return 'NA'
    return f'{value / 1e3:.3f}'


def fmt_ms(value):
    if value is None:
        return 'NA'
    return f'{value / 1e6:.3f}'


def fmt_ops(value):
    if value is None:
        return 'NA'
    return f'{value:.0f}'


def fmt_ratio(value):
    if value is None:
        return 'NA'
    return f'{value:.2f}x'


def stress_label(item):
    scenario = item.get('scenario')
    if scenario and scenario != 'unknown':
        return scenario

    name = item.get('name', '')
    if '::' in name:
        return name.split('::')[-1]

    return name or 'unknown'


def verdict_for_summary(criterion_summary, stress_summary):
    noisy_criterion = criterion_summary['criterion_bands']['noisy'] + criterion_summary['criterion_bands']['untrustworthy']
    noisy_stress = stress_summary['stress_bands']['noisy'] + stress_summary['stress_bands']['untrustworthy']
    regressions = criterion_summary['regressed'] + stress_summary['regressed']

    if regressions == 0 and noisy_criterion == 0 and noisy_stress == 0:
        return 'stable'
    if noisy_criterion > 0 or noisy_stress > 0:
        return 'mixed and noisy'
    return 'mixed'


OUT_MD.parent.mkdir(parents=True, exist_ok=True)
with OUT_MD.open('w', encoding='utf-8') as f:
    criterion_variance_counts = criterion_variance_bands
    stress_variance_counts = stress_variance_bands

    criterion_summary = {
        'count': len(entries),
        'suites': len(criterion_suite_groups),
        'median': criterion_delta_summary['median'],
        'p90': criterion_delta_summary['p90'],
        'best': criterion_delta_summary['fastest'],
        'worst': criterion_delta_summary['slowest'],
        'noisiest': criterion_delta_summary['noisiest'],
        'slowest_top': criterion_delta_summary['slowest_top'],
        'criterion_bands': criterion_variance_counts,
        'improved': criterion_delta_summary['improved'],
        'regressed': criterion_delta_summary['regressed'],
        'unchanged': criterion_delta_summary['unchanged'],
        'new': criterion_delta_summary['new'],
        'missing': criterion_delta_summary['missing'],
    }
    stress_summary = {
        'count': len(stress_entries),
        'suites': len(stress_suite_groups),
        'median': stress_delta_summary['median'],
        'p90': stress_delta_summary['p90'],
        'best': stress_delta_summary['best'],
        'worst': stress_delta_summary['worst'],
        'noisiest': stress_delta_summary['noisiest'],
        'stress_bands': stress_variance_counts,
        'improved': stress_delta_summary['improved'],
        'regressed': stress_delta_summary['regressed'],
        'unchanged': stress_delta_summary['unchanged'],
        'new': stress_delta_summary['new'],
        'missing': stress_delta_summary['missing'],
    }

    verdict = verdict_for_summary(criterion_summary, stress_summary)
    criterion_median = criterion_summary['median']
    criterion_p90 = criterion_summary['p90']
    stress_median = stress_summary['median']
    stress_p90 = stress_summary['p90']

    f.write('# Benchmark & Stress Test Summary\n\n')
    f.write('Generated from Criterion benchmarks and stress tests.\n\n')

    f.write('## Executive Summary\n\n')
    f.write(f'- Verdict: {verdict}.\n')
    f.write(f'- Criterion benchmarks: {criterion_summary["count"]} across {criterion_summary["suites"]} suites.\n')
    f.write(f'- Stress scenarios: {stress_summary["count"]} across {stress_summary["suites"]} suites.\n')
    if criterion_median is not None:
        f.write(f'- Criterion median latency: {fmt_us(criterion_median)} us; p90: {fmt_us(criterion_p90)} us.\n')
    if stress_median is not None:
        f.write(f'- Stress median throughput: {fmt_ops(stress_median)} ops/sec; p90: {fmt_ops(stress_p90)} ops/sec.\n')
    f.write(f'- Criterion variance bands: stable {criterion_summary["criterion_bands"]["stable"]}, acceptable {criterion_summary["criterion_bands"]["acceptable"]}, noisy {criterion_summary["criterion_bands"]["noisy"]}, untrustworthy {criterion_summary["criterion_bands"]["untrustworthy"]}.\n')
    f.write(f'- Stress variance bands: stable {stress_summary["stress_bands"]["stable"]}, acceptable {stress_summary["stress_bands"]["acceptable"]}, noisy {stress_summary["stress_bands"]["noisy"]}, untrustworthy {stress_summary["stress_bands"]["untrustworthy"]}.\n')
    f.write(f'- Baseline delta coverage: criterion improved {criterion_summary["improved"]}, regressed {criterion_summary["regressed"]}, new {criterion_summary["new"]}, missing {criterion_summary["missing"]}; stress improved {stress_summary["improved"]}, regressed {stress_summary["regressed"]}, new {stress_summary["new"]}, missing {stress_summary["missing"]}.\n')
    if criterion_summary['best'] is not None:
        f.write(f'- Fastest criterion benchmark: {criterion_summary["best"]["benchmark"]} at {fmt_us(criterion_summary["best"]["mean"])} us.\n')
    if criterion_summary['worst'] is not None:
        f.write(f'- Slowest criterion benchmark: {criterion_summary["worst"]["benchmark"]} at {fmt_us(criterion_summary["worst"]["mean"])} us.\n')
    if stress_summary['best'] is not None:
        f.write(f'- Best stress scenario: {stress_label(stress_summary["best"])} in {stress_summary["best"]["suite"]} at {fmt_ops(stress_summary["best"]["throughput_ops_per_s"])} ops/sec.\n')
    if stress_summary['worst'] is not None:
        f.write(f'- Weakest stress scenario: {stress_label(stress_summary["worst"])} in {stress_summary["worst"]["suite"]} at {fmt_ops(stress_summary["worst"]["throughput_ops_per_s"])} ops/sec.\n')

    f.write('\n## Key Findings\n\n')
    criterion_bucket_line = ', '.join(f'{bucket}={count}' for bucket, count in sorted(criterion_latency_bands.items(), key=lambda item: item[0]))
    stress_noisy_labels = ', '.join(f"{item['suite']}:{stress_label(item)}" for item in stress_summary['noisiest'])
    f.write(f'- Criterion latency shape: {criterion_bucket_line}.\n')
    f.write(f'- Criterion noisiest benches: {", ".join(item["benchmark"] for item in criterion_summary["noisiest"])}.\n')
    f.write(f'- Stress noisiest scenarios: {stress_noisy_labels}.\n')
    if stress_delta_summary.get('best_layer') is not None:
        layer_name, layer_mean = stress_delta_summary['best_layer']
        f.write(f'- Best average transport layer: {layer_name} at {fmt_ops(layer_mean)} ops/sec.\n')

    f.write('\n## Risk Areas\n\n')
    risky_criterion = [item for item in criterion_comparisons if item['variance_band'] in ('noisy', 'untrustworthy')]
    risky_stress = [item for item in stress_comparisons if item['variance_band'] in ('noisy', 'untrustworthy')]
    if risky_criterion:
        top_criterion_risk = ', '.join(item['benchmark'] for item in sorted(risky_criterion, key=lambda x: x['rel_stddev'] or -1, reverse=True)[:5])
        f.write(f'- Criterion instability needs review: {top_criterion_risk}.\n')
    else:
        f.write('- Criterion instability looks contained.\n')
    if risky_stress:
        top_stress_risk = ', '.join(f"{item['suite']}:{stress_label(item)}" for item in sorted(risky_stress, key=lambda x: x['rel_stddev_runs'] or -1, reverse=True)[:5])
        f.write(f'- Stress instability needs review: {top_stress_risk}.\n')
    else:
        f.write('- Stress instability looks contained.\n')
    if criterion_missing or stress_missing:
        f.write(f'- Missing baseline entries: criterion {len(criterion_missing)}, stress {len(stress_missing)}.\n')

    f.write('\n## Criterion Benchmarks\n\n')
    f.write('### Distribution\n\n')
    f.write('| bucket | count | share |\n')
    f.write('|---|---:|---:|\n')
    total_criterion = len(entries) or 1
    for bucket in ['<10us', '10-100us', '100us-1ms', '>1ms']:
        count = criterion_latency_bands.get(bucket, 0)
        f.write(f'| {bucket} | {count} | {count / total_criterion:.1%} |\n')

    f.write('\n### Variance Bands\n\n')
    f.write('| band | count | share |\n')
    f.write('|---|---:|---:|\n')
    total_variance = len(entries) or 1
    for band in ['stable', 'acceptable', 'noisy', 'untrustworthy']:
        count = criterion_variance_counts.get(band, 0)
        f.write(f'| {band} | {count} | {count / total_variance:.1%} |\n')

    f.write('\n### Baseline Comparison\n\n')
    if previous_criterion_rows:
        f.write('| outcome | count |\n')
        f.write('|---|---:|\n')
        for label in ['improved', 'regressed', 'unchanged', 'new', 'missing']:
            f.write(f'| {label} | {criterion_summary[label]} |\n')
        f.write('\n')
        f.write('| benchmark | current_us | baseline_us | delta | variance |\n')
        f.write('|---|---:|---:|---:|---|\n')
        movers = sorted(
            [item for item in criterion_comparisons if item['delta_pct'] is not None],
            key=lambda item: abs(item['delta_pct']),
            reverse=True,
        )[:10]
        for item in movers:
            f.write(
                f"| {item['benchmark']} | {fmt_us(item['mean'])} | {fmt_us(item['baseline_mean'])} | {format_delta(item['delta_pct'])} | {item['variance_band']} |\n"
            )
    else:
        f.write('No previous Criterion CSV found, so deltas are unavailable.\n')

    f.write('\n### Suite Snapshots\n\n')
    f.write('| suite | count | median_us | p90_us | max_us | unstable | slowest 3 | noisiest 3 |\n')
    f.write('|---|---:|---:|---:|---:|---:|---|---|\n')
    for suite_name in criterion_suite_order:
        suite_entries = criterion_suite_groups[suite_name]
        suite_means = [item['mean'] for item in suite_entries]
        suite_median = statistics.median(suite_means) if suite_means else None
        suite_p90 = percentile(suite_means, 0.90)
        suite_max = max(suite_means) if suite_means else None
        unstable = sum(1 for item in suite_entries if item['rel_stddev'] is not None and item['rel_stddev'] > 0.10)
        slowest_names = ', '.join(
            item['benchmark'].replace('\\', '/').split('/', 1)[-1]
            for item in sorted(suite_entries, key=lambda item: item['mean'], reverse=True)[:3]
        )
        noisiest_names = ', '.join(
            item['benchmark'].replace('\\', '/').split('/', 1)[-1]
            for item in sorted(suite_entries, key=lambda item: item['rel_stddev'] or -1, reverse=True)[:3]
        )
        f.write(
            f"| {suite_name} | {len(suite_entries)} | {fmt_us(suite_median)} | {fmt_us(suite_p90)} | {fmt_us(suite_max)} | {unstable} | {slowest_names} | {noisiest_names} |\n"
        )

    f.write('\n### Detailed Criterion Tables\n\n')
    for suite_name in criterion_suite_order:
        suite_entries = sorted(criterion_suite_groups[suite_name], key=lambda item: item['mean'])
        f.write(f'#### {suite_name}\n\n')
        f.write('| benchmark | current_us | baseline_delta | variance |\n')
        f.write('|---|---:|---:|---|\n')
        for item in suite_entries:
            comparison = criterion_comparison_map.get(item['benchmark'], {})
            f.write(
                f"| {item['benchmark'].replace('\\', '/').split('/', 1)[-1]} | {fmt_us(item['mean'])} | {format_delta(comparison.get('delta_pct'))} | {item['variance_band'] if 'variance_band' in item else variance_band(item['rel_stddev'])} |\n"
            )
        f.write('\n')

    f.write('## Stress Tests\n\n')
    if stress_entries:
        f.write('### Distribution\n\n')
        f.write('| band | count | share |\n')
        f.write('|---|---:|---:|\n')
        total_stress = len(stress_entries) or 1
        for band in ['stable', 'acceptable', 'noisy', 'untrustworthy']:
            count = stress_variance_counts.get(band, 0)
            f.write(f'| {band} | {count} | {count / total_stress:.1%} |\n')

        f.write('\n### Transport Comparison\n\n')
        if stress_layer_groups:
            layer_throughputs = {
                layer: statistics.mean(item['throughput_ops_per_s'] for item in items)
                for layer, items in stress_layer_groups.items()
            }
            best_layer_name, best_layer_throughput = max(layer_throughputs.items(), key=lambda item: item[1])
            f.write('| layer | scenarios | avg_ops_per_sec | ratio_to_best |\n')
            f.write('|---|---:|---:|---:|\n')
            for layer_name, throughput in sorted(layer_throughputs.items(), key=lambda item: item[1], reverse=True):
                scenarios = len(stress_layer_groups[layer_name])
                ratio = throughput / best_layer_throughput if best_layer_throughput else None
                f.write(f'| {layer_name} | {scenarios} | {fmt_ops(throughput)} | {fmt_ratio(ratio)} |\n')

            f.write('\n')
            direct_layer = layer_throughputs.get('direct')
            if direct_layer:
                for layer_name, throughput in sorted(layer_throughputs.items(), key=lambda item: item[1], reverse=True):
                    if layer_name == 'direct':
                        continue
                    slowdown = direct_layer / throughput if throughput else None
                    f.write(f'- {layer_name} averages {fmt_ratio(slowdown)} versus direct transport.\n')
        else:
            f.write('No layer metadata was recorded for these stress scenarios.\n')

        f.write('\n### Baseline Comparison\n\n')
        if previous_stress_rows:
            f.write('| outcome | count |\n')
            f.write('|---|---:|\n')
            for label in ['improved', 'regressed', 'unchanged', 'new', 'missing']:
                f.write(f'| {label} | {stress_summary[label]} |\n')
            f.write('\n')
            f.write('| suite | scenario | current_ops_per_sec | baseline_ops_per_sec | delta | variance |\n')
            f.write('|---|---|---:|---:|---:|---|\n')
            movers = sorted(
                [item for item in stress_comparisons if item['delta_pct'] is not None],
                key=lambda item: abs(item['delta_pct']),
                reverse=True,
            )[:10]
            for item in movers:
                f.write(
                    f"| {item['suite']} | {stress_label(item)} | {fmt_ops(item['throughput_ops_per_s'])} | {fmt_ops(item['baseline_throughput_ops_per_s'])} | {format_delta(item['delta_pct'])} | {item['variance_band']} |\n"
                )
        else:
            f.write('No previous stress CSV found, so deltas are unavailable.\n')

        f.write('\n### Suite Snapshots\n\n')
        f.write('| suite | count | median_ops_per_sec | p90_ops_per_sec | best | worst | unstable |\n')
        f.write('|---|---:|---:|---:|---|---|---:|\n')
        for suite_name in sorted(stress_suite_groups.keys()):
            suite_tests = stress_suite_groups[suite_name]
            throughputs = [item['throughput_ops_per_s'] for item in suite_tests]
            median_tp = statistics.median(throughputs) if throughputs else None
            p90_tp = percentile(throughputs, 0.90)
            best_item = max(suite_tests, key=lambda item: item['throughput_ops_per_s'])
            worst_item = min(suite_tests, key=lambda item: item['throughput_ops_per_s'])
            unstable = sum(1 for item in suite_tests if item['rel_stddev_runs'] is not None and item['rel_stddev_runs'] > 0.10)
            f.write(
                f"| {suite_name} | {len(suite_tests)} | {fmt_ops(median_tp)} | {fmt_ops(p90_tp)} | {stress_label(best_item)} | {stress_label(worst_item)} | {unstable} |\n"
            )

        f.write('\n### Detailed Stress Tables\n\n')
        for suite_name in sorted(stress_suite_groups.keys()):
            suite_tests = sorted(stress_suite_groups[suite_name], key=lambda item: item['throughput_ops_per_s'], reverse=True)
            f.write(f'#### {suite_name}\n\n')
            f.write('| scenario | layer | ops_per_sec | baseline_delta | variance | runs |\n')
            f.write('|---|---|---:|---:|---|---:|\n')
            for item in suite_tests:
                comparison = stress_comparison_map.get((item['suite'], item['name'], item['scenario']), {})
                f.write(
                    f"| {stress_label(item)} | {item.get('layer') or 'NA'} | {fmt_ops(item['throughput_ops_per_s'])} | {format_delta(comparison.get('delta_pct'))} | {item['variance_band'] if 'variance_band' in item else variance_band(item['rel_stddev_runs'])} | {item['num_runs']} |\n"
                )
            f.write('\n')
    else:
        f.write('## Stress Tests\n\nNo stress test results found.\n')

if CRITERION_ROOT.exists():
    print(f"Wrote {OUT_CSV} (criterion) with {len(entries)} entries.")
if STRESS_ROOT.exists():
    print(f"Wrote {STRESS_CSV} (stress) with {len(stress_entries)} entries.")
print(f"Wrote {OUT_MD} (summary).")
