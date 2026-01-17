#!/usr/bin/env python3
"""
Produce a CSV and Markdown summary of Criterion `estimates.json` files under target/criterion.
- Fields extracted: mean, mean_ci_lower, mean_ci_upper, std_dev (point_estimate), relative_stddev
- Flags high variance when relative_stddev > 0.10 (10%)
- Also writes human-friendly mean_us and mean_ms columns assuming the raw values are nanoseconds (common default). If your harness uses different units, ignore conversions.
"""
from pathlib import Path
import json
import csv

ROOT = Path(__file__).resolve().parents[1] / 'target' / 'criterion'
OUT_CSV = ROOT / 'benchmark_summary.csv'
OUT_MD = ROOT / 'benchmark_summary.md'

entries = []
for p in ROOT.rglob('new/estimates.json'):
    try:
        data = json.loads(p.read_text())
    except Exception as e:
        print(f"skipping {p} (read error): {e}")
        continue
    # Determine benchmark id as path relative to ROOT, omit trailing '/new/estimates.json'
    benchmark = str(p.relative_to(ROOT).parent.parent)
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

# Write CSV
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

# Write a small Markdown summary: top 10 fastest and slowest, and high-variance list
sorted_by_mean = sorted(entries, key=lambda x: x['mean'])
fastest = sorted_by_mean[:10]
slowest = sorted_by_mean[-10:][::-1]
high_var = [e for e in entries if e['high_variance']]

with OUT_MD.open('w', encoding='utf-8') as f:
    f.write('# Criterion benchmark summary\n\n')
    f.write('Note: mean_us / mean_ms assume raw numbers are in nanoseconds. If that is not the case, ignore those columns.\n\n')
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
        f.write('| benchmark | mean | std_dev | rel_stddev | file |\n')
        f.write('|---|---:|---:|---:|---|\n')
        for e in sorted(high_var, key=lambda x: x['rel_stddev'], reverse=True):
            f.write(f"| {e['benchmark']} | {e['mean']:.6f} | {e['std_dev'] or 'NA'} | {e['rel_stddev']:.6f} | {e['file']} |\n")

print(f"Wrote {OUT_CSV} and {OUT_MD} with {len(entries)} entries.")
