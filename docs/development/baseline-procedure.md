# Performance baseline procedure

The 0.5.0 milestone records absolute performance results for an exact source
SHA. For each run, record the named row, metric, workload, configuration,
machine and source SHA. Repeat measurements to characterize noise and retain
all raw results; mark unstable rows inconclusive. Acceptance does not require
base/candidate comparisons, relative deltas, or before/after measurements.
Correctness work uses a failing invariant test and a relevant no-regression
check; assurance work specifies detection power and compute budget.

## Machine and environment

- One machine, recorded in the issue summary: CPU model, core count, memory,
  storage device and filesystem, OS version, `rustc --version`, and the
  source SHA. The first 0.5.0 baseline is an Apple Silicon macOS machine on
  APFS; its results describe that hardware and configuration.
- Prefer a quiet machine without interactive calls, builds or other benchmark
  runs. On Linux `--require-quiet-env` rejects adverse load/governor
  observations. On macOS the harness observes only the power source, so record
  `uptime` before and after every run. A loaded run cannot establish a quiet
  baseline. Preserve its load observations and diagnostics and mark the result
  inconclusive when they prevent a reliable measurement.
- Mains power, performance power mode, and the laptop lid open.
- Release profile as the repository defines it (`lto = "thin"`,
  `codegen-units = 1`), the system allocator, default features.

## Rows

| Row | Target | Shape |
| --- | --- | --- |
| fillseq | `tier2_subsystem_baseline_matrix` | 100k sequential single-put `buffered()` commits |
| fillrandom | `tier2_subsystem_baseline_matrix` | 100k random single-put `buffered()` commits |
| fillrandom sync | `tier2_subsystem_baseline_matrix` | 10k random single-put `sync()` commits |
| readrandom hot | `tier2_subsystem_baseline_matrix` | 100k random gets against a memtable-resident set |
| readrandom cold | `tier2_subsystem_baseline_matrix` | 100k random gets after flush and a cold reopen |
| seekrandom | `tier2_subsystem_baseline_matrix` | 20k scans of 10 rows from random start keys |
| point-read amplification | `tier2_subsystem_read_amplification` | SSTs and blocks touched per read |
| strict commit latency | `tier2_subsystem_durability_commit_latency` | 1/16/64-writer `sync()` commits |
| hybrid durability lag | `tier2_subsystem_hybrid_durability_lag` | commit latency against cloud-durable lag |
| YCSB A–F | `tier4_ycsb_workload_a` … `_f` | long-running mixed workloads |
| recovery | `tier4_system_recovery_throughput` | reopen after flush and compaction |
| compaction CPU | `tier2_subsystem_compaction` | process CPU nanoseconds per logical input MiB for production `compact_all` over eight independently flushed SSTs |

Keys are 16 bytes and values 100 bytes in the baseline matrix, one operation
per transaction, to match the `db_bench` workloads the rows are named after.
A RocksDB `db_bench` run with matched parameters on the same machine may be
recorded as context, never as a parity claim; see
[Performance Targets](performance-targets.md). The first 0.5.0 baseline is
Midge-only.

## Absolute matrix commands

Run each target three times through targeted GitHub Actions dispatches, using
the same runner class and profile for every repeat. Keep the raw report in each
Actions artifact and save the run links. The workflow log records the runner,
toolchain and uptime. If running locally for diagnosis, keep generated files
under `target/`; do not commit them. The harness rejects `--quiet`; use
`--no-progress`.

Repeat for `tier2_subsystem_read_amplification`,
`tier2_subsystem_durability_commit_latency`,
`tier2_subsystem_hybrid_durability_lag`, and
`tier4_system_recovery_throughput`. For the YCSB targets run
`BENCH_RUNS=3 cargo bench --locked --bench tier4_ycsb_workload_a -- --no-progress --require-quiet-env`
and the same for `_b` through `_f`.

`--save-baseline` saves only runs without diagnostics; a row with a variance
diagnostic still writes `latest.json`, which is what the copy above keeps.

## Current-source measurement

Run the existing Cargo target three times at the same source SHA, with
identical settings and independent fresh fixtures. Keep every raw report and
link each workflow run. The raw `latest.json` stays attached to its Actions
run, and the workflow logs record uptime around the invocation. Report each
run and its summary statistics; do not calculate a cross-revision delta.
Unstable or inconsistent measurements are inconclusive, not a pass obtained by
averaging.

## Reporting results

Every artifact is `cntryl-stress.v2` JSON. For each benchmark in
`.summaries[]`, record `stats.p50`, `stats.p99`, and
`stats.relative_std_dev` of the primary metric, and the trust class from
`.benchmark_specs[].metadata.trust_class` in the issue summary. The baseline
value for a row is the median of the three runs' `p50`. When the three runs disagree by more
than the row's tier guardrail band (5% for Tier 1, 8–10% for Tier 2, 15% for
Tier 4 throughput), the row is recorded as inconclusive with all three
values, not averaged, and rerun on a quieter machine. Link the Actions runs and
leave the raw reports attached there; raw JSON and per-run uptime files do not
need to be committed to the repository.

## Issue summary

Post a concise summary to the owning issue with:

- workflow run links, source SHA, runner, toolchain and benchmark commands;
- one table per target: row, median p50, p99 range, RSD range, trust class,
  and the inconclusive marker where it applies;
- the absolute per-run results and the source/configuration they describe.

GitHub Actions retains raw `latest.json` reports as workflow artifacts. Do not
commit raw reports, per-run uptime logs or copied benchmark artifacts.

## Setting issue targets

Performance issues name the row, metric and any applicable absolute limit.
Record current-source results against that definition; if no absolute limit is
specified, measurements are diagnostic and do not imply a pass/fail
performance comparison. The baseline summary remains a release obligation;
workflow artifacts carry the raw runs. External comparison is deferred until a
matched run exists; the Midge-only matrix does not establish RocksDB or Pebble
parity.
