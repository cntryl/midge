# Performance baseline procedure

The 0.5.0 milestone judges every performance change against a baseline taken
with this procedure. The rule in the milestone plan is that no implementation
PR for a Phase 1–3 issue starts until the issue carries a numeric target
relative to the current baseline, and the PR records before and after on the
named rows. This page says how the baseline is taken so two runs are
comparable.

## Machine and environment

- One machine, recorded in the evidence file: CPU model, core count, memory,
  storage device and filesystem, OS version, `rustc --version`, and the
  source SHA. The first 0.5.0 baseline is an Apple Silicon macOS machine on
  APFS; its numbers are for relative deltas within the milestone, not for
  publication as absolute performance.
- A quiet machine. No interactive desktop sessions (screen sharing, calls),
  no other builds or test runs, no other benchmark. On Linux pass
  `--require-quiet-env` so the harness fails on an adverse load or governor
  observation. On macOS the harness observes only the power source, so
  record `uptime` immediately before and after each run in the evidence
  file; a run whose load average exceeds the core count is discarded.
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

Keys are 16 bytes and values 100 bytes in the baseline matrix, one operation
per transaction, to match the `db_bench` workloads the rows are named after.
A RocksDB `db_bench` run with matched parameters on the same machine may be
recorded as context, never as a parity claim; see
[Performance Targets](performance-targets.md). The first 0.5.0 baseline is
Midge-only.

## Commands

Run each target three times in a row and keep every artifact. The harness
rejects `--quiet`; use `--no-progress`.

```bash
for run in 1 2 3; do
  uptime >> target/stress/baseline-uptime.txt
  cargo bench --bench tier2_subsystem_baseline_matrix -- --no-progress
  cp target/stress/tier2-subsystem-baseline-matrix/latest.json \
     "target/stress/baseline-matrix-run$run.json"
  uptime >> target/stress/baseline-uptime.txt
done
```

Repeat for `tier2_subsystem_read_amplification`,
`tier2_subsystem_durability_commit_latency`,
`tier2_subsystem_hybrid_durability_lag`, and
`tier4_system_recovery_throughput`. For the YCSB targets run
`BENCH_RUNS=3 cargo bench --bench tier4_ycsb_workload_a -- --no-progress`
and the same for `_b` through `_f`.

`--save-baseline` saves only runs without diagnostics; a row with a variance
diagnostic still writes `latest.json`, which is what the copy above keeps.

## Reading the artifacts

Every artifact is `cntryl-stress.v2` JSON. For each benchmark in
`.summaries[]`, record `stats.p50`, `stats.p99`, and
`stats.relative_std_dev` of the primary metric, and the trust class from
`.benchmark_specs[].metadata.trust_class`. The baseline value for a row is
the median of the three runs' `p50`. When the three runs disagree by more
than the row's tier guardrail band (5% for Tier 1, 8–10% for Tier 2, 15% for
Tier 4 throughput), the row is recorded as inconclusive with all three
values, not averaged, and rerun on a quieter machine.

## Evidence file

Commit `docs/development/evidence/performance-baseline-<version>-<sha>.md`
with:

- the machine and environment section filled in, including every `uptime`
  line and the `rustc` version;
- one table per target: row, median p50, p99 range, RSD range, trust class,
  and the inconclusive marker where it applies;
- the exact commands and the artifact filenames;
- what changed since the previous baseline file, if any.

Attach the raw `latest.json` copies under
`docs/development/evidence/baseline-<version>-<sha>/`.

## Setting targets from the baseline

A Phase 1–3 issue's target names the row, the metric, the baseline value,
and the required direction and size (for example "`readrandom hot` p50 at
most 0.8× baseline"). Targets are written into the issue before its branch
is created and checked in the PR with the same procedure, three runs,
against the same baseline file.
