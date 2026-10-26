# Performance baseline procedure

The 0.5.0 milestone judges performance changes using alternating base/candidate
measurements on the same machine. Before a Phase 1–3 performance issue starts,
record its named row, metric, required relative change and base SHA (`develop`
at the PR's merge base). The evidence matrix required by #929 supplies absolute
context; an unavailable quiet-machine baseline does not block an A/B result.
Correctness work uses a failing invariant test and a relevant no-regression
check; assurance work specifies detection power and compute budget.

## Machine and environment

- One machine, recorded in the evidence file: CPU model, core count, memory,
  storage device and filesystem, OS version, `rustc --version`, and the
  source SHA. The first 0.5.0 baseline is an Apple Silicon macOS machine on
  APFS; its numbers are for relative deltas within the milestone, not for
  publication as absolute performance.
- Prefer a quiet machine without interactive calls, builds or other benchmark
  runs. On Linux `--require-quiet-env` rejects adverse load/governor
  observations. On macOS the harness observes only the power source, so record
  `uptime` before and after every run. A loaded run cannot establish an idle
  absolute baseline. It may contribute to an A/B comparison only with all load
  observations and diagnostics retained; unstable pairs are inconclusive.
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

Run each target three times in a row and keep every artifact. The harness
rejects `--quiet`; use `--no-progress`.

```bash
mkdir -p target/stress
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

## Performance PR comparison

Run three alternating pairs in one window on the same machine: base A1,
candidate B1, base A2, candidate B2, base A3, candidate B3. Use the existing
Cargo targets in each checkout, identical settings and independent fresh
fixtures. Record both immutable SHAs and save each run's `latest.json` before
the next invocation overwrites it. Record `uptime` around all six invocations.

For a lower-is-better metric report each `1 - B_i/A_i`; for a
higher-is-better metric report each `B_i/A_i - 1`. Report all three pair deltas
and their median against the issue's required improvement. Include p99 and
correctness/health guardrails rather than judging only the primary p50.
Alternation reduces drift bias; it does not cancel abrupt load changes or
validate unstable measurements. A row with inconsistent pairs or unresolved
variance diagnostics is inconclusive, not a pass obtained by averaging.

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
- what changed since the previous baseline file, if any;
- for a performance PR, both SHAs, A/B order, all six artifacts, three pair
  deltas, their median and the target verdict.

Attach the raw `latest.json` copies under
`docs/development/evidence/baseline-<version>-<sha>/`.

## Setting issue targets

A Phase 1–3 performance target names the row, metric, base SHA and required
direction/size (for example "`readrandom hot` p50 at least 20% lower than the
merge base"). Write the target before implementation and verify it with the
three A/B pairs above. The absolute matrix remains a release evidence
obligation. External comparison is deferred until a matched run exists; the
Midge-only matrix does not establish RocksDB or Pebble parity.
