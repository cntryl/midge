# Performance Targets

Midge performance targets are regression guardrails. They are not roadmap
claims, public service-level objectives, or parity claims against RocksDB,
Pebble, or any other engine.

Use these targets to decide when a benchmark result needs investigation. A
single miss does not prove a defect by itself; repeat the benchmark on the same
machine, with the same configuration, and compare against a saved baseline.

## Regression Posture

Tier 1 stress microbenchmarks:

- Investigate a regression greater than 5% from a saved baseline.
- Treat smaller movements as noise unless they repeat across runs or affect a
  known critical path.

Tier 2 stress subsystem benchmarks:

- Investigate a regression greater than 8-10% from a saved baseline.
- For transaction latency guardrails, also investigate any explicit benchmark
  assertion failure.

Tier 3 and Tier 4 stress benchmarks:

- Investigate a sustained throughput drop greater than 15% across repeated runs.
- Investigate p99 latency growth greater than 20% across repeated runs.
- Prefer at least 3 runs for normal stress checks and 5 runs when deciding
  whether to accept a meaningful change in Tier 4 results.
- No workflow enforces these budgets automatically: every `bench-tier*.yml`
  workflow is dispatched by hand (Tiers 5 and 6 also on a schedule), and
  `ci.yml` runs no benchmarks. The 15% budget for the memory-mode batched-write
  row is checked with three alternating base/candidate pairs on the same
  machine, reporting the median pair delta and retaining inconclusive rows,
  following
  [the baseline procedure](baseline-procedure.md).

## External LSM Comparisons

RocksDB, Pebble, or other LSM comparisons are context only. They are valid only
for local mode and only when all of the following match:

- Same durability level.
- Same key size and value size.
- Same batch size and operation mix.
- Same compaction state and comparable warmed/cold cache state.
- Same machine, filesystem, and storage device.

Cloud and hybrid modes are excluded from external LSM parity comparisons.
Evaluate those modes against Midge durability, recovery, upload health, durable
frontier lag, write stalls, and bounded backlog.

## Guardrail Targets

Transaction latency and coalescing:

- The concurrent coalescing signal should keep
  `avg_txn_records_per_append >= 7.0`.
- Every measured commit sample must be accounted for exactly once.

Local throughput:

- Investigate when batched local buffered throughput falls below 50% of the
  memory-mode baseline for the same workload.
- PR #205 intentionally removed the memory-only caller grouping path to keep
  one transaction submission contract across storage modes. The resulting
  roughly 20% memory-mode reduction is the accepted comparison baseline, not
  an unreported production durability regression. Future changes are compared
  against their immediate base revision and must remain within the Tier 4 15%
  budget unless the PR explicitly revises this policy with evidence.

Cloud buffered async:

- `cloud_async_wal_uploads_failed == 0`.
- `write_stalls_cloud == 0` for default async durability benches.
- Commit p99 should be no more than `max(2x local buffered p99, 5ms)` for the
  same operation shape.

Cloud strict acknowledgement and sync seal:

- `cloud_async_wal_uploads_failed == 0`.
- For strict acknowledgement cases, `wal_cloud_durable_lag_end == 0`.
- Sync seal cases tag durable lag but do not claim cloud acknowledgement unless
  `WriteOptions::cloud_strict()` is used.

Hybrid:

- Tag local budget usage, pending evictions, cloud durable lag, and pending
  uploads.
- Fail only on failed uploads, no-space stalls, or local usage over 100%.
- Treat throughput and latency as baseline-regressed metrics rather than fixed
  absolute targets.

## Benchmark Ownership

- `tier4_system_checkpoint_write_amplification` owns the preregistered local
  checkpoint campaign: three cells, three fresh release repeats and a separate
  conditional policy gate. See
  [Checkpoint Write Amplification](checkpoint-write-amplification.md) for the
  5% byte or 20%-plus-5ms time predicate and accounting limits. The recorded
  outcome is below; this campaign does not change the general regression budgets.
- `tier4_system_local_throughput_regression` owns the local buffered throughput
  guard against memory mode and end-to-end local batch throughput.
- `tier2_subsystem_baseline_matrix` owns the `db_bench`-shaped rows every
  0.5.0 performance change is judged against: fillseq, fillrandom (buffered and
  sync), readrandom hot and cold, and seekrandom, with 16-byte keys and
  100-byte values on production defaults. Baselines are taken with
  [the baseline procedure](baseline-procedure.md).
- `tier2_subsystem_transaction_latency` owns public transaction lifecycle
  latency, commit percentile reporting, and buffered coalescing.
- `tier2_subsystem_durability_commit_latency` owns 1-, 16-, and 64-writer local
  strict throughput, commit p50/p99, WAL append/fsync counts, and commits per
  physical fsync.
- `tier2_subsystem_hybrid_durability_lag` owns the hybrid claim's two numbers
  side by side on the object-store simulator: `cloud_async()` and
  `cloud_strict()` commit p50/p99 against the cloud-durable lag sampled during
  the run (sequences and probe wait time), provider puts per commit, and bytes
  uploaded per committed byte, with the local `sync()` row as the floor. Rows
  are `gate` trust; the simulator is not a provider, so Sqrzl and live-provider
  lag are qualified separately.
- `tier4_system_strict_group_commit` owns fixed-work complete-system strict
  throughput through flush, compaction, clean shutdown, reopen digest
  verification, and final SST footprint. The current window selection and
  promotion evidence are recorded in
  `local-strict-group-commit-promotion-2026-07-24.md`.
- `tier4_system_compaction_backpressure` owns sustained local throughput and
  stall signals while compaction is active.
- `tier4_system_recovery_throughput` owns recovery throughput after flush and
  compaction in the supported storage modes.
- Tier 4 YCSB workloads own long-running workload regressions and hybrid
  backlog health tags.

## Recommended Verification

### Preregistered 0.5.0 pipeline targets

Issues #941 and #906 use aggregate reference
`172c941201f42e0206ca912d080e65c23ff4133c`, which includes ordered fsync and
completed-prefix acknowledgement. These targets are hypotheses for qualification,
not measured gains. Before concurrent apply starts, also pin the completed
#941/#947 prerequisite SHA to isolate apply's contribution.

| Change | Primary target | Guardrails |
| --- | --- | --- |
| #941 writer encoding | At least 20% lower event-loop CPU/service nanoseconds per committed 4 KiB unit on `tier2_durability_commit_sync_local_4k_rotation` | Writer encode/compression/CRC/XXH3/append service per unit at most 10% worse; 16/64-writer Strict throughput at least 10% higher in each row, commit p99 at most 5% worse |
| #906 concurrent apply | At least 10% higher throughput on `tier4_system_local_throughput_regression` | Transaction p99 at most 5% worse; 16/64-writer Strict throughput at least 10% higher in each row, commit p99 at most 5% worse |

For both, retain `tier4_complete_local_strict_group_commit_total` as the complete
system guard, including its digest, reopen and footprint checks. Use three
alternating base/candidate pairs on the same machine; retain both exact SHAs,
six raw artifacts, all pair deltas and their median. Missing CPU/service timing
is inconclusive and cannot be inferred from wall latency. Unstable observations
remain inconclusive. Use the existing benchmark facilities without a new runner.

Held-worker read/control progress, byte-compatible WAL, ordered whole-transaction
visibility, fencing and terminal memory ownership remain hard correctness gates.

#947 uses `90eb0b5c` as its writer-encoding reference. Its raw/fallback value
path targets one fewer allocation and 4096 fewer copied value bytes for the
deterministic incompressible 4 KiB fixture before final frame construction.
Borrowed pointer identity and byte-equal frames cover the construction claim;
allocation/copy measurements remain required where existing facilities expose
them. The rotation benchmark uses compressible repeated bytes, so its A/B
result is an end-to-end guardrail and does not prove this raw-value saving.

Run focused checks before broad suites:

```bash
cargo bench --bench tier2_subsystem_baseline_matrix -- --no-progress
cargo bench --bench tier2_subsystem_transaction_latency -- --no-progress
cargo bench --bench tier2_subsystem_durability_commit_latency -- --no-progress
cargo bench --bench tier4_system_strict_group_commit -- --no-progress
cargo bench --bench tier4_system_local_throughput_regression -- --no-progress
BENCH_RUNS=3 cargo bench --bench tier4_system_compaction_backpressure -- --no-progress
BENCH_RUNS=3 cargo bench --bench tier4_system_recovery_throughput -- --no-progress
BENCH_RUNS=3 cargo bench --bench tier4_ycsb_workload_a -- --no-progress
```

The harness rejects `--quiet`; `--no-progress` suppresses the progress stream
and `--help` lists the other options.

Before merging benchmark changes, also run:

```bash
cargo test --lib --quiet
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
git diff --check
```

### Recorded checkpoint campaign

The unchanged checkpoint cadence misses the preregistered filesystem-issued
snapshot payload target in cells B and C of [run 37274074026](https://github.com/cntryl/midge/actions/runs/37274074026),
attempt 1, measured source `c8f0de80de7925c4e3fe92b665639e4615e53281`. All nine fresh release
attempts passed construction and source/native/owner readback; B and C each
miss in all three repeats. A has one time miss across three repeats.
[The checkpoint guide](checkpoint-write-amplification.md#recorded-hosted-outcome)
retains every row, exact target, immutable receipt and the separate final-hour
acceptance boundary. This result measures engine-Fs payload and publication
time; it does not approve a cadence change or a physical-device cost claim.
