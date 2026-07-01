# Performance Targets

Midge performance targets are regression guardrails. They are not roadmap
claims, public service-level objectives, or parity claims against RocksDB,
Pebble, or any other engine.

Use these targets to decide when a benchmark result needs investigation. A
single miss does not prove a defect by itself; repeat the benchmark on the same
machine, with the same configuration, and compare against a saved baseline.

## Regression Posture

Tier 1 Criterion benchmarks:

- Investigate a regression greater than 5% from a saved baseline.
- Treat smaller movements as noise unless they repeat across runs or affect a
  known critical path.

Tier 2 Criterion benchmarks:

- Investigate a regression greater than 8-10% from a saved baseline.
- For transaction latency guardrails, also investigate any explicit benchmark
  assertion failure.

Tier 3 and Tier 4 stress benchmarks:

- Investigate a sustained throughput drop greater than 15% across repeated runs.
- Investigate p99 latency growth greater than 20% across repeated runs.
- Prefer at least 3 runs for normal stress checks and 5 runs when deciding
  whether to accept a meaningful change in Tier 4 results.

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

- The buffered direct-submit path should keep
  `write_group_follower_wait_us <= 1.0` on average.
- The concurrent coalescing signal should keep
  `avg_txn_records_per_append >= 7.0`.
- Every measured commit sample must be accounted for exactly once.

Local throughput:

- Investigate when batched local buffered throughput falls below 50% of the
  memory-mode baseline for the same workload.

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

- `tier2_subsystem_local_throughput_regression` owns the local buffered
  throughput guard against memory mode.
- `tier2_subsystem_transaction_latency` owns public transaction lifecycle
  latency, commit percentile reporting, and buffered coalescing.
- `tier4_system_engine_batch_throughput` owns end-to-end local batch-size
  scaling; cloud coverage is intentionally limited to representative cases.
- `tier4_system_durability_cloud` owns cloud commit latency tags and cloud WAL
  health guardrails.
- Tier 4 YCSB workloads own long-running workload regressions and hybrid
  backlog health tags.

## Recommended Verification

Run focused checks before broad suites:

```bash
cargo bench --bench tier2_subsystem_transaction_latency -- --quiet
cargo bench --bench tier2_subsystem_local_throughput_regression -- --quiet
BENCH_RUNS=5 cargo bench --bench tier4_system_engine_batch_throughput -- --quiet
BENCH_RUNS=5 cargo bench --bench tier4_system_durability_cloud -- --quiet
BENCH_RUNS=3 cargo bench --bench tier4_ycsb_workload_a -- --quiet
```

Before merging benchmark changes, also run:

```bash
cargo test --lib --quiet
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
git diff --check
```
