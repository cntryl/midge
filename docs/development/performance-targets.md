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
  [the baseline procedure](baseline-procedure.md). The current three-run
  values for these six rows are in the
  [0.5.0 baseline matrix](evidence/performance-baseline-0.5.0-64389f99.md).
- `tier2_subsystem_transaction_latency` owns public transaction lifecycle
  latency, commit percentile reporting, and buffered coalescing.
- `tier2_subsystem_durability_commit_latency` owns 1-, 16-, and 64-writer local
  strict throughput, commit p50/p99, WAL append/fsync counts, commits per
  physical fsync, average transactions per append, and the 16-writer hot-key
  comparison against the distinct-key row.
  [WAL sync accounting](wal-sync-accounting.md) distinguishes the writer-only
  metric from ledger file/directory calls and the explicitly scoped accounted
  sum; adding those counters does not approve amortized ledger floors.
- `tier2_subsystem_hybrid_durability_lag` owns the hybrid claim's two numbers
  side by side on the object-store simulator: `cloud_async()` and
  `cloud_strict()` commit p50/p99 against the cloud-durable lag sampled during
  the run (sequences and probe wait time), provider puts per commit, and bytes
  uploaded per committed byte, with the local `sync()` row as the floor. Rows
  are `gate` trust; the simulator is not a provider, so Sqrzl and live-provider
  lag are qualified separately.
- `tier5_hybrid_durability_lag` contains the Sqrzl diagnostic counterparts and
  local floor. The [provider qualification guide](hybrid-lag-provider-qualification.md)
  defines sampler health and observed native HTTP cost scope; execution and
  committed provider evidence remain pending.
- `tier4_system_strict_group_commit` owns fixed-work complete-system strict
  throughput through flush, compaction, clean shutdown, reopen digest
  verification, and final SST footprint. The current window selection and
  promotion evidence are recorded in
  `local-strict-group-commit-promotion-2026-07-24.md`.
- `tier4_system_compaction_backpressure` owns sustained local throughput and
  stall signals while compaction is active.
- `tier4_system_recovery_throughput` owns recovery throughput after flush and
  compaction in the supported storage modes.
- Its `tier4_recovery_cloud_inventory_10k` row measures production manifest
  SST validation over 10,000 filesystem-mock objects with 1 ms simulated HEAD
  delay; fixture seeding is outside the row's explicit timer. This fixed-work
  row is intentionally diagnostic because the stress harness classifies its
  throughput as capped. Keep its source-pinned A/B results and trust boundary
  explicit in
  `evidence/issue-927-inventory-ab-2026-10-26/README.md`.
- Tier 4 YCSB workloads own long-running workload regressions and hybrid
  backlog health tags.

## Recommended Verification

### Preregistered 0.5.0 pipeline targets

Issues #941 and #906 use aggregate reference
`172c941201f42e0206ca912d080e65c23ff4133c`, which includes ordered fsync and
completed-prefix acknowledgement. These targets are hypotheses for qualification,
not measured gains. Before concurrent apply starts, also pin the completed
#941/#947 prerequisite SHA to isolate apply's contribution.

The pinned isolated #906 baseline is
`c1e6e9423fdd55f7f2766c3d83220c8eadc96f7e`, after those resident Strict
encoding and raw-value copy candidates. Within-group concurrent effects must
settle before one ordered visibility publication; partial or unknown apply
fences subsequent mutation until recovery.

| Change | Primary target | Guardrails |
| --- | --- | --- |
| #941 writer encoding | At least 20% lower event-loop CPU/service nanoseconds per committed 4 KiB unit on `tier2_durability_commit_sync_local_4k_rotation` | Writer encode/compression/CRC/XXH3/append service per unit at most 10% worse; 16/64-writer Strict throughput at least 10% higher in each row, commit p99 at most 5% worse |
| #906 concurrent apply | At least 10% higher throughput on `tier4_system_local_throughput_regression` | Transaction p99 at most 5% worse; 16/64-writer Strict throughput at least 10% higher in each row, commit p99 at most 5% worse |
| #940 async WAL fsync completion | On the 16- and 64-writer local Strict rows in `tier2_subsystem_durability_commit_latency`, require at least 10% higher commits per successful physical WAL fsync in the median of three alternating base/candidate pairs | Report all six pair values; commit p99 and strict throughput each no more than 5% worse. Report ledger file/directory sync attempts and successes separately; do not present writer-only WAL fsync counts as total physical fsyncs. Base `e9a0d86b7be0c1b298ef8f546326c5893aedba04`; candidate `e849fd96a5f528f794c313f6857a06c5b135c38b`. |
| #905 overlapping strict writes | In `tier2_subsystem_durability_commit_latency`, the 16-writer single-shared-hot-key Strict row must reach at least 80% of the distinct-key row's throughput; `avg_txn_records_per_append` must remain at least 7.0 | Report three alternating base/candidate pairs and their deltas/medians for both rows, without adding a separate speedup claim. Base `e9a0d86b7be0c1b298ef8f546326c5893aedba04`; candidate `e849fd96a5f528f794c313f6857a06c5b135c38b`. |
| #946 detached conflict validation | On `abort_on_write_conflict_sst_backed`, median of three alternating pair deltas for both p50 and p99 total conflict-checked commit latency must be no worse than +5% | Report every pair delta; zero SST opens on the event loop remains a behavioral correctness gate. This is a bounded-latency target, not a claim that detached validation improves isolated commit latency. |
| #908 write slowdown | On `tier4_system_compaction_backpressure` local, with the configured soft limits enabled under the preregistered sustainable workload, median of three alternating pair deltas for p50 and p99 end-to-end commit latency must each be no worse than +5% | Report admission delay separately from total commit latency; zero hard stalls is an independent workload gate. The admission-delay maximum does not bound WAL/provider service time or promise zero stalls under arbitrary overload. |
| #926 logical SST moves | Informational readout from `tier4_compaction_time_ordered_ingest_write_amplification`; no minimum delta or A/B campaign is required for issue closure | Each sample writes 16,384 ordered 512-byte values in 64-write buffered transactions, flushes, then runs production `compact_all`; background compaction is disabled until that explicit call. The fixture must begin with only L0 files and end with every SST above L0. It records published compaction output SST bytes (not flush bytes) and confirms logical moves by preserved SST identity. Use it to observe write-amplification and throughput during development; do not claim improvement from one run. |
| #948 grandparent output cuts | On `tier4_system_compaction_grandparent_cuts`, report the three alternating pair deltas for compaction-output bytes per logical input byte; no minimum byte reduction is preregistered | Sustained throughput must not regress by more than 15%, consistent with the Tier 4 guardrail. The fixed workload uses seed `0x948020261006`, 2.7M base keys, 550K uniformly sampled updates, 16-byte keys, and 128-byte deterministic high-entropy values with default production options. Setup repeats production `compact_all` until the layout stabilizes, then requires the update span to overlap more than 320 MiB across at least two actual L2 SSTs, each individually smaller than the 320 MiB limit. Use the release profile with five measured samples and one warmup per invocation; five is the stress harness minimum for its quality assessment. Any noisy invocation remains inconclusive and must be rerun. The output-byte metric is published compaction output SST bytes; it excludes flush bytes and is not whole-engine physical write amplification. The focused executor regression separately asserts that the candidate overlap-roll branch fires. Base `e9a0d86b7be0c1b298ef8f546326c5893aedba04`; candidate `e849fd96a5f528f794c313f6857a06c5b135c38b`. Evidence: `docs/development/evidence/issue-948-ab-2026-10-25/`. Three clean pairs completed: candidate throughput changes were -15.79%, -15.28%, and -15.54% (median -15.54%), exceeding the 15% guardrail; output ratios were 6.174 base and 10.816 candidate in each pair. The target therefore does not qualify a performance benefit. |
| #949 finalized compaction output and merge heap | On `tier2_subsystem_compaction`, the median of three alternating pair deltas for process CPU nanoseconds per logical input MiB must be lower on the candidate | Fixture: eight independently flushed SSTs with 100,000 unique 16-byte keys and 128-byte deterministic high-entropy values per SST; measure one production `compact_all`, record process CPU across compaction workers, wall time and published output bytes separately. Require five measured samples per invocation and no unresolved Tier 2 variance diagnostic (10% RSD guardrail). Base `8883610fae287e0473064049e47f5477e7354db2`; candidate `ef67da29ad7e4caba7bb0de384dd0b0b0b2fb361`. This comparison covers the replace-top borrowed-key heap and finalized writer identity changes together, so it does not isolate their individual CPU effects. Three accepted pairs completed with reductions of 14.06%, 11.40%, and 11.71% (median 11.71%). Evidence: [issue-949 A/B results](evidence/issue-949-ab-2026-10-25/README.md). |
| #918 SST V5 restart/header change | `tier4_system_sst_v5` reports exact production decoder work for a unique key and a 128-version key group at latest and older snapshots. `tier4_system_sst_footprint` reports flushed SST bytes for 8,192 16-byte-key/32-byte-value rows and is compatible with the V4 baseline. | Unique-key lookup must decode at most 23 entries; repeated-version latest and older-snapshot lookups at most 150 each. Compare SST bytes per logical key across three alternating base/candidate pairs and report all deltas; footprint improvement is diagnostic until a representative baseline establishes a preregistered minimum. Both fixtures use identical high-entropy key/value data and release settings. V5 point-read lookup stats are collected through the hidden benchmark facade over the production SST reader. |

The #908 Tier 4 row retains one writer, 64 writes per transaction, 512-byte
values, and 512 KiB memtables. It records one p50/p99 sample for each successful
end-to-end transaction, including construction and any hard-stall retry; the
runtime's caller-side admission delay total/count/mean and observed hard-stall
count are separate outputs. Jev selected L0 soft limit 2 with a 100 μs maximum
admission delay (confidence 0.85) as the candidate starting setting. The
candidate-only Cargo feature is `candidate-write-backpressure-bench`. Since the
base SHA predates `WriteBackpressureConfig`, create a base worktree and apply the
benchmark-only overlay commit containing `Cargo.toml`,
`benches/bench_support/config.rs`, and
`benches/tier4_system_compaction_backpressure.rs`; run the base without the
feature and the candidate with
`--features candidate-write-backpressure-bench`. The runtime source SHA at base
must remain `e9a0d86b7be0c1b298ef8f546326c5893aedba04`; the overlay changes only
the measurement harness. The focused compile passed in both feature modes; no
latency or stall outcome is claimed until the alternating A/B is run.

For #941 and #906, retain `tier4_complete_local_strict_group_commit_total` as
the complete system guard, including its digest, reopen and footprint checks.
Use three alternating base/candidate pairs on the same machine; retain both
exact SHAs, six raw artifacts, all pair deltas and their median. Missing
CPU/service timing is inconclusive and cannot be inferred from wall latency.
Unstable observations remain inconclusive. Use the existing benchmark
facilities without a new runner.

Held-worker read/control progress, byte-compatible WAL, ordered whole-transaction
visibility, fencing and terminal memory ownership remain hard correctness gates.

#947 uses `90eb0b5c` as its writer-encoding reference. Its raw/fallback value
path targets one fewer allocation and 4,096 fewer copied value bytes for the
deterministic incompressible 4 KiB fixture before final frame construction.
Borrowed pointer identity and byte-equal frames cover the construction claim. A
dedicated public Strict 4 KiB commit test now counts process allocation events
over 16 commits. Three alternating pairs at reference
`90eb0b5c684267f45605e1f2d42580b4da45cbf9` and candidate
`c1e6e9423fdd55f7f2766c3d83220c8eadc96f7e` show a median reduction of 19
allocations per 16 commits (1.188 per commit). Evidence and host load snapshots
are in `docs/development/evidence/issue-947-public-strict-allocation-ab-2026-10-25/`.
The source path removes the intermediate 4,096-byte raw-value copy; no runtime
copy-byte counter is available. The rotation benchmark uses compressible
repeated bytes, so its A/B result remains a separate end-to-end guardrail and
does not prove this raw-value saving.

Run focused checks before broad suites:

```bash
cargo bench --bench tier2_subsystem_baseline_matrix -- --no-progress
cargo bench --bench tier2_subsystem_transaction_latency -- --no-progress
cargo bench --bench tier2_subsystem_durability_commit_latency -- --no-progress
cargo bench --bench tier4_system_strict_group_commit -- --no-progress
cargo bench --bench tier4_system_local_throughput_regression -- --no-progress
BENCH_RUNS=3 cargo bench --bench tier4_system_compaction_backpressure -- --no-progress
cargo bench --bench tier4_system_compaction_grandparent_cuts -- --profile release --samples 5 --warmup-samples 1 --no-progress
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
