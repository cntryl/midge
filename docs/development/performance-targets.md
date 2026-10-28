# Performance Targets

Midge performance targets are regression guardrails. They are not roadmap
claims, public service-level objectives, or parity claims against RocksDB,
Pebble, or any other engine.

Use these targets to decide when a benchmark result needs investigation. A
single miss does not prove a defect by itself; repeat the benchmark on the same
machine and with the same configuration. Midge 0.5.0 performance evidence is
absolute and tied to one source SHA; comparing revisions is not an acceptance
requirement.

## Regression Posture

Tier 1 through Tier 4 stress benchmarks:

- Prefer at least 3 runs for normal stress checks and 5 runs when an absolute
  Tier 4 result needs stronger noise characterization.
- Preserve each run and mark a row inconclusive when its variance diagnostics
  exceed the documented tier guardrail.
- Investigate explicit benchmark assertion failures and correctness or health
  guardrail failures.
- No workflow enforces these budgets automatically: every `bench-tier*.yml`
  workflow is dispatched by hand (Tiers 5 and 6 also on a schedule), and
  `ci.yml` runs no benchmarks. See
  [the baseline procedure](baseline-procedure.md) for source-pinned absolute
  reporting.

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
  roughly 20% memory-mode reduction is a documented mode difference, not an
  unreported production durability regression. Report the row values for the
  exact source SHA; no cross-revision percentage gate applies.

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
- Report absolute throughput and latency for the exact source SHA; no
  cross-revision performance threshold applies.

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
  [0.5.0 baseline matrix status](https://github.com/cntryl/midge/issues/929).
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
  also report sampled cloud-frontier interarrival and, on cloud rows, the
  configured upload memory budget; these do not represent exact catalog-CAS
  timing or live queue-byte utilization. Rows are `gate` trust; the simulator
  is not a provider, so Sqrzl and live-provider lag are qualified separately.
  #916 has no accepted numeric latency target yet, so the new cloud-strict
  observations are diagnostic and carry no performance-improvement gate.
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
  throughput as capped. Keep its source-pinned absolute results and trust boundary
  explicit on
  [issue #927](https://github.com/cntryl/midge/issues/927).
- Tier 4 YCSB workloads own long-running workload regressions and hybrid
  backlog health tags.

## Recommended Verification

### 0.5.0 pipeline measurements and guards

The #906 current-source measurement includes the already integrated #941/#947
writer work. Record the full source SHA and the enabled workload/configuration.
Within-group concurrent effects must settle before one ordered visibility
publication; partial or unknown apply fences subsequent mutation until
recovery.

| Change | Primary target | Guardrails |
| --- | --- | --- |
| #941 writer encoding | Record absolute event-loop CPU/service nanoseconds per committed 4 KiB unit on `tier2_durability_commit_sync_local_4k_rotation`, with writer service time separated | Record 16/64-writer Strict throughput and commit p99 at the exact source SHA. Byte equality and held-encoder read/control progress are correctness gates; performance values are diagnostic. |
| #906 concurrent apply | Record absolute throughput on `tier4_system_local_throughput_regression` at the exact source SHA | Record transaction p99 and 16/64-writer Strict throughput. Preserve complete-system digest, reopen and footprint checks; performance values are diagnostic. |
| #940 async WAL fsync completion | Historical closed issue; its accounting distinguishes writer WAL fsyncs from ledger file/directory sync attempts and successes | No current performance gate. Do not present writer-only WAL fsync counts as total physical fsyncs. |
| #905 overlapping strict writes | Historical closed issue; its recorded acceptance compared the 16-writer shared-hot-key and distinct-key Strict rows and tracked `avg_txn_records_per_append` | No current performance gate. |
| #946 detached conflict validation | Record absolute p50 and p99 total conflict-checked commit latency on `abort_on_write_conflict_sst_backed` | Zero SST opens on the event loop remains a behavioral correctness gate. The latency values are diagnostic. |
| #908 write slowdown | On `tier4_system_compaction_backpressure` local, record absolute p50/p99 end-to-end commit latency and caller-side admission delay for the configured sustainable workload | Zero hard stalls is an independent workload gate. The admission-delay maximum does not bound WAL/provider service time or promise zero stalls under arbitrary sustained overload. |
| #926 logical SST moves | Informational absolute readout from `tier4_compaction_time_ordered_ingest_write_amplification`; no minimum target is required for issue closure | Each sample writes 16,384 ordered 512-byte values in 64-write buffered transactions, flushes, then runs production `compact_all`; background compaction is disabled until that explicit call. The fixture must begin with only L0 files and end with every SST above L0. It records published compaction output SST bytes (not flush bytes) and confirms logical moves by preserved SST identity. |
| #948 grandparent output cuts | Record absolute compaction output bytes per logical input byte and sustained throughput at the exact source SHA | Preserve the fixed layout, overlap-boundary and correctness assertions; no cross-revision performance threshold applies. See [issue #948](https://github.com/cntryl/midge/issues/948) for its workload shape. |
| #949 finalized compaction output and merge heap | Record absolute process CPU per logical input MiB in `tier2_subsystem_compaction` at the exact source SHA | The issue's optimization is covered by AC1/AC3 behavioral and identity checks; CPU and wall-time measurements are diagnostic and require no relative improvement. |
| #918 SST V5 restart/header change | `tier4_system_sst_v5` reports exact production decoder work for unique-key and 128-version key-group reads at latest and older snapshots. `tier4_system_sst_footprint` reports flushed SST bytes for 8,192 16-byte-key/32-byte-value rows. | Unique-key lookup must decode at most 23 entries; repeated-version latest and older-snapshot lookups at most 150 each. Report absolute SST bytes per logical key; footprint is diagnostic. Both fixtures use identical high-entropy key/value data and release settings. |

Historical measurements below remain records of prior completed runs, not
acceptance requirements for current source.

- #948 used seed `0x948020261006`, 2.7M base keys, 550K uniformly sampled
  updates, 16-byte keys and 128-byte deterministic high-entropy values. Three
  clean historical pairs at base `e9a0d86b7be0c1b298ef8f546326c5893aedba04`
  and candidate `e849fd96a5f528f794c313f6857a06c5b135c38b` measured throughput
  changes of -15.79%, -15.28% and -15.54% (median -15.54%). Output ratios were
  6.174 base and 10.816 candidate in each pair. The results and artifacts are
  on [issue #948](https://github.com/cntryl/midge/issues/948).
- #949 measured process CPU per logical input MiB at base
  `8883610fae287e0473064049e47f5477e7354db2` and candidate
  `ef67da29ad7e4caba7bb0de384dd0b0b0b2fb361`. Three historical pairs recorded
  reductions of 14.06%, 11.40% and 11.71% (median 11.71%). The comparison
  covered replace-top borrowed-key heap and finalized writer identity changes
  together; it did not isolate their individual CPU effects. See
  [issue #949](https://github.com/cntryl/midge/issues/949).

The #908 Tier 4 row retains one writer, 64 writes per transaction, 512-byte
values, and 512 KiB memtables. It records one p50/p99 sample for each successful
end-to-end transaction, including construction and any hard-stall retry; the
runtime's caller-side admission delay total/count/mean and observed hard-stall
count are separate outputs. Jev selected L0 soft limit 2 with a 100 μs maximum
admission delay (confidence 0.85) as the candidate starting setting. The
candidate-only Cargo feature is `candidate-write-backpressure-bench`. Record
the absolute p50/p99, admission-delay and stall-count values for the exact
source SHA and feature configuration; no base checkout or comparison overlay
is required.

For #941 and #906, retain `tier4_complete_local_strict_group_commit_total` as
the complete system check, including its digest, reopen and footprint
assertions. Record absolute CPU/service and throughput observations where
available; missing CPU/service timing is inconclusive and cannot be inferred
from wall latency. Unstable observations remain inconclusive. Use the existing
benchmark facilities without a new runner.

Held-worker read/control progress, byte-compatible WAL, ordered whole-transaction
visibility, fencing and terminal memory ownership remain hard correctness gates.

#947 uses `90eb0b5c` as its writer-encoding reference. Its raw/fallback value
path targets one fewer allocation and 4,096 fewer copied value bytes for the
deterministic incompressible 4 KiB fixture before final frame construction.
Borrowed pointer identity and byte-equal frames cover the construction claim. A
dedicated public Strict 4 KiB commit test now counts process allocation events
over 16 commits. Historical measurements at reference
`90eb0b5c684267f45605e1f2d42580b4da45cbf9` and candidate
`c1e6e9423fdd55f7f2766c3d83220c8eadc96f7e` show a median reduction of 19
allocations per 16 commits (1.188 per commit). Evidence and host load snapshots
are summarized on [issue #947](https://github.com/cntryl/midge/issues/947).
The source path removes the intermediate 4,096-byte raw-value copy; no runtime
copy-byte counter is available. The rotation benchmark uses compressible
repeated bytes, so its historical result is a separate end-to-end observation
and does not prove this raw-value saving. These historical results do not
establish a current acceptance requirement.

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
