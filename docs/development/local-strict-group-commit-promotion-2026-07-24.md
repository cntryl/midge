# Local strict group commit promotion

Date: 2026-07-24

## Decision

Promote the 100 microsecond candidate window for queued, independent local
`Strict` transactions. An empty queue does not open the window, so a sequential
writer stays on the direct fsync path.

Every grouped transaction retains its own `TxnBatch` WAL frame. The group uses
one append and one fsync, applies no memtable changes until that barrier
succeeds, publishes one snapshot, and acknowledges transactions in FIFO order.
Cloud, memory-only, spilled, mixed-policy, differently ordered, empty, and
overlapping requests remain on their existing paths.

## Window screen

Each window used three alternating no-group baseline/candidate release captures
with 512 fixed transactions per row. Throughput is the arithmetic mean in
transactions per second.

| Window (us) | 1 writer | 16 writers | 64 writers | Decision |
|---:|---:|---:|---:|---|
| 0 | 244 | 3,216 | 11,284 | Reject: sequential p99 gate |
| 10 | 248 | 2,885 | 11,057 | Survives |
| 25 | 217 | 3,028 | 10,023 | Reject: sequential latency/throughput gate |
| 50 | 246 | 3,406 | 11,526 | Survives |
| 100 | 254 | 3,431 | 13,309 | Select |

The 100 microsecond window had the highest geometric-mean concurrent
throughput. Its lead over 50 microseconds was greater than 2%, so the
shorter-window tie-break did not apply.

## Five-pair validation

The selected build was validated with five alternating isolated
baseline/candidate captures.

| Row | Baseline | Candidate | Ratio or change |
|---|---:|---:|---:|
| Sequential throughput | 255 tx/s | 244 tx/s | -4.2% |
| Sequential commit p50 | 3,982 us | 4,009 us | +0.7% |
| Sequential commit p99 | 5,053 us | 5,278 us | +4.4% |
| 16-writer throughput | 258 tx/s | 3,403 tx/s | 13.2x |
| 16-writer commit p99 | 69,068 us | 8,986 us | -87.0% |
| 16-writer commits/fsync | 1.00 | 14.02 | 14.0x |
| 64-writer throughput | 255 tx/s | 11,858 tx/s | 46.4x |
| 64-writer commit p99 | 261,554 us | 8,449 us | -96.8% |
| 64-writer commits/fsync | 1.00 | 50.63 | 50.6x |

The concurrent throughput ratios have a 24.7x geometric mean and every
concurrent row exceeds the 2x per-row floor.

The complete-system benchmark used 1,024 strict transactions, 16 writers,
128 KiB memtables, four explicit flush waves, completed compaction passes,
clean shutdown, reopen, point and scan digest verification, and final physical
SST footprint capture.

| Complete-system row | Baseline | Candidate | Ratio or change |
|---|---:|---:|---:|
| Strict ingest throughput | 211 tx/s | 1,545 tx/s | 7.3x |
| Total throughput through reopen verification | 205 tx/s | 1,283 tx/s | 6.2x |
| Physical fsyncs | 1,024 | 102 | -90.0% |
| Commits/fsync | 1.00 | 10.02 | 10.0x |
| Final SST count | 1.0 | 1.0 | unchanged |
| Final SST bytes | 19,301 | 19,233 | -0.4% |

Every complete-system capture completed compaction and clean shutdown, then
passed point-read and scan digest verification after reopen.

The existing transaction-latency benchmark also ran for five alternating
baseline/candidate captures against the pre-change snapshot-maintenance
behavior:

| Guard row | Change |
|---|---:|
| Buffered coalescing-signal throughput | -1.6% |
| Concurrent Buffered throughput | +126.1% |
| Concurrent Buffered commit p99 | -7.3% |
| Sequential Buffered throughput | +8.1% |
| Sequential Buffered commit p99 | -11.5% |
| Read-only begin-transaction throughput | +174.4% |

`BestEffort`, spilled, memory-only, and cloud requests are ineligible for the
new grouping path. Their full functional, recovery, and provider suites remain
part of the repository-wide verification below.

## Commands

```bash
cargo bench --bench tier2_subsystem_durability_commit_latency --no-default-features --no-run
cargo bench --bench tier4_system_strict_group_commit --no-default-features --no-run
```

Screening and validation invoked the resulting release binaries with one fixed
sample per alternating capture:

```bash
<binary> --samples 1 --warmup-samples 0 --cooldown-samples 0 --no-progress
```

The promotion is also gated by the focused WAL/runtime/failure/crash suites,
the full test and lint ladder, benchmark contract validation, module-size
validation, and diff hygiene checks.
