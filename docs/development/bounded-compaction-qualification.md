# Bounded compaction qualification

This report records the Issue #270 Tier-4 qualification of bounded, partitioned
compaction. Results are populated from completed runs; no value below is a
placeholder.

## Build and host

- Git SHA: `4faff6344ed3f5cd2facdb0448074b72567d1a5f`
- Host: `Mac17,2`, Apple M5, arm64, 10 physical cores
- Memory: 25,769,803,776 bytes (24 GiB)
- Engine memory budget: 4 GiB
- Derived compaction-owned pool: 256 MiB
- Workload: one primary address entry plus postal, locality, and street lookup
  entries per base record (4 logical entries and 88 logical key/value bytes per
  base record)
- Storage: local filesystem, write-heavy throughput profile, 256 MiB memtable,
  background compaction disabled
- Qualification-only SST targets: 16 MiB at 1M, 64 MiB at 10M, and 128 MiB at
  65.3M base records. These use the hidden `failpoints` injection and do not add
  or change a production tuning surface. Targets scale so every rung produces
  multiple outputs without exceeding the planner's 64-input overlap safety
  limit.
- Crash protocol: the worker writes and syncs its evidence, then calls
  `process::exit` without engine shutdown. After the child is confirmed dead,
  the parent removes only the private process-local `.midge_leader.lock`, waits
  for the authoritative lease to expire, verifies crash recovery, shuts down,
  reopens cleanly, and verifies the digest again.

## Commands

The three commands used the same committed benchmark binary and fresh database
paths:

```text
cargo bench --bench tier4_bounded_compaction_qualification --features failpoints -- --base-records 1000000 --path /tmp/midge-bounded-final-1m-4faff63 --output /tmp/midge-bounded-final-1m-4faff63.json
cargo bench --bench tier4_bounded_compaction_qualification --features failpoints -- --base-records 10000000 --path /tmp/midge-bounded-final-10m-4faff63 --output /tmp/midge-bounded-final-10m-4faff63.json
cargo bench --bench tier4_bounded_compaction_qualification --features failpoints -- --base-records 65300000 --path /tmp/midge-bounded-final-65m3-4faff63 --output /tmp/midge-bounded-final-65m3-4faff63.json
```

Before each run the harness required at least `5 * logical dataset bytes + 20
GiB` free on the target filesystem. Observed available/required bytes were:

| Base records | Available | Required |
| ---: | ---: | ---: |
| 1,000,000 | 221,599,793,152 | 21,914,836,480 |
| 10,000,000 | 221,569,212,416 | 25,874,836,480 |
| 65,300,000 | 221,004,984,320 | 50,206,836,480 |

## Scale results

Peak RSS is the worker process maximum sampled by `sysinfo` every 100 ms across
ingest, compaction, and the pre-crash digest. Write amplification is cumulative
runtime-reported compaction bytes rewritten divided by pre-compaction SST bytes;
it includes emergent leveled follow-ups. Debt is the authoritative L0 layout
remaining after the explicit compaction request returns.

| Base records | Logical entries | Logical bytes | Ingest | Compaction | Peak RSS | Rewritten | Write amp | Outputs | L0 debt | Stalls / residue |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 4,000,000 | 88,000,000 | 4.911 s | 3.463 s | 1,430,913,024 B | 29,244,962 B | 1.0207x | 2 | 0 files / 0 B | 0 / 0 |
| 10,000,000 | 40,000,000 | 880,000,000 | 47.375 s | 278.811 s | 5,880,250,368 B | 1,213,888,332 B | 4.1602x | 6 | 0 files / 0 B | 0 / 0 |
| 65,300,000 | 261,200,000 | 5,746,400,000 | 333.216 s | 196.698 s | 4,342,530,048 B | 390,152,846 B | 0.2047x | 2 | 238 files / 1,730,982,144 B | 0 / 0 |

The 65.3M run intentionally reports its remaining bounded compaction debt rather
than implying that one manual request rewrote the full database. Its selected
input/output work exceeded both the 256 MiB compaction pool and 128 MiB target,
while the complete 261.2M-entry authority was verified after both reopen paths.

Final output sizes were:

- 1M, 16 MiB target: 16,605,831 and 12,639,131 bytes.
- 10M, 64 MiB target: 66,760,323; 66,718,927; 66,631,509;
  66,550,216; 66,498,445; and 9,990,534 bytes.
- 65.3M, 128 MiB target: 133,385,606 and 65,242,070 bytes.

Every output was at or below its target. The harness would also accept only the
asserted final user-key/block/metadata allowance of one configured block plus 16
KiB and 64 bytes; no run consumed that allowance. Output names were unique in
the manifest and every output was readable before the crash handoff.

## Read latency

Point samples used 1,000 evenly spaced primary keys. Prefix samples used 200
postal prefixes with a 100-entry limit.

| Base records | Cold point p95 / p99 | Warm point p95 / p99 | Cold prefix p95 / p99 | Warm prefix p95 / p99 |
| ---: | ---: | ---: | ---: | ---: |
| 1,000,000 | 114 / 140 us | 7 / 8 us | 101 / 124 us | 85 / 92 us |
| 10,000,000 | 72 / 77 us | 8 / 8 us | 231 / 242 us | 192 / 198 us |
| 65,300,000 | 155 / 196 us | 72 / 78 us | 16,457 / 16,678 us | 16,448 / 16,663 us |

## Recovery evidence

| Base records | Ordered XXH3-128 digest | Crash open | Clean reopen | Entries after both reopens |
| ---: | --- | ---: | ---: | ---: |
| 1,000,000 | `09cbddf004f89b4f6482757058761b56` | 1.995 s | 0.014 s | 4,000,000 |
| 10,000,000 | `0cdbdb0fa0e16e6b6468209d0bd70b43` | 1.965 s | 0.020 s | 40,000,000 |
| 65,300,000 | `48a5a18119d32b7e956dc85a7a70c0b9` | 1.786 s | 0.085 s | 261,200,000 |

For every rung, the digest and entry count before the deliberate crash, after
crash recovery, and after clean reopen were identical.

## Result artifact digests

The checked results above were transcribed from these JSON artifacts. SHA-256:

```text
25157ed0ae70a7c57cd2b32e38613abcc915fbf936dc37fa9f567bf49bee9bb3  /tmp/midge-bounded-final-1m-4faff63.json
54a22fdb27ac1ce74727f46d0921692b46377abecd7a174cd899cbf14e6fbf9e  /tmp/midge-bounded-final-10m-4faff63.json
bcb165208957ebf3dd01a04fa3091ab64a096cdad87de90cf89f9aa4fd9859c7  /tmp/midge-bounded-final-65m3-4faff63.json
```

The raw databases and JSON files are local qualification artifacts, not source
fixtures. The populated commands, SHA, host, measurements, recovery digests,
and artifact hashes are retained here for review and reproducibility.
