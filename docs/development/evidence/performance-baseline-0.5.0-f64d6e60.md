# Midge 0.5.0 performance baseline matrix status

This is the evidence matrix checkpoint for issue #929 at source
`f64d6e607d5990eac82441732cb2654dfac475aa` (2026-10-26). It is not a
qualified absolute baseline: the prescribed three-run absolute campaign has
not been completed. Existing issue A/B evidence is listed for traceability but
does not substitute for three absolute measurements of the same source.

## Machine and campaign status

- Host: Apple M5, 10 cores, macOS arm64, APFS, AC power; Rust
  `1.99.0 (b940084d7 2026-09-28)`.
- `uptime` at checkpoint: `4:22 up 9:25, 3 users, load averages: 1.88 2.37 2.27`.
- This load snapshot is not a quiet-campaign qualification. No baseline row was
  measured for this matrix checkpoint. Collect uptime immediately before and
  after every future run, retain all raw harness artifacts, and mark rows with
  unstable load or variance inconclusive.
- Build profile and workload shapes are defined by
  [the baseline procedure](../baseline-procedure.md). Its commands are the
  canonical run instructions.

## Absolute rows

All values are intentionally `not measured` here. A row is complete only
after three runs on this source with raw artifacts, p50/p99/RSD, trust class,
and exact commands recorded. The referenced A/B campaigns are issue-specific
and are not absolute baselines.

| Row | Benchmark | Absolute status | Existing related evidence |
| --- | --- | --- | --- |
| fillseq | `tier2_subsystem_baseline_matrix` | Not measured | None |
| fillrandom buffered | `tier2_subsystem_baseline_matrix` | Not measured | None |
| fillrandom sync | `tier2_subsystem_baseline_matrix` | Not measured | #940 strict fsync A/B is a different preregistered comparison |
| readrandom hot | `tier2_subsystem_baseline_matrix` | Not measured | None |
| readrandom cold | `tier2_subsystem_baseline_matrix` | Not measured | None |
| seekrandom | `tier2_subsystem_baseline_matrix` | Not measured | None |
| point-read amplification | `tier2_subsystem_read_amplification` | Not measured | None |
| strict commit latency, 1/16/64 writers | `tier2_subsystem_durability_commit_latency` | Not measured | #905, #940, #941, #978 are issue-specific A/B campaigns |
| hybrid durability lag | `tier2_subsystem_hybrid_durability_lag` | Not measured | No absolute matrix campaign |
| YCSB A | `tier4_ycsb_workload_a` | Not measured | None |
| YCSB B | `tier4_ycsb_workload_b` | Not measured | None |
| YCSB C | `tier4_ycsb_workload_c` | Not measured | None |
| YCSB D | `tier4_ycsb_workload_d` | Not measured | None |
| YCSB E | `tier4_ycsb_workload_e` | Not measured | None |
| YCSB F | `tier4_ycsb_workload_f` | Not measured | None |
| recovery | `tier4_system_recovery_throughput` | Not measured | #927 10,000-SST inventory A/B is diagnostic fixed-work evidence, not the absolute recovery row |
| compaction CPU | `tier2_subsystem_compaction` | Not measured | #949 is an issue-specific A/B comparison |

## Relative campaigns retained separately

The repository contains issue-scoped evidence under `issue-905-*`,
`issue-927-*`, `issue-940-*`, `issue-941-*`, `issue-947-*`, `issue-948-*`,
`issue-949-*`, and `issue-978-*`. Each README records its own source revisions,
workload, limitations, and pair verdict. These results remain valid only for
their stated scopes. They are not combined into synthetic absolute rows, and
unstable, diagnostic-trust, or incomplete comparisons remain inconclusive for
their intended gates.

RocksDB/Pebble parity remains unmeasured. The matrix is Midge-only; external
comparisons require a separately recorded matched workload and machine.
