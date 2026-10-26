# Strict local commit latency baseline evidence

Source: `330c67a585c8bd078b0059cd93358fac61537a90` (2026-10-26).
Command: `cargo bench --locked --bench tier2_subsystem_durability_commit_latency -- --no-progress --require-quiet-env`.
Each full invocation has five measured fixed-work samples per row. Host
observations report Apple M5, 10 cores, macOS arm64, Rust 1.99.0, and AC power.
The harness observes power source only; no lab-idle claim is made.

Use three acceptable runs per row. Runs 1, 2, and 3 qualify the single-writer,
16-writer, 16-writer shared-hot-key, and 4 KiB rotation rows. The 64-writer row
uses runs 1, 3, and filtered run 4. Its run 2 result is retained but excluded:
the harness classified it noisy at 10.75% RSD. All selected rows have passing
correctness, gate trust, and no diagnostics. Jev favored per-row clean-run
selection over requiring the same invocation numbers for every row (confidence
0.94); the raw classifications were checked directly.

| Row | Median throughput (median of selected run medians) | Selected within-run RSDs | Selected run spread |
| --- | ---: | --- | ---: |
| 1 writer, 128-byte values | 97.97 transactions/s | 0.33%, 1.23%, 6.51% | 3.30% |
| 16 writers, 128-byte values | 1,478.28 transactions/s | 0.74%, 1.20%, 1.17% | 2.80% |
| 16 writers, shared hot key | 1,484.18 transactions/s | 0.36%, 2.15%, 0.67% | 3.23% |
| 64 writers, 128-byte values | 5,626.33 transactions/s | 0.57%, 1.22%, 1.94% | 2.89% |
| 4 KiB values with memtable rotation | 81.84 transactions/s | 0.45%, 1.45%, 3.24% | 3.89% |

The three selected run p99 throughput values ranged 97.22–103.93, 1,466.29–
1,534.19, 1,473.48–1,573.76, 5,622.91–5,870.59, and 79.91–83.74 transactions/s
in table order. In the 64-writer row, runs 1/3/4 had median commits per
physical fsync of 63.50, 63.75, and 63.63 respectively. The 16-writer distinct
row reported 15.85–15.91 commits per fsync across selected runs. The one-writer
row remained at one commit per fsync.

Raw JSON SHA-256:

| Run | Scope | SHA-256 |
| --- | --- | --- |
| 1 | Full suite | `cc2647ece2dab70a320004ce2ae5af7a81ba7f5c96f1851b321456f833df9bd4` |
| 2 | Full suite; 64-writer row noisy | `a99d19e255ad6bb40b22205e38ce91d92e49270f7f41231cc6a8a8e272141946` |
| 3 | Full suite | `4d27e962945239da5fc6652a2f4204f0979c9e56f9fe53acbf48d2dd3d8d3d55` |
| 4 | Filtered 64-writer row | `27ba764de5407ad3df73982283da7252f909664767607594a3240ca7be9da348` |

All artifacts record the same exact source commit and scenario parameters.
Uptime snapshots before and after are retained for runs 2–4. Run 1's after
snapshot is retained; its before snapshot was not captured. Raw JSON still
records the AC-power observation for run 1. Run 4 used
`--workload tier2_durability_commit_sync_local_64_writers`.
