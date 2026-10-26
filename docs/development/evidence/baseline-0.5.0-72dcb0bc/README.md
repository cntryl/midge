# Tier 2 read amplification baseline evidence

These measurements were captured at source revision
`72dcb0bcadd80a2851c528384573b5269b9a4691` on 2026-10-26 with
`cargo bench --locked --bench tier2_subsystem_read_amplification -- --no-progress --require-quiet-env`.
Each invocation used five measured samples and one warmup in the release
profile. The host was an Apple M5 (10 cores), macOS arm64, Rust 1.99.0, on AC
power. The benchmark harness observed power source only; these measurements do
not claim an otherwise idle host.

Runs 1, 2, and 4 are the three-run summary. Run 3 is retained below but excluded
because `long_scan` was classified noisy. The three selected runs have
acceptable quality, passing correctness, and no diagnostics for all rows.
The benchmark source still sets `trust_class=diagnostic` and
`diagnostic_reason=pending_three_clean_baselines`; therefore the results remain
diagnostic context and do not qualify the rows as gate baselines. Jev advised
preserving that source trust classification (confidence 1.0); source and
artifacts were independently checked.

| Scenario | Median throughput (median of run medians) | p99 range across runs | Within-run RSDs | Between-run median spread |
| --- | ---: | ---: | ---: | ---: |
| `long_scan` | 1,088,628.83 row/s | 1,151,039.31–1,281,999.63 | 4.07%, 8.27%, 9.25% | 0.61% |
| `point_hit_miss` | 873,246.70 query/s | 888,742.79–1,012,217.89 | 7.31%, 1.25%, 4.27% | 5.35% |
| `mixed_point_short_scan` | 491,921.59 query/s | 487,751.48–555,312.57 | 1.61%, 5.98%, 1.36% | 2.29% |

The point scenarios each performed 32,768 point reads (24,576 hits and 8,192
misses) over three overlapping L0 SSTs. Both observed 48,768 SST touches,
73,344 blocks read, 1.4883 SST touches per point read, and 2.2383 blocks read
per point read. The mixed scenario additionally performed 1,024 short scans
over 8,192 rows. `long_scan` performed 64 scans over 16,384 rows.

Raw artifact SHA-256:

| Run | Quality | SHA-256 |
| --- | --- | --- |
| 1 | all rows acceptable | `2b66d4aa512a61b3eb7240b1f95c06232793c0a6da41ee26321b55486c29ff1c` |
| 2 | all rows acceptable | `56b795daff11c5df424a1d0de1cb6de6def4d89222ba22aa70ca447b57299d05` |
| 3 | `long_scan` noisy; excluded | `9e063b3d004d3078722b80986be83bbade4187921a5f4bb5583e764d4f3cf864` |
| 4 | all rows acceptable | `ed83a886439b71cd3ffe5e34fd00caed42a4f9ac9288fce5ce46936211ee6e3f` |

Each JSON records the same source commit, benchmark parameters, sample data,
correctness results, trust metadata, and quality classification. Uptime
snapshots before and after each invocation are retained alongside the JSON.
