# 0.5.0 baseline campaign at `64389f99`

This directory contains three absolute baseline runs for the six rows in
`tier2_subsystem_baseline_matrix`. Every row has three acceptable runs; the
reported baseline p50 is the median of those runs.

## Source and environment

- Source: `64389f99c264e397599a049e18836f38a962c321`.
- Host: Apple M5, 10 cores, 24 GiB memory, macOS 27.0.1 arm64; internal
  `APPLE SSD AP1024Z` over Apple Fabric, APFS data volume; AC power.
- Rust: `rustc 1.99.0 (b940084d7 2026-09-28)`.
- Harness: cntryl-stress 0.5.1, release build, default stress profile, five
  measured samples and one warmup per row.
- Command, run three times: `cargo bench --locked --bench tier2_subsystem_baseline_matrix -- --no-progress --require-quiet-env`.
- The quiet-environment flag recorded AC power and no adverse observation.
  Each uptime snapshot below records 1/5/15 minute load averages on the
  10-core host. All three JSON artifacts report five measured samples plus one
  warmup per row and no harness diagnostics.

| Run | Load before | Load after | Raw JSON SHA-256 |
| --- | --- | --- | --- |
| 1 | 2.28/2.22/2.18 | 2.32/1.91/1.95 | `3566b0d6a5707eee76f61811eb6d2abfcc5dca41acf828041221f7c803321c7c` |
| 2 | 2.04/1.90/1.94 | 1.65/1.76/1.80 | `650d69d01c3e14488a8d724d16f622f2d243e0494b4dfef14015edb8276df3b3` |
| 3 | 1.78/1.79/1.81 | 1.74/1.73/1.73 | `23853e74878064738c053d6cc259a402a6893e8c4af0f697c36ad9801abbce2e` |

Before and after uptime text files are preserved for every run. Each raw JSON
records the exact source SHA, invocation, AC power observation, six row
summaries, and diagnostics.

## Results

Values are the harness primary-metric throughput in operations per second.
`p99` is the 99th percentile of per-sample throughput, not operation latency.
All rows report `quality=acceptable`, `trust_class=gate`, with no diagnostics.

| Row | p50 run 1 | p50 run 2 | p50 run 3 | Median p50 | p99 range | RSD range | p50 spread | Trust |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| fillseq buffered | 7,425.08 | 7,433.98 | 7,404.69 | 7,425.08 | 7,436.56–7,461.01 | 0.26–0.47% | 0.39% | gate |
| fillrandom buffered | 7,355.20 | 7,345.80 | 7,355.50 | 7,355.20 | 7,364.04–7,410.95 | 0.10–0.45% | 0.13% | gate |
| fillrandom sync | 96.82 | 96.71 | 96.74 | 96.74 | 96.85–96.90 | 0.04–0.35% | 0.12% | gate |
| readrandom hot | 2,557,182.37 | 2,550,846.86 | 2,519,547.47 | 2,550,846.86 | 2,544,334.06–2,571,872.54 | 0.70–1.24% | 1.48% | gate |
| readrandom cold | 1,378,997.30 | 1,394,813.63 | 1,383,835.64 | 1,383,835.64 | 1,387,781.31–1,413,736.25 | 1.14–1.41% | 1.14% | gate |
| seekrandom | 58,824.81 | 59,382.12 | 59,246.93 | 59,246.93 | 59,074.39–59,627.67 | 0.25–0.39% | 0.94% | gate |

All three p50 values for every row are within the 8–10% Tier 2 stability band.
Jev weakly favored accepting these six rows with the load context retained
(choice confidence 0.34); the disposition is grounded in the raw artifacts,
which show acceptable quality, passed correctness, and no diagnostics. The
records do not claim lab-idle conditions.

## Remaining matrix

The other planned absolute rows remain unmeasured: point-read amplification;
strict commit latency with 1, 16, and 64 writers; hybrid durability lag; YCSB
A, B, C, D, E, and F; recovery; and compaction CPU. Run these under the procedure in
[`baseline-procedure.md`](../../baseline-procedure.md). No external
RocksDB/Pebble parity is claimed.
