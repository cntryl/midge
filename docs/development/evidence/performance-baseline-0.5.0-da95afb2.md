# Midge 0.5.0 performance baseline matrix status

This cumulative checkpoint adds simulator hybrid-lag results at source
`da95afb262346f0f1c9d985a781b1d5bfa81b4a9` (2026-10-26). Earlier matrix
groups remain source-pinned to `64389f99`, `72dcb0bc`, and `330c67a`. The full
matrix and #929 remain open.

| Row | Absolute status |
| --- | --- |
| fillseq buffered | Median 7,425.08 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| fillrandom buffered | Median 7,355.20 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| fillrandom sync | Median 96.74 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| readrandom hot | Median 2,550,846.86 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| readrandom cold | Median 1,383,835.64 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| seekrandom | Median 59,246.93 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| point-read amplification | 3 acceptable runs per row; diagnostic trust only at `72dcb0bc` |
| strict commit latency, 1/16/64 writers | 3 acceptable runs per row; gate trust at `330c67a` |
| strict commit latency, 16 writers shared hot key | 3 acceptable runs; gate trust at `330c67a` |
| strict commit latency, 4 KiB rotation | 3 acceptable runs; gate trust at `330c67a` |
| hybrid `cloud_async`, one client | 3 acceptable runs; gate trust at `da95afb2` |
| hybrid local `sync()` floor | 3 acceptable runs; 13.45% spread; gate trust at `da95afb2` |
| hybrid `cloud_strict`, one client | Inconclusive; repeated sampler gaps above 40 ms at `da95afb2` |
| YCSB A–F | Not measured |
| recovery | Not measured |
| compaction CPU | Not measured |

See [hybrid-lag raw evidence](hybrid-lag-0.5.0-da95afb2/README.md),
[strict-commit evidence](durability-latency-0.5.0-330c67a/README.md),
[read-amplification evidence](baseline-0.5.0-72dcb0bc/README.md), and the
[earlier six-row checkpoint](performance-baseline-0.5.0-64389f99.md). The
simulator does not qualify a cloud provider. Issue-specific A/B artifacts are
separate from these absolute baselines; no external-engine parity claim is
made.
