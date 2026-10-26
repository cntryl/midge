# Midge 0.5.0 performance baseline matrix status

This checkpoint adds an inconclusive compaction CPU attempt at source
`c2929e8e81d06691fb765882bfeffcdcc99b45d9` (2026-10-26). Earlier row groups
remain pinned to their original source revisions. The full #929 matrix remains
incomplete.

| Row | Absolute status |
| --- | --- |
| fillseq buffered | Median 7,425.08 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| fillrandom buffered | Median 7,355.20 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| fillrandom sync | Median 96.74 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| readrandom hot | Median 2,550,846.86 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| readrandom cold | Median 1,383,835.64 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| seekrandom | Median 59,246.93 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| point-read amplification | 3 acceptable runs per row; diagnostic trust at `72dcb0bc` |
| strict commit latency, 1/16/64 writers | 3 acceptable runs per row; gate trust at `330c67a` |
| strict commit latency, 16 writers shared hot key | 3 acceptable runs; gate trust at `330c67a` |
| strict commit latency, 4 KiB rotation | 3 acceptable runs; gate trust at `330c67a` |
| hybrid `cloud_async`, one client | 3 acceptable runs; gate trust at `da95afb2` |
| hybrid local `sync()` floor | 3 acceptable runs; 13.45% spread; gate trust at `da95afb2` |
| hybrid `cloud_strict`, one client | Inconclusive; sampler gaps exceeded 40 ms at `da95afb2` |
| local recovery after flush | Median 16.94 reopens/s; 3 acceptable runs, diagnostic trust at `f726628c` |
| compaction CPU per logical input MiB | Inconclusive at `c2929e8e`; one clean default run, three noisy defaults, and a noisy batched trial |
| other recovery rows | Not measured |
| YCSB A–F | Not measured |

See [compaction CPU attempt](compaction-cpu-0.5.0-c2929e8e/README.md),
[local recovery evidence](recovery-baseline-0.5.0-f726628c/README.md),
[hybrid-lag evidence](hybrid-lag-0.5.0-da95afb2/README.md), and the
[earlier six-row checkpoint](performance-baseline-0.5.0-64389f99.md). These
absolute measurements do not replace issue-specific A/B targets or qualify
provider parity.
