# Midge 0.5.0 performance baseline matrix status

This checkpoint adds a local after-flush recovery observation at source
`f726628ce0ebe5e7acfa05619a31833d36968f5a` (2026-10-26). The recovery row is
diagnostic trust. Earlier row groups remain tied to the source revisions shown
below. #929's full matrix remains incomplete.

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
| local recovery after flush | Median 16.94 reopens/s; 3 acceptable runs, diagnostic trust |
| other recovery rows | Not measured |
| YCSB A–F | Not measured |
| compaction CPU | Not measured |

See [local recovery evidence](recovery-baseline-0.5.0-f726628c/README.md),
[hybrid-lag evidence](hybrid-lag-0.5.0-da95afb2/README.md),
[strict-commit evidence](durability-latency-0.5.0-330c67a/README.md), and the
[earlier six-row checkpoint](performance-baseline-0.5.0-64389f99.md). These
absolute measurements do not replace issue-specific A/B targets or qualify
provider parity.
