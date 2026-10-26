# Midge 0.5.0 performance baseline matrix status

This checkpoint adds strict local commit latency evidence at source
`330c67a585c8bd078b0059cd93358fac61537a90` (2026-10-26). The earlier six
Tier 2 baseline-matrix rows remain tied to source `64389f99`; read-amplification
diagnostic context remains tied to `72dcb0bc`. The full matrix is still open.

| Row | Absolute status |
| --- | --- |
| fillseq buffered | Median 7,425.08 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| fillrandom buffered | Median 7,355.20 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| fillrandom sync | Median 96.74 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| readrandom hot | Median 2,550,846.86 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| readrandom cold | Median 1,383,835.64 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| seekrandom | Median 59,246.93 ops/s; 3 acceptable runs, gate trust at `64389f99` |
| point-read amplification | 3 acceptable runs per row; diagnostic trust only at `72dcb0bc` |
| strict commit latency, 1 writer | Median 97.97 transactions/s; 3 acceptable runs, gate trust |
| strict commit latency, 16 writers | Median 1,478.28 transactions/s; 3 acceptable runs, gate trust |
| strict commit latency, 64 writers | Median 5,626.33 transactions/s; 3 acceptable runs, gate trust |
| strict commit latency, 16 writers shared hot key | Median 1,484.18 transactions/s; 3 acceptable runs, gate trust |
| strict commit latency, 4 KiB rotation | Median 81.84 transactions/s; 3 acceptable runs, gate trust |
| hybrid durability lag | Not measured |
| YCSB A–F | Not measured |
| recovery | Not measured |
| compaction CPU | Not measured |

See [strict-commit raw evidence](durability-latency-0.5.0-330c67a/README.md),
[read-amplification evidence](baseline-0.5.0-72dcb0bc/README.md), and the
[earlier six-row matrix checkpoint](performance-baseline-0.5.0-64389f99.md).
Issue-specific A/B artifacts are separate from these absolute baselines. No
row makes an external-engine parity claim.
