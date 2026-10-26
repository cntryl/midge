# Midge 0.5.0 performance baseline matrix status

This matrix checkpoint is tied to source
`64389f99c264e397599a049e18836f38a962c321` (2026-10-26). The six
`tier2_subsystem_baseline_matrix` rows have three acceptable absolute runs
each. Their medians and variability assessments are complete; the overall
matrix remains incomplete because other planned rows have not been measured.

The first run's exact environment, raw JSON, SHA-256, uptime, row values, and
remaining work are recorded in
[`baseline-0.5.0-64389f99`](baseline-0.5.0-64389f99/README.md).

| Row | Absolute status |
| --- | --- |
| fillseq buffered | Median 7,425.08 ops/s; 3 acceptable runs, gate trust |
| fillrandom buffered | Median 7,355.20 ops/s; 3 acceptable runs, gate trust |
| fillrandom sync | Median 96.74 ops/s; 3 acceptable runs, gate trust |
| readrandom hot | Median 2,550,846.86 ops/s; 3 acceptable runs, gate trust |
| readrandom cold | Median 1,383,835.64 ops/s; 3 acceptable runs, gate trust |
| seekrandom | Median 59,246.93 ops/s; 3 acceptable runs, gate trust |
| point-read amplification | Three acceptable runs for all rows at source `72dcb0bc`; diagnostic trust only, not a qualified gate baseline (see [run evidence](baseline-0.5.0-72dcb0bc/README.md)) |
| strict commit latency, 1 writer | Not measured |
| strict commit latency, 16 writers | Not measured |
| strict commit latency, 64 writers | Not measured |
| hybrid durability lag | Not measured |
| YCSB A | Not measured |
| YCSB B | Not measured |
| YCSB C | Not measured |
| YCSB D | Not measured |
| YCSB E | Not measured |
| YCSB F | Not measured |
| recovery | Not measured |
| compaction CPU | Not measured |

Issue-specific A/B artifacts elsewhere in `evidence/` remain separate from
these absolute baseline rows. No row has an external-engine parity claim.
