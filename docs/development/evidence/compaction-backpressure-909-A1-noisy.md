# #909 compaction backpressure A1 diagnostic

This is the first baseline invocation for the #909 Tier 4 comparison. It is
preserved as an inconclusive diagnostic; it does not satisfy AC3.

| Field | Observation |
| --- | --- |
| Source SHA | `4bc5e47b272778cd9000e1332264b70e336c2efa` |
| Row | `tier4_compaction_write_backpressure_local` |
| Throughput p50 | 6,727.8 writes/s |
| Throughput p99 | 7,839.7 writes/s |
| Throughput RSD | 20.4% (`noisy`, `diagnostic`) |
| Sample range | 4,775.8–7,883.1 writes/s across five measured samples |
| Load before | 1/5/15-minute averages: 7.80 / 9.51 / 12.62 |
| Load after | 1/5/15-minute averages: 18.03 / 11.16 / 12.66 |

The run used the existing smoke window: one writer, 64-key transactions,
512-byte values, 512 KiB memtables, one-second warmup and eight seconds of
continuous measurement. The stress harness reported stable wall-clock sample
durations but high variance in completed operation counts. A first optimized
build also ran immediately before the measurement window, and system load
increased during the invocation.

Raw artifact: [compaction-backpressure-909-A1-noisy.json](compaction-backpressure-909-A1-noisy.json).
The six-run comparison must restart from A1 in a quieter window; this A1 must
not be combined with later runs as a passing pair.
