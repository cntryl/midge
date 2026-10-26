# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792993141421629000-0000052213-00000000000000000000`
- Total time: `424.94s`

## Summary

```text
Summary
  benchmarks:      2
  gate:            passed
  correctness_ok:  2
  correctness_bad: 0
  budget_failed:   0
  regressions:     0
  diagnostics:     0
  quality_failed:  0
  improvements:    0
Quality
  authoritative:   0
  acceptable:      2
  noisy:           0
  untrustworthy:   0
Trust
  gate:            2
  diagnostic:      0
  experimental:    0
  invalid:         0
```

## Environment

- `power_source`: ac

## Memory

- Peak RSS: 17.8 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_16_writers_hot_key/tier2_durability_commit_sync_local_16_writers_hot_key`)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_16_writers | 2 | 1.85K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 27.24s |
| tier2_durability_commit_sync_local_16_writers_hot_key | 2 | 123.76 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 397.70s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 523.000 | 519.861..526.939 | 0.77% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 523.000 | 519.861..526.939 | 0.77% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.663 | 15.546..15.758 | 0.77% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | avg_txn_records_per_append (Ratio) | 15.663 | 15.546..15.758 | 0.77% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_appends (Count) | 8192.000 | 8192.000..8192.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | physical_fsyncs (Count) | 8192.000 | 8192.000..8192.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | commits_per_fsync (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers_hot_key | avg_txn_records_per_append (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 8.98ms | 16.32ms | 21.89ms | 47.24ms |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 529.000 | 529.000 | - | 529.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 529.000 | 529.000 | - | 529.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.815 | 15.815 | - | 15.815 |
| tier2_durability_commit_sync_local_16_writers | avg_txn_records_per_append | 5 | 15.815 | 15.815 | - | 15.815 |
| tier2_durability_commit_sync_local_16_writers_hot_key | latency | 40960 | 130.55ms | 143.65ms | 149.95ms | 158.87ms |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_appends | 5 | 8192.000 | 8192.000 | - | 8192.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | physical_fsyncs | 5 | 8192.000 | 8192.000 | - | 8192.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | avg_txn_records_per_append | 5 | 1.000 | 1.000 | - | 1.000 |
