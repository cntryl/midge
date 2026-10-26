# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792993307729483000-0000054344-00000000000000000000`
- Total time: `68.04s`

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

- Peak RSS: 18.2 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_16_writers_hot_key/tier2_durability_commit_sync_local_16_writers_hot_key`)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_16_writers | 2 | 1.47K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 34.12s |
| tier2_durability_commit_sync_local_16_writers_hot_key | 2 | 1.48K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 33.92s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts (Count) | 516.000 | 514.400..518.800 | 0.49% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs (Count) | 516.000 | 514.400..518.800 | 0.49% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs (Count) | 1032.000 | 1028.800..1037.600 | 0.49% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 516.000 | 514.400..518.800 | 0.49% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 516.000 | 514.400..518.800 | 0.49% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.876 | 15.791..15.925 | 0.48% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | avg_txn_records_per_append (Ratio) | 15.876 | 15.791..15.925 | 0.48% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_sync_attempts (Count) | 515.000 | 514.008..517.192 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_syncs (Count) | 515.000 | 514.008..517.192 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_and_ledger_accounted_syncs (Count) | 1030.000 | 1028.015..1034.385 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_appends (Count) | 515.000 | 514.008..517.192 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | physical_fsyncs (Count) | 515.000 | 514.008..517.192 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | commits_per_fsync (Ratio) | 15.907 | 15.839..15.937 | 0.35% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers_hot_key | avg_txn_records_per_append (Ratio) | 15.907 | 15.839..15.937 | 0.35% | HigherIsBetter |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 11.08ms | 12.91ms | 21.89ms | 24.68ms |
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts | 5 | 521.000 | 521.000 | - | 521.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs | 5 | 521.000 | 521.000 | - | 521.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs | 5 | 1042.000 | 1042.000 | - | 1042.000 |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 521.000 | 521.000 | - | 521.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 521.000 | 521.000 | - | 521.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.907 | 15.907 | - | 15.907 |
| tier2_durability_commit_sync_local_16_writers | avg_txn_records_per_append | 5 | 15.907 | 15.907 | - | 15.907 |
| tier2_durability_commit_sync_local_16_writers_hot_key | latency | 40960 | 11.08ms | 12.12ms | 21.89ms | 22.94ms |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_sync_attempts | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_syncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_and_ledger_accounted_syncs | 5 | 1036.000 | 1036.000 | - | 1036.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_appends | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | physical_fsyncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | commits_per_fsync | 5 | 15.938 | 15.938 | - | 15.938 |
| tier2_durability_commit_sync_local_16_writers_hot_key | avg_txn_records_per_append | 5 | 15.938 | 15.938 | - | 15.938 |
