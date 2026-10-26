# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792994383559555000-0000054698-00000000000000000000`
- Total time: `67.07s`

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

- Peak RSS: 18.7 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_16_writers_hot_key/tier2_durability_commit_sync_local_16_writers_hot_key`)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_16_writers | 2 | 1.49K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 33.70s |
| tier2_durability_commit_sync_local_16_writers_hot_key | 2 | 1.50K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 33.37s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts (Count) | 517.000 | 515.926..518.074 | 0.24% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs (Count) | 517.000 | 515.926..518.074 | 0.24% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs (Count) | 1034.000 | 1031.853..1036.147 | 0.24% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 517.000 | 515.926..518.074 | 0.24% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 517.000 | 515.926..518.074 | 0.24% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.845 | 15.812..15.878 | 0.24% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | avg_txn_records_per_append (Ratio) | 15.845 | 15.812..15.878 | 0.24% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_sync_attempts (Count) | 516.000 | 513.808..516.992 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_syncs (Count) | 516.000 | 513.808..516.992 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_and_ledger_accounted_syncs (Count) | 1032.000 | 1027.615..1033.985 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_appends (Count) | 516.000 | 513.808..516.992 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | physical_fsyncs (Count) | 516.000 | 513.808..516.992 | 0.35% | Informational |
| tier2_durability_commit_sync_local_16_writers_hot_key | commits_per_fsync (Ratio) | 15.876 | 15.845..15.944 | 0.35% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers_hot_key | avg_txn_records_per_append (Ratio) | 15.876 | 15.845..15.944 | 0.35% | HigherIsBetter |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 11.21ms | 13.57ms | 22.68ms | 40.26ms |
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs | 5 | 1036.000 | 1036.000 | - | 1036.000 |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.907 | 15.907 | - | 15.907 |
| tier2_durability_commit_sync_local_16_writers | avg_txn_records_per_append | 5 | 15.907 | 15.907 | - | 15.907 |
| tier2_durability_commit_sync_local_16_writers_hot_key | latency | 40960 | 11.21ms | 12.91ms | 21.89ms | 23.90ms |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_sync_attempts | 5 | 517.000 | 517.000 | - | 517.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_syncs | 5 | 517.000 | 517.000 | - | 517.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_and_ledger_accounted_syncs | 5 | 1034.000 | 1034.000 | - | 1034.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_appends | 5 | 517.000 | 517.000 | - | 517.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | physical_fsyncs | 5 | 517.000 | 517.000 | - | 517.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | commits_per_fsync | 5 | 15.969 | 15.969 | - | 15.969 |
| tier2_durability_commit_sync_local_16_writers_hot_key | avg_txn_records_per_append | 5 | 15.969 | 15.969 | - | 15.969 |
