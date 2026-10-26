# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792994313059607000-0000054657-00000000000000000000`
- Total time: `69.07s`

## Summary

```text
Summary
  benchmarks:      2
  gate:            failed quality (1 intended gates are not trustworthy)
  correctness_ok:  2
  correctness_bad: 0
  budget_failed:   0
  regressions:     0
  diagnostics:     0
  quality_failed:  1
  improvements:    0
Quality
  authoritative:   0
  acceptable:      1
  noisy:           1
  untrustworthy:   0
Trust
  gate:            1
  diagnostic:      1
  experimental:    0
  invalid:         0
```

## Environment

- `power_source`: ac

## Memory

- Peak RSS: 18.1 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_16_writers_hot_key/tier2_durability_commit_sync_local_16_writers_hot_key`)

## Needs attention

- ! tier2_durability_commit_sync_local_16_writers quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Use deterministic fixtures and move setup outside the measured work. (at `benches/tier2_subsystem_durability_commit_latency.rs:164`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_16_writers | 2 | 1.37K transaction/s | diagnostic | fixed_ops | mode=fixed_ops | noisy | 5 | 36.49s |
| tier2_durability_commit_sync_local_16_writers_hot_key | 2 | 1.54K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 32.57s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts (Count) | 517.000 | 516.401..518.399 | 0.22% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs (Count) | 517.000 | 516.401..518.399 | 0.22% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs (Count) | 1034.000 | 1032.801..1036.799 | 0.22% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 517.000 | 516.401..518.399 | 0.22% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 517.000 | 516.401..518.399 | 0.22% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.845 | 15.803..15.864 | 0.22% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | avg_txn_records_per_append (Ratio) | 15.845 | 15.803..15.864 | 0.22% | HigherIsBetter |
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
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 13.17ms | 21.89ms | 66.85ms | 93.65ms |
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs | 5 | 1038.000 | 1038.000 | - | 1038.000 |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.876 | 15.876 | - | 15.876 |
| tier2_durability_commit_sync_local_16_writers | avg_txn_records_per_append | 5 | 15.876 | 15.876 | - | 15.876 |
| tier2_durability_commit_sync_local_16_writers_hot_key | latency | 40960 | 11.08ms | 14.75ms | 43.78ms | 47.54ms |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_sync_attempts | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_file_syncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_and_ledger_accounted_syncs | 5 | 1036.000 | 1036.000 | - | 1036.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | wal_appends | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | physical_fsyncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers_hot_key | commits_per_fsync | 5 | 15.938 | 15.938 | - | 15.938 |
| tier2_durability_commit_sync_local_16_writers_hot_key | avg_txn_records_per_append | 5 | 15.938 | 15.938 | - | 15.938 |
