# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792991144593764000-0000049342-00000000000000000000`
- Total time: `415.46s`

## Summary

```text
Summary
  benchmarks:      4
  gate:            passed
  correctness_ok:  4
  correctness_bad: 0
  budget_failed:   0
  regressions:     0
  diagnostics:     0
  quality_failed:  0
  improvements:    0
Quality
  authoritative:   0
  acceptable:      4
  noisy:           0
  untrustworthy:   0
Trust
  gate:            4
  diagnostic:      0
  experimental:    0
  invalid:         0
```

## Environment

- `power_source`: ac

## Memory

- Peak RSS: 30.7 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_64_writers/tier2_durability_commit_sync_local_64_writers`)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local | 2 | 97.49 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 126.31s |
| tier2_durability_commit_sync_local_16_writers | 2 | 1.51K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 33.30s |
| tier2_durability_commit_sync_local_64_writers | 2 | 5.87K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 34.27s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 79.71 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 221.57s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local | ledger_file_sync_attempts (Count) | 2050.000 | 2050.000..2050.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | ledger_file_syncs (Count) | 2050.000 | 2050.000..2050.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | ledger_directory_sync_attempts (Count) | 2.000 | 2.000..2.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | ledger_directory_syncs (Count) | 2.000 | 2.000..2.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | wal_and_ledger_accounted_syncs (Count) | 4100.000 | 4100.000..4100.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | wal_appends (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | physical_fsyncs (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | commits_per_fsync (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts (Count) | 517.000 | 515.500..518.100 | 0.29% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs (Count) | 517.000 | 515.500..518.100 | 0.29% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs (Count) | 1034.000 | 1031.000..1036.200 | 0.29% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 517.000 | 515.500..518.100 | 0.29% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 517.000 | 515.500..518.100 | 0.29% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.845 | 15.812..15.891 | 0.29% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | ledger_file_sync_attempts (Count) | 516.000 | 514.632..517.768 | 0.35% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_file_syncs (Count) | 516.000 | 514.632..517.768 | 0.35% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_64_writers | wal_and_ledger_accounted_syncs (Count) | 1032.000 | 1029.264..1035.536 | 0.35% | Informational |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 516.000 | 514.632..517.768 | 0.35% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 516.000 | 514.632..517.768 | 0.35% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 63.504 | 63.287..63.673 | 0.35% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs (Count) | 5455.000 | 5455.000..5455.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 10839.000 | 10664.319..11567.281 | 4.63% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 14207.000 | 13611.965..15832.435 | 8.60% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10164592667.000 | 9825438057.965..11070370230.035 | 6.80% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 9996708.000 | 5144755.230..59406277.970 | 95.90% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1182397962.000 | 1139673414.071..1344702042.729 | 9.42% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 476074252.000 | 448887377.020..544228011.380 | 10.95% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 31344100917.000 | 30636764378.059..33746601953.941 | 5.51% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 10.94ms | 17.17ms | 20.58ms | 31.09ms |
| tier2_durability_commit_sync_local | ledger_file_sync_attempts | 5 | 2050.000 | 2050.000 | - | 2050.000 |
| tier2_durability_commit_sync_local | ledger_file_syncs | 5 | 2050.000 | 2050.000 | - | 2050.000 |
| tier2_durability_commit_sync_local | ledger_directory_sync_attempts | 5 | 2.000 | 2.000 | - | 2.000 |
| tier2_durability_commit_sync_local | ledger_directory_syncs | 5 | 2.000 | 2.000 | - | 2.000 |
| tier2_durability_commit_sync_local | wal_and_ledger_accounted_syncs | 5 | 4100.000 | 4100.000 | - | 4100.000 |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 11.08ms | 12.78ms | 21.89ms | 23.16ms |
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs | 5 | 1038.000 | 1038.000 | - | 1038.000 |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.907 | 15.907 | - | 15.907 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 11.86ms | 15.01ms | 22.41ms | 25.55ms |
| tier2_durability_commit_sync_local_64_writers | ledger_file_sync_attempts | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_file_syncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_64_writers | wal_and_ledger_accounted_syncs | 5 | 1036.000 | 1036.000 | - | 1036.000 |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 63.751 | 63.751 | - | 63.751 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 18.74ms | 19.49ms | - | 19.49ms |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs | 5 | 5455.000 | 5455.000 | - | 5455.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 11775.000 | 11775.000 | - | 11775.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 16927.000 | 16927.000 | - | 16927.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 11511099566.000 | 11511099566.000 | - | 11511099566.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 66557209.000 | 66557209.000 | - | 66557209.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1440991713.000 | 1440991713.000 | - | 1440991713.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 588292632.000 | 588292632.000 | - | 588292632.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 35025251999.000 | 35025251999.000 | - | 35025251999.000 |
