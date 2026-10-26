# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792992346433025000-0000049655-00000000000000000000`
- Total time: `454.18s`

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

- Peak RSS: 30.8 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_64_writers/tier2_durability_commit_sync_local_64_writers`)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local | 2 | 90.94 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 134.69s |
| tier2_durability_commit_sync_local_16_writers | 2 | 1.35K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 37.19s |
| tier2_durability_commit_sync_local_64_writers | 2 | 5.08K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 39.37s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 72.80 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 242.92s |

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
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts (Count) | 517.000 | 515.816..517.384 | 0.17% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs (Count) | 517.000 | 515.816..517.384 | 0.17% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs (Count) | 1034.000 | 1031.632..1034.768 | 0.17% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 517.000 | 515.816..517.384 | 0.17% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 517.000 | 515.816..517.384 | 0.17% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.845 | 15.833..15.882 | 0.17% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | ledger_file_sync_attempts (Count) | 517.000 | 515.760..518.640 | 0.32% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_file_syncs (Count) | 517.000 | 515.760..518.640 | 0.32% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_64_writers | wal_and_ledger_accounted_syncs (Count) | 1034.000 | 1031.519..1037.281 | 0.32% | Informational |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 517.000 | 515.760..518.640 | 0.32% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 517.000 | 515.760..518.640 | 0.32% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 63.381 | 63.181..63.533 | 0.32% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs (Count) | 5455.000 | 5455.000..5455.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 19231.000 | 18106.152..20522.248 | 7.14% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 19151.000 | 19016.752..19528.448 | 1.51% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 11957358603.000 | 11682384632.352..12482269459.248 | 3.78% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 26192166.000 | 14402149.764..51268199.836 | 64.05% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1449673044.000 | 1417765022.638..1489620797.762 | 2.82% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 581028793.000 | 564540443.490..589529138.910 | 2.47% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 35116549043.000 | 34441740594.308..35924783340.092 | 2.40% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 12.52ms | 20.05ms | 63.70ms | 90.21ms |
| tier2_durability_commit_sync_local | ledger_file_sync_attempts | 5 | 2050.000 | 2050.000 | - | 2050.000 |
| tier2_durability_commit_sync_local | ledger_file_syncs | 5 | 2050.000 | 2050.000 | - | 2050.000 |
| tier2_durability_commit_sync_local | ledger_directory_sync_attempts | 5 | 2.000 | 2.000 | - | 2.000 |
| tier2_durability_commit_sync_local | ledger_directory_syncs | 5 | 2.000 | 2.000 | - | 2.000 |
| tier2_durability_commit_sync_local | wal_and_ledger_accounted_syncs | 5 | 4100.000 | 4100.000 | - | 4100.000 |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 16.32ms | 21.63ms | 35.91ms | 40.32ms |
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts | 5 | 517.000 | 517.000 | - | 517.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs | 5 | 517.000 | 517.000 | - | 517.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs | 5 | 1034.000 | 1034.000 | - | 1034.000 |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 517.000 | 517.000 | - | 517.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 517.000 | 517.000 | - | 517.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.907 | 15.907 | - | 15.907 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 17.69ms | 22.41ms | 36.44ms | 52.45ms |
| tier2_durability_commit_sync_local_64_writers | ledger_file_sync_attempts | 5 | 520.000 | 520.000 | - | 520.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_file_syncs | 5 | 520.000 | 520.000 | - | 520.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_64_writers | wal_and_ledger_accounted_syncs | 5 | 1040.000 | 1040.000 | - | 1040.000 |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 520.000 | 520.000 | - | 520.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 520.000 | 520.000 | - | 520.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 63.504 | 63.504 | - | 63.504 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 22.94ms | 23.12ms | - | 23.12ms |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs | 5 | 5455.000 | 5455.000 | - | 5455.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 20687.000 | 20687.000 | - | 20687.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 19663.000 | 19663.000 | - | 19663.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 12587979540.000 | 12587979540.000 | - | 12587979540.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 69102083.000 | 69102083.000 | - | 69102083.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1509836417.000 | 1509836417.000 | - | 1509836417.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 592327375.000 | 592327375.000 | - | 592327375.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 36145299084.000 | 36145299084.000 | - | 36145299084.000 |
