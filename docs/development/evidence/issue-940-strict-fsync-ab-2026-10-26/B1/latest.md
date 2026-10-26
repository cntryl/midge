# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792990360174609000-0000049141-00000000000000000000`
- Total time: `418.95s`

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
| tier2_durability_commit_sync_local | 2 | 97.13 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 127.16s |
| tier2_durability_commit_sync_local_16_writers | 2 | 1.46K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 34.27s |
| tier2_durability_commit_sync_local_64_writers | 2 | 5.66K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 35.51s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 79.29 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 222.00s |

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
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts (Count) | 517.000 | 516.920..517.880 | 0.11% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs (Count) | 517.000 | 516.920..517.880 | 0.11% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs (Count) | 1034.000 | 1033.840..1035.760 | 0.11% | Informational |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 517.000 | 516.920..517.880 | 0.11% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 517.000 | 516.920..517.880 | 0.11% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.845 | 15.818..15.848 | 0.11% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | ledger_file_sync_attempts (Count) | 516.000 | 514.616..516.184 | 0.17% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_file_syncs (Count) | 516.000 | 514.616..516.184 | 0.17% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_64_writers | wal_and_ledger_accounted_syncs (Count) | 1032.000 | 1029.232..1032.368 | 0.17% | Informational |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 516.000 | 514.616..516.184 | 0.17% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 516.000 | 514.616..516.184 | 0.17% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 63.504 | 63.481..63.675 | 0.17% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs (Count) | 5455.000 | 5455.000..5455.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 10927.000 | 10764.014..11173.186 | 2.13% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 15351.000 | 15278.397..15522.803 | 0.91% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10596070620.000 | 10574210044.016..10610556802.784 | 0.20% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 13350209.000 | 10354697.369..14218503.431 | 17.94% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1345087874.000 | 1340956570.905..1362610995.095 | 0.91% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 561709543.000 | 555969719.217..566389546.783 | 1.06% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 32275197418.000 | 32225467713.059..32348478920.141 | 0.22% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 10.94ms | 11.99ms | 20.05ms | 25.09ms |
| tier2_durability_commit_sync_local | ledger_file_sync_attempts | 5 | 2050.000 | 2050.000 | - | 2050.000 |
| tier2_durability_commit_sync_local | ledger_file_syncs | 5 | 2050.000 | 2050.000 | - | 2050.000 |
| tier2_durability_commit_sync_local | ledger_directory_sync_attempts | 5 | 2.000 | 2.000 | - | 2.000 |
| tier2_durability_commit_sync_local | ledger_directory_syncs | 5 | 2.000 | 2.000 | - | 2.000 |
| tier2_durability_commit_sync_local | wal_and_ledger_accounted_syncs | 5 | 4100.000 | 4100.000 | - | 4100.000 |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 11.21ms | 20.05ms | 47.97ms | 58.08ms |
| tier2_durability_commit_sync_local_16_writers | ledger_file_sync_attempts | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_file_syncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_16_writers | wal_and_ledger_accounted_syncs | 5 | 1036.000 | 1036.000 | - | 1036.000 |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.845 | 15.845 | - | 15.845 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 11.86ms | 13.83ms | 24.51ms | 46.83ms |
| tier2_durability_commit_sync_local_64_writers | ledger_file_sync_attempts | 5 | 516.000 | 516.000 | - | 516.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_file_syncs | 5 | 516.000 | 516.000 | - | 516.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_64_writers | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_64_writers | wal_and_ledger_accounted_syncs | 5 | 1032.000 | 1032.000 | - | 1032.000 |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 516.000 | 516.000 | - | 516.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 516.000 | 516.000 | - | 516.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 63.751 | 63.751 | - | 63.751 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 19.53ms | 19.53ms | - | 19.57ms |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs | 5 | 5455.000 | 5455.000 | - | 5455.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 11351.000 | 11351.000 | - | 11351.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 15647.000 | 15647.000 | - | 15647.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10615170904.000 | 10615170904.000 | - | 10615170904.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 14349792.000 | 14349792.000 | - | 14349792.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1365734086.000 | 1365734086.000 | - | 1365734086.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 568865332.000 | 568865332.000 | - | 568865332.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 32397585125.000 | 32397585125.000 | - | 32397585125.000 |
