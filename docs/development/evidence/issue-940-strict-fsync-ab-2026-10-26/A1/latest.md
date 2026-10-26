# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792989833921036000-0000047002-00000000000000000000`
- Total time: `353.50s`

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

- Peak RSS: 30.1 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_64_writers/tier2_durability_commit_sync_local_64_writers`)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local | 2 | 123.57 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 100.14s |
| tier2_durability_commit_sync_local_16_writers | 2 | 1.85K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 27.22s |
| tier2_durability_commit_sync_local_64_writers | 2 | 7.03K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 28.76s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 89.21 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 197.37s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local | wal_appends (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | physical_fsyncs (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | commits_per_fsync (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 522.000 | 519.670..527.930 | 0.90% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 522.000 | 519.670..527.930 | 0.90% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.693 | 15.518..15.763 | 0.89% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 517.000 | 515.224..517.576 | 0.26% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 517.000 | 515.224..517.576 | 0.26% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 63.381 | 63.310..63.600 | 0.26% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 8695.000 | 8635.803..8808.597 | 1.13% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12487.000 | 12420.130..12534.670 | 0.52% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10700537588.000 | 10589966698.424..10807041831.576 | 1.16% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 9984334.000 | 9214383.362..14725099.838 | 26.26% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1786889829.000 | 1764109535.520..1796792761.280 | 1.05% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 509745455.000 | 503159099.055..518835348.545 | 1.75% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 28644466040.000 | 28485510890.231..28908054959.369 | 0.84% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 8.16ms | 9.11ms | 16.71ms | 19.60ms |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 8.98ms | 16.71ms | 17.96ms | 22.91ms |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 531.000 | 531.000 | - | 531.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 531.000 | 531.000 | - | 531.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.754 | 15.754 | - | 15.754 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 9.63ms | 11.08ms | 18.22ms | 20.08ms |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 63.627 | 63.627 | - | 63.627 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 17.69ms | 18.22ms | - | 18.24ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 8887.000 | 8887.000 | - | 8887.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 12551.000 | 12551.000 | - | 12551.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10895934033.000 | 10895934033.000 | - | 10895934033.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 16552708.000 | 16552708.000 | - | 16552708.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1800998459.000 | 1800998459.000 | - | 1800998459.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 520340125.000 | 520340125.000 | - | 520340125.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 29098800875.000 | 29098800875.000 | - | 29098800875.000 |
