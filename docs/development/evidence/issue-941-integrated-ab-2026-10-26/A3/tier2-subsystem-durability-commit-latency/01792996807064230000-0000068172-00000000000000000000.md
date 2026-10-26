# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792996807064230000-0000068172-00000000000000000000`
- Total time: `190.11s`

## Summary

```text
Summary
  benchmarks:      1
  gate:            passed
  correctness_ok:  1
  correctness_bad: 0
  budget_failed:   0
  regressions:     0
  diagnostics:     0
  quality_failed:  0
  improvements:    0
Quality
  authoritative:   0
  acceptable:      1
  noisy:           0
  untrustworthy:   0
Trust
  gate:            1
  diagnostic:      0
  experimental:    0
  invalid:         0
```

## Environment

- `power_source`: ac

## Memory

- Peak RSS: 14.0 MiB (process-wide)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | 2 | 92.46 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 190.11s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 8751.000 | 8580.005..9200.395 | 3.98% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12399.000 | 12351.847..12442.953 | 0.42% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10381251700.000 | 9766739822.959..11134502987.041 | 7.47% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 9332791.000 | 3638058.939..29657291.061 | 89.15% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1675925876.000 | 1548462322.197..1743408961.803 | 6.76% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 481064210.000 | 438258332.134..506128983.066 | 8.20% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 27618099458.000 | 26151954211.102..29426046988.898 | 6.72% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 8435295.006 | 7931182.109..9017450.377 | 7.31% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 5877.258 | 5537.997..6032.727 | 4.88% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 124996.015 | 115415.001..129692.166 | 6.65% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 130873.273 | 120960.671..135717.219 | 6.56% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 17.43ms | 19.94ms | - | 19.94ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 9519.000 | 9519.000 | - | 9519.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 12463.000 | 12463.000 | - | 12463.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 11539834287.000 | 11539834287.000 | - | 11539834287.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 43054750.000 | 43054750.000 | - | 43054750.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1718563041.000 | 1718563041.000 | - | 1718563041.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 519063033.000 | 519063033.000 | - | 519063033.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 30346322750.000 | 30346322750.000 | - | 30346322750.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 9331833.814 | 9331833.814 | - | 9331833.814 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 6023.648 | 6023.648 | - | 6023.648 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 131046.469 | 131046.469 | - | 131046.469 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 137070.117 | 137070.117 | - | 137070.117 |
