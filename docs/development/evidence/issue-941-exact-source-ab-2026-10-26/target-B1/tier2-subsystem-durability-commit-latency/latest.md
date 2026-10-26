# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792999087395711000-0000076894-00000000000000000000`
- Total time: `223.44s`

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

- Peak RSS: 14.6 MiB (process-wide)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | 2 | 78.76 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 223.44s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 10799.000 | 10759.654..10863.946 | 0.55% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 15471.000 | 15403.638..15554.362 | 0.56% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10758504008.000 | 10743174094.611..10774771970.989 | 0.17% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 10829625.000 | 9988909.669..13000390.331 | 14.94% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1334554285.000 | 1329000348.875..1357816092.325 | 1.22% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 565865084.000 | 562008682.677..568119218.923 | 0.62% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 32488805167.000 | 32474787920.021..32528802997.979 | 0.09% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 329042.529 | 326380.936..330267.483 | 0.68% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 9143.383 | 9085.781..9170.139 | 0.53% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 41078.832 | 39917.391..42744.448 | 3.90% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 50126.675 | 49033.220..51884.539 | 3.22% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 19.53ms | 19.53ms | - | 19.54ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 10911.000 | 10911.000 | - | 10911.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 15623.000 | 15623.000 | - | 15623.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10782133966.000 | 10782133966.000 | - | 10782133966.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 13913291.000 | 13913291.000 | - | 13913291.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1369434951.000 | 1369434951.000 | - | 1369434951.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 568469208.000 | 568469208.000 | - | 568469208.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 32547377166.000 | 32547377166.000 | - | 32547377166.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 329852.004 | 329852.004 | - | 329852.004 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 9164.472 | 9164.472 | - | 9164.472 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 43383.516 | 43383.516 | - | 43383.516 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 52526.900 | 52526.900 | - | 52526.900 |
