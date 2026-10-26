# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792999286393331000-0000076982-00000000000000000000`
- Total time: `188.16s`

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

- Peak RSS: 14.5 MiB (process-wide)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | 2 | 94.36 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 188.16s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 9567.000 | 9490.759..9595.241 | 0.62% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12567.000 | 12507.694..12581.506 | 0.34% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10345636746.000 | 10205443830.717..10409155560.083 | 1.13% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 9114917.000 | 8223915.842..12060184.558 | 21.58% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1385040999.000 | 1374203130.550..1390509768.650 | 0.67% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 401138050.000 | 398208622.861..408130801.139 | 1.40% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 27228237125.000 | 26899024490.342..27364074759.258 | 0.98% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 492278.694 | 483493.231..495029.998 | 1.35% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 5981.406 | 5891.707..6035.600 | 1.38% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 134347.871 | 133072.412..135371.074 | 0.98% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 140329.278 | 139028.223..141342.570 | 0.94% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 15.40ms | 16.02ms | - | 16.02ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 9599.000 | 9599.000 | - | 9599.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 12583.000 | 12583.000 | - | 12583.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10446877360.000 | 10446877360.000 | - | 10446877360.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 13956250.000 | 13956250.000 | - | 13956250.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1390397000.000 | 1390397000.000 | - | 1390397000.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 412781043.000 | 412781043.000 | - | 412781043.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 27444488625.000 | 27444488625.000 | - | 27444488625.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 496248.616 | 496248.616 | - | 496248.616 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 6054.058 | 6054.058 | - | 6054.058 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 135674.123 | 135674.123 | - | 135674.123 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 141564.970 | 141564.970 | - | 141564.970 |
