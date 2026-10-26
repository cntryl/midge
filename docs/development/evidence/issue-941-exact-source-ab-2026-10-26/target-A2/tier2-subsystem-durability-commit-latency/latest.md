# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792999472940990000-0000077043-00000000000000000000`
- Total time: `183.54s`

## Summary

```text
Summary
  benchmarks:      1
  gate:            failed quality (1 intended gates are not trustworthy)
  correctness_ok:  1
  correctness_bad: 0
  budget_failed:   0
  regressions:     0
  diagnostics:     0
  quality_failed:  1
  improvements:    0
Quality
  authoritative:   0
  acceptable:      0
  noisy:           1
  untrustworthy:   0
Trust
  gate:            0
  diagnostic:      1
  experimental:    0
  invalid:         0
```

## Environment

- `power_source`: ac

## Memory

- Peak RSS: 14.5 MiB (process-wide)

## Needs attention

- ! tier2_durability_commit_sync_local_4k_rotation quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Use deterministic fixtures and move setup outside the measured work. (at `benches/tier2_subsystem_durability_commit_latency.rs:80`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | 2 | 99.12 transaction/s | diagnostic | fixed_ops | mode=fixed_ops | noisy | 5 | 183.54s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 9551.000 | 9143.796..9692.604 | 3.32% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12663.000 | 10511.333..13589.067 | 14.57% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10441953576.000 | 8368241941.191..11296699385.609 | 16.99% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 12286833.000 | 10850211.053..16081488.547 | 22.16% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1397183418.000 | 1126087414.661..1504067219.339 | 16.40% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 405005254.000 | 321607545.505..429858858.095 | 16.43% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 27965168541.000 | 22985522083.973..29803431598.827 | 14.73% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 475350.575 | 354737.083..524698.814 | 22.05% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 5924.600 | 5021.179..6480.330 | 14.47% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 136561.316 | 112812.582..146488.073 | 14.82% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 142746.034 | 117867.660..152934.505 | 14.77% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 16.71ms | 16.80ms | - | 16.80ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 9607.000 | 9607.000 | - | 9607.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 13255.000 | 13255.000 | - | 13255.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10878574379.000 | 10878574379.000 | - | 10878574379.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 18640000.000 | 18640000.000 | - | 18640000.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1459971124.000 | 1459971124.000 | - | 1459971124.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 411436878.000 | 411436878.000 | - | 411436878.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 28543078792.000 | 28543078792.000 | - | 28543078792.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 508189.880 | 508189.880 | - | 508189.880 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 6425.722 | 6425.722 | - | 6425.722 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 143866.311 | 143866.311 | - | 143866.311 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 149790.911 | 149790.911 | - | 149790.911 |
