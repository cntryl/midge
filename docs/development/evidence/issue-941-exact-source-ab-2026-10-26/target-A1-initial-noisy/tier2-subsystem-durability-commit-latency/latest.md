# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792998695789537000-0000074716-00000000000000000000`
- Total time: `418.53s`

## Summary

```text
Summary
  benchmarks:      4
  gate:            failed quality (1 intended gates are not trustworthy)
  correctness_ok:  4
  correctness_bad: 0
  budget_failed:   0
  regressions:     0
  diagnostics:     0
  quality_failed:  1
  improvements:    0
Quality
  authoritative:   0
  acceptable:      3
  noisy:           1
  untrustworthy:   0
Trust
  gate:            3
  diagnostic:      1
  experimental:    0
  invalid:         0
```

## Environment

- `power_source`: ac

## Memory

- Peak RSS: 30.5 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_64_writers/tier2_durability_commit_sync_local_64_writers`)

## Needs attention

- ! tier2_durability_commit_sync_local_64_writers quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Inspect one-off setup, cache, I/O, or background interference before updating a baseline. (at `benches/tier2_subsystem_durability_commit_latency.rs:80`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local | 2 | 111.21 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 111.16s |
| tier2_durability_commit_sync_local_16_writers | 2 | 1.68K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 33.36s |
| tier2_durability_commit_sync_local_64_writers | 2 | 4.50K transaction/s | diagnostic | fixed_ops | mode=fixed_ops | noisy | 5 | 78.26s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 90.41 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 195.75s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local | wal_appends (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | physical_fsyncs (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | commits_per_fsync (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 534.000 | 528.199..543.001 | 1.58% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 534.000 | 528.199..543.001 | 1.58% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.341 | 15.090..15.506 | 1.55% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 592.000 | -262.843..3360.043 | 133.45% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 592.000 | -262.843..3360.043 | 133.45% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 55.351 | 24.718..62.742 | 49.60% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 9559.000 | 9503.825..9620.575 | 0.70% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12647.000 | 12611.470..12711.330 | 0.45% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10796680046.000 | 10759977914.966..10826746978.634 | 0.35% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 10088042.000 | 9517453.038..10895097.362 | 7.70% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1443187085.000 | 1432999457.835..1451283058.965 | 0.72% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 415768459.000 | 414524417.214..418035068.386 | 0.48% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 28323266332.000 | 28292285524.757..28339540192.043 | 0.10% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 492423.901 | 491406.713..500616.367 | 1.06% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 6045.243 | 5966.489..6079.876 | 1.07% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 139254.115 | 137345.351..140976.268 | 1.49% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 145171.172 | 143390.185..146977.799 | 1.41% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 9.11ms | 10.81ms | 19.01ms | 22.90ms |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 9.24ms | 16.91ms | 54.79ms | 84.03ms |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 550.000 | 550.000 | - | 550.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 550.000 | 550.000 | - | 550.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.486 | 15.486 | - | 15.486 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 10.94ms | 574.62ms | 591.40ms | 610.30ms |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 5243.000 | 5243.000 | - | 5243.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 5243.000 | 5243.000 | - | 5243.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 57.287 | 57.287 | - | 57.287 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 16.45ms | 16.45ms | - | 16.51ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 9663.000 | 9663.000 | - | 9663.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 12743.000 | 12743.000 | - | 12743.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10837651225.000 | 10837651225.000 | - | 10837651225.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 11531333.000 | 11531333.000 | - | 11531333.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1457302331.000 | 1457302331.000 | - | 1457302331.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 418509380.000 | 418509380.000 | - | 418509380.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 28338390626.000 | 28338390626.000 | - | 28338390626.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 502333.022 | 502333.022 | - | 502333.022 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 6072.830 | 6072.830 | - | 6072.830 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 142193.811 | 142193.811 | - | 142193.811 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 148203.160 | 148203.160 | - | 148203.160 |
