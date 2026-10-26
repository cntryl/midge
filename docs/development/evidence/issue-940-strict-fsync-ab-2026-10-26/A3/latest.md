# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792991503200188000-0000049455-00000000000000000000`
- Total time: `343.97s`

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

- Peak RSS: 30.2 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_64_writers/tier2_durability_commit_sync_local_64_writers`)

## Needs attention

- ! tier2_durability_commit_sync_local_16_writers quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Use deterministic fixtures and move setup outside the measured work. (at `benches/tier2_subsystem_durability_commit_latency.rs:77`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local | 2 | 122.12 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 101.03s |
| tier2_durability_commit_sync_local_16_writers | 2 | 2.22K transaction/s | diagnostic | fixed_ops | mode=fixed_ops | noisy | 5 | 23.63s |
| tier2_durability_commit_sync_local_64_writers | 2 | 7.32K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 27.42s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 91.04 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 191.89s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local | wal_appends (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | physical_fsyncs (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | commits_per_fsync (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 522.000 | 521.360..524.240 | 0.31% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 522.000 | 521.360..524.240 | 0.31% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.693 | 15.626..15.713 | 0.31% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 519.000 | 515.656..521.144 | 0.60% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 519.000 | 515.656..521.144 | 0.60% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 63.137 | 62.878..63.546 | 0.60% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 8879.000 | 8736.727..9120.473 | 2.45% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12407.000 | 12345.547..12500.453 | 0.71% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10358327420.000 | 10137421889.493..10961097657.707 | 4.45% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 10615041.000 | 6094993.509..29635272.491 | 75.16% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1689892540.000 | 1656077335.386..1787774034.214 | 4.36% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 486710750.000 | 458292220.360..536318497.640 | 8.95% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 27580569750.000 | 26993945360.294..29340472272.506 | 4.75% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 8.98ms | 11.86ms | 16.91ms | 1.31s |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 8.98ms | 14.61ms | 17.96ms | 18.99ms |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 525.000 | 525.000 | - | 525.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 525.000 | 525.000 | - | 525.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.724 | 15.724 | - | 15.724 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 9.50ms | 14.61ms | 18.48ms | 19.25ms |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 523.000 | 523.000 | - | 523.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 523.000 | 523.000 | - | 523.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 63.627 | 63.627 | - | 63.627 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 17.69ms | 18.22ms | - | 18.34ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 9175.000 | 9175.000 | - | 9175.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 12543.000 | 12543.000 | - | 12543.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 11370807772.000 | 11370807772.000 | - | 11370807772.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 41013625.000 | 41013625.000 | - | 41013625.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1851585749.000 | 1851585749.000 | - | 1851585749.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 575086339.000 | 575086339.000 | - | 575086339.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 30505056833.000 | 30505056833.000 | - | 30505056833.000 |
