# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792990721260777000-0000049254-00000000000000000000`
- Total time: `342.26s`

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
| tier2_durability_commit_sync_local | 2 | 123.47 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 100.50s |
| tier2_durability_commit_sync_local_16_writers | 2 | 1.85K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 27.19s |
| tier2_durability_commit_sync_local_64_writers | 2 | 7.37K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 27.58s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 94.24 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 186.99s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local | wal_appends (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | physical_fsyncs (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | commits_per_fsync (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 521.000 | 519.944..524.056 | 0.45% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 521.000 | 519.944..524.056 | 0.45% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.724 | 15.632..15.755 | 0.45% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 516.000 | 514.360..517.640 | 0.36% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 516.000 | 514.360..517.640 | 0.36% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 63.504 | 63.302..63.707 | 0.36% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 8687.000 | 8640.879..8710.721 | 0.46% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12415.000 | 12351.005..12453.395 | 0.47% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10313604028.000 | 9836094362.408..10490449518.792 | 3.67% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 9535417.000 | 9059970.588..9829146.212 | 4.65% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1686335711.000 | 1619726693.246..1714651293.554 | 3.25% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 476634957.000 | 455197824.511..495868904.289 | 4.88% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 27601137874.000 | 26403880943.458..27971947123.342 | 3.29% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 8.85ms | 12.12ms | 47.45ms | 83.75ms |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 8.98ms | 16.06ms | 18.22ms | 19.63ms |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 526.000 | 526.000 | - | 526.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 526.000 | 526.000 | - | 526.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.754 | 15.754 | - | 15.754 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 9.50ms | 10.55ms | 17.96ms | 19.94ms |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 518.000 | 518.000 | - | 518.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 63.875 | 63.875 | - | 63.875 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 17.43ms | 17.69ms | - | 17.73ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 8719.000 | 8719.000 | - | 8719.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 12455.000 | 12455.000 | - | 12455.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10429931538.000 | 10429931538.000 | - | 10429931538.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 10013417.000 | 10013417.000 | - | 10013417.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1710090670.000 | 1710090670.000 | - | 1710090670.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 503407789.000 | 503407789.000 | - | 503407789.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 27798831210.000 | 27798831210.000 | - | 27798831210.000 |
