# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792991888405139000-0000049564-00000000000000000000`
- Total time: `341.50s`

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

- ! tier2_durability_commit_sync_local quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Use deterministic fixtures and move setup outside the measured work. (at `benches/tier2_subsystem_durability_commit_latency.rs:77`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local | 2 | 131.22 transaction/s | diagnostic | fixed_ops | mode=fixed_ops | noisy | 5 | 93.87s |
| tier2_durability_commit_sync_local_16_writers | 2 | 1.85K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 27.24s |
| tier2_durability_commit_sync_local_64_writers | 2 | 6.98K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 28.87s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 91.92 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 191.52s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local | wal_appends (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | physical_fsyncs (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | commits_per_fsync (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 521.000 | 520.138..526.262 | 0.67% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 521.000 | 520.138..526.262 | 0.67% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.724 | 15.567..15.749 | 0.66% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 518.000 | 515.514..518.886 | 0.37% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 518.000 | 515.514..518.886 | 0.37% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 63.259 | 63.150..63.564 | 0.37% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 8783.000 | 8484.289..9529.711 | 6.62% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12407.000 | 12216.787..13035.613 | 3.70% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10217959098.000 | 9867028615.557..11392635704.843 | 8.19% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 10491208.000 | -236352048.692..768912481.892 | 215.35% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1697537997.000 | 1656574410.408..1769687835.992 | 3.77% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 487039796.000 | 469827135.166..520764204.834 | 5.87% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 27116983292.000 | 26649147273.669..29162245244.331 | 5.14% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 8.85ms | 11.73ms | 15.79ms | 1.26s |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 8.98ms | 16.91ms | 17.96ms | 24.02ms |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 529.000 | 529.000 | - | 529.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 529.000 | 529.000 | - | 529.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.724 | 15.724 | - | 15.724 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 9.76ms | 13.17ms | 18.22ms | 23.91ms |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 519.000 | 519.000 | - | 519.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 63.751 | 63.751 | - | 63.751 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 17.69ms | 18.41ms | - | 18.41ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 10071.000 | 10071.000 | - | 10071.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 13455.000 | 13455.000 | - | 13455.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 12160923756.000 | 12160923756.000 | - | 12160923756.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 1292059625.000 | 1292059625.000 | - | 1292059625.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1787962954.000 | 1787962954.000 | - | 1787962954.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 534923466.000 | 534923466.000 | - | 534923466.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 30371360083.000 | 30371360083.000 | - | 30371360083.000 |
