# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792995953934506000-0000067781-00000000000000000000`
- Total time: `353.12s`

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

- Peak RSS: 30.4 MiB (process-wide; largest growth at `tier2-subsystem-durability-commit-latency/tier2_subsystem_durability_commit_latency::tier2_durability_commit_sync_local_64_writers/tier2_durability_commit_sync_local_64_writers`)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local | 2 | 123.43 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 100.16s |
| tier2_durability_commit_sync_local_16_writers | 2 | 1.87K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 27.16s |
| tier2_durability_commit_sync_local_64_writers | 2 | 6.96K transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 29.03s |
| tier2_durability_commit_sync_local_4k_rotation | 2 | 89.41 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 196.77s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local | wal_appends (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | physical_fsyncs (Count) | 2048.000 | 2048.000..2048.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local | commits_per_fsync (Ratio) | 1.000 | 1.000..1.000 | 0.00% | HigherIsBetter |
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 520.000 | 519.298..524.702 | 0.59% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 520.000 | 519.298..524.702 | 0.59% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.754 | 15.613..15.775 | 0.59% | HigherIsBetter |
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 519.000 | 515.855..525.345 | 1.04% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 519.000 | 515.855..525.345 | 1.04% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 63.137 | 62.381..63.515 | 1.03% | HigherIsBetter |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 8799.000 | 8694.987..8842.213 | 0.96% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12431.000 | 12388.391..12470.409 | 0.38% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10722099895.000 | 10701305185.835..10744571712.965 | 0.23% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 9677166.000 | 9131729.598..11737403.202 | 14.24% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1755074507.000 | 1713990863.773..1781864402.627 | 2.22% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 509709415.000 | 496686340.426..519704509.574 | 2.58% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 28614482584.000 | 28571456581.406..28694409819.394 | 0.24% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 8713484.566 | 8701195.907..8733090.571 | 0.21% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 5953.404 | 5938.462..5980.528 | 0.40% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 126975.896 | 125591.045..127500.906 | 0.86% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 132906.757 | 131556.269..133454.671 | 0.82% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local | latency | 10240 | 8.22ms | 9.24ms | 16.06ms | 19.96ms |
| tier2_durability_commit_sync_local | wal_appends | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | physical_fsyncs | 5 | 2048.000 | 2048.000 | - | 2048.000 |
| tier2_durability_commit_sync_local | commits_per_fsync | 5 | 1.000 | 1.000 | - | 1.000 |
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 8.98ms | 16.06ms | 18.22ms | 31.85ms |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 527.000 | 527.000 | - | 527.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 527.000 | 527.000 | - | 527.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.754 | 15.754 | - | 15.754 |
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 9.76ms | 15.93ms | 17.96ms | 20.85ms |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 530.000 | 530.000 | - | 530.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 530.000 | 530.000 | - | 530.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 63.381 | 63.381 | - | 63.381 |
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 17.69ms | 17.69ms | - | 17.74ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 8863.000 | 8863.000 | - | 8863.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 12487.000 | 12487.000 | - | 12487.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10760462165.000 | 10760462165.000 | - | 10760462165.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 13069250.000 | 13069250.000 | - | 13069250.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1781431916.000 | 1781431916.000 | - | 1781431916.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 519786792.000 | 519786792.000 | - | 519786792.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 28746088418.000 | 28746088418.000 | - | 28746088418.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 8746962.739 | 8746962.739 | - | 8746962.739 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 5992.321 | 5992.321 | - | 5992.321 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 127626.808 | 127626.808 | - | 127626.808 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 133600.793 | 133600.793 | - | 133600.793 |
