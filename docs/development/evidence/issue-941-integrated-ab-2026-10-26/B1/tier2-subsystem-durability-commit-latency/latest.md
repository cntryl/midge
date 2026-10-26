# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792996184929620000-0000067924-00000000000000000000`
- Total time: `216.48s`

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

- Peak RSS: 14.7 MiB (process-wide)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | 2 | 81.02 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 216.48s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs (Count) | 5455.000 | 5455.000..5455.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 11079.000 | 10720.660..11920.540 | 6.05% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 13839.000 | 13421.391..15021.409 | 6.42% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10305671886.000 | 9677160315.296..10995861215.904 | 7.28% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 11299042.000 | -68367788.953..252017572.153 | 199.03% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1243379711.000 | 1177986454.474..1302636814.326 | 5.73% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 499844206.000 | 444580381.178..531604071.622 | 10.17% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 31613030208.000 | 30353674893.508..32948123707.692 | 4.68% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 325059.632 | 306417.099..333324.449 | 4.80% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 8970.704 | 8682.923..9091.418 | 2.62% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 39383.163 | 38615.052..40625.633 | 2.89% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 48336.207 | 47458.516..49556.509 | 2.47% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 19.27ms | 19.47ms | - | 19.47ms |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs | 5 | 5455.000 | 5455.000 | - | 5455.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 12439.000 | 12439.000 | - | 12439.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 15383.000 | 15383.000 | - | 15383.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 11443924068.000 | 11443924068.000 | - | 11443924068.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 418740833.000 | 418740833.000 | - | 418740833.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1346712544.000 | 1346712544.000 | - | 1346712544.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 557001917.000 | 557001917.000 | - | 557001917.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 33717707460.000 | 33717707460.000 | - | 33717707460.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 332003.680 | 332003.680 | - | 332003.680 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 9097.122 | 9097.122 | - | 9097.122 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 41408.879 | 41408.879 | - | 41408.879 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 50468.213 | 50468.213 | - | 50468.213 |
