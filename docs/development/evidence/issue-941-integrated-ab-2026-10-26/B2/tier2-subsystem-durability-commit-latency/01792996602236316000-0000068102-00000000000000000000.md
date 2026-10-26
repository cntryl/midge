# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792996602236316000-0000068102-00000000000000000000`
- Total time: `216.08s`

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
| tier2_durability_commit_sync_local_4k_rotation | 2 | 80.98 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 216.08s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs (Count) | 5455.000 | 5455.000..5455.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 10791.000 | 10674.037..11435.963 | 3.93% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 14455.000 | 14162.303..15144.497 | 3.82% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10023887296.000 | 9824248717.847..10722326660.553 | 4.99% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 11121333.000 | 409167.206..41619699.594 | 111.86% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1227199295.000 | 1195742650.235..1342899618.565 | 6.61% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 481230130.000 | 456813821.659..528842363.141 | 8.34% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 31243387042.000 | 30768974271.087..32508045279.313 | 3.14% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 327822.766 | 322905.053..330180.683 | 1.27% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 9207.873 | 9126.728..9347.457 | 1.36% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 37043.259 | 36654.736..37793.024 | 1.74% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 46251.132 | 45851.169..47070.776 | 1.50% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 19.27ms | 19.52ms | - | 19.52ms |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs | 5 | 5455.000 | 5455.000 | - | 5455.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 11623.000 | 11623.000 | - | 11623.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 15367.000 | 15367.000 | - | 15367.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 11088498297.000 | 11088498297.000 | - | 11088498297.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 62980584.000 | 62980584.000 | - | 62980584.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1406486087.000 | 1406486087.000 | - | 1406486087.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 557058873.000 | 557058873.000 | - | 557058873.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 33228852418.000 | 33228852418.000 | - | 33228852418.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 331574.760 | 331574.760 | - | 331574.760 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 9366.951 | 9366.951 | - | 9366.951 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 38021.004 | 38021.004 | - | 38021.004 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 47186.336 | 47186.336 | - | 47186.336 |
