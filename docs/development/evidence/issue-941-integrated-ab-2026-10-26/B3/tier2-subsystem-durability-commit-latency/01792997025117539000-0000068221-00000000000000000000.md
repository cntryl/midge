# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792997025117539000-0000068221-00000000000000000000`
- Total time: `213.03s`

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
| tier2_durability_commit_sync_local_4k_rotation | 2 | 82.84 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 213.03s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs (Count) | 2725.000 | 2725.000..2725.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs (Count) | 0.000 | 0.000..0.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs (Count) | 5455.000 | 5455.000..5455.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 10767.000 | 10661.704..10977.896 | 1.67% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 14207.000 | 14005.957..14542.443 | 2.14% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 9912798082.000 | 9836631588.987..9982968322.613 | 0.84% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 11651417.000 | 9850851.959..12864665.241 | 15.14% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1204010374.000 | 1195986142.260..1211618276.940 | 0.74% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 468153878.000 | 455315969.563..478360416.437 | 2.82% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 30907638791.000 | 30741342568.416..31067300131.584 | 0.60% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 316168.413 | 312424.538..323116.757 | 1.92% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 8829.405 | 8718.144..8897.186 | 1.16% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 38245.464 | 37586.963..38983.533 | 2.08% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 46881.526 | 46412.940..47772.887 | 1.65% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 17.69ms | 19.47ms | - | 19.47ms |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_sync_attempts | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_file_syncs | 5 | 2725.000 | 2725.000 | - | 2725.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_sync_attempts | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | ledger_directory_syncs | 5 | 0.000 | 0.000 | - | 0.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_and_ledger_accounted_syncs | 5 | 5455.000 | 5455.000 | - | 5455.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 11095.000 | 11095.000 | - | 11095.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 14783.000 | 14783.000 | - | 14783.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 9986405134.000 | 9986405134.000 | - | 9986405134.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 13705334.000 | 13705334.000 | - | 13705334.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1213626589.000 | 1213626589.000 | - | 1213626589.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 479710331.000 | 479710331.000 | - | 479710331.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 31144000917.000 | 31144000917.000 | - | 31144000917.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 326416.419 | 326416.419 | - | 326416.419 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 8908.404 | 8908.404 | - | 8908.404 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 39511.695 | 39511.695 | - | 39511.695 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 48328.970 | 48328.970 | - | 48328.970 |
