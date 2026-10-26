# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792999663937862000-0000077132-00000000000000000000`
- Total time: `184.78s`

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
| tier2_durability_commit_sync_local_4k_rotation | 2 | 93.26 transaction/s | gate | fixed_ops | mode=fixed_ops | acceptable | 5 | 184.77s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us (Microseconds) | 9591.000 | 9406.092..10159.908 | 4.40% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us (Microseconds) | 12535.000 | 12497.391..12659.009 | 0.73% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs (Count) | 2730.000 | 2730.000..2730.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total (Nanoseconds) | 10402626865.000 | 10217202908.725..10666882223.675 | 2.46% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max (Nanoseconds) | 10511208.000 | 9689918.669..11972197.331 | 12.02% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits (Count) | 80.000 | 80.000..80.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total (Nanoseconds) | 1397001126.000 | 1373401704.715..1433652595.685 | 2.45% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count (Count) | 85.000 | 85.000..85.000 | 0.00% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total (Nanoseconds) | 407286248.000 | 398249177.591..413122422.809 | 2.09% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns (Nanoseconds) | 27384232335.000 | 26901093696.984..28020287753.016 | 2.32% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit (Nanoseconds) | 487679.497 | 479172.808..499905.992 | 2.42% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit (Nanoseconds) | 6074.354 | 5910.927..6192.068 | 2.65% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit (Nanoseconds) | 134254.582 | 132290.585..138154.895 | 2.47% | Informational |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit (Nanoseconds) | 140085.391 | 138299.173..144249.302 | 2.40% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_4k_rotation | latency | 15 | 16.19ms | 16.19ms | - | 16.24ms |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commit_p95_us | 5 | 10495.000 | 10495.000 | - | 10495.000 |
| tier2_durability_commit_sync_local_4k_rotation | non_rotation_commit_p95_us | 5 | 12735.000 | 12735.000 | - | 12735.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsyncs | 5 | 2730.000 | 2730.000 | - | 2730.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_total | 5 | 10793932823.000 | 10793932823.000 | - | 10793932823.000 |
| tier2_durability_commit_sync_local_4k_rotation | wal_fsync_ns_max | 5 | 12613250.000 | 12613250.000 | - | 12613250.000 |
| tier2_durability_commit_sync_local_4k_rotation | rotation_commits | 5 | 80.000 | 80.000 | - | 80.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_build_ns_total | 5 | 1458741037.000 | 1458741037.000 | - | 1458741037.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_count | 5 | 85.000 | 85.000 | - | 85.000 |
| tier2_durability_commit_sync_local_4k_rotation | flush_publish_ns_total | 5 | 415720586.000 | 415720586.000 | - | 415720586.000 |
| tier2_durability_commit_sync_local_4k_rotation | elapsed_ns | 5 | 28327579958.000 | 28327579958.000 | - | 28327579958.000 |
| tier2_durability_commit_sync_local_4k_rotation | event_loop_write_service_ns_per_4k_commit | 5 | 502882.876 | 502882.876 | - | 502882.876 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_ns_per_4k_commit | 5 | 6272.515 | 6272.515 | - | 6272.515 |
| tier2_durability_commit_sync_local_4k_rotation | wal_append_service_ns_per_4k_commit | 5 | 140638.829 | 140638.829 | - | 140638.829 |
| tier2_durability_commit_sync_local_4k_rotation | wal_encode_and_append_service_ns_per_4k_commit | 5 | 146726.332 | 146726.332 | - | 146726.332 |
