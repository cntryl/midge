# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01793000426185969000-0000077456-00000000000000000000`
- Total time: `65.51s`

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

- Peak RSS: 16.7 MiB (process-wide)

## Needs attention

- ! tier2_durability_commit_sync_local_16_writers quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Inspect one-off setup, cache, I/O, or background interference before updating a baseline. (at `benches/tier2_subsystem_durability_commit_latency.rs:80`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_16_writers | 2 | 1.35K transaction/s | diagnostic | fixed_ops | mode=fixed_ops | noisy | 5 | 65.51s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_16_writers | wal_appends (Count) | 540.000 | 232.659..1477.341 | 83.04% | Informational |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs (Count) | 540.000 | 232.659..1477.341 | 83.04% | Informational |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync (Ratio) | 15.170 | 8.497..17.436 | 39.32% | HigherIsBetter |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_16_writers | latency | 40960 | 11.08ms | 143.65ms | 152.04ms | 154.28ms |
| tier2_durability_commit_sync_local_16_writers | wal_appends | 5 | 2125.000 | 2125.000 | - | 2125.000 |
| tier2_durability_commit_sync_local_16_writers | physical_fsyncs | 5 | 2125.000 | 2125.000 | - | 2125.000 |
| tier2_durability_commit_sync_local_16_writers | commits_per_fsync | 5 | 15.634 | 15.634 | - | 15.634 |
