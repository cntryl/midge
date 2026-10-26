# tier2-subsystem-durability-commit-latency

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01792998755925608000-0000074832-00000000000000000000`
- Total time: `42.49s`

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

- Peak RSS: 28.5 MiB (process-wide)

## Needs attention

- ! tier2_durability_commit_sync_local_64_writers quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Use deterministic fixtures and move setup outside the measured work. (at `benches/tier2_subsystem_durability_commit_latency.rs:80`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier2_durability_commit_sync_local_64_writers | 2 | 5.24K transaction/s | diagnostic | fixed_ops | mode=fixed_ops | noisy | 5 | 42.49s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| tier2_durability_commit_sync_local_64_writers | wal_appends (Count) | 565.000 | 398.885..1187.915 | 56.73% | Informational |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs (Count) | 565.000 | 398.885..1187.915 | 56.73% | Informational |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync (Ratio) | 57.996 | 34.073..63.436 | 34.35% | HigherIsBetter |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| tier2_durability_commit_sync_local_64_writers | latency | 163840 | 10.68ms | 35.39ms | 566.23ms | 582.85ms |
| tier2_durability_commit_sync_local_64_writers | wal_appends | 5 | 1589.000 | 1589.000 | - | 1589.000 |
| tier2_durability_commit_sync_local_64_writers | physical_fsyncs | 5 | 1589.000 | 1589.000 | - | 1589.000 |
| tier2_durability_commit_sync_local_64_writers | commits_per_fsync | 5 | 60.681 | 60.681 | - | 60.681 |
