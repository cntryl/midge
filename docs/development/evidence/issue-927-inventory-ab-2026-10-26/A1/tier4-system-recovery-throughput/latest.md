# tier4-system-recovery-throughput

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01793002191101420000-0000084439-00000000000000000000`
- Total time: `108.69s`

## Summary

```text
Summary
  benchmarks:      1
  gate:            failed quality (no intended gate rows)
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
  gate:            0
  diagnostic:      1
  experimental:    0
  invalid:         0
```

## Environment

- `power_source`: ac

## Memory

- Peak RSS: 25.7 MiB (process-wide)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| tier4_recovery_cloud_inventory_10k | 4 | 578.06 sst/s | diagnostic | duration | mode=duration | acceptable | 5 | 108.69s |
