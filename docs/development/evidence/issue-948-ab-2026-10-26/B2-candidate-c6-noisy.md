# tier4-system-compaction-grandparent-cuts

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01793026648899739000-0000004586-00000000000000000000`
- Total time: `265.32s`

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

- Peak RSS: 139.6 MiB (process-wide)

## Needs attention

- ! sustained_write_throughput_under_compaction quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Inspect one-off setup, cache, I/O, or background interference before updating a baseline. (at `benches/tier4_system_compaction_grandparent_cuts.rs:277`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| sustained_write_throughput_under_compaction | 4 | 9.33M byte/s | diagnostic | duration | mode=duration | noisy | 5 | 265.32s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_bytes (Bytes) | 384221780.000 | 384221780.000..384221780.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_files (Count) | 3.000 | 3.000..3.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | largest_pre_measurement_l2_overlap_file_bytes (Bytes) | 133994180.000 | 133994180.000..133994180.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | compaction_output_bytes (Bytes) | 494770046.000 | 494770046.000..494770046.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | compaction_output_bytes_per_logical_input_byte (Ratio) | 6.247 | 6.247..6.247 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | measured_compactions_run (Count) | 4.000 | 4.000..4.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | final_sst_count (Count) | 5.000 | 5.000..5.000 | 0.00% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_bytes | 5 | 384221780.000 | 384221780.000 | - | 384221780.000 |
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_files | 5 | 3.000 | 3.000 | - | 3.000 |
| sustained_write_throughput_under_compaction | largest_pre_measurement_l2_overlap_file_bytes | 5 | 133994180.000 | 133994180.000 | - | 133994180.000 |
| sustained_write_throughput_under_compaction | compaction_output_bytes | 5 | 494770046.000 | 494770046.000 | - | 494770046.000 |
| sustained_write_throughput_under_compaction | compaction_output_bytes_per_logical_input_byte | 5 | 6.247 | 6.247 | - | 6.247 |
| sustained_write_throughput_under_compaction | measured_compactions_run | 5 | 4.000 | 4.000 | - | 4.000 |
| sustained_write_throughput_under_compaction | final_sst_count | 5 | 5.000 | 5.000 | - | 5.000 |
