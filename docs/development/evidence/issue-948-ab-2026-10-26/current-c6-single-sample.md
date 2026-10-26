# tier4-system-compaction-grandparent-cuts

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01793011422200495000-0000000431-00000000000000000000`
- Total time: `46.51s`

## Summary

```text
Summary
  benchmarks:      1
  gate:            failed quality (1 invalid row)
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
  noisy:           0
  untrustworthy:   1
Trust
  gate:            0
  diagnostic:      0
  experimental:    0
  invalid:         1
```

## Environment

- `power_source`: ac

## Memory

- Peak RSS: 136.9 MiB (process-wide)

## Needs attention

- ! sustained_write_throughput_under_compaction quality gate failed: quality=untrustworthy below min=acceptable too_few_samples: The row has too few measured samples to make a stable decision. fix: Collect at least five measured samples, or use the release profile for gate-quality rows. (at `benches/tier4_system_compaction_grandparent_cuts.rs:277`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| sustained_write_throughput_under_compaction | 4 | 6.83M byte/s | invalid | duration | mode=duration | untrustworthy | 1 | 46.51s |

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
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_bytes | 1 | 384221780.000 | 384221780.000 | - | 384221780.000 |
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_files | 1 | 3.000 | 3.000 | - | 3.000 |
| sustained_write_throughput_under_compaction | largest_pre_measurement_l2_overlap_file_bytes | 1 | 133994180.000 | 133994180.000 | - | 133994180.000 |
| sustained_write_throughput_under_compaction | compaction_output_bytes | 1 | 494770046.000 | 494770046.000 | - | 494770046.000 |
| sustained_write_throughput_under_compaction | compaction_output_bytes_per_logical_input_byte | 1 | 6.247 | 6.247 | - | 6.247 |
| sustained_write_throughput_under_compaction | measured_compactions_run | 1 | 4.000 | 4.000 | - | 4.000 |
| sustained_write_throughput_under_compaction | final_sst_count | 1 | 5.000 | 5.000 | - | 5.000 |
