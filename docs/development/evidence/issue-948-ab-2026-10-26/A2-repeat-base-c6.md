# tier4-system-compaction-grandparent-cuts

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01793026218508803000-0000099700-00000000000000000000`
- Total time: `178.48s`

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

- Peak RSS: 137.5 MiB (process-wide)

## Needs attention

- none

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| sustained_write_throughput_under_compaction | 4 | 12.54M byte/s | gate | duration | mode=duration | acceptable | 5 | 178.48s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_bytes (Bytes) | 379000755.000 | 378999386.556..379003992.644 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_files (Count) | 3.000 | 3.000..3.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | largest_pre_measurement_l2_overlap_file_bytes (Bytes) | 133965638.000 | 133965610.558..133965690.642 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | compaction_output_bytes (Bytes) | 488945469.000 | 488944671.670..488949610.730 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | compaction_output_bytes_per_logical_input_byte (Ratio) | 6.174 | 6.174..6.174 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | measured_compactions_run (Count) | 4.000 | 4.000..4.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | final_sst_count (Count) | 4.000 | 4.000..4.000 | 0.00% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_bytes | 5 | 379004360.000 | 379004360.000 | - | 379004360.000 |
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_files | 5 | 3.000 | 3.000 | - | 3.000 |
| sustained_write_throughput_under_compaction | largest_pre_measurement_l2_overlap_file_bytes | 5 | 133965718.000 | 133965718.000 | - | 133965718.000 |
| sustained_write_throughput_under_compaction | compaction_output_bytes | 5 | 488951409.000 | 488951409.000 | - | 488951409.000 |
| sustained_write_throughput_under_compaction | compaction_output_bytes_per_logical_input_byte | 5 | 6.174 | 6.174 | - | 6.174 |
| sustained_write_throughput_under_compaction | measured_compactions_run | 5 | 4.000 | 4.000 | - | 4.000 |
| sustained_write_throughput_under_compaction | final_sst_count | 5 | 4.000 | 4.000 | - | 4.000 |
