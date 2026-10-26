# tier4-system-compaction-grandparent-cuts

- Schema: `cntryl-stress.v2`
- Profile: `release`
- Completed: `01793012518593807000-0000007985-00000000000000000000`
- Total time: `222.22s`

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

- Peak RSS: 138.8 MiB (process-wide)

## Needs attention

- ! sustained_write_throughput_under_compaction quality gate failed: quality=noisy below min=acceptable high_variance: Measured samples varied enough that this row needs attention. fix: Use deterministic fixtures and move setup outside the measured work. (at `benches/tier4_system_compaction_grandparent_cuts.rs:277`)

## Benchmarks

| Benchmark | Tier | Value | Trust | Mode | Question | Quality | Samples | Wall |
|---|---:|---|---|---|---|---|---:|---:|
| sustained_write_throughput_under_compaction | 4 | 9.44M byte/s | diagnostic | duration | mode=duration | noisy | 5 | 222.22s |

## Observations

| Benchmark | Observation | Median | 95% CI | RSD | Direction |
|---|---|---:|---:|---:|---|
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_bytes (Bytes) | 367477470.000 | 366553848.847..377621641.153 | 1.70% | Informational |
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_files (Count) | 3.000 | 3.000..3.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | largest_pre_measurement_l2_overlap_file_bytes (Bytes) | 133965774.000 | 133965692.063..133965815.537 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | compaction_output_bytes (Bytes) | 845865521.000 | 531742653.697..874459180.303 | 27.80% | Informational |
| sustained_write_throughput_under_compaction | compaction_output_bytes_per_logical_input_byte (Ratio) | 10.680 | 6.714..11.041 | 27.80% | Informational |
| sustained_write_throughput_under_compaction | measured_compactions_run (Count) | 4.000 | 4.000..4.000 | 0.00% | Informational |
| sustained_write_throughput_under_compaction | final_sst_count (Count) | 3.000 | 2.920..3.880 | 16.11% | Informational |

## Tail Distributions

| Benchmark | Series | N | p90 | p99 | p99.9 | Max |
|---|---|---:|---:|---:|---:|---:|
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_bytes | 5 | 379004312.000 | 379004312.000 | - | 379004312.000 |
| sustained_write_throughput_under_compaction | pre_measurement_l2_overlap_files | 5 | 3.000 | 3.000 | - | 3.000 |
| sustained_write_throughput_under_compaction | largest_pre_measurement_l2_overlap_file_bytes | 5 | 133965829.000 | 133965829.000 | - | 133965829.000 |
| sustained_write_throughput_under_compaction | compaction_output_bytes | 5 | 845872045.000 | 845872045.000 | - | 845872045.000 |
| sustained_write_throughput_under_compaction | compaction_output_bytes_per_logical_input_byte | 5 | 10.680 | 10.680 | - | 10.680 |
| sustained_write_throughput_under_compaction | measured_compactions_run | 5 | 4.000 | 4.000 | - | 4.000 |
| sustained_write_throughput_under_compaction | final_sst_count | 5 | 4.000 | 4.000 | - | 4.000 |
