# Performance Tuning

This guide describes the *high-level* knobs that influence performance. For the authoritative set of options, see `OpenOptions` in `src/engine/api/options.rs` and the user-facing guide in `docs/API_GUIDE.md`.

## Start with the big knobs

### 1) Pick a goal

Use `Goal` to bias derived parameters:

- `Goal::Latency`: prioritize low tail latency (often more cache, smaller batches).
- `Goal::Throughput`: prioritize sustained throughput.

### 2) Set a memory budget

`MemoryBudget` controls how much memory the engine assumes it can use. Auto derives from the
effective memory limit (cgroup-aware when available). More budget generally improves:

- read latency (larger block cache)
- write smoothing (larger memtables / buffering)

### 3) Set a workload profile

`WorkloadProfile` is a hint for expected access patterns:

- `WriteHeavy`
- `ReadMostly`
- `RangeScan`
- `TtlHeavy`

These influence derived settings such as block sizes, cache allocation, and compaction aggressiveness.

## Practical tuning tips

- If you see read amplification (many SST reads per lookup), increase memory budget and consider a more read-oriented workload profile.
- If you see frequent flush/compaction pressure, consider a write-oriented goal/workload profile and ensure the underlying storage has sufficient write bandwidth.
- For range scans, larger blocks and scan-oriented workloads tend to help; bloom filters are less useful for long sequential scans.

## Cloud-specific considerations

For cloud storage backends:

- Local cache sizing and disk speed often dominate read latency.
- Network variability makes throughput benchmarks noisier; compare changes carefully.
- Keep cache directories on fast local SSD when possible.

See `docs/CLOUD_SETUP.md` for setup and operational recommendations.

## Measuring improvements

- Use targeted Criterion benches in `benches/` to validate hotpath changes.
- Use integration tests to validate correctness under recovery and durability scenarios.

See `docs/BENCHMARKS.md` for benchmarking practices.
