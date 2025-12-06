# Benchmark Guidelines

## Purpose

Defines how to design, execute, and maintain benchmarks for Midge. Emphasizes **fast feedback**, **reproducibility**, and **actionable data**.

## Core Principles

1. Benchmarks must drive decisions.
   Every benchmark should help confirm or reject a hypothesis (“does batching improve throughput?”).

2. Fast feedback matters.
   Quick, approximate results are more useful day-to-day than long, perfect runs you never execute.

3. Reproducibility over realism.
   Use fixed inputs, stable environments, and deterministic seeds.

4. Focus on what users feel.
   Throughput, latency, and resource use matter more than synthetic metrics like iteration counts.

5. Actionable results only.
   Every run should produce a number that can be compared, trended, or gated in CI.

## Benchmark Tiers

Midge uses a 6-tier benchmark system. See `benches/TIER_LADDER.md` for authoritative definitions.

| Tier                      | Latency Range | Purpose                                               | Criterion Config                           | Frequency          |
| ------------------------- | ------------- | ----------------------------------------------------- | ------------------------------------------ | ------------------ |
| **Tier 1 — Hot Path**     | ns → µs       | Critical path microbenchmarks (bloom, TLV, memtable)  | 200ms warmup, 500ms measure, 20 samples    | Every PR (CI gate) |
| **Tier 2 — Subsystem**    | µs → ms       | Component boundaries (flush, WAL append, SST reads)   | 300ms warmup, 1s measure, 15 samples       | Daily CI           |
| **Tier 3 — System**       | ms → 10ms     | Full engine operations (put/get full stack, recovery) | 300ms warmup, 700ms measure, 10 samples    | Pre-commit         |
| **Tier 4 — Integration**  | 10ms → 100ms  | YCSB workloads with realistic load patterns           | 500ms warmup, 3s measure, 10 samples       | Nightly            |
| **Tier 5 — Soak**         | 100ms → s     | Long-running stress tests (compaction, concurrency)   | 1s warmup, 10s measure, 5 samples          | Weekly             |
| **Tier 6 — Capacity**     | minutes+      | Multi-hour stability and resource leak detection      | 2s warmup, 30s measure, 3 samples          | Release only       |

Each benchmark must use `criterion_config_for_tier(BenchTier::TierN)` from `benches/criterion_helper.rs`.

## Design Checklist

- **Question:** What are we learning?
- **Metric:** ops/sec, GB/s, µs latency, or amplification ratio.
- **Baseline:** Reference version or previous commit.
- **Threshold:** ± 5 % change triggers review.
- **Repeatability:** Fixed seed and input set.
- **Output:** Numeric summary + optional histogram.

## Environment Requirements

To ensure comparability:

- **Hardware:** Record CPU model, core count, memory, disk/NVMe.
- **Software:** Compiler version, build flags, allocator, OS kernel.
- **Isolation:** Disable turbo scaling, background jobs, and network noise.
- **Location:** Run in tmpfs or RAM disk when testing logic only.

Document these in `BENCH_ENV.md`.

## Directory Layout

```
benches/
├── criterion_helper.rs      # Tier-based Criterion configs
├── tier1_hotpath/           # Critical path microbenchmarks
├── tier2_subsystem/         # Component boundaries
├── tier3_system/            # Full engine operations
├── tier4_integration/       # YCSB workloads
├── tier5_soak/              # Long-running stress tests
├── tier6_capacity/          # Multi-hour capacity tests
├── README.md                # Benchmark catalog
└── TIER_LADDER.md           # Authoritative tier definitions
```

See `benches/README.md` for complete benchmark catalog and `TIER_LADDER.md` for tier requirements.

## Benchmark Patterns

### Throughput (bulk ops)

```rust
b.iter(|| {
    for record in &records {
        black_box(target.insert(record));
    }
});
```

### Latency (single op)

```rust
b.iter(|| {
    black_box(target.query(key));
});
```

### Scaling (input-size sweep)

```rust
for size in [1_000, 10_000, 100_000] {
    group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &s| { … });
}
```

Use `black_box()` to prevent optimization and set up data outside the loop.

## Configuration Profiles

Midge uses tier-based configurations via `criterion_helper.rs`. Each benchmark imports:

```rust
#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion_helper::{criterion_config_for_tier, BenchTier};

criterion_group! {
    name = bench_group;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_fn
}
```

**Important:** For Tier 1 and Tier 2, always set `group.sampling_mode(SamplingMode::Flat)` for fast iterations.

**Note:** Some Tier 1 benchmarks manually override measurement time to 200ms for ultra-fast feedback. This is acceptable for development iteration but should be documented.

## CI Integration

- **Fast mode:** run Tier 1–2 with quick config.
- **Perf mode:** Tier 3 gated behind `--features perf`.
- Regression gate:

  ```bash
  cargo bench -- --save-baseline main
  cargo bench -- --baseline main --fail-threshold 0.05
  ```

- Fail CI if throughput ↓ > 5 % or latency ↑ > 10 %.

## Result Reporting

- Export raw Criterion results (`target/criterion`) to CI artifacts.
- Optionally push summaries to Prometheus, Influx, or CSV for trending.
- Track regressions across commits — visualize percent deltas, not absolute numbers.

## Best Practices

✅ Do

- Pre-allocate all inputs outside the measurement loop.
- Use deterministic data patterns.
- Benchmark steady-state performance only.
- Add brief doc comments describing intent (“measures insert throughput under contention”).

❌ Don’t

- Include correctness checks — that’s what tests are for.
- Mix I/O randomness unless that’s the point.
- Benchmark rare code paths.
- Run benchmarks with debug builds.

## Maintenance

- Review results weekly; update baselines monthly.
- Delete stale or redundant benchmarks.
- Add a short changelog entry when benchmark behavior changes.

Refer to existing benchmarks in `benches/tier1_hotpath/` through `tier6_capacity/` for concrete examples.
