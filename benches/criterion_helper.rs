//! Criterion configuration helper with tier-based tuning.
//!
//! Usage in benchmarks:
//! ```
//! use criterion_helper::{criterion_config_for_tier, BenchTier};
//! criterion_group!(name = my_bench;
//!     config = criterion_config_for_tier(BenchTier::Tier1Hot);
//!     targets = bench_fn);
//! ```
//!
//! For backward compatibility, calling `criterion_config()` without arguments
//! defaults to `Tier2Subsystem`.
//!
//! NOTE: For Tier1 and Tier2, set `SamplingMode::Flat` on the benchmark group:
//! `group.sampling_mode(SamplingMode::Flat)`.

use criterion::Criterion;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum BenchTier {
    Tier1Hot,
    Tier2Subsystem,
}

#[allow(dead_code)]
pub fn criterion_config_for_tier(tier: BenchTier) -> Criterion {
    match tier {
        // ---------------------------------------------------------------
        // Tier 1 — Hotpath (ns → µs)
        //
        // Ultra-tight loops: bloom probe, cache lookup, TLV parse.
        // Goal: stable sub-microsecond signals.
        // Windows has higher system jitter, so we use:
        // - Longer warmup (CPU ramp-up, cache warmth)
        // - Longer measurement window (average out timer noise)
        // - More samples (statistical stability)
        // - Looser noise threshold (accept Windows jitter)
        // ---------------------------------------------------------------
        BenchTier::Tier1Hot => Criterion::default()
            .warm_up_time(Duration::from_millis(800))
            .measurement_time(Duration::from_millis(2000))
            .sample_size(10)
            .noise_threshold(0.05)
            .confidence_level(0.90)
            .significance_level(0.10)
            .nresamples(10_000)
            .without_plots(),

        // ---------------------------------------------------------------
        // Tier 2 — Subsystem (µs → ms)
        //
        // Component-level latencies: memtable insert, block read, WAL append.
        // Used very frequently during perf tuning.
        // ---------------------------------------------------------------
        BenchTier::Tier2Subsystem => Criterion::default()
            .warm_up_time(Duration::from_millis(300))
            .measurement_time(Duration::from_secs(1))
            .sample_size(15)
            .noise_threshold(0.02)
            .confidence_level(0.95)
            .nresamples(30_000)
            .without_plots(),
    }
}
