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
    Tier3System,
    Tier4Integration,
    Tier5Soak,
    Tier6Capacity,
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

        // ---------------------------------------------------------------
        // Tier 3 — System (ms → 100ms)
        //
        // Full-engine operations: flush, compact, scan, put/get full stack.
        // Tuned for FAST FEEDBACK LOOPS while preserving meaningful signal.
        //
        // Typical runtime across all Tier 3 benches:
        //     ~2–3 minutes total on dev hardware.
        // ---------------------------------------------------------------
        BenchTier::Tier3System => Criterion::default()
            .warm_up_time(Duration::from_millis(1))
            .measurement_time(Duration::from_millis(1))
            .sample_size(10)
            .noise_threshold(0.05)
            .confidence_level(0.90)
            .nresamples(5_000)
            .without_plots(),

        // ---------------------------------------------------------------
        // Tier 4 — Durability / Integration
        //
        // fsync, WAL sync, SST write, manifest updates.
        // Used in release tuning + durability regression.
        // ---------------------------------------------------------------
        BenchTier::Tier4Integration => Criterion::default()
            .warm_up_time(Duration::from_secs(3))
            .measurement_time(Duration::from_secs(20))
            .sample_size(20)
            .noise_threshold(0.10)
            .confidence_level(0.95)
            .nresamples(10_000)
            .without_plots(),

        // ---------------------------------------------------------------
        // Tier 5 — Soak (stress, concurrency, compaction pressure)
        //
        // Focus is forward progress, regressions, deadlock detection.
        // Not about micro-precision.
        // ---------------------------------------------------------------
        BenchTier::Tier5Soak => Criterion::default()
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(10))
            .sample_size(10)
            .noise_threshold(0.10)
            .confidence_level(0.85)
            .significance_level(0.20)
            .nresamples(5_000)
            .without_plots(),

        // ---------------------------------------------------------------
        // Tier 6 — Capacity / Chaos / Long Runtime Stability
        //
        // Intended for multi-minute to multi-hour runs;
        // Criterion gives a consistent harness but not fine-grained timing.
        // ---------------------------------------------------------------
        BenchTier::Tier6Capacity => Criterion::default()
            .warm_up_time(Duration::from_secs(2))
            .measurement_time(Duration::from_secs(30))
            .sample_size(10)
            .noise_threshold(0.20)
            .confidence_level(0.80)
            .significance_level(0.20)
            .nresamples(2_000)
            .without_plots(),
    }
}
