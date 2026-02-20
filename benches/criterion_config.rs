//! Criterion configuration helper with tier-based tuning.
//!
//! Usage in benchmarks:
//! ```
//! #[path = "./criterion_config.rs"] mod criterion_config;
//! use criterion_config::criterion_config_for_tier1; // or criterion_config_for_tier2
//! criterion_group!(name = my_bench;
//!     config = criterion_config_for_tier1();
//!     targets = bench_fn);
//! ```
//!
//! NOTE: For Tier1 and Tier2, set `SamplingMode::Flat` on the benchmark group:
//! `group.sampling_mode(SamplingMode::Flat)`.

use criterion::Criterion;
use std::time::Duration;

#[allow(dead_code)]
pub fn criterion_config_for_tier1() -> Criterion {
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
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(100)
        .without_plots()
}

#[allow(dead_code)]
pub fn criterion_config_for_tier2() -> Criterion {
    // ---------------------------------------------------------------
    // Tier 2 — Subsystem (µs → ms)
    //
    // Component-level latencies: memtable insert, block read, WAL append.
    // Used very frequently during perf tuning.
    // ---------------------------------------------------------------
    Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(50)
        .without_plots()
}
