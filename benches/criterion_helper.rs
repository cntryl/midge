/// Criterion configuration helper with tier-based tuning.
///
/// Usage in benchmarks:
/// ```
/// use criterion_helper::{criterion_config, BenchTier};
/// criterion_group!(name = my_bench; config = criterion_config(BenchTier::Tier1Hot); targets = bench_fn);
/// ```
///
/// For backward compatibility, `criterion_config()` without arguments defaults to Tier2Subsystem.
///
/// NOTE: `SamplingMode::Flat` should be set on individual `BenchmarkGroup`s for Tier1/Tier2
/// benchmarks where iterations are fast. Use `group.sampling_mode(SamplingMode::Flat)`.
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

/// Tier-specific Criterion configuration.
#[allow(dead_code)]
pub fn criterion_config_for_tier(tier: BenchTier) -> Criterion {
    match tier {
        // ---------------------------------------------------------------
        // Tier 1: Hotpath (ns → µs)
        //
        // Ultra-tight loops: bloom probe, cache lookup, TLV parse.
        // Goal: stable sub-microsecond measurements.
        // NOTE: Use group.sampling_mode(SamplingMode::Flat) in benchmarks.
        // ---------------------------------------------------------------
        BenchTier::Tier1Hot => Criterion::default()
            .warm_up_time(Duration::from_millis(200))
            .measurement_time(Duration::from_millis(500))
            .sample_size(20)
            .noise_threshold(0.015)
            .confidence_level(0.95)
            .nresamples(20_000)
            .without_plots(),

        // ---------------------------------------------------------------
        // Tier 2: Subsystem (µs → ms)
        //
        // Component-level: memtable insert, SST block read, WAL append.
        // Goal: measure individual subsystem latency.
        // NOTE: Use group.sampling_mode(SamplingMode::Flat) in benchmarks.
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
        // Tier 3: Integration (ms → 10ms)
        //
        // Full engine ops: put/get through full stack, flush, scan.
        // Goal: end-to-end latency with real I/O.
        // ---------------------------------------------------------------
        BenchTier::Tier3System => Criterion::default()
            .warm_up_time(Duration::from_millis(500))
            .measurement_time(Duration::from_secs(2))
            .sample_size(10)
            .noise_threshold(0.03)
            .confidence_level(0.95)
            .nresamples(20_000)
            .without_plots(),

        // ---------------------------------------------------------------
        // Tier 4: Durability (10ms → 100ms)
        //
        // fsync-heavy: WAL sync, SST write, manifest update.
        // Goal: measure durable write latency.
        // ---------------------------------------------------------------
        BenchTier::Tier4Integration => Criterion::default()
            .warm_up_time(Duration::from_millis(500))
            .measurement_time(Duration::from_secs(3))
            .sample_size(8)
            .noise_threshold(0.05)
            .confidence_level(0.90)
            .nresamples(10_000)
            .without_plots(),

        // ---------------------------------------------------------------
        // Tier 5: Stress / Load / Compaction Pressure
        //
        // Progress tracking, not fine timing. Goals:
        // - detect deadlocks
        // - measure wallclock throughput
        // - observe compactor/reactor latencies
        // - simulate multi-threaded hot load
        // ---------------------------------------------------------------
        BenchTier::Tier5Soak => Criterion::default()
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(10))
            .sample_size(5)
            .noise_threshold(0.10)
            .confidence_level(0.85)
            .significance_level(0.20)
            .nresamples(5_000)
            .without_plots(),

        // ---------------------------------------------------------------
        // Tier 6: Soak / Chaos / Long Runtime Stability
        //
        // Often run overnight on dedicated hardware with chaos triggers
        // (compaction storms, WAL corruption sims). Criterion used to
        // standardize harness; low sample counts since iterations may
        // take seconds → minutes.
        // ---------------------------------------------------------------
        BenchTier::Tier6Capacity => Criterion::default()
            .warm_up_time(Duration::from_secs(2))
            .measurement_time(Duration::from_secs(30))
            .sample_size(3)
            .noise_threshold(0.20)
            .confidence_level(0.80)
            .significance_level(0.20)
            .nresamples(2_000)
            .without_plots(),
    }
}
