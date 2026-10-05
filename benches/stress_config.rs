//! Shared helpers for `cntryl-stress` benchmark files.
//!
//! Current `cntryl-stress` measurements are named rows:
//! Tier 1 uses `measure` or `measure_batch` for hot paths, Tier 2 uses
//! fixed-operation `measure` or `measure_batch`, and Tiers 3+ use
//! fixed-duration `measure_batch` or externally timed `record_external`.

#![allow(dead_code)]

use cntryl_stress::{LogicalUnit, OperationOutcome, StressContext};
use std::time::Instant;

#[path = "bench_support/stress.rs"]
pub mod bench_stress;
#[path = "bench_support/config.rs"]
pub mod config;
#[path = "bench_support/ycsb.rs"]
pub mod ycsb;
#[path = "bench_support/zipfian.rs"]
pub mod zipfian;

pub type MidgeOptions = config::MidgeOptions;
pub type StorageMode = config::StorageMode;

#[must_use]
pub fn measured_write_options(opts: &MidgeOptions) -> cntryl_midge::WriteOptions {
    config::measured_write_options(opts)
}

#[must_use]
pub fn memory_opts() -> MidgeOptions {
    config::memory_opts()
}

#[must_use]
pub fn opts_for_mode(mode: &str) -> MidgeOptions {
    config::opts_for_mode(mode)
}

#[must_use]
pub fn write_coordination_opts_for_mode(mode: &str) -> MidgeOptions {
    config::write_coordination_opts_for_mode(mode)
}

pub fn init_benchmark_telemetry() -> cntryl_midge::MidgeResult<()> {
    cntryl_midge::init_benchmark_telemetry()
}

#[allow(dead_code)]
pub struct BenchConfig;

impl Default for BenchConfig {
    fn default() -> Self {
        Self
    }
}

#[allow(dead_code)]
pub fn measure_hot_path_batch(
    ctx: &mut StressContext,
    name: impl Into<String>,
    logical_operations_per_iteration: u64,
    f: impl FnMut(),
) {
    let _completed = ctx.measure_batch(name, logical_operations_per_iteration, f);
}

#[allow(dead_code)]
pub fn measure_external<R>(
    ctx: &mut StressContext,
    name: impl Into<String>,
    logical_unit: &'static str,
    completed_operations: u64,
    f: impl FnOnce() -> R,
) -> R {
    let started_at = Instant::now();
    let result = f();
    ctx.record_external_outcome(
        name,
        started_at.elapsed(),
        LogicalUnit::new(logical_unit),
        OperationOutcome::success(completed_operations),
    );
    result
}

#[allow(dead_code)]
pub fn measure_counted<R>(
    ctx: &mut StressContext,
    name: impl Into<String>,
    logical_unit: &'static str,
    f: impl FnOnce() -> (R, u64),
) -> R {
    let started_at = Instant::now();
    let (result, completed_operations) = f();
    ctx.record_external_outcome(
        name,
        started_at.elapsed(),
        LogicalUnit::new(logical_unit),
        OperationOutcome::success(completed_operations),
    );
    result
}

#[allow(dead_code)]
pub fn parameter(ctx: &mut StressContext, key: &'static str, value: impl ToString) {
    ctx.parameter(key, value);
}

#[allow(dead_code)]
pub fn mark_validated_micro(ctx: &mut StressContext, logical_unit: &'static str) {
    ctx.parameter("logical_unit", logical_unit);
    ctx.metadata("validated_micro", "true");
}

#[allow(dead_code)]
pub fn mark_diagnostic(ctx: &mut StressContext, reason: &'static str) {
    ctx.metadata("diagnostic_reason", reason);
}

#[allow(dead_code)]
pub fn mark_local_rsd_diagnostic(ctx: &mut StressContext) {
    mark_diagnostic(ctx, "local_rsd_above_5pct");
    ctx.parameter("local_gate_rsd_limit_pct", 5);
}

#[allow(dead_code)]
pub fn mark_capped_probe(ctx: &mut StressContext, cap_source: &'static str) {
    mark_diagnostic(ctx, "intentional_capped_probe");
    ctx.parameter("capped_probe", "true");
    ctx.parameter("cap_source", cap_source);
}

#[allow(dead_code)]
pub fn mark_duration_plateau_probe(ctx: &mut StressContext, cap_source: &'static str) {
    mark_diagnostic(ctx, "duration_throughput_plateau_probe");
    ctx.parameter("capped_probe", "duration_plateau");
    ctx.parameter("cap_source", cap_source);
}

#[allow(dead_code)]
pub fn logical_bytes(ctx: &mut StressContext, bytes: u64) {
    ctx.parameter("logical_bytes", bytes);
}

/// Resolve the Tier 4 UI profile without changing native harness sample counts.
///
/// Smoke keeps each scenario's original short window or single complete cycle.
/// Standard and full require the workflow to pass the same native sample duration.
///
/// # Errors
///
/// Returns an invalid argument for an unrecognized profile.
pub fn tier4_profile_window(
    profile: Option<&str>,
) -> cntryl_midge::MidgeResult<Option<std::time::Duration>> {
    use cntryl_midge::MidgeError;
    use std::time::Duration;

    match profile.unwrap_or("smoke") {
        "smoke" => Ok(None),
        "standard" => Ok(Some(Duration::from_mins(10))),
        "full" => Ok(Some(Duration::from_hours(1))),
        unknown => Err(MidgeError::InvalidArgument(format!(
            "unknown Tier 4 profile: {unknown}"
        ))),
    }
}

/// Keep a continuous workload on its existing engine for the selected window.
///
/// # Errors
///
/// Returns an invalid argument for an unrecognized profile.
pub fn tier4_duration_for_profile(
    profile: Option<&str>,
    smoke_duration: std::time::Duration,
) -> cntryl_midge::MidgeResult<std::time::Duration> {
    Ok(tier4_profile_window(profile)?.unwrap_or(smoke_duration))
}

/// Resolve the UI duration from the benchmark process environment.
///
/// # Panics
///
/// Panics when `MIDGE_BENCH_PROFILE` is not a recognized profile.
#[must_use]
pub fn tier4_measured_duration(smoke_duration: std::time::Duration) -> std::time::Duration {
    let profile = std::env::var("MIDGE_BENCH_PROFILE").ok();
    tier4_duration_for_profile(profile.as_deref(), smoke_duration)
        .expect("valid Tier 4 workload profile")
}

/// Repeat bounded complete cycles for long Tier 4 profiles using the native harness.
///
/// Smoke records one cycle's existing internal elapsed time. Long profiles measure
/// the complete callback, including cycle setup and teardown, and aggregate only
/// the logical operations supplied after each actual cycle completes.
///
/// # Panics
///
/// Panics when the UI profile is unknown or the supplied cycle fails its checks.
pub fn measure_tier4_cycles(
    ctx: &mut StressContext,
    name: impl Into<String>,
    logical_unit: &'static str,
    mut cycle: impl FnMut() -> (u64, std::time::Duration),
) -> u64 {
    let profile = std::env::var("MIDGE_BENCH_PROFILE").ok();
    let window = tier4_profile_window(profile.as_deref()).expect("valid Tier 4 workload profile");
    ctx.parameter(
        "selected_measured_secs",
        window.map_or(0, |duration| duration.as_secs()),
    );
    let mut cycles = 0_u64;
    if let Some(duration) = window {
        ctx.parameter(
            "measurement_window_shape",
            "complete_cycles_including_setup_teardown",
        );
        let started_at = Instant::now();
        ctx.measure_outcome(name, LogicalUnit::new(logical_unit), || {
            let (completed, _) = cycle();
            cycles += 1;
            OperationOutcome::success(completed)
        });
        assert!(
            started_at.elapsed() >= duration,
            "Tier 4 native sample duration must cover the selected profile window; pass matching --sample-duration-ms"
        );
    } else {
        ctx.parameter("measurement_window_shape", "one_cycle_original_clock");
        let (completed, elapsed) = cycle();
        cycles = 1;
        ctx.record_external_outcome(
            name,
            elapsed,
            LogicalUnit::new(logical_unit),
            OperationOutcome::success(completed),
        );
    }
    ctx.parameter("completed_cycles", cycles);
    ctx.parameter("cycle_cardinality_scope", "per_completed_cycle");
    cycles
}
