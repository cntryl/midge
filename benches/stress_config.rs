//! Shared helpers for `cntryl-stress` benchmark files.
//!
//! Current `cntryl-stress` measurements are named rows:
//! Tier 1 uses `measure` or `measure_batch` for hot paths, Tier 2 uses
//! fixed-operation `measure` or `measure_batch`, and Tiers 3+ use
//! fixed-duration `measure_batch` or externally timed `record_external`.

#![allow(dead_code)]

use cntryl_stress::StressContext;
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
    completed_operations: u64,
    f: impl FnOnce() -> R,
) -> R {
    let started_at = Instant::now();
    let result = f();
    ctx.record_external(name, started_at.elapsed(), completed_operations);
    result
}

#[allow(dead_code)]
pub fn measure_external_counted<R>(
    ctx: &mut StressContext,
    name: impl Into<String>,
    f: impl FnOnce() -> (R, u64),
) -> R {
    let started_at = Instant::now();
    let (result, completed_operations) = f();
    ctx.record_external(name, started_at.elapsed(), completed_operations);
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
    ctx.metadata("trust_class", "diagnostic");
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

#[allow(dead_code)]
pub trait MidgeStressContextExt {
    fn tag(&mut self, key: &'static str, value: impl ToString) -> &mut Self;
    fn set_elements(&mut self, completed: u64) -> &mut Self;
    fn set_bytes(&mut self, bytes: u64) -> &mut Self;
}

impl MidgeStressContextExt for StressContext {
    fn tag(&mut self, key: &'static str, value: impl ToString) -> &mut Self {
        self.parameter(key, value)
    }

    fn set_elements(&mut self, completed: u64) -> &mut Self {
        self.operations(completed)
    }

    fn set_bytes(&mut self, bytes: u64) -> &mut Self {
        self.parameter("logical_bytes", bytes)
    }
}
