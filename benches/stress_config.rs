//! Shared helpers for `cntryl-stress` benchmark files.
//!
//! Current `cntryl-stress` measurements are named rows:
//! Tier 1 uses `measure` or `measure_batch` for hot paths, Tier 2 uses
//! fixed-operation `measure` or `measure_batch`, and Tiers 3+ use
//! fixed-duration `measure_batch` or externally timed `record_external`.

use cntryl_stress::StressContext;
use std::time::Instant;

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
