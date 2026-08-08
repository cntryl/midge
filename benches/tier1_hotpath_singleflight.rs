//! Tier 1 — production keyed group-commit hot path benchmarks
//!
//! Measures the generic "accumulate → flush once → notify many waiters" primitive.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::common::KeyedGroupCommit;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const SUBMIT_AND_WAIT_BATCH_ROUNDS: usize = 8192;

fn run_flush_waiters(ctx: &mut StressContext, scenario: &'static str, waiters: usize) {
    ctx.parameter("waiters", waiters);
    ctx.parameter("logical_unit", "accumulator_flush");
    ctx.parameter("waiters_per_logical_operation", waiters);
    ctx.metadata("validated_micro", "true");

    ctx.measure(scenario, || {
        let acc: KeyedGroupCommit<u64, u64> = KeyedGroupCommit::new(1);
        for i in 0..waiters {
            acc.join(i as u64);
        }
        black_box(acc.rotate_from_to(&1, 2).expect("matching generation"));
        black_box(acc.complete(&1));
    });
}

fn run_submit_and_wait(ctx: &mut StressContext, scenario: &'static str, waiters: usize) {
    let logical_ops = SUBMIT_AND_WAIT_BATCH_ROUNDS as u64;

    ctx.parameter("waiters", waiters);
    ctx.parameter("rounds", SUBMIT_AND_WAIT_BATCH_ROUNDS);
    ctx.parameter("logical_unit", "submit_flush_wait_cycle");
    ctx.parameter("waiters_per_logical_operation", waiters);

    stress_config::measure_hot_path_batch(ctx, scenario, logical_ops, || {
        let mut total = 0u64;
        for _ in 0..SUBMIT_AND_WAIT_BATCH_ROUNDS {
            let acc: KeyedGroupCommit<u64, u64> = KeyedGroupCommit::new(1);

            for i in 0..waiters {
                acc.join(black_box(i as u64));
            }
            black_box(acc.rotate_from_to(&1, 2).expect("matching generation"));
            total = total.wrapping_add(acc.complete(&1).len() as u64);
        }
        black_box(total);
    });
}

#[stress(
    tier = 1,
    metadata(component = "singleflight", scenario = "flush_waiters_1")
)]
fn flush_waiters_1(ctx: &mut StressContext) {
    run_flush_waiters(ctx, "flush_waiters_1", 1);
}

#[stress(
    tier = 1,
    metadata(component = "singleflight", scenario = "flush_waiters_4")
)]
fn flush_waiters_4(ctx: &mut StressContext) {
    run_flush_waiters(ctx, "flush_waiters_4", 4);
}

#[stress(
    tier = 1,
    metadata(component = "singleflight", scenario = "flush_waiters_16")
)]
fn flush_waiters_16(ctx: &mut StressContext) {
    run_flush_waiters(ctx, "flush_waiters_16", 16);
}

#[stress(
    tier = 1,
    metadata(component = "singleflight", scenario = "flush_waiters_64")
)]
fn flush_waiters_64(ctx: &mut StressContext) {
    run_flush_waiters(ctx, "flush_waiters_64", 64);
}

#[stress(
    tier = 1,
    metadata(component = "singleflight", scenario = "submit_and_wait_1")
)]
fn submit_and_wait_1(ctx: &mut StressContext) {
    run_submit_and_wait(ctx, "submit_and_wait_1", 1);
}

#[stress(
    tier = 1,
    metadata(component = "singleflight", scenario = "submit_and_wait_4")
)]
fn submit_and_wait_4(ctx: &mut StressContext) {
    run_submit_and_wait(ctx, "submit_and_wait_4", 4);
}

#[stress(
    tier = 1,
    metadata(component = "singleflight", scenario = "submit_and_wait_16")
)]
fn submit_and_wait_16(ctx: &mut StressContext) {
    run_submit_and_wait(ctx, "submit_and_wait_16", 16);
}

#[stress(
    tier = 1,
    metadata(component = "singleflight", scenario = "submit_and_wait_64")
)]
fn submit_and_wait_64(ctx: &mut StressContext) {
    run_submit_and_wait(ctx, "submit_and_wait_64", 64);
}

stress_main!();
