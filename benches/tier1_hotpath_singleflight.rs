//! Tier 1 — Singleflight/Accumulator hot path benchmarks
//!
//! Measures the generic "accumulate → flush once → notify many waiters" primitive.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::common::Accumulator;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const SUBMIT_AND_WAIT_BATCH_ROUNDS: usize = 64;

fn run_flush_waiters(ctx: &mut StressContext, scenario: &'static str, waiters: usize) {
    ctx.parameter("waiters", waiters);

    ctx.measure(scenario, || {
        let acc: Accumulator<u64, u64> = Accumulator::new();
        let mut receivers = Vec::with_capacity(waiters);
        for i in 0..waiters {
            receivers.push(acc.submit_async(i as u64));
        }

        let ran = acc.flush_now(|batch| batch.len() as u64);
        for receiver in receivers {
            black_box(receiver.recv());
        }
        black_box(ran);
    });
}

fn run_submit_and_wait(ctx: &mut StressContext, scenario: &'static str, waiters: usize) {
    let logical_ops = (waiters * SUBMIT_AND_WAIT_BATCH_ROUNDS) as u64;
    ctx.parameter("waiters", waiters);
    ctx.parameter("rounds", SUBMIT_AND_WAIT_BATCH_ROUNDS);

    stress_config::measure_hot_path_batch(ctx, scenario, logical_ops, || {
        let mut total = 0u64;
        for _ in 0..SUBMIT_AND_WAIT_BATCH_ROUNDS {
            let acc: Accumulator<u64, u64> = Accumulator::new();
            let mut receivers = Vec::with_capacity(waiters);

            for i in 0..waiters {
                receivers.push(acc.submit_async(black_box(i as u64)));
            }

            let ran = acc.flush_now(|batch| batch.len() as u64);
            for receiver in receivers {
                black_box(receiver.recv());
            }
            total = total.wrapping_add(ran);
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
