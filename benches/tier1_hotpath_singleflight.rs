//! Tier 1 — Singleflight/Accumulator hot path benchmarks
//!
//! Measures the generic "accumulate → flush once → notify many waiters" primitive.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::common::Accumulator;
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

const SUBMIT_AND_WAIT_BATCH_ROUNDS: usize = 16;

cntryl_stress::stress_allocator!();

fn run_flush_waiters(ctx: &mut StressContext, waiters: usize) {
    ctx.parameter("waiters", waiters);

    ctx.measure_micro(|| {
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

fn run_submit_and_wait(ctx: &mut StressContext, waiters: usize) {
    let logical_ops = (waiters * SUBMIT_AND_WAIT_BATCH_ROUNDS) as u64;
    ctx.parameter("waiters", waiters);
    ctx.parameter("rounds", SUBMIT_AND_WAIT_BATCH_ROUNDS);

    stress_config::measure_micro_batch(ctx, logical_ops, || {
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

#[stress_test(
    tier = 1,
    metadata(component = "singleflight", scenario = "flush_waiters_1")
)]
fn flush_waiters_1(ctx: &mut StressContext) {
    run_flush_waiters(ctx, 1);
}

#[stress_test(
    tier = 1,
    metadata(component = "singleflight", scenario = "flush_waiters_4")
)]
fn flush_waiters_4(ctx: &mut StressContext) {
    run_flush_waiters(ctx, 4);
}

#[stress_test(
    tier = 1,
    metadata(component = "singleflight", scenario = "flush_waiters_16")
)]
fn flush_waiters_16(ctx: &mut StressContext) {
    run_flush_waiters(ctx, 16);
}

#[stress_test(
    tier = 1,
    metadata(component = "singleflight", scenario = "flush_waiters_64")
)]
fn flush_waiters_64(ctx: &mut StressContext) {
    run_flush_waiters(ctx, 64);
}

#[stress_test(
    tier = 1,
    metadata(component = "singleflight", scenario = "submit_and_wait_1")
)]
fn submit_and_wait_1(ctx: &mut StressContext) {
    run_submit_and_wait(ctx, 1);
}

#[stress_test(
    tier = 1,
    metadata(component = "singleflight", scenario = "submit_and_wait_4")
)]
fn submit_and_wait_4(ctx: &mut StressContext) {
    run_submit_and_wait(ctx, 4);
}

#[stress_test(
    tier = 1,
    metadata(component = "singleflight", scenario = "submit_and_wait_16")
)]
fn submit_and_wait_16(ctx: &mut StressContext) {
    run_submit_and_wait(ctx, 16);
}

#[stress_test(
    tier = 1,
    metadata(component = "singleflight", scenario = "submit_and_wait_64")
)]
fn submit_and_wait_64(ctx: &mut StressContext) {
    run_submit_and_wait(ctx, 64);
}

stress_main!();
