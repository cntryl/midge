//! Tier 1 — Event Loop Microbenchmarks
//!
//! Measures pure dispatch overhead without coordination or blocking.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::handler::handle;
use cntryl_midge::message::MessageKind;
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};
use std::collections::VecDeque;

const INNER_LOOPS: usize = 128;
const INNER_LOOP_OPS: u64 = 128;
const DIRECT_INNER_LOOPS: u64 = 1_000_000;

cntryl_stress::stress_allocator!();

#[inline]
fn dispatch_message(kind: MessageKind, counter: &mut u64) {
    match kind {
        MessageKind::Noop | MessageKind::StartupPing | MessageKind::GetRuntimeConfig => {
            handle(counter);
        }
    }
}

fn build_messages(count: usize) -> Vec<MessageKind> {
    let mut messages = Vec::with_capacity(count);
    for i in 0..count {
        let kind = match i % 3 {
            0 => MessageKind::Noop,
            1 => MessageKind::StartupPing,
            _ => MessageKind::GetRuntimeConfig,
        };
        messages.push(kind);
    }
    messages
}

#[stress_test(tier = 1, metadata(component = "event_loop", scenario = "direct_call"))]
fn direct_call(ctx: &mut StressContext) {
    ctx.parameter("inner_loops", DIRECT_INNER_LOOPS);

    stress_config::measure_micro_batch(ctx, DIRECT_INNER_LOOPS, || {
        let mut counter = 0u64;
        for _ in 0..DIRECT_INNER_LOOPS {
            handle(&mut counter);
            black_box(counter);
        }
        black_box(counter);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "event_loop", scenario = "dispatch_only")
)]
fn dispatch_only(ctx: &mut StressContext) {
    let messages = build_messages(INNER_LOOPS);
    ctx.parameter("inner_loops", INNER_LOOPS);

    stress_config::measure_micro_batch(ctx, INNER_LOOP_OPS, || {
        let mut counter = 0u64;
        for kind in &messages {
            dispatch_message(black_box(*kind), &mut counter);
        }
        black_box(counter);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "event_loop", scenario = "mailbox_vecdeque")
)]
fn mailbox_vecdeque(ctx: &mut StressContext) {
    let messages = build_messages(INNER_LOOPS);
    ctx.parameter("inner_loops", INNER_LOOPS);

    stress_config::measure_micro_batch(ctx, INNER_LOOP_OPS, || {
        let mut queue = VecDeque::with_capacity(messages.len());
        for kind in &messages {
            queue.push_back(*kind);
        }

        let mut counter = 0u64;
        while let Some(kind) = queue.pop_front() {
            dispatch_message(black_box(kind), &mut counter);
        }
        black_box(counter);
    });
}

stress_main!();
