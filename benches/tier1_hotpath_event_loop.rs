//! Tier 1 — Event Loop Microbenchmarks
//!
//! Measures pure dispatch overhead without coordination or blocking.

#[path = "./bench_support/event_loop.rs"]
mod event_loop_support;
#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use event_loop_support::{handle, MessageKind};
use std::collections::VecDeque;

const MESSAGE_BATCH_SIZE: usize = 128;
const MESSAGE_BATCH_REPEATS: usize = 4096;
const DIRECT_CALLS_PER_BATCH: usize = 256;
const DIRECT_BATCH_REPEATS: usize = 4096;

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

#[stress(tier = 1, metadata(component = "event_loop", scenario = "direct_call"))]
fn direct_call(ctx: &mut StressContext) {
    ctx.parameter("calls_per_logical_operation", DIRECT_CALLS_PER_BATCH);
    ctx.parameter("batch_repeats", DIRECT_BATCH_REPEATS);
    ctx.parameter("logical_unit", "direct_call_batch");

    stress_config::measure_hot_path_batch(
        ctx,
        "direct_call",
        u64::try_from(DIRECT_BATCH_REPEATS).expect("batch count fits in u64"),
        || {
            let mut counter = 0u64;
            for _ in 0..DIRECT_BATCH_REPEATS {
                for _ in 0..DIRECT_CALLS_PER_BATCH {
                    handle(black_box(&mut counter));
                }
                black_box(counter);
            }
            black_box(counter);
        },
    );
}

fn run_message_batches(messages: &[MessageKind]) -> u64 {
    let mut counter = 0u64;
    for _ in 0..MESSAGE_BATCH_REPEATS {
        for kind in messages {
            dispatch_message(black_box(*kind), &mut counter);
        }
        black_box(counter);
    }
    counter
}

#[stress(
    tier = 1,
    metadata(component = "event_loop", scenario = "dispatch_only")
)]
fn dispatch_only(ctx: &mut StressContext) {
    let messages = build_messages(MESSAGE_BATCH_SIZE);
    ctx.parameter("messages_per_logical_operation", MESSAGE_BATCH_SIZE);
    ctx.parameter("batch_repeats", MESSAGE_BATCH_REPEATS);
    ctx.parameter("logical_unit", "message_batch");

    stress_config::measure_hot_path_batch(
        ctx,
        "dispatch_only",
        u64::try_from(MESSAGE_BATCH_REPEATS).expect("batch count fits in u64"),
        || {
            let counter = run_message_batches(&messages);
            black_box(counter);
        },
    );
}

fn run_mailbox_batches(messages: &[MessageKind], queue: &mut VecDeque<MessageKind>) -> u64 {
    let mut counter = 0u64;
    for _ in 0..MESSAGE_BATCH_REPEATS {
        queue.clear();
        for kind in messages {
            queue.push_back(*kind);
        }

        while let Some(kind) = queue.pop_front() {
            dispatch_message(black_box(kind), &mut counter);
        }
        black_box(counter);
    }
    counter
}

#[stress(
    tier = 1,
    metadata(component = "event_loop", scenario = "mailbox_vecdeque")
)]
fn mailbox_vecdeque(ctx: &mut StressContext) {
    let messages = build_messages(MESSAGE_BATCH_SIZE);
    let mut queue = VecDeque::with_capacity(messages.len());
    ctx.parameter("messages_per_logical_operation", MESSAGE_BATCH_SIZE);
    ctx.parameter("batch_repeats", MESSAGE_BATCH_REPEATS);
    ctx.parameter("logical_unit", "mailbox_batch");

    stress_config::measure_hot_path_batch(
        ctx,
        "mailbox_vecdeque",
        u64::try_from(MESSAGE_BATCH_REPEATS).expect("batch count fits in u64"),
        || {
            let counter = run_mailbox_batches(&messages, &mut queue);
            black_box(counter);
        },
    );
}

stress_main!();
