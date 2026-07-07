//! Tier 2 — Event Loop Coordination Benchmarks
//!
//! Measures cross-thread channel and park/wake coordination overhead.

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use crossbeam::channel;
#[path = "./bench_support/event_loop.rs"]
mod event_loop_support;
use event_loop_support::{handle, MessageKind};

const CHANNEL_CROSS_THREAD_MESSAGES: usize = 4_194_304;
const CHANNEL_CROSS_THREAD_OPS: u64 = 4_194_304;
const PARK_WAKE_MESSAGES: usize = 32_768;
const PARK_WAKE_OPS: u64 = 32_768;
const DIRECT_INNER_LOOPS: u64 = 1_000_000;

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

#[stress(tier = 2, metadata(component = "event_loop", scenario = "direct_call"))]
fn direct_call(ctx: &mut StressContext) {
    ctx.parameter("inner_loops", DIRECT_INNER_LOOPS);

    let _completed = ctx.measure_batch("direct_call", DIRECT_INNER_LOOPS, || {
        let mut counter = 0u64;
        for _ in 0..DIRECT_INNER_LOOPS {
            handle(&mut counter);
            black_box(counter);
        }
        black_box(counter);
    });
}

#[stress(
    tier = 2,
    metadata(component = "event_loop", scenario = "channel_cross_thread")
)]
fn channel_cross_thread(ctx: &mut StressContext) {
    let messages = build_messages(CHANNEL_CROSS_THREAD_MESSAGES);
    let message_count = messages.len();
    ctx.parameter("message_count", message_count);

    let _completed = ctx.measure_batch("channel_cross_thread", CHANNEL_CROSS_THREAD_OPS, || {
        let (msg_tx, msg_rx) = channel::bounded(1024);
        let (start_tx, start_rx) = channel::bounded(1);
        let (done_tx, done_rx) = channel::bounded(1);

        let consumer = std::thread::spawn(move || {
            let mut counter = 0u64;
            let _ = start_rx.recv();
            for _ in 0..message_count {
                if let Ok(kind) = msg_rx.recv() {
                    dispatch_message(kind, &mut counter);
                }
            }
            let _ = done_tx.send(counter);
        });

        let _ = start_tx.send(());
        for kind in &messages {
            let _ = msg_tx.send(black_box(*kind));
        }
        let _ = done_rx.recv();
        let _ = consumer.join();
    });
}

#[stress(tier = 2, metadata(component = "event_loop", scenario = "park_wake"))]
fn park_wake(ctx: &mut StressContext) {
    let messages = build_messages(PARK_WAKE_MESSAGES);
    let message_count = messages.len();
    ctx.parameter("message_count", message_count);

    let _completed = ctx.measure_batch("park_wake", PARK_WAKE_OPS, || {
        let (msg_tx, msg_rx) = channel::bounded(0);
        let (start_tx, start_rx) = channel::bounded(1);
        let (done_tx, done_rx) = channel::bounded(1);

        let consumer = std::thread::spawn(move || {
            let mut counter = 0u64;
            let _ = start_rx.recv();
            for _ in 0..message_count {
                if let Ok(kind) = msg_rx.recv() {
                    dispatch_message(kind, &mut counter);
                }
            }
            let _ = done_tx.send(counter);
        });

        let _ = start_tx.send(());
        for kind in &messages {
            let _ = msg_tx.send(black_box(*kind));
        }
        let _ = done_rx.recv();
        let _ = consumer.join();
    });
}

stress_main!();
