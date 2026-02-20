//! Tier 2 — Event Loop Microbenchmarks (Coordination Cost)
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Measures coordination overhead (cross-thread channel and park/wake).

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::handler::handle;
use cntryl_midge::message::MessageKind;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier2;
use crossbeam::channel;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

const INNER_LOOPS: u64 = 64;
const DIRECT_INNER_LOOPS: u64 = 1_000_000;

#[inline]
fn dispatch_message(kind: MessageKind, counter: &mut u64) {
    match kind {
        MessageKind::Noop => handle(counter),
        MessageKind::StartupPing => handle(counter),
        MessageKind::GetRuntimeConfig => handle(counter),
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

fn record_ops(
    results: &RefCell<HashMap<&'static str, f64>>,
    name: &'static str,
    ops: u64,
    dur: std::time::Duration,
) {
    let ops_per_sec = ops as f64 / dur.as_secs_f64();
    results.borrow_mut().insert(name, ops_per_sec);
}

fn print_table(results: &RefCell<HashMap<&'static str, f64>>) {
    let results = results.borrow();
    let Some(&baseline) = results.get("direct_call") else {
        return;
    };

    println!("Bench name | ops/sec | relative to direct call");
    for name in ["direct_call", "channel_cross_thread", "park_wake"] {
        if let Some(&ops) = results.get(name) {
            let relative = ops / baseline;
            println!("{name} | {:.0} | {:.2}x", ops, relative);
        }
    }
}

fn bench_tier2(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_loop/tier2");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let results: RefCell<HashMap<&'static str, f64>> = RefCell::new(HashMap::new());

    group.bench_function("direct_call", |b| {
        b.iter_custom(|iters| {
            let iters = iters.max(1);
            let mut counter = 0u64;
            let start = Instant::now();
            let ops = iters.saturating_mul(DIRECT_INNER_LOOPS);
            for _ in 0..ops {
                handle(&mut counter);
                black_box(counter);
            }
            let mut duration = start.elapsed();
            if duration.is_zero() {
                duration = std::time::Duration::from_nanos(1);
            }
            black_box(counter);
            record_ops(&results, "direct_call", ops, duration);
            duration
        })
    });

    group.bench_function("channel_cross_thread", |b| {
        b.iter_custom(|iters| {
            let iters = iters.max(1);
            let ops = iters.saturating_mul(INNER_LOOPS);
            let messages = build_messages(ops as usize);
            let message_count = messages.len();
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

            let start = Instant::now();
            let _ = start_tx.send(());
            for kind in messages {
                let _ = msg_tx.send(black_box(kind));
            }
            let _ = done_rx.recv();
            let duration = start.elapsed();

            let _ = consumer.join();
            record_ops(&results, "channel_cross_thread", ops, duration);
            duration
        })
    });

    group.bench_function("park_wake", |b| {
        b.iter_custom(|iters| {
            let iters = iters.max(1);
            let ops = iters.saturating_mul(INNER_LOOPS);
            let messages = build_messages(ops as usize);
            let message_count = messages.len();
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

            let start = Instant::now();
            let _ = start_tx.send(());
            for kind in messages {
                let _ = msg_tx.send(black_box(kind));
            }
            let _ = done_rx.recv();
            let duration = start.elapsed();

            let _ = consumer.join();
            record_ops(&results, "park_wake", ops, duration);
            duration
        })
    });

    group.finish();
    print_table(&results);
}

criterion_group!(
    name = tier2_subsystem_event_loop;
    config = criterion_config_for_tier2();
    targets = bench_tier2
);
criterion_main!(tier2_subsystem_event_loop);
