//! Tier 1 — Singleflight/Accumulator hot path benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Measures the core primitive used for "accumulate → flush once → notify many waiters".
//! This is intentionally not WAL-specific; it benchmarks the generic fan-out flush.

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::common::Accumulator;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier1;
use std::hint::black_box;

const SUBMIT_AND_WAIT_BATCH_ROUNDS: usize = 16;

fn bench_singleflight_flush_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_singleflight_flush_fanout");
    group.sampling_mode(SamplingMode::Flat);

    // Fan-out sizes that are realistic for "coalesce concurrent single puts".
    for &waiters in &[1usize, 4, 16, 64] {
        group.throughput(Throughput::Elements(waiters as u64));

        group.bench_function(format!("flush_waiters_{waiters}"), |b| {
            b.iter_batched(
                || {
                    // Setup (not timed): create a generation with N waiters.
                    let acc: Accumulator<u64, u64> = Accumulator::new();
                    let mut receivers = Vec::with_capacity(waiters);
                    for i in 0..waiters {
                        // Each submit creates a waiter; setup cost intentionally excluded.
                        let rx = acc.submit_async(i as u64);
                        receivers.push(rx);
                    }
                    (acc, receivers)
                },
                |(acc, receivers)| {
                    // Timed: perform exactly one flush and fan-out the result.
                    let ran = acc.flush_now(|batch| batch.len() as u64);

                    // Consume receivers so Criterion measures the full fan-out cost
                    // (including wakeups) and doesn't allow the optimizer to lie.
                    for r in receivers {
                        black_box(r.recv());
                    }
                    black_box(ran)
                },
                BatchSize::SmallInput,
            )
        });

        // Optional: includes submit_async + flush + wait, mapping closely to the
        // "submit then block until flush completes" call path.
        group.throughput(Throughput::Elements(
            (waiters * SUBMIT_AND_WAIT_BATCH_ROUNDS) as u64,
        ));
        group.bench_function(format!("submit_and_wait_{waiters}"), |b| {
            b.iter(|| {
                let mut total = 0u64;
                for _ in 0..SUBMIT_AND_WAIT_BATCH_ROUNDS {
                    let acc: Accumulator<u64, u64> = Accumulator::new();
                    let mut receivers = Vec::with_capacity(waiters);

                    for i in 0..waiters {
                        receivers.push(acc.submit_async(black_box(i as u64)));
                    }

                    let ran = acc.flush_now(|batch| batch.len() as u64);

                    for r in receivers {
                        black_box(r.recv());
                    }

                    total = total.wrapping_add(ran);
                }
                black_box(total)
            })
        });
    }

    group.finish();
}

criterion_group!(
    name = tier1_hotpath_singleflight;
    config = criterion_config_for_tier1();
    targets = bench_singleflight_flush_fanout
);
criterion_main!(tier1_hotpath_singleflight);
