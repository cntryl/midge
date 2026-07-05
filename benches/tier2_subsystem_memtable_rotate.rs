//! Tier 2 — Memtable Rotate Benchmarks
//!
//! Measures fill + drain cycle cost for small and large memtables.

use cntryl_midge::sst::SkipListMemtable;
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const PUT_1KB_BATCH_SIZE: usize = 4_096;
const PUT_4KB_BATCH_SIZE: usize = 65_536;

fn make_kv_pairs(count: usize) -> Vec<(Bytes, Bytes)> {
    (0..count)
        .map(|i| {
            (
                Bytes::from(format!("key_{i:010}")),
                Bytes::from(format!("value_{i:010}")),
            )
        })
        .collect()
}

fn make_value(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

fn make_fixed_value_pairs(count: usize, value_size: usize) -> Vec<(Bytes, Bytes)> {
    let value = Bytes::from(make_value(value_size));
    (0..count)
        .map(|i| (Bytes::from(format!("put_key_{i:010}")), value.clone()))
        .collect()
}

fn run_memtable_rotate(ctx: &mut StressContext, scenario: &'static str, count: usize) {
    let kv_pairs = make_kv_pairs(count);
    ctx.parameter("entry_count", count);

    let _completed = ctx.measure_batch(scenario, count as u64, || {
        let memtable = SkipListMemtable::new();
        for (seq, (key, value)) in kv_pairs.iter().enumerate() {
            memtable
                .put_bytes_with_seq(key.clone(), value.clone(), seq as u64 + 1, None)
                .unwrap();
        }
        black_box(memtable.iter_all(u64::MAX));
    });
}

#[stress(tier = 2, metadata(component = "memtable", scenario = "rotate_small"))]
fn rotate_small(ctx: &mut StressContext) {
    run_memtable_rotate(ctx, "rotate_small", 1_000);
}

#[stress(tier = 2, metadata(component = "memtable", scenario = "rotate_large"))]
fn rotate_large(ctx: &mut StressContext) {
    run_memtable_rotate(ctx, "rotate_large", 10_000);
}

fn run_put_value_size(ctx: &mut StressContext, scenario: &'static str, value_size: usize) {
    let batch_size = if value_size >= 4096 {
        PUT_4KB_BATCH_SIZE
    } else {
        PUT_1KB_BATCH_SIZE
    };
    let kv_pairs = make_fixed_value_pairs(batch_size, value_size);
    ctx.parameter("scenario", scenario);
    ctx.parameter("entry_count", batch_size);
    ctx.parameter("value_size", value_size);

    let _completed = ctx.measure_batch(scenario, batch_size as u64, || {
        let memtable = SkipListMemtable::new();
        for (seq, (key, value)) in kv_pairs.iter().enumerate() {
            memtable
                .put_bytes_with_seq(
                    black_box(key.clone()),
                    black_box(value.clone()),
                    seq as u64 + 1,
                    None,
                )
                .unwrap();
        }
        black_box(memtable);
    });
}

#[stress(
    tier = 2,
    metadata(component = "memtable", scenario = "put_single_1kb")
)]
fn put_single_1kb(ctx: &mut StressContext) {
    run_put_value_size(ctx, "put_single_1kb", 1024);
}

#[stress(
    tier = 2,
    metadata(component = "memtable", scenario = "put_single_4kb")
)]
fn put_single_4kb(ctx: &mut StressContext) {
    run_put_value_size(ctx, "put_single_4kb", 4096);
}

stress_main!();
