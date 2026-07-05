//! Tier 1 — Iterator Hot Path Benchmarks
//!
//! Covers in-memory skiplist traversal used during scans.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::iterators::skiplist::SkipList;
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

const ITER_SINGLE_STEP_BATCH_SIZE: usize = 16_384;
const SEEK_WINDOW_COUNT: usize = 64;

cntryl_stress::stress_allocator!();

#[inline]
fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{i:010}"))
}

fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn create_populated_skiplist(count: usize) -> SkipList {
    let sl = SkipList::new();
    let value = make_value(64);

    for i in 0..count {
        let key = make_key(i);
        sl.upsert(key, Some(value.clone()), i as u64);
    }

    sl
}

fn run_iter_sequential(ctx: &mut StressContext, count: usize) {
    let sl = create_populated_skiplist(count);
    ctx.parameter("key_count", count);

    stress_config::measure_micro_batch(ctx, count as u64, || {
        let entries = sl.range(None, None);
        black_box(entries.len());
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "iterator", scenario = "sequential_10_keys")
)]
fn sequential_10_keys(ctx: &mut StressContext) {
    run_iter_sequential(ctx, 10);
}

#[stress_test(
    tier = 1,
    metadata(component = "iterator", scenario = "sequential_50_keys")
)]
fn sequential_50_keys(ctx: &mut StressContext) {
    run_iter_sequential(ctx, 50);
}

#[stress_test(
    tier = 1,
    metadata(component = "iterator", scenario = "sequential_100_keys")
)]
fn sequential_100_keys(ctx: &mut StressContext) {
    run_iter_sequential(ctx, 100);
}

fn run_range(ctx: &mut StressContext, scenario: &'static str, start: usize, end: usize) {
    let sl = create_populated_skiplist(100);
    let start_key = make_key(start);
    let end_key = make_key(end);
    ctx.parameter("scenario", scenario);
    ctx.parameter("range_width", end.saturating_sub(start));

    stress_config::measure_micro_batch(ctx, end.saturating_sub(start) as u64, || {
        let entries = sl.range(
            Some(black_box(start_key.as_ref())),
            Some(black_box(end_key.as_ref())),
        );
        black_box(entries.len());
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "iterator", scenario = "narrow_20_keys")
)]
fn narrow_20_keys(ctx: &mut StressContext) {
    run_range(ctx, "narrow_20_keys", 40, 60);
}

#[stress_test(tier = 1, metadata(component = "iterator", scenario = "wide_80_keys"))]
fn wide_80_keys(ctx: &mut StressContext) {
    run_range(ctx, "wide_80_keys", 10, 90);
}

#[stress_test(
    tier = 1,
    metadata(component = "iterator", scenario = "next_after_seek")
)]
fn next_after_seek(ctx: &mut StressContext) {
    let sl = create_populated_skiplist(256);
    let start_keys: Vec<Bytes> = (64..64 + SEEK_WINDOW_COUNT).map(make_key).collect();
    let end_keys: Vec<Bytes> = (65..65 + SEEK_WINDOW_COUNT).map(make_key).collect();
    let mut key_index = 0usize;
    ctx.parameter("batch_size", ITER_SINGLE_STEP_BATCH_SIZE);
    ctx.parameter("seek_windows", SEEK_WINDOW_COUNT);

    stress_config::measure_micro_batch(ctx, ITER_SINGLE_STEP_BATCH_SIZE as u64, || {
        let mut seen = 0usize;
        for _ in 0..ITER_SINGLE_STEP_BATCH_SIZE {
            let idx = key_index % SEEK_WINDOW_COUNT;
            key_index = key_index.wrapping_add(1);
            let entries = sl.range(
                Some(black_box(start_keys[idx].as_ref())),
                Some(black_box(end_keys[idx].as_ref())),
            );
            seen += usize::from(!entries.is_empty());
        }
        black_box(seen);
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "iterator", scenario = "range_beginning")
)]
fn range_beginning(ctx: &mut StressContext) {
    run_range(ctx, "beginning", 0, 10);
}

#[stress_test(tier = 1, metadata(component = "iterator", scenario = "range_middle"))]
fn range_middle(ctx: &mut StressContext) {
    run_range(ctx, "middle", 45, 55);
}

#[stress_test(tier = 1, metadata(component = "iterator", scenario = "range_end"))]
fn range_end(ctx: &mut StressContext) {
    run_range(ctx, "end", 90, 100);
}

#[stress_test(
    tier = 1,
    metadata(component = "iterator", scenario = "unbounded_50_keys")
)]
fn unbounded_50_keys(ctx: &mut StressContext) {
    let sl = create_populated_skiplist(50);
    ctx.parameter("key_count", 50);

    stress_config::measure_micro_batch(ctx, 50, || {
        let entries = sl.range(None, None);
        black_box(entries.len());
    });
}

#[stress_test(
    tier = 1,
    metadata(component = "iterator", scenario = "bounded_50_keys")
)]
fn bounded_50_keys(ctx: &mut StressContext) {
    run_range(ctx, "bounded", 0, 50);
}

stress_main!();
