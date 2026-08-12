//! Tier 1 — Iterator Hot Path Benchmarks
//!
//! Covers in-memory skiplist traversal used during scans.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::iterators::skiplist::SkipList;
use cntryl_midge::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};

const ITER_SINGLE_STEP_BATCH_SIZE: usize = 1_048_576;
const RANGE_SCAN_BATCH_SIZE: usize = 512;
const SEEK_WINDOW_COUNT: usize = 64;

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

#[inline(never)]
fn consume_entries(entries: &[(Bytes, Bytes)]) -> usize {
    let mut checksum = entries.len();
    for (key, value) in entries {
        checksum = checksum.wrapping_add(key.len());
        checksum = checksum.wrapping_add(value.len());
        checksum = checksum.wrapping_add(usize::from(key.first().copied().unwrap_or_default()));
        checksum = checksum.wrapping_add(usize::from(value.first().copied().unwrap_or_default()));
    }
    checksum
}

fn run_iter_sequential(ctx: &mut StressContext, scenario: &'static str, count: usize) {
    let sl = create_populated_skiplist(count);
    ctx.parameter("key_count", count);
    ctx.parameter("range_scan_batch_size", RANGE_SCAN_BATCH_SIZE);
    ctx.parameter("logical_unit", "range_scan");

    stress_config::measure_hot_path_batch(ctx, scenario, RANGE_SCAN_BATCH_SIZE as u64, || {
        let mut checksum = 0usize;
        for _ in 0..RANGE_SCAN_BATCH_SIZE {
            let entries = sl.range(None, None);
            checksum = checksum.wrapping_add(consume_entries(black_box(&entries)));
        }
        black_box(checksum);
    });
}

#[stress(
    tier = 1,
    metadata(component = "iterator", scenario = "sequential_10_keys")
)]
fn sequential_10_keys(ctx: &mut StressContext) {
    run_iter_sequential(ctx, "sequential_10_keys", 10);
}

#[stress(
    tier = 1,
    metadata(component = "iterator", scenario = "sequential_50_keys")
)]
fn sequential_50_keys(ctx: &mut StressContext) {
    run_iter_sequential(ctx, "sequential_50_keys", 50);
}

#[stress(
    tier = 1,
    metadata(component = "iterator", scenario = "sequential_100_keys")
)]
fn sequential_100_keys(ctx: &mut StressContext) {
    run_iter_sequential(ctx, "sequential_100_keys", 100);
}

fn run_range(ctx: &mut StressContext, scenario: &'static str, start: usize, end: usize) {
    let sl = create_populated_skiplist(100);
    let start_key = make_key(start);
    let end_key = make_key(end);
    ctx.parameter("scenario", scenario);
    ctx.parameter("range_width", end.saturating_sub(start));
    ctx.parameter("range_scan_batch_size", RANGE_SCAN_BATCH_SIZE);
    ctx.parameter("logical_unit", "range_scan");

    stress_config::measure_hot_path_batch(ctx, scenario, RANGE_SCAN_BATCH_SIZE as u64, || {
        let mut checksum = 0usize;
        for _ in 0..RANGE_SCAN_BATCH_SIZE {
            let entries = sl.range(
                Some(black_box(start_key.as_ref())),
                Some(black_box(end_key.as_ref())),
            );
            checksum = checksum.wrapping_add(consume_entries(black_box(&entries)));
        }
        black_box(checksum);
    });
}

#[stress(
    tier = 1,
    metadata(component = "iterator", scenario = "narrow_20_keys")
)]
fn narrow_20_keys(ctx: &mut StressContext) {
    run_range(ctx, "narrow_20_keys", 40, 60);
}

#[stress(tier = 1, metadata(component = "iterator", scenario = "wide_80_keys"))]
fn wide_80_keys(ctx: &mut StressContext) {
    run_range(ctx, "wide_80_keys", 10, 90);
}

#[stress(
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
    stress_config::mark_validated_micro(ctx, "iterator_seek_next");

    stress_config::measure_hot_path_batch(
        ctx,
        "next_after_seek",
        ITER_SINGLE_STEP_BATCH_SIZE as u64,
        || {
            let mut seen = 0usize;
            for _ in 0..ITER_SINGLE_STEP_BATCH_SIZE {
                let idx = key_index % SEEK_WINDOW_COUNT;
                key_index = key_index.wrapping_add(1);
                let entries = sl.range(
                    Some(black_box(start_keys[idx].as_ref())),
                    Some(black_box(end_keys[idx].as_ref())),
                );
                seen = seen.wrapping_add(consume_entries(black_box(&entries)));
            }
            black_box(seen);
        },
    );
}

#[stress(
    tier = 1,
    role = "diagnostic",
    metadata(component = "iterator", scenario = "range_beginning")
)]
fn range_beginning(ctx: &mut StressContext) {
    stress_config::mark_local_rsd_diagnostic(ctx);
    run_range(ctx, "range_beginning", 0, 10);
}

#[stress(
    tier = 1,
    role = "diagnostic",
    metadata(component = "iterator", scenario = "range_middle")
)]
fn range_middle(ctx: &mut StressContext) {
    stress_config::mark_local_rsd_diagnostic(ctx);
    run_range(ctx, "range_middle", 45, 55);
}

#[stress(
    tier = 1,
    role = "diagnostic",
    metadata(component = "iterator", scenario = "range_end")
)]
fn range_end(ctx: &mut StressContext) {
    stress_config::mark_local_rsd_diagnostic(ctx);
    run_range(ctx, "range_end", 90, 100);
}

#[stress(
    tier = 1,
    metadata(component = "iterator", scenario = "unbounded_50_keys")
)]
fn unbounded_50_keys(ctx: &mut StressContext) {
    run_iter_sequential(ctx, "unbounded_50_keys", 50);
}

#[stress(
    tier = 1,
    role = "diagnostic",
    metadata(component = "iterator", scenario = "bounded_50_keys")
)]
fn bounded_50_keys(ctx: &mut StressContext) {
    stress_config::mark_local_rsd_diagnostic(ctx);
    run_range(ctx, "bounded_50_keys", 0, 50);
}

stress_main!();
