//! Tier 1 — Iterator Hot Path Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers iterator hot paths (operations that occur per-key during scans):
//! - Sequential iteration over memtable (iter_all)
//! - Range scan with bounds (start/end keys)
//! - Iterator seek operation
//!
//! These are pure in-memory skiplist traversals that happen on every
//! key during range scans, making them critical hot paths.

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::iterators::skiplist::SkipList;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Pre-compute a key (deterministic, no allocation in hot path)
#[inline]
fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

/// Pre-compute value of given size
fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// Create a populated skiplist for iteration benchmarks
fn create_populated_skiplist(count: usize) -> SkipList {
    let sl = SkipList::new();
    let value = make_value(64);
    
    for i in 0..count {
        let key = make_key(i);
        sl.upsert(key, Some(value.clone()), i as u64);
    }
    
    sl
}

// ─── Sequential Iteration Benchmarks ─────────────────────────────────────────

/// Benchmark sequential iteration over entire skiplist (hot path for full scans)
fn bench_iter_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator/sequential");
    group.sampling_mode(SamplingMode::Flat);

    // Small dataset for tier-1 speed
    for &count in &[10, 50, 100] {
        let sl = create_populated_skiplist(count);
        
        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(format!("{}_keys", count), |b| {
            b.iter(|| {
                // Simulate sequential iteration by collecting all entries
                let entries = sl.range(None, None);
                black_box(entries.len());
            })
        });
    }

    group.finish();
}

/// Benchmark range scan with bounds (hot path for bounded queries)
fn bench_range_bounded(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator/range_bounded");
    group.sampling_mode(SamplingMode::Flat);

    // Pre-populate with 100 keys
    let sl = create_populated_skiplist(100);
    
    // Precompute range bounds (avoid allocation in hot path)
    let start_key_narrow = make_key(40);
    let end_key_narrow = make_key(60);
    
    let start_key_wide = make_key(10);
    let end_key_wide = make_key(90);

    // Narrow range (20 keys)
    group.throughput(Throughput::Elements(20));
    group.bench_function("narrow_20_keys", |b| {
        b.iter(|| {
            let entries = sl.range(
                Some(black_box(start_key_narrow.as_ref())),
                Some(black_box(end_key_narrow.as_ref())),
            );
            black_box(entries.len());
        })
    });

    // Wide range (80 keys)
    group.throughput(Throughput::Elements(80));
    group.bench_function("wide_80_keys", |b| {
        b.iter(|| {
            let entries = sl.range(
                Some(black_box(start_key_wide.as_ref())),
                Some(black_box(end_key_wide.as_ref())),
            );
            black_box(entries.len());
        })
    });

    group.finish();
}

/// Benchmark single-step iteration (per-key cost during scan)
fn bench_iter_single_step(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator/single_step");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let sl = create_populated_skiplist(100);
    
    // Precompute start key
    let start_key = make_key(50);

    // Benchmark cost of getting next single entry after seek
    group.bench_function("next_after_seek", |b| {
        b.iter(|| {
            // Seek to position, then get one entry
            let entries = sl.range(Some(black_box(start_key.as_ref())), Some(black_box(&make_key(51))));
            black_box(entries.first());
        })
    });

    group.finish();
}

/// Benchmark range scan at different positions (beginning, middle, end)
fn bench_range_position(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator/range_position");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    let sl = create_populated_skiplist(100);
    
    // Precompute range keys
    let start_beginning = make_key(0);
    let end_beginning = make_key(10);
    
    let start_middle = make_key(45);
    let end_middle = make_key(55);
    
    let start_end = make_key(90);
    let end_end = make_key(100);

    group.bench_function("beginning", |b| {
        b.iter(|| {
            let entries = sl.range(
                Some(black_box(start_beginning.as_ref())),
                Some(black_box(end_beginning.as_ref())),
            );
            black_box(entries.len());
        })
    });

    group.bench_function("middle", |b| {
        b.iter(|| {
            let entries = sl.range(
                Some(black_box(start_middle.as_ref())),
                Some(black_box(end_middle.as_ref())),
            );
            black_box(entries.len());
        })
    });

    group.bench_function("end", |b| {
        b.iter(|| {
            let entries = sl.range(
                Some(black_box(start_end.as_ref())),
                Some(black_box(end_end.as_ref())),
            );
            black_box(entries.len());
        })
    });

    group.finish();
}

/// Benchmark unbounded vs bounded range scans
fn bench_range_bounds_vs_unbounded(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterator/bounds_overhead");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50));

    let sl = create_populated_skiplist(50);
    
    let start_key = make_key(0);
    let end_key = make_key(50);

    group.bench_function("unbounded", |b| {
        b.iter(|| {
            let entries = sl.range(None, None);
            black_box(entries.len());
        })
    });

    group.bench_function("bounded", |b| {
        b.iter(|| {
            let entries = sl.range(
                Some(black_box(start_key.as_ref())),
                Some(black_box(end_key.as_ref())),
            );
            black_box(entries.len());
        })
    });

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier1_hotpath_iterator;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets =
        bench_iter_sequential,
        bench_range_bounded,
        bench_iter_single_step,
        bench_range_position,
        bench_range_bounds_vs_unbounded
}
criterion_main!(tier1_hotpath_iterator);
