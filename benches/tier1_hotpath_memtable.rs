//! Tier 1 — Memtable Hot Path Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical memtable hot paths:
//! - Insert operations (single and batch, various value sizes)
//! - Point lookups (hit/miss)

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::sst::{SkipListMemtable, Memtable};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Pre-compute a key (deterministic, no allocation in hot path)
#[inline]
fn make_key(i: usize) -> Vec<u8> {
    format!("key_{:010}", i).into_bytes()
}

/// Pre-compute value of given size
fn make_value(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

fn make_value_indexed(i: usize) -> Vec<u8> {
    format!("value_{}", i).into_bytes()
}

// ─── Insert Benchmarks ───────────────────────────────────────────────────────

/// Benchmark single key-value insertion into a warm memtable
fn bench_put_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/put_single");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Pre-create keys and values outside measurement
    let keys: Vec<Vec<u8>> = (0..1000).map(make_key).collect();
    let small_val = make_value(64);
    let medium_val = make_value(1024);
    let large_val = make_value(4096);

    // Warm memtable - pre-populated with some data
    let memtable = SkipListMemtable::new();
    for key in keys.iter().take(100) {
        let _ = memtable.put(key.clone(), small_val.clone());
    }

    let mut counter = 100usize;

    group.bench_function("64b_value", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            let _ = memtable.put(black_box(keys[idx].clone()), black_box(small_val.clone()));
        })
    });

    group.bench_function("1kb_value", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            let _ = memtable.put(black_box(keys[idx].clone()), black_box(medium_val.clone()));
        })
    });

    group.bench_function("4kb_value", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            let _ = memtable.put(black_box(keys[idx].clone()), black_box(large_val.clone()));
        })
    });

    group.finish();
}

/// Benchmark sequential insertions (batch pattern)
fn bench_put_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/put_batch");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    // Pre-create keys and values
    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(128);

    group.bench_function("100_inserts", |b| {
        b.iter(|| {
            let memtable = SkipListMemtable::new();
            for key in &keys {
                let _ = memtable.put(black_box(key.clone()), black_box(value.clone()));
            }
            black_box(memtable)
        })
    });

    group.finish();
}

// ─── Lookup Benchmarks ───────────────────────────────────────────────────────

/// Benchmark point lookup (hit and miss)
fn bench_get_point(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/get_point");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    // Pre-compute all keys outside benchmark
    let keys: Vec<Vec<u8>> = (0..1000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..1000).map(make_value_indexed).collect();

    let memtable = SkipListMemtable::new();
    for i in 0..1000 {
        let _ = memtable.put(keys[i].clone(), values[i].clone());
    }

    let hit_key = keys[500].clone();
    let miss_key = make_key(2000);

    group.bench_function("hit", |b| {
        b.iter(|| {
            let result = memtable.get(black_box(&hit_key));
            black_box(result)
        })
    });

    group.bench_function("miss", |b| {
        b.iter(|| {
            let result = memtable.get(black_box(&miss_key));
            black_box(result)
        })
    });

    group.finish();
}

/// Benchmark delete operations
fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/delete");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let keys: Vec<Vec<u8>> = (0..1000).map(make_key).collect();
    let value = make_value(128);

    // Warm memtable
    let memtable = SkipListMemtable::new();
    for key in &keys {
        let _ = memtable.put(key.clone(), value.clone());
    }

    let mut counter = 0usize;

    group.bench_function("delete", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            let _ = memtable.delete(black_box(keys[idx].clone()));
        })
    });

    group.finish();
}

// ─── Size Benchmark ─────────────────────────────────────────────────────────

/// Benchmark memtable size tracking
fn bench_size_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/size_bytes");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(1024);

    let memtable = SkipListMemtable::new();
    for key in &keys {
        let _ = memtable.put(key.clone(), value.clone());
    }

    group.bench_function("size_query", |b| {
        b.iter(|| black_box(memtable.size_bytes()))
    });

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier1_hotpath_memtable;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets =
        bench_put_single,
        bench_put_batch,
        bench_get_point,
        bench_delete,
        bench_size_bytes
}
criterion_main!(tier1_hotpath_memtable);
