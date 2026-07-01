//! Tier 1 — Memtable Hot Path Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical memtable hot paths:
//! - Insert operations (single and batch, various value sizes)
//! - Point lookups (hit/miss)

#[path = "./criterion_config.rs"]
mod criterion_config;

use cntryl_midge::sst::{Memtable, SkipListMemtable};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use criterion_config::criterion_config_for_tier1;
use std::hint::black_box;

const LOOKUP_BATCH_SIZE: usize = 1024;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Pre-compute a key (deterministic, no allocation in hot path)
#[inline]
fn make_key(i: usize) -> Vec<u8> {
    format!("key_{i:010}").into_bytes()
}

/// Pre-compute value of given size
fn make_value(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

fn make_value_indexed(i: usize) -> Vec<u8> {
    format!("value_{i}").into_bytes()
}

// ─── Insert Benchmarks ───────────────────────────────────────────────────────

/// Benchmark single key-value insertion into a fresh memtable
fn bench_put_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/put_single");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let small_val = make_value(64);
    let medium_val = make_value(1024);
    let large_val = make_value(4096);

    // Use iter_batched to create a fresh memtable for each iteration bundle.
    // This prevents unbounded accumulation of nodes in a single memtable.
    // With BatchSize::SmallInput, Criterion will batch ~8-16 iterations together,
    // creating a new memtable for each batch.

    group.bench_function("64b_value", |b| {
        b.iter_batched(
            || {
                let memtable = SkipListMemtable::new();
                // Warm with 100 initial inserts (different keys)
                for i in 0..100 {
                    let _ = memtable.put(make_key(i), small_val.clone());
                }
                memtable
            },
            |memtable| {
                // Each iteration uses a fresh memtable from setup.
                // Insert one key (deterministic, unique across all batches).
                static COUNTER: std::sync::atomic::AtomicUsize =
                    std::sync::atomic::AtomicUsize::new(100);
                let idx = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = memtable.put(black_box(make_key(idx)), black_box(small_val.clone()));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("1kb_value", |b| {
        b.iter_batched(
            || {
                let memtable = SkipListMemtable::new();
                for i in 0..100 {
                    let _ = memtable.put(make_key(i), medium_val.clone());
                }
                memtable
            },
            |memtable| {
                static COUNTER: std::sync::atomic::AtomicUsize =
                    std::sync::atomic::AtomicUsize::new(100);
                let idx = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = memtable.put(black_box(make_key(idx)), black_box(medium_val.clone()));
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("4kb_value", |b| {
        b.iter_batched(
            || {
                let memtable = SkipListMemtable::new();
                for i in 0..100 {
                    let _ = memtable.put(make_key(i), large_val.clone());
                }
                memtable
            },
            |memtable| {
                static COUNTER: std::sync::atomic::AtomicUsize =
                    std::sync::atomic::AtomicUsize::new(100);
                let idx = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let _ = memtable.put(black_box(make_key(idx)), black_box(large_val.clone()));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark sequential insertions (batch pattern)
fn bench_put_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/put_batch");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    // Pre-create keys and values outside the hot loop
    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(128);

    group.bench_function("100_inserts", |b| {
        b.iter_batched(
            || {
                let memtable = SkipListMemtable::new();
                let items: Vec<(Vec<u8>, Vec<u8>)> = keys
                    .iter()
                    .map(|key| (key.clone(), value.clone()))
                    .collect();
                (memtable, items)
            },
            |(memtable, items)| {
                for (key, val) in items {
                    let _ = memtable.put(black_box(key), black_box(val));
                }
                black_box(memtable)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ─── Lookup Benchmarks ───────────────────────────────────────────────────────

/// Benchmark point lookup (hit and miss)
fn bench_get_point(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/get_point");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(LOOKUP_BATCH_SIZE as u64));

    // Pre-compute all keys outside benchmark
    let keys: Vec<Vec<u8>> = (0..1000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..1000).map(make_value_indexed).collect();

    let memtable = SkipListMemtable::new();
    for i in 0..1000 {
        let _ = memtable.put(keys[i].clone(), values[i].clone());
    }

    let hit_key = keys[500].as_slice();
    let miss_key = make_key(2000);

    group.bench_function("hit", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for _ in 0..LOOKUP_BATCH_SIZE {
                if memtable.get(black_box(hit_key)).unwrap().is_some() {
                    hits += 1;
                }
            }
            black_box(hits)
        });
    });

    group.bench_function("miss", |b| {
        b.iter(|| {
            let mut misses = 0usize;
            for _ in 0..LOOKUP_BATCH_SIZE {
                if memtable.get(black_box(&miss_key)).unwrap().is_none() {
                    misses += 1;
                }
            }
            black_box(misses)
        });
    });

    group.finish();
}

/// Benchmark delete operations
fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/delete");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let keys: Vec<Vec<u8>> = (0..100).map(make_key).collect();
    let value = make_value(128);

    group.bench_function("delete", |b| {
        b.iter_batched(
            || {
                // Create a warm memtable per iteration to prevent unbounded
                // version-chain growth that causes OOM on CI runners.
                let mt = SkipListMemtable::new();
                for key in &keys {
                    let _ = mt.put(key.clone(), value.clone());
                }
                let key = keys[50].clone();
                (mt, key)
            },
            |(memtable, key)| {
                let _ = memtable.delete(black_box(key));
            },
            BatchSize::SmallInput,
        );
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
        b.iter(|| black_box(memtable.size_bytes()));
    });

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier1_hotpath_memtable;
    config = criterion_config_for_tier1();
    targets =
        bench_put_single,
        bench_put_batch,
        bench_get_point,
        bench_delete,
        bench_size_bytes
}
criterion_main!(tier1_hotpath_memtable);
