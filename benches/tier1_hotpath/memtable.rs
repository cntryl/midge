//! Tier 1 — Memtable Hot Path Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical memtable hot paths:
//! - Insert operations (single and batch, various value sizes)
//! - Point lookups (hit/miss)
//! - Version retrieval
//! - Forward/reverse iteration
//! - Bloom filter optimization

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::core::memtable::MemTable;
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

fn make_value_indexed(i: usize) -> Bytes {
    Bytes::from(format!("value_{}", i))
}

// ─── Insert Benchmarks ───────────────────────────────────────────────────────

/// Benchmark single key-value insertion into a warm memtable
fn bench_put_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/put_single");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Pre-create keys and values outside measurement
    let keys: Vec<Bytes> = (0..1000).map(make_key).collect();
    let small_val = make_value(64);
    let medium_val = make_value(1024);
    let large_val = make_value(4096);

    // Warm memtable - pre-populated with some data
    let memtable = MemTable::new();
    for key in keys.iter().take(100) {
        memtable.put(key.as_ref(), small_val.as_ref());
    }

    let mut counter = 100usize;

    group.bench_function("64b_value", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            memtable.put(black_box(keys[idx].as_ref()), black_box(small_val.as_ref()));
        })
    });

    group.bench_function("1kb_value", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            memtable.put(
                black_box(keys[idx].as_ref()),
                black_box(medium_val.as_ref()),
            );
        })
    });

    group.bench_function("4kb_value", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            memtable.put(black_box(keys[idx].as_ref()), black_box(large_val.as_ref()));
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
    let keys: Vec<Bytes> = (0..100).map(make_key).collect();
    let value = make_value(128);

    group.bench_function("100_inserts", |b| {
        let memtable = MemTable::new();
        b.iter(|| {
            for key in &keys {
                memtable.put(black_box(key.as_ref()), black_box(value.as_ref()));
            }
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
    let keys: Vec<Bytes> = (0..1000).map(make_key).collect();
    let values: Vec<Bytes> = (0..1000).map(make_value_indexed).collect();

    let memtable = MemTable::new();
    for i in 0..1000 {
        memtable.put(keys[i].as_ref(), values[i].as_ref());
    }

    let hit_key = keys[500].clone();
    let miss_key = make_key(2000);

    group.bench_function("hit", |b| {
        b.iter(|| black_box(memtable.get(black_box(hit_key.as_ref()))))
    });

    group.bench_function("miss", |b| {
        b.iter(|| black_box(memtable.get(black_box(miss_key.as_ref()))))
    });

    group.finish();
}

/// Benchmark getting latest version from multi-version key
fn bench_get_latest_version(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/get_latest_version");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let memtable = MemTable::new();
    let key = make_key(42);
    // Add multiple versions
    for i in 0..5 {
        memtable.put_with_seq(key.as_ref(), make_value_indexed(i).as_ref(), i as u64);
    }

    group.bench_function("5_versions", |b| {
        b.iter(|| black_box(memtable.get(black_box(key.as_ref()))))
    });

    group.finish();
}

// ─── Iteration Benchmarks ────────────────────────────────────────────────────

/// Benchmark forward seek (32 steps)
fn bench_seek_forward(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/seek_forward");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(32));
    group.measurement_time(std::time::Duration::from_millis(200));

    let keys: Vec<Bytes> = (0..100).map(make_key).collect();
    let values: Vec<Bytes> = (0..100).map(make_value_indexed).collect();

    let memtable = MemTable::new();
    for i in 0..100 {
        memtable.put(keys[i].as_ref(), values[i].as_ref());
    }

    let start_key = keys[10].clone();

    group.bench_function("32_steps", |b| {
        b.iter(|| {
            let all_keys = memtable.get_all_keys();
            let results: Vec<_> = all_keys
                .iter()
                .filter(|k| k.as_ref() >= start_key.as_ref())
                .take(32)
                .cloned()
                .collect();
            black_box(results)
        })
    });

    group.finish();
}

/// Benchmark reverse seek (32 steps)
fn bench_seek_reverse(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/seek_reverse");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(32));
    group.measurement_time(std::time::Duration::from_millis(200));

    let keys: Vec<Bytes> = (0..100).map(make_key).collect();
    let values: Vec<Bytes> = (0..100).map(make_value_indexed).collect();

    let memtable = MemTable::new();
    for i in 0..100 {
        memtable.put(keys[i].as_ref(), values[i].as_ref());
    }

    let start_key = keys[50].clone();

    group.bench_function("32_steps", |b| {
        b.iter(|| {
            let all_keys = memtable.get_all_keys();
            let mut results: Vec<_> = all_keys
                .iter()
                .filter(|k| k.as_ref() <= start_key.as_ref())
                .cloned()
                .collect();
            results.reverse();
            results.truncate(32);
            black_box(results)
        })
    });

    group.finish();
}

// ─── Bloom Filter Benchmarks ─────────────────────────────────────────────────

/// Benchmark bloom filter optimization for negative lookups
fn bench_bloom_hint(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable/bloom_hint");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(std::time::Duration::from_millis(200));

    let keys: Vec<Bytes> = (0..1000).map(make_key).collect();
    let values: Vec<Bytes> = (0..1000).map(make_value_indexed).collect();

    // Without bloom hint
    let memtable_no_bloom = MemTable::new();
    for i in 0..1000 {
        memtable_no_bloom.put(keys[i].as_ref(), values[i].as_ref());
    }

    // With bloom hint
    let memtable_with_bloom = MemTable::with_bloom_hint(1000);
    for i in 0..1000 {
        memtable_with_bloom.put(keys[i].as_ref(), values[i].as_ref());
    }

    let miss_key = make_key(100_000);

    group.bench_function("miss_no_bloom", |b| {
        b.iter(|| black_box(memtable_no_bloom.get(black_box(miss_key.as_ref()))))
    });

    group.bench_function("miss_with_bloom", |b| {
        b.iter(|| black_box(memtable_with_bloom.get(black_box(miss_key.as_ref()))))
    });

    group.finish();
}

// ─── Criterion Setup ─────────────────────────────────────────────────────────

criterion_group! {
    name = tier1_memtable;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets =
        bench_put_single,
        bench_put_batch,
        bench_get_point,
        bench_get_latest_version,
        bench_seek_forward,
        bench_seek_reverse,
        bench_bloom_hint
}
criterion_main!(tier1_memtable);
