//! Tier 1 — Memtable insert hot path
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers memtable insertion hot paths:
//! - Key insertion with different value sizes
//! - Sequential insertion patterns
//!
//! Note: Measures insertion into an existing warm memtable,
//! not memtable creation (which is setup overhead).

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::core::memtable::MemTable;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

/// Pre-compute a key (deterministic, no allocation in hot path)
#[inline]
fn make_key(i: usize) -> Bytes {
    let mut buf = [0u8; 18];
    buf[..4].copy_from_slice(b"key_");
    // Format number in the remaining bytes
    let s = format!("{:010}", i);
    buf[4..14].copy_from_slice(s.as_bytes());
    Bytes::copy_from_slice(&buf[..14])
}

/// Pre-compute value of given size
fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// Benchmark single key-value insertion into a warm memtable
fn bench_memtable_put_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_put_single");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    // Pre-create keys and values outside measurement
    let keys: Vec<Bytes> = (0..1000).map(make_key).collect();
    let small_val = make_value(64);
    let medium_val = make_value(1024);
    let large_val = make_value(4096);

    // Warm memtable - pre-populated with some data
    let memtable = MemTable::new();
    for i in 0..100 {
        memtable.put(keys[i].as_ref(), small_val.as_ref());
    }

    let mut counter = 100usize;

    group.bench_function("put_small_64b", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            memtable.put(black_box(keys[idx].as_ref()), black_box(small_val.as_ref()));
        })
    });

    group.bench_function("put_medium_1kb", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            memtable.put(
                black_box(keys[idx].as_ref()),
                black_box(medium_val.as_ref()),
            );
        })
    });

    group.bench_function("put_large_4kb", |b| {
        b.iter(|| {
            let idx = counter % keys.len();
            counter = counter.wrapping_add(1);
            memtable.put(black_box(keys[idx].as_ref()), black_box(large_val.as_ref()));
        })
    });

    group.finish();
}

/// Benchmark sequential insertions (batch pattern)
fn bench_memtable_seq_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_seq_insert");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    // Pre-create keys and values
    let keys: Vec<Bytes> = (0..100).map(make_key).collect();
    let value = make_value(128);

    group.bench_function("seq_insert_100", |b| {
        let memtable = MemTable::new();
        b.iter(|| {
            for key in &keys {
                memtable.put(black_box(key.as_ref()), black_box(value.as_ref()));
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_memtable_insert;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_memtable_put_single, bench_memtable_seq_insert
}
criterion_main!(tier1_hotpath_memtable_insert);
