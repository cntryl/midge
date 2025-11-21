//! Tier 1 — Memtable insert hot path
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers memtable insertion hot paths:
//! - Key insertion with different value sizes
//! - Sequential insertion patterns

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::collections::BTreeMap;
use std::hint::black_box;

// Mock memtable for hot path testing
struct MockMemtable {
    data: BTreeMap<Bytes, Bytes>,
}

impl MockMemtable {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    fn put(&mut self, key: Bytes, value: Bytes) {
        self.data.insert(key, value);
    }
}

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// Benchmark small key-value insertion
fn bench_memtable_put_key_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_put_key_small");
    group.measurement_time(std::time::Duration::from_millis(200));

    group.bench_function("put_small_kv", |b| {
        b.iter_batched(
            || MockMemtable::new(),
            |mut memtable| {
                memtable.put(make_key(42), make_value(64));
                black_box(&memtable);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark medium key-value insertion
fn bench_memtable_put_key_medium(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_put_key_medium");
    group.measurement_time(std::time::Duration::from_millis(200));

    group.bench_function("put_medium_kv", |b| {
        b.iter_batched(
            || MockMemtable::new(),
            |mut memtable| {
                memtable.put(make_key(42), make_value(1024));
                black_box(&memtable);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark large key-value insertion
fn bench_memtable_put_key_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_put_key_large");
    group.measurement_time(std::time::Duration::from_millis(200));

    group.bench_function("put_large_kv", |b| {
        b.iter_batched(
            || MockMemtable::new(),
            |mut memtable| {
                memtable.put(make_key(42), make_value(4096));
                black_box(&memtable);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark sequential insertions
fn bench_memtable_seq_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_seq_insert");
    group.measurement_time(std::time::Duration::from_millis(200));

    group.bench_function("seq_insert_100", |b| {
        b.iter_batched(
            || MockMemtable::new(),
            |mut memtable| {
                for i in 0..100 {
                    memtable.put(make_key(i), make_value(128));
                }
                black_box(&memtable);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = memtable_insert_group;
    config = criterion_config();
    targets = bench_memtable_put_key_small, bench_memtable_put_key_medium, bench_memtable_put_key_large, bench_memtable_seq_insert
}
criterion_main!(memtable_insert_group);