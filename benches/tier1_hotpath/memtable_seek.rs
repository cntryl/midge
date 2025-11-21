//! Tier 1 — Memtable seek hot path
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers memtable seek/lookup hot paths:
//! - Point lookups and version finding
//! - Forward and reverse iteration

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::collections::BTreeMap;
use std::hint::black_box;

// Mock memtable for hot path testing
struct MockMemtable {
    data: BTreeMap<Bytes, Vec<Bytes>>, // Key -> multiple versions
}

impl MockMemtable {
    fn new() -> Self {
        Self {
            data: BTreeMap::new(),
        }
    }

    fn put(&mut self, key: Bytes, value: Bytes) {
        self.data.entry(key).or_insert_with(Vec::new).push(value);
    }

    fn get(&self, key: &Bytes) -> Option<&Bytes> {
        self.data.get(key).and_then(|versions| versions.last())
    }

    fn seek_forward(&self, start_key: &Bytes, steps: usize) -> Vec<Bytes> {
        let mut results = Vec::new();
        let mut iter = self.data.range::<Bytes, _>(start_key..);
        for _ in 0..steps {
            if let Some((key, _)) = iter.next() {
                results.push(key.clone());
            } else {
                break;
            }
        }
        results
    }

    fn seek_reverse(&self, start_key: &Bytes, steps: usize) -> Vec<Bytes> {
        let mut results = Vec::new();
        let mut iter = self.data.range::<Bytes, _>(..=start_key).rev();
        for _ in 0..steps {
            if let Some((key, _)) = iter.next() {
                results.push(key.clone());
            } else {
                break;
            }
        }
        results
    }
}

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

fn make_value(i: usize) -> Bytes {
    Bytes::from(format!("value_{}", i))
}

/// Benchmark point lookup
fn bench_memtable_get_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_get_point_lookup");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut memtable = MockMemtable::new();
    // Pre-populate
    for i in 0..1000 {
        memtable.put(make_key(i), make_value(i));
    }

    group.bench_function("point_lookup_hit", |b| {
        b.iter(|| {
            let result = memtable.get(&make_key(500));
            black_box(result);
        })
    });

    group.bench_function("point_lookup_miss", |b| {
        b.iter(|| {
            let result = memtable.get(&make_key(2000)); // Not present
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark getting latest version
fn bench_memtable_get_latest_version(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_get_latest_version");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut memtable = MockMemtable::new();
    let key = make_key(42);
    // Add multiple versions
    for i in 0..5 {
        memtable.put(key.clone(), make_value(i));
    }

    group.bench_function("get_latest_version", |b| {
        b.iter(|| {
            let result = memtable.get(&key);
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark forward seek
fn bench_memtable_seek_forward_32steps(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_seek_forward_32steps");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut memtable = MockMemtable::new();
    // Pre-populate sequential keys
    for i in 0..100 {
        memtable.put(make_key(i), make_value(i));
    }

    group.bench_function("seek_forward_32", |b| {
        b.iter(|| {
            let results = memtable.seek_forward(&make_key(10), 32);
            black_box(results);
        })
    });

    group.finish();
}

/// Benchmark reverse seek
fn bench_memtable_seek_reverse_32steps(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_seek_reverse_32steps");
    group.measurement_time(std::time::Duration::from_millis(200));

    let mut memtable = MockMemtable::new();
    // Pre-populate sequential keys
    for i in 0..100 {
        memtable.put(make_key(i), make_value(i));
    }

    group.bench_function("seek_reverse_32", |b| {
        b.iter(|| {
            let results = memtable.seek_reverse(&make_key(50), 32);
            black_box(results);
        })
    });

    group.finish();
}

criterion_group! {
    name = memtable_seek_group;
    config = criterion_config();
    targets = bench_memtable_get_point_lookup, bench_memtable_get_latest_version, bench_memtable_seek_forward_32steps, bench_memtable_seek_reverse_32steps
}
criterion_main!(memtable_seek_group);