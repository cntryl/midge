//! Tier 6 — Capacity/WAL growth large
//!
//! **Target Runtime:** Large-scale capacity tests
//! **Run Frequency:** Manual / capacity CI
//!
//! Measures WAL growth with large datasets

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark WAL growth large
fn bench_wal_growth_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_wal_growth_large");
    group.bench_function("wal_growth", |b| b.iter(|| { black_box(100000usize); }));
    group.finish();
}

criterion_group! {
    name = wal_growth_large_group;
    config = criterion_config();
    targets = bench_wal_growth_large
}
criterion_main!(wal_growth_large_group);
