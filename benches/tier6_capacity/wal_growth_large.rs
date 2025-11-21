//! Tier 6 — WAL growth benchmark (stub)
//!
//! Placeholder for WAL growth tests with large datasets.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_wal_growth_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_wal_growth_large");
    group.bench_function("noop", |b| b.iter(|| { black_box(1024usize); }));
    group.finish();
}

criterion_group! {
    name = wal_growth_large_group;
    config = criterion_config();
    targets = bench_wal_growth_large
}
criterion_main!(wal_growth_large_group);
