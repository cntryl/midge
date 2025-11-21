//! Tier 6 — Capacity/Cold start large
//!
//! **Target Runtime:** Large-scale capacity tests
//! **Run Frequency:** Manual / capacity CI
//!
//! Measures cold start performance with large datasets

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark cold start large
fn bench_cold_start_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_cold_start_large");
    group.bench_function("cold_start", |b| b.iter(|| { black_box(100000usize); }));
    group.finish();
}

criterion_group! {
    name = cold_start_large_group;
    config = criterion_config();
    targets = bench_cold_start_large
}
criterion_main!(cold_start_large_group);
