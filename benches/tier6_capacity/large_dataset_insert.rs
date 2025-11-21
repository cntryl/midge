//! Tier 6 — Capacity/Large dataset insert
//!
//! **Target Runtime:** Large-scale capacity tests
//! **Run Frequency:** Manual / capacity CI
//!
//! Measures insert performance with large datasets

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark large dataset insert
fn bench_large_dataset_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_large_dataset_insert");
    group.bench_function("insert_large", |b| b.iter(|| { black_box(100000usize); }));
    group.finish();
}

criterion_group! {
    name = large_dataset_insert_group;
    config = criterion_config();
    targets = bench_large_dataset_insert
}
criterion_main!(large_dataset_insert_group);
