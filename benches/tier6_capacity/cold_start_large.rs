//! Tier 6 — cold start with a large dataset (stub)
//!
//! Minimal placeholder bench to compile the cold-start test harness.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_cold_start_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_cold_start_large");
    group.bench_function("noop", |b| b.iter(|| { black_box(7u8); }));
    group.finish();
}

criterion_group! {
    name = cold_start_large_group;
    config = criterion_config();
    targets = bench_cold_start_large
}
criterion_main!(cold_start_large_group);
