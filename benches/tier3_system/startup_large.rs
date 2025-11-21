//! Tier 3 — Startup large dataset bench (stub)
//!
//! Placeholder bench to emulate DB startup time for large datasets.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_startup_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_startup_large");
    group.bench_function("noop", |b| b.iter(|| { black_box(0usize); }));
    group.finish();
}

criterion_group! {
    name = startup_large_group;
    config = criterion_config();
    targets = bench_startup_large
}
criterion_main!(startup_large_group);
