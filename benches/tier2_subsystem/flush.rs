//! Tier 2 — Flush Benchmarks (stub)
//!
//! Placeholder for flush benchmarks to keep the suite compiling.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush");
    group.bench_function("noop", |b| b.iter(|| { black_box(8u8); }));
    group.finish();
}

criterion_group! {
    name = flush_group;
    config = criterion_config();
    targets = bench_flush
}
criterion_main!(flush_group);
