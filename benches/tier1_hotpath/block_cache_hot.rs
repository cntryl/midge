//! Tier 1 — Hot Path Block Cache Benchmarks
//!
//! Minimal compile-time stub for `block_cache_hot` to satisfy CI/build.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_block_cache_hot(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_block_cache_hot");
    group.bench_function("nop", |b| b.iter(|| { black_box(0); }));
    group.finish();
}

criterion_group! {
    name = block_cache_hot_group;
    config = criterion_config();
    targets = bench_block_cache_hot
}
criterion_main!(block_cache_hot_group);
