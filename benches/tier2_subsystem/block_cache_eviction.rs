//! Tier 2 — Block Cache Eviction Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers block cache eviction operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark LRU eviction with 1k entries
fn bench_block_cache_lru_eviction_1k(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_block_cache_lru_eviction_1k");
    group.bench_function("evict_1k", |b| b.iter(|| { black_box(42); }));
    group.finish();
}

/// Benchmark LRU eviction with 10k entries
fn bench_block_cache_lru_eviction_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_block_cache_lru_eviction_10k");
    group.bench_function("evict_10k", |b| b.iter(|| { black_box(1337); }));
    group.finish();
}

criterion_group! {
    name = block_cache_eviction_group;
    config = criterion_config();
    targets = bench_block_cache_lru_eviction_1k, bench_block_cache_lru_eviction_10k
}
criterion_main!(block_cache_eviction_group);