//! Tier 6 — Capacity large dataset compaction bench (stub)
//!
//! Placeholder bench for compaction behavior on large datasets.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_large_dataset_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_large_dataset_compaction");
    group.bench_function("noop", |b| b.iter(|| { black_box(1usize); }));
    group.finish();
}

criterion_group! {
    name = large_dataset_compaction_group;
    config = criterion_config();
    targets = bench_large_dataset_compaction
}
criterion_main!(large_dataset_compaction_group);
