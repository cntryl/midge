//! Tier 1 — Memtable insert hot path (stub)
//!
//! Minimal compile-time bench placeholder.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_memtable_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_insert");
    group.bench_function("noop", |b| b.iter(|| { black_box(42); }));
    group.finish();
}

criterion_group! {
    name = memtable_insert_group;
    config = criterion_config();
    targets = bench_memtable_insert
}
criterion_main!(memtable_insert_group);
