//! Tier 2 — Memtable Full Benchmark (stub)
//!
//! Placeholder bench to catch regressions in memtable full behavior.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_memtable_full(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_full");
    group.bench_function("noop", |b| b.iter(|| { black_box(0usize); }));
    group.finish();
}

criterion_group! {
    name = memtable_full_group;
    config = criterion_config();
    targets = bench_memtable_full
}
criterion_main!(memtable_full_group);
