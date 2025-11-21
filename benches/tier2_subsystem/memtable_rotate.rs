//! Tier 2 — Memtable rotate benchmark (stub)
//!
//! Placeholder bench to check memtable rotation behavior.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_memtable_rotate(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_memtable_rotate");
    group.bench_function("noop", |b| b.iter(|| { black_box(7); }));
    group.finish();
}

criterion_group! {
    name = memtable_rotate_group;
    config = criterion_config();
    targets = bench_memtable_rotate
}
criterion_main!(memtable_rotate_group);
