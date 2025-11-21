//! Tier 1 — Memtable seek hot path (stub)
//!
//! This minimal bench ensures the bench suite compiles.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_memtable_seek(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_memtable_seek");
    group.bench_function("noop", |b| b.iter(|| { black_box(()); }));
    group.finish();
}

criterion_group! {
    name = memtable_seek_group;
    config = criterion_config();
    targets = bench_memtable_seek
}
criterion_main!(memtable_seek_group);
