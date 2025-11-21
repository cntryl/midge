//! Tier 3 — Startup WAL replay bench (stub)
//!
//! Placeholder bench for validating WAL replay startup.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_startup_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_startup_wal");
    group.bench_function("noop", |b| b.iter(|| { black_box(128usize); }));
    group.finish();
}

criterion_group! {
    name = startup_wal_group;
    config = criterion_config();
    targets = bench_startup_wal
}
criterion_main!(startup_wal_group);
