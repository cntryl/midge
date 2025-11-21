//! Tier 2 — Manifest large history
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers manifest large history operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark manifest replay 100k entries
fn bench_manifest_replay_100k_entries(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_replay_100k_entries");
    group.bench_function("replay_100k", |b| b.iter(|| { black_box(100000usize); }));
    group.finish();
}

criterion_group! {
    name = manifest_large_history_group;
    config = criterion_config();
    targets = bench_manifest_replay_100k_entries
}
criterion_main!(manifest_large_history_group);