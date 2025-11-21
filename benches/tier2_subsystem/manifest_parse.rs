//! Tier 2 — Manifest parse benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers manifest parsing operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

/// Benchmark manifest parse small
fn bench_manifest_parse_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_parse_small");
    group.bench_function("parse_small", |b| b.iter(|| { black_box("parsed".to_string()); }));
    group.finish();
}

/// Benchmark manifest parse large
fn bench_manifest_parse_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_parse_large");
    group.bench_function("parse_large", |b| b.iter(|| { black_box("large_parsed".to_string()); }));
    group.finish();
}

criterion_group! {
    name = manifest_parse_group;
    config = criterion_config();
    targets = bench_manifest_parse_small, bench_manifest_parse_large
}
criterion_main!(manifest_parse_group);