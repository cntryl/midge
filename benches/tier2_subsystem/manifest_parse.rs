//! Tier 2 — Manifest parse benchmark (stub)
//!
//! Minimal placeholder bench. Add real parsing workloads later.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn bench_manifest_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_parse");
    group.bench_function("noop", |b| b.iter(|| { black_box(13u8); }));
    group.finish();
}

criterion_group! {
    name = manifest_parse_group;
    config = criterion_config();
    targets = bench_manifest_parse
}
criterion_main!(manifest_parse_group);
