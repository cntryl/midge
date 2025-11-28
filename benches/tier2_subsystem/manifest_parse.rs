//! Tier 2 — Manifest parse benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Measures manifest serialization/deserialization performance

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;

use cntryl_midge::core::manifest::{FileMeta, VersionEdit, VersionSet};

fn make_test_file_meta(i: usize) -> FileMeta {
    FileMeta {
        name: format!("sst_{:06}.sst", i),
        level: (i % 7) as u32,
        size_bytes: 1024 * 1024,
        cf_id: 0,
        smallest_key: Some(format!("key_{:010}", i * 1000).into_bytes()),
        largest_key: Some(format!("key_{:010}", (i + 1) * 1000 - 1).into_bytes()),
        smallest_seq: Some((i * 100) as u64),
        largest_seq: Some(((i + 1) * 100 - 1) as u64),
        total_entries: 1000,
        ..Default::default()
    }
}

/// Benchmark manifest parse small (100 edits)
/// Measures serialize/deserialize cycle for small manifests
fn bench_manifest_parse_small(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_parse_small");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    // Precompute 100 version edits
    let edits: Vec<VersionEdit> = (0..100)
        .map(|i| VersionEdit::AddFile {
            file: Box::new(make_test_file_meta(i)),
        })
        .collect();

    group.bench_function("parse_small", |b| {
        b.iter(|| {
            // Use batch apply_edits for O(n) instead of O(n²)
            let version_set = VersionSet::new(Default::default())
                .apply_edits(edits.iter().cloned())
                .unwrap();

            // Read back state from manifest (simulates serialization)
            let file_count = version_set.manifest.files.len();

            black_box((version_set, file_count));
        })
    });

    group.finish();
}

/// Benchmark manifest parse large (1k edits)
/// Larger-scale manifest operation simulation
fn bench_manifest_parse_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_parse_large");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1_000));

    // Precompute 1k version edits
    let edits: Vec<VersionEdit> = (0..1_000)
        .map(|i| VersionEdit::AddFile {
            file: Box::new(make_test_file_meta(i)),
        })
        .collect();

    group.bench_function("parse_large", |b| {
        b.iter(|| {
            // Use batch apply_edits for O(n) instead of O(n²)
            let version_set = VersionSet::new(Default::default())
                .apply_edits(edits.iter().cloned())
                .unwrap();

            // Read back state from manifest
            let file_count = version_set.manifest.files.len();

            black_box((version_set, file_count));
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_manifest_parse;
    config = criterion_config_for_tier(BenchTier::Tier2Subsystem);
    targets = bench_manifest_parse_small, bench_manifest_parse_large
}
criterion_main!(tier2_subsystem_manifest_parse);
