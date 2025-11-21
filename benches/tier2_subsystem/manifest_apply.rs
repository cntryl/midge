//! Tier 2 — Manifest apply benchmark
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers manifest application operations (VersionSet edits)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::core::manifest::{FileMeta, Manifest, VersionEdit, VersionSet};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn create_file_meta(i: usize, level: u32) -> FileMeta {
    let key = format!("key_{:010}", i);
    FileMeta {
        name: format!("sst_{:06}.sst", i),
        level,
        size_bytes: 4096,
        cf_id: 0,
        smallest_key: Some(key.as_bytes().to_vec()),
        largest_key: Some(format!("key_{:010}", i + 100).as_bytes().to_vec()),
        smallest_seq: Some(i as u64),
        largest_seq: Some((i + 100) as u64),
        sublevel: 0,
        cloud_location: None,
        cloud_checksum: None,
        cloud_uploaded_at: None,
        cloud_state: None,
        point_tombstone_count: 0,
        range_tombstone_count: 0,
        total_entries: 100,
    }
}

/// Benchmark applying 100 VersionEdit operations (AddFile + RemoveFiles mix)
fn bench_manifest_apply_100_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_apply_100_ops");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));
    group.measurement_time(std::time::Duration::from_millis(500));

    // Precompute all edits outside the benchmark
    let mut edits = Vec::with_capacity(100);
    for i in 0..50 {
        // Add files
        edits.push(VersionEdit::AddFile {
            file: Box::new(create_file_meta(i, 0)),
        });
    }
    for i in 0..50 {
        // Remove files (simulate compaction)
        edits.push(VersionEdit::RemoveFiles {
            names: vec![format!("sst_{:06}.sst", i)],
        });
    }

    group.bench_function("apply_100", |b| {
        b.iter(|| {
            let mut version = VersionSet::new(Manifest::default());
            
            // Apply all 100 edits
            for edit in &edits {
                version = version.apply_edit(edit.clone()).unwrap();
            }
            
            black_box(version);
        })
    });

    group.finish();
}

/// Benchmark applying 10k VersionEdit operations
fn bench_manifest_apply_10k_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_apply_10k_ops");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));
    group.measurement_time(std::time::Duration::from_secs(2));
    group.sample_size(10); // Fewer samples for large operation

    // Precompute all edits outside the benchmark
    let mut edits = Vec::with_capacity(10_000);
    for i in 0..5_000 {
        // Add files at various levels
        edits.push(VersionEdit::AddFile {
            file: Box::new(create_file_meta(i, i as u32 % 7)), // Levels 0-6
        });
    }
    for i in 0..5_000 {
        // Remove files
        edits.push(VersionEdit::RemoveFiles {
            names: vec![format!("sst_{:06}.sst", i)],
        });
    }

    group.bench_function("apply_10k", |b| {
        b.iter(|| {
            let mut version = VersionSet::new(Manifest::default());
            
            // Apply all 10k edits
            for edit in &edits {
                version = version.apply_edit(edit.clone()).unwrap();
            }
            
            black_box(version);
        })
    });

    group.finish();
}

criterion_group! {
    name = manifest_apply_group;
    config = criterion_config();
    targets = bench_manifest_apply_100_ops, bench_manifest_apply_10k_ops
}
criterion_main!(manifest_apply_group);