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
        sst_seq: i as u64,
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
            // Use batch apply_edits for O(n) instead of O(n²)
            let version = VersionSet::new(Manifest::default())
                .apply_edits(edits.iter().cloned())
                .unwrap();

            black_box(version);
        })
    });

    group.finish();
}

/// Benchmark applying 10k VersionEdit operations
/// Uses realistic batched removes (like compaction) instead of individual removes
fn bench_manifest_apply_10k_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_apply_10k_ops");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));
    group.measurement_time(std::time::Duration::from_secs(2));
    group.sample_size(10); // Fewer samples for large operation

    // Precompute all edits outside the benchmark
    // Realistic pattern: add files, then batch remove (compaction-style)
    let mut edits = Vec::with_capacity(600);

    // Add 5000 files
    for i in 0..5_000 {
        edits.push(VersionEdit::AddFile {
            file: Box::new(create_file_meta(i, i as u32 % 7)),
        });
    }

    // Batch removes in groups of 10 (realistic compaction pattern)
    // 500 batches × 10 files = 5000 removes = 10k total operations
    for batch in 0..500 {
        let names: Vec<String> = (0..10)
            .map(|j| format!("sst_{:06}.sst", batch * 10 + j))
            .collect();
        edits.push(VersionEdit::RemoveFiles { names });
    }

    group.bench_function("apply_10k", |b| {
        b.iter(|| {
            // Use batch apply_edits for O(n) instead of O(n²)
            let version = VersionSet::new(Manifest::default())
                .apply_edits(edits.iter().cloned())
                .unwrap();

            black_box(version);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_manifest_apply;
    config = criterion_config();
    targets = bench_manifest_apply_100_ops, bench_manifest_apply_10k_ops
}
criterion_main!(tier2_subsystem_manifest_apply);
