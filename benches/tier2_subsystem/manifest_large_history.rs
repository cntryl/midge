//! Tier 2 — Manifest large history
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers manifest large history operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;

use cntryl_midge::core::manifest::{FileMeta, VersionEdit, VersionSet};

fn make_test_file_meta(i: usize) -> FileMeta {
    FileMeta {
        name: format!("sst_{:06}.sst", i),
        level: (i % 7) as u32,   // Spread across levels 0-6
        size_bytes: 1024 * 1024, // 1MB files
        cf_id: 0,
        smallest_key: Some(format!("key_{:010}", i * 1000).into_bytes()),
        largest_key: Some(format!("key_{:010}", (i + 1) * 1000 - 1).into_bytes()),
        smallest_seq: Some((i * 100) as u64),
        largest_seq: Some(((i + 1) * 100 - 1) as u64),
        total_entries: 1000,
        ..Default::default()
    }
}

/// Benchmark manifest replay 100k entries
fn bench_manifest_replay_100k_entries(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_manifest_replay_100k_entries");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100_000));

    // Precompute 100k version edits
    let edits: Vec<VersionEdit> = (0..100_000)
        .map(|i| VersionEdit::AddFile {
            file: Box::new(make_test_file_meta(i)),
        })
        .collect();

    group.bench_function("replay_100k", |b| {
        b.iter(|| {
            let version_set = VersionSet::new(Default::default());

            // Apply all 100k edits using batch API (O(n) instead of O(n²))
            let final_version = version_set.apply_edits(edits.iter().cloned()).unwrap();

            black_box(final_version);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier2_subsystem_manifest_large_history;
    config = criterion_config();
    targets = bench_manifest_replay_100k_entries
}
criterion_main!(tier2_subsystem_manifest_large_history);
