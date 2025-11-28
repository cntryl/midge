//! Tier 6 — Capacity/Large dataset compaction
//!
//! **Target Runtime:** Large-scale capacity tests (minutes)
//! **Run Frequency:** Manual / capacity CI
//!
//! Measures compaction throughput with large datasets (100k+ keys)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark compaction throughput on large dataset (100k keys)
/// Measures time to compact L0 → L1 with significant data volume
fn bench_large_dataset_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_large_dataset_compaction");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(30));
    group.sample_size(10);
    group.throughput(Throughput::Elements(100_000));

    group.bench_function("compact_100k_keys_l0_to_l1", |b| {
        b.iter(|| {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("large_compact");

            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: path },
                memtable_size: 512 * 1024, // Small memtable = many L0 files
                enable_compaction: false,  // Manual compaction control
                ..Default::default()
            };

            let engine = MidgeEngine::open(opts).unwrap();
            let cf = engine.default_column_family();

            // Populate 100k keys (creates many L0 files)
            for i in 0..100_000 {
                let key = format!("compact_key_{:010}", i);
                let val = vec![b'x'; 128]; // 128-byte values
                engine.put(&cf, key.as_bytes(), &val).unwrap();

                if i % 2000 == 0 {
                    engine.flush().unwrap(); // Create L0 file
                }
            }
            engine.flush().unwrap();

            // Measure compaction time: L0 → L1
            let start = std::time::Instant::now();
            let _ = engine.compact_level(&cf, 0);
            let elapsed = start.elapsed();

            black_box(elapsed);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier6_capacity_large_dataset_compaction;
    config = criterion_config();
    targets = bench_large_dataset_compaction
}
criterion_main!(tier6_capacity_large_dataset_compaction);
