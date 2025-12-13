//! Tier 6 — Capacity/Large dataset insert
//!
//! **Target Runtime:** Large-scale capacity tests (minutes)
//! **Run Frequency:** Manual / capacity CI
//!
//! Measures sustained insert throughput with large datasets (100k+ operations)

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark sustained insert performance over 100k operations
/// Measures overall throughput including memtable flushes and background compaction
fn bench_large_dataset_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_large_dataset_insert");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(30));
    group.sample_size(10);
    group.throughput(Throughput::Elements(100_000));

    group.bench_function("sequential_insert_100k_keys", |b| {
        b.iter(|| {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("large_insert");

            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: path },
                memtable_size: 2 * 1024 * 1024, // 2MB memtable
                enable_compaction: true,
                ..Default::default()
            };

            let engine = MidgeEngine::open(opts).unwrap();
            let cf = engine.default_column_family();

            // Insert 100k keys with realistic value sizes
            let start = std::time::Instant::now();
            for i in 0..100_000 {
                let key = format!("insert_key_{:010}", i);
                let val = vec![b'v'; 256]; // 256-byte values (~25MB total)
                engine.put(cf, key.as_bytes(), &val).unwrap();
            }
            engine.flush().unwrap();
            let elapsed = start.elapsed();

            black_box(elapsed);
        })
    });

    group.finish();
}

criterion_group! {
    name = tier6_capacity_large_dataset_insert;
    config = criterion_config_for_tier(BenchTier::Tier6Capacity);
    targets = bench_large_dataset_insert
}
criterion_main!(tier6_capacity_large_dataset_insert);
