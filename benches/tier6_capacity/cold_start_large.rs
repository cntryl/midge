//! Tier 6 — Capacity/Cold start large
//!
//! **Target Runtime:** Large-scale capacity tests (minutes)
//! **Run Frequency:** Manual / capacity CI
//!
//! Measures engine startup time with large persistent datasets (100k+ keys)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark cold start performance with large dataset (100k keys)
/// Measures manifest loading, SST file discovery, and recovery time
fn bench_cold_start_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("capacity_cold_start_large");
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(30));
    group.sample_size(10);

    // Pre-create large dataset once
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("cold_start_large");

    // Setup: Create 100k keys across many SST files
    {
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk {
                db_path: path.clone(),
            },
            memtable_size: 256 * 1024, // Small memtable = many SST files
            enable_compaction: true,
            ..Default::default()
        };

        let engine = MidgeEngine::open(opts).unwrap();
        let cf = engine.default_column_family();

        // Populate 100k keys
        for i in 0..100_000 {
            let key = format!("large_key_{:010}", i);
            let val = format!("large_value_{}", i);
            engine.put(&cf, key.as_bytes(), val.as_bytes()).unwrap();

            if i % 5000 == 0 {
                engine.flush().unwrap();
            }
        }
        engine.flush().unwrap();
        drop(engine); // Close cleanly
    }

    group.throughput(Throughput::Elements(100_000));
    group.bench_function("startup_100k_keys", |b| {
        b.iter(|| {
            // Cold start: Open existing database
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk {
                    db_path: path.clone(),
                },
                memtable_size: 256 * 1024,
                enable_compaction: true,
                ..Default::default()
            };

            let engine = MidgeEngine::open(opts).unwrap();

            // Verify database is accessible
            let cf = engine.default_column_family();
            let result = engine.get(&cf, b"large_key_0000000000").unwrap();

            black_box((engine, result));
        })
    });

    group.finish();
}

criterion_group! {
    name = tier6_capacity_cold_start_large;
    config = criterion_config_for_tier(BenchTier::Tier6Capacity);
    targets = bench_cold_start_large
}
criterion_main!(tier6_capacity_cold_start_large);
