//! Tier 3 — Startup large dataset bench
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers engine startup with large manifest (many SST files)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark engine startup with large manifest (simulated via many flushes)
fn bench_engine_startup_100k_sst_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_large_manifest");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1)); // One startup operation
    group.measurement_time(std::time::Duration::from_secs(5));
    group.sample_size(10);

    group.bench_function("startup_with_many_ssts", |b| {
        b.iter(|| {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("startup_large");

            // Create database and populate with many small flushes to simulate many SST files
            {
                let opts = MidgeOptions {
                    storage_mode: StorageMode::LocalDisk {
                        db_path: path.clone(),
                    },
                    memtable_size: 64 * 1024, // Small memtable = more SSTs
                    enable_compaction: false,
                    ..Default::default()
                };
                let engine = MidgeEngine::open(opts).unwrap();
                let cf = engine.default_column_family();

                // Write 5000 keys with periodic flushes to create many SST files
                for i in 0..5000 {
                    let key = format!("key_{:010}", i);
                    let val = format!("value_{}", i);
                    engine.put(&cf, key.as_bytes(), val.as_bytes()).unwrap();

                    // Flush every 100 keys to create ~50 SST files
                    if i % 100 == 99 {
                        engine.flush().unwrap();
                    }
                }
                engine.flush().unwrap();
                // Engine dropped here, closing cleanly
            }

            // Now measure startup time (manifest loading + recovery)
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: path },
                memtable_size: 64 * 1024,
                enable_compaction: false,
                ..Default::default()
            };
            let engine = MidgeEngine::open(opts).unwrap();
            black_box(engine);
        })
    });

    group.finish();
}

criterion_group! {
    name = startup_large_group;
    config = criterion_config();
    targets = bench_engine_startup_100k_sst_files
}
criterion_main!(startup_large_group);
