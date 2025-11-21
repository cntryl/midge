//! Tier 3 — Startup WAL replay bench
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers engine startup with WAL replay

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use tempfile::TempDir;

/// Benchmark engine startup with WAL replay (50k operations)
fn bench_engine_startup_from_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_from_wal");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50_000)); // 50k WAL ops replayed
    group.measurement_time(std::time::Duration::from_secs(5));
    group.sample_size(10);

    group.bench_function("replay_50k_wal_ops", |b| {
        b.iter(|| {
            let tmp = TempDir::new().expect("tempdir");
            let path = tmp.path().join("startup_wal");
            
            // Phase 1: Create WAL with 50k operations WITHOUT flushing
            {
                let opts = MidgeOptions {
                    storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
                    memtable_size: 100 * 1024 * 1024, // Large memtable = no auto flush
                    enable_compaction: false,
                    wal_sync: false, // Faster WAL writes for setup
                    ..Default::default()
                };
                let engine = MidgeEngine::open(opts).unwrap();
                let cf = engine.default_column_family();
                
                // Write 50k ops to WAL without flushing
                for i in 0..50_000 {
                    let key = format!("key_{:010}", i);
                    let val = format!("value_{}", i);
                    engine.put(&cf, key.as_bytes(), val.as_bytes()).unwrap();
                }
                // DO NOT flush - keep data only in WAL
                // Engine closes, WAL persisted
            }

            // Phase 2: Measure startup time (WAL replay into memtable)
            let opts = MidgeOptions {
                storage_mode: StorageMode::LocalDisk { db_path: path },
                memtable_size: 100 * 1024 * 1024,
                enable_compaction: false,
                ..Default::default()
            };
            let engine = MidgeEngine::open(opts).unwrap();
            
            // Verify data was recovered from WAL
            let cf = engine.default_column_family();
            let result = engine.get(&cf, b"key_0000025000");
            black_box(result);
        })
    });

    group.finish();
}

criterion_group! {
    name = startup_wal_group;
    config = criterion_config();
    targets = bench_engine_startup_from_wal
}
criterion_main!(startup_wal_group);