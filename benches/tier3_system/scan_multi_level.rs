//! Tier 3 — Scan multi-level benchmark
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers LSM scans across multiple levels

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use tempfile::TempDir;

fn setup_multi_level_db() -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("scan_multi");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 1 * 1024 * 1024,
        enable_compaction: true, // Enable to get multi-level LSM
        ..Default::default()
    };
    (MidgeEngine::open(opts).unwrap(), tmp)
}

/// Benchmark scanning across multiple LSM levels (50k keys)
fn bench_scan_multi_level_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_multi_level_range");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50_000));
    group.measurement_time(std::time::Duration::from_secs(8));
    group.sample_size(10);

    group.bench_function("scan_50k_keys", |b| {
        b.iter(|| {
            let (engine, _tmp) = setup_multi_level_db();
            let cf = engine.default_column_family();
            
            // Populate with 50k keys to trigger multiple flushes and compactions
            for i in 0..50_000 {
                let key = format!("key_{:010}", i);
                let val = format!("value_{}", i);
                engine.put(&cf, key.as_bytes(), val.as_bytes()).unwrap();
                
                // Flush periodically to create multiple files
                if i % 5000 == 4999 {
                    engine.flush().unwrap();
                }
            }
            
            // Trigger compactions to spread data across levels
            engine.flush().unwrap();
            let _ = engine.compact_level(&cf, 0); // Compact L0 to L1

            // Now scan a large range across all levels
            let query = Query::new()
                .start_key("key_0000000000".as_bytes())
                .end_key("key_0000049999".as_bytes());
            
            let results = engine.scan(&cf, query).unwrap();
            black_box(results.len());
        })
    });

    group.finish();
}

criterion_group! {
    name = scan_multi_level_group;
    config = criterion_config();
    targets = bench_scan_multi_level_range
}
criterion_main!(scan_multi_level_group);
