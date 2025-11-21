//! Tier 3 — Scan L0-only bench
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers L0-only scan operations (memtable + L0 SSTs)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use tempfile::TempDir;

fn setup_l0_only_db() -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("scan_l0");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 2 * 1024 * 1024,
        enable_compaction: false, // Keep everything in L0
        ..Default::default()
    };
    (MidgeEngine::open(opts).unwrap(), tmp)
}

/// Benchmark scanning L0 SSTs (10k keys spread across multiple L0 files)
fn bench_scan_l0_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_l0_direct");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));
    group.measurement_time(std::time::Duration::from_secs(5));
    group.sample_size(10);

    group.bench_function("scan_l0_10k_keys", |b| {
        b.iter(|| {
            let (engine, _tmp) = setup_l0_only_db();
            let cf = engine.default_column_family();
            
            // Populate and flush multiple times to create multiple L0 files
            for batch in 0..5 {
                for i in 0..2000 {
                    let key = format!("key_{:010}", batch * 2000 + i);
                    let val = format!("value_{}", i);
                    engine.put(&cf, key.as_bytes(), val.as_bytes()).unwrap();
                }
                engine.flush().unwrap(); // Creates L0 SST
            }

            // Now scan the entire range (all L0 files)
            let query = Query::new()
                .start_key("key_0000000000".as_bytes().into())
                .end_key("key_9999999999".as_bytes().into());
            
            let results = engine.scan(&cf, query).unwrap();
            black_box(results.len());
        })
    });

    group.finish();
}

criterion_group! {
    name = scan_l0_only_group;
    config = criterion_config();
    targets = bench_scan_l0_direct
}
criterion_main!(scan_l0_only_group);