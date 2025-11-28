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

/// Pre-generate keys without format! allocations
fn make_key(i: usize) -> Vec<u8> {
    let mut key = vec![0u8; 14];
    key[..4].copy_from_slice(b"key_");
    let mut n = i;
    for j in (4..14).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    key
}

/// Pre-generate values without format! allocations
fn make_value(i: usize) -> Vec<u8> {
    let mut val = Vec::with_capacity(16);
    val.extend_from_slice(b"value_");
    if i == 0 {
        val.push(b'0');
    } else {
        let start = val.len();
        let mut n = i;
        while n > 0 {
            val.push(b'0' + (n % 10) as u8);
            n /= 10;
        }
        val[start..].reverse();
    }
    val
}

/// Benchmark scanning L0 SSTs (10k keys spread across multiple L0 files)
fn bench_scan_l0_direct(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_l0_direct");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));
    group.measurement_time(std::time::Duration::from_secs(5));
    group.sample_size(10);

    // Pre-compute keys and values outside the benchmark loop
    let keys: Vec<Vec<u8>> = (0..10_000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..2000).map(make_value).collect();

    group.bench_function("scan_l0_10k_keys", |b| {
        b.iter_batched(
            || {
                let (engine, tmp) = setup_l0_only_db();
                let cf = engine.default_column_family();

                // Populate and flush multiple times to create multiple L0 files
                for batch in 0..5 {
                    for i in 0..2000 {
                        let idx = batch * 2000 + i;
                        engine.put(&cf, &keys[idx], &values[i]).unwrap();
                    }
                    engine.flush().unwrap(); // Creates L0 SST
                }

                (engine, tmp)
            },
            |(engine, _tmp)| {
                // Now scan the entire range (all L0 files)
                let cf = engine.default_column_family();
                let query = Query::new()
                    .start_key("key_0000000000".as_bytes().into())
                    .end_key("key_9999999999".as_bytes().into());

                let results = engine.scan(&cf, query).unwrap();
                black_box(results.len());
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier3_system_scan_l0_only;
    config = criterion_config();
    targets = bench_scan_l0_direct
}
criterion_main!(tier3_system_scan_l0_only);
