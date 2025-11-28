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
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use tempfile::TempDir;

fn setup_multi_level_db() -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join("scan_multi");
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 1024 * 1024,
        enable_compaction: true, // Enable to get multi-level LSM
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

/// Benchmark scanning across multiple LSM levels (50k keys)
fn bench_scan_multi_level_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_multi_level_range");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50_000));
    group.measurement_time(std::time::Duration::from_secs(8));
    group.sample_size(10);

    // Pre-compute keys and values outside the benchmark loop
    let keys: Vec<Vec<u8>> = (0..50_000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..50_000).map(make_value).collect();

    group.bench_function("scan_50k_keys", |b| {
        b.iter_batched(
            || {
                let (engine, tmp) = setup_multi_level_db();
                let cf = engine.default_column_family();

                // Populate with 50k keys to trigger multiple flushes and compactions
                for i in 0..50_000 {
                    engine.put(&cf, &keys[i], &values[i]).unwrap();

                    // Flush periodically to create multiple files
                    if i % 5000 == 4999 {
                        engine.flush().unwrap();
                    }
                }

                // Trigger compactions to spread data across levels
                engine.flush().unwrap();
                let _ = engine.compact_level(&cf, 0); // Compact L0 to L1

                (engine, tmp)
            },
            |(engine, _tmp)| {
                // Now scan a large range across all levels
                let cf = engine.default_column_family();
                let query = Query::new()
                    .start_key("key_0000000000".as_bytes().into())
                    .end_key("key_0000049999".as_bytes().into());

                let results = engine.scan(&cf, query).unwrap();
                black_box(results.len());
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier3_system_scan_multi_level;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_scan_multi_level_range
}
criterion_main!(tier3_system_scan_multi_level);
