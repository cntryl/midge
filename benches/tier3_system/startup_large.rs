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
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use tempfile::TempDir;

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

/// Benchmark engine startup with large manifest (simulated via many flushes)
fn bench_engine_startup_100k_sst_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_large_manifest");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1)); // One startup operation
    group.measurement_time(std::time::Duration::from_secs(5));
    group.sample_size(10);

    // Pre-compute keys and values outside the benchmark loop
    let keys: Vec<Vec<u8>> = (0..5000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..5000).map(make_value).collect();

    group.bench_function("startup_with_many_ssts", |b| {
        b.iter_batched(
            || {
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
                        engine.put(&cf, &keys[i], &values[i]).unwrap();

                        // Flush every 100 keys to create ~50 SST files
                        if i % 100 == 99 {
                            engine.flush().unwrap();
                        }
                    }
                    engine.flush().unwrap();
                    // Engine dropped here, closing cleanly
                }

                (path, tmp)
            },
            |(path, _tmp)| {
                // Now measure startup time (manifest loading + recovery)
                let opts = MidgeOptions {
                    storage_mode: StorageMode::LocalDisk { db_path: path },
                    memtable_size: 64 * 1024,
                    enable_compaction: false,
                    ..Default::default()
                };
                let engine = MidgeEngine::open(opts).unwrap();
                black_box(engine);
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier3_system_startup_large;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_engine_startup_100k_sst_files
}
criterion_main!(tier3_system_startup_large);
