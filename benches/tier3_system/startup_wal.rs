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

/// Benchmark engine startup with WAL replay (50k operations)
fn bench_engine_startup_from_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_from_wal");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50_000)); // 50k WAL ops replayed
    group.measurement_time(std::time::Duration::from_secs(5));
    group.sample_size(10);

    // Pre-compute keys and values outside the benchmark loop
    let keys: Vec<Vec<u8>> = (0..50_000).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..50_000).map(make_value).collect();

    group.bench_function("replay_50k_wal_ops", |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().expect("tempdir");
                let path = tmp.path().join("startup_wal");

                // Phase 1: Create WAL with 50k operations WITHOUT flushing
                {
                    let opts = MidgeOptions {
                        storage_mode: StorageMode::LocalDisk {
                            db_path: path.clone(),
                        },
                        memtable_size: 100 * 1024 * 1024, // Large memtable = no auto flush
                        enable_compaction: false,
                        wal_sync: false, // Faster WAL writes for setup
                        ..Default::default()
                    };
                    let engine = MidgeEngine::open(opts).unwrap();
                    let cf = engine.default_column_family();

                    // Write 50k ops to WAL without flushing
                    for i in 0..50_000 {
                        engine.put(&cf, &keys[i], &values[i]).unwrap();
                    }
                    // DO NOT flush - keep data only in WAL
                    // Engine closes, WAL persisted
                }

                (path, tmp)
            },
            |(path, _tmp)| {
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
                let result = engine.get(&cf, b"key_0000025000").unwrap();
                black_box(result);
            },
            criterion::BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier3_system_startup_wal;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_engine_startup_from_wal
}
criterion_main!(tier3_system_startup_wal);
