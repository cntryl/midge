//! Tier 3 — Startup large dataset bench
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers engine startup with large manifest (many SST files)
//!
//! ## Design Notes
//!
//! - Returns engine from timed closures to exclude teardown from timing
//! - Precomputes all keys/values outside hot loops
//! - Uses unique paths to avoid cross-iteration interference
//! - Throughput measured in bytes

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key size in bytes
const KEY_SIZE: usize = 14;
/// Value size in bytes  
const VALUE_SIZE: usize = 128;
/// Bytes per operation
const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

/// Generate unique path for benchmark database
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_startup_large_{}_{}_{}", prefix, pid, counter))
}

#[inline]
fn make_key(i: usize) -> Bytes {
    let mut key = vec![0u8; KEY_SIZE];
    key[..4].copy_from_slice(b"key_");
    let mut n = i;
    for j in (4..KEY_SIZE).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    Bytes::from(key)
}

#[inline]
fn make_value_fixed(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn precompute_kv(n: usize, value_size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value_fixed(value_size));
    }
    (keys, vals)
}

/// Benchmark engine startup with large manifest (simulated via many flushes)
fn bench_engine_startup_100k_sst_files(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_large_manifest");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 5_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    // Throughput = bytes in manifest being loaded
    let bytes_total = (num_keys as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));
    group.bench_function("startup_with_many_ssts", |b| {
        b.iter_batched(
            || {
                let path = unique_bench_path("large_manifest");
                let _ = std::fs::remove_dir_all(&path);

                // Create database and populate with many small flushes
                {
                    let opts = MidgeOptions {
                        storage_mode: StorageMode::LocalDisk {
                            db_path: path.clone(),
                        },
                        memtable_size: 64 * 1024, // Small memtable = more SSTs
                        enable_compaction: false,
                        wal_sync: false,
                        ..Default::default()
                    };
                    let engine = MidgeEngine::open(opts).unwrap();
                    let cf = engine.default_column_family();

                    // Write keys with periodic flushes to create ~50 SST files
                    for i in 0..num_keys {
                        engine.put(&cf, &keys[i], &vals[i]).unwrap();

                        if i % 100 == 99 {
                            engine.flush().unwrap();
                        }
                    }
                    engine.flush().unwrap();
                }

                path
            },
            |path| {
                // Measure startup time (manifest loading + recovery)
                let opts = MidgeOptions {
                    storage_mode: StorageMode::LocalDisk { db_path: path },
                    memtable_size: 64 * 1024,
                    enable_compaction: false,
                    wal_sync: false,
                    ..Default::default()
                };
                MidgeEngine::open(opts).unwrap() // prevent Drop during timing
            },
            BatchSize::LargeInput,
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
