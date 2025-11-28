//! Tier 3 — Startup WAL replay bench
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers engine startup with WAL replay
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
use std::hint::black_box;
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
    std::env::temp_dir().join(format!("midge_bench_startup_wal_{}_{}_{}", prefix, pid, counter))
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

/// Benchmark engine startup with WAL replay (50k operations)
fn bench_engine_startup_from_wal(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_startup_from_wal");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 50_000usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));
    group.bench_function("replay_50k_wal_ops", |b| {
        b.iter_batched(
            || {
                let path = unique_bench_path("wal_replay");
                let _ = std::fs::remove_dir_all(&path);

                // Create WAL with 50k operations WITHOUT flushing
                {
                    let opts = MidgeOptions {
                        storage_mode: StorageMode::LocalDisk {
                            db_path: path.clone(),
                        },
                        memtable_size: 100 * 1024 * 1024, // Large memtable = no auto flush
                        enable_compaction: false,
                        wal_sync: false,
                        ..Default::default()
                    };
                    let engine = MidgeEngine::open(opts).unwrap();
                    let cf = engine.default_column_family();

                    // Write ops to WAL without flushing
                    for i in 0..num_ops {
                        engine.put(&cf, &keys[i], &vals[i]).unwrap();
                    }
                    // DO NOT flush - keep data only in WAL
                }

                path
            },
            |path| {
                // Measure startup time (WAL replay into memtable)
                let opts = MidgeOptions {
                    storage_mode: StorageMode::LocalDisk { db_path: path },
                    memtable_size: 100 * 1024 * 1024,
                    enable_compaction: false,
                    wal_sync: false,
                    ..Default::default()
                };
                let engine = MidgeEngine::open(opts).unwrap();

                // Verify data was recovered from WAL
                let cf = engine.default_column_family();
                black_box(engine.get(&cf, &keys[25_000]).unwrap());

                engine // prevent Drop during timing
            },
            BatchSize::LargeInput,
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
