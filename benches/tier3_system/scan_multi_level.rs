//! Tier 3 — Scan multi-level benchmark
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Covers LSM scans across multiple levels
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
use cntryl_midge::{MidgeEngine, MidgeOptions, Query, StorageMode};
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
    std::env::temp_dir().join(format!("midge_bench_scan_multi_{}_{}_{}", prefix, pid, counter))
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

fn setup_db(name: &str) -> MidgeEngine {
    let path = unique_bench_path(name);
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 1024 * 1024,
        enable_compaction: true, // Enable to get multi-level LSM
        wal_sync: false,
        ..Default::default()
    };
    MidgeEngine::open(opts).unwrap()
}

/// Benchmark scanning across multiple LSM levels (50k keys)
fn bench_scan_multi_level_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_scan_multi_level_range");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 50_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    let bytes_total = (num_keys as u64) * BYTES_PER_OP;

    // Precompute query bounds
    let start_key: Bytes = Bytes::from_static(b"key_0000000000");
    let end_key: Bytes = Bytes::from_static(b"key_0000049999");

    group.throughput(Throughput::Bytes(bytes_total));
    group.bench_function("scan_50k_keys", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("scan_multi");
                let cf = engine.default_column_family();

                // Populate with 50k keys to trigger multiple flushes and compactions
                for i in 0..num_keys {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();

                    // Flush periodically to create multiple files
                    if i % 5000 == 4999 {
                        engine.flush().unwrap();
                    }
                }

                // Trigger compactions to spread data across levels
                engine.flush().unwrap();
                let _ = engine.compact_level(&cf, 0); // Compact L0 to L1

                engine
            },
            |engine| {
                // Scan a large range across all levels
                let cf = engine.default_column_family();
                let query = Query::new()
                    .start_key(start_key.clone())
                    .end_key(end_key.clone());

                let results = engine.scan(&cf, query).unwrap();
                black_box(results.len());

                engine // prevent Drop during timing
            },
            BatchSize::LargeInput,
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
