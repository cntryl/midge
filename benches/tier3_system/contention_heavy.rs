//! Tier 3 — Contention-heavy benchmark
//!
//! **Target Runtime:** ~2 minutes
//! **Run Frequency:** Nightly / release builds
//!
//! Covers heavy contention scenarios with multiple threads competing for
//! concurrent access to the storage engine.
//!
//! ## Benchmarks
//!
//! - `system_engine_heavy_write_contention`: 16 threads writing concurrently
//! - `system_engine_heavy_read_contention`: 16 threads reading same keys
//! - `system_engine_mixed_contention`: Mixed read/write workload

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key sizes for consistent throughput measurement
const SHARED_KEY_SIZE: usize = 10; // "key_" + 6 digits
const THREAD_KEY_SIZE: usize = 14; // "t00_key_" + 6 digits
const VALUE_SIZE: usize = 64; // Fixed value size for accurate throughput

/// Pre-generate keys with format "key_{:06}"
#[inline]
fn make_key(i: usize) -> Vec<u8> {
    let mut key = vec![0u8; SHARED_KEY_SIZE];
    key[..4].copy_from_slice(b"key_");
    let mut n = i;
    for j in (4..SHARED_KEY_SIZE).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    key
}

/// Pre-generate keys with format "t{:02}_key_{:06}"
#[inline]
fn make_thread_key(tid: usize, i: usize) -> Vec<u8> {
    let mut key = vec![0u8; THREAD_KEY_SIZE];
    key[0] = b't';
    key[1] = b'0' + (tid / 10) as u8;
    key[2] = b'0' + (tid % 10) as u8;
    key[3] = b'_';
    key[4..8].copy_from_slice(b"key_");
    let mut n = i;
    for j in (8..THREAD_KEY_SIZE).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    key
}

/// Pre-generate fixed-size value
#[inline]
fn make_value(i: usize) -> Vec<u8> {
    let mut val = vec![0u8; VALUE_SIZE];
    // Store index in first 8 bytes for verification
    if VALUE_SIZE >= 8 {
        val[..8].copy_from_slice(&(i as u64).to_be_bytes());
    }
    // Fill rest with pattern
    let pattern = (i % 256) as u8;
    for byte in val.iter_mut().skip(8) {
        *byte = pattern;
    }
    val
}

/// Generate unique path for benchmark database
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_contention_{}_{}_{}", prefix, pid, counter))
}

fn setup_db(prefix: &str) -> (MidgeEngine, PathBuf) {
    let path = unique_bench_path(prefix);
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
        memtable_size: 8 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: false, // Disable WAL sync for raw throughput
        ..Default::default()
    };
    (MidgeEngine::open(opts).expect("failed to open engine"), path)
}

fn cleanup_path(path: PathBuf) {
    let _ = std::fs::remove_dir_all(&path);
}

/// Benchmark heavy write contention (16 threads, 1000 ops each)
fn bench_engine_heavy_write_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_heavy_write_contention");
    group.sampling_mode(SamplingMode::Flat);
    
    let num_threads = 16;
    let ops_per_thread = 1_000;
    let total_ops = num_threads * ops_per_thread;
    let total_bytes = total_ops * (THREAD_KEY_SIZE + VALUE_SIZE);
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    // Pre-compute all keys and values outside the benchmark loop
    let keys: Vec<Vec<Vec<u8>>> = (0..num_threads)
        .map(|tid| (0..ops_per_thread).map(|i| make_thread_key(tid, i)).collect())
        .collect();
    let values: Vec<Vec<u8>> = (0..ops_per_thread).map(make_value).collect();
    let keys = Arc::new(keys);
    let values = Arc::new(values);

    group.bench_function("write_16_threads", |b| {
        let keys = Arc::clone(&keys);
        let values = Arc::clone(&values);
        b.iter(|| {
            let (engine, path) = setup_db("write_contention");
            let engine = Arc::new(engine);
            let cf = engine.default_column_family();

            thread::scope(|scope| {
                for tid in 0..num_threads {
                    let engine = Arc::clone(&engine);
                    let cf = cf.clone();
                    let keys = Arc::clone(&keys);
                    let values = Arc::clone(&values);

                    scope.spawn(move || {
                        for i in 0..ops_per_thread {
                            engine
                                .put(&cf, &keys[tid][i], &values[i])
                                .expect("put failed");
                        }
                    });
                }
            });

            cleanup_path(path);
            black_box(());
        })
    });

    group.finish();
}

/// Benchmark heavy read contention (16 threads reading same keys)
fn bench_engine_heavy_read_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_heavy_read_contention");
    group.sampling_mode(SamplingMode::Flat);
    
    let num_threads = 16;
    let num_keys = 2_000;
    let reads_per_thread = num_keys;
    let total_reads = num_threads * reads_per_thread;
    // Throughput in reads (elements) since read data varies
    group.throughput(Throughput::Elements(total_reads as u64));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    // Pre-compute keys and values outside the benchmark loop
    let keys: Vec<Vec<u8>> = (0..num_keys).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..num_keys).map(make_value).collect();
    let keys = Arc::new(keys);
    let values = Arc::new(values);

    group.bench_function("read_16_threads", |b| {
        let keys = Arc::clone(&keys);
        let values = Arc::clone(&values);
        b.iter(|| {
            let (engine, path) = setup_db("read_contention");
            let cf = engine.default_column_family();

            // Pre-populate with data (using precomputed keys/values)
            for i in 0..num_keys {
                engine.put(&cf, &keys[i], &values[i]).expect("put failed");
            }
            engine.flush().expect("flush failed");

            let engine = Arc::new(engine);

            thread::scope(|scope| {
                for _ in 0..num_threads {
                    let engine = Arc::clone(&engine);
                    let cf = cf.clone();
                    let keys = Arc::clone(&keys);

                    scope.spawn(move || {
                        for i in 0..num_keys {
                            let _ = engine.get(&cf, &keys[i]);
                        }
                    });
                }
            });

            cleanup_path(path);
            black_box(());
        })
    });

    group.finish();
}

/// Benchmark mixed read/write contention
fn bench_engine_mixed_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_engine_mixed_contention");
    group.sampling_mode(SamplingMode::Flat);
    
    let num_threads = 16;
    let ops_per_thread = 1_500;
    let total_ops = num_threads * ops_per_thread;
    // Approximately half reads, half writes
    let total_bytes = (total_ops / 2) * (SHARED_KEY_SIZE + VALUE_SIZE);
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.measurement_time(std::time::Duration::from_secs(3));
    group.sample_size(10);

    // Pre-compute keys outside the benchmark loop
    let keys: Vec<Vec<u8>> = (0..ops_per_thread).map(make_key).collect();
    // Pre-compute thread-specific values with fixed size
    let thread_values: Vec<Vec<Vec<u8>>> = (0..num_threads)
        .map(|tid| (0..ops_per_thread).map(|i| make_value(tid * ops_per_thread + i)).collect())
        .collect();
    let keys = Arc::new(keys);
    let thread_values = Arc::new(thread_values);

    group.bench_function("mixed_16_threads", |b| {
        let keys = Arc::clone(&keys);
        let thread_values = Arc::clone(&thread_values);
        b.iter(|| {
            let (engine, path) = setup_db("mixed_contention");
            let cf = engine.default_column_family();

            // Pre-populate with init values
            let init_value = make_value(0);
            for i in 0..ops_per_thread {
                engine.put(&cf, &keys[i], &init_value).expect("put failed");
            }

            let engine = Arc::new(engine);

            thread::scope(|scope| {
                for tid in 0..num_threads {
                    let engine = Arc::clone(&engine);
                    let cf = cf.clone();
                    let keys = Arc::clone(&keys);
                    let thread_values = Arc::clone(&thread_values);

                    scope.spawn(move || {
                        for i in 0..ops_per_thread {
                            if (tid + i) % 2 == 0 {
                                // Write
                                engine
                                    .put(&cf, &keys[i], &thread_values[tid][i])
                                    .expect("put failed");
                            } else {
                                // Read
                                let _ = engine.get(&cf, &keys[i]);
                            }
                        }
                    });
                }
            });

            cleanup_path(path);
            black_box(());
        })
    });

    group.finish();
}

criterion_group! {
    name = tier3_system_contention_heavy;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets = bench_engine_heavy_write_contention, bench_engine_heavy_read_contention, bench_engine_mixed_contention
}
criterion_main!(tier3_system_contention_heavy);
