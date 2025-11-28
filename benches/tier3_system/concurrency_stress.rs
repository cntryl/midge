//! Tier 3 — Concurrency Stress & Compaction Benchmarks
//!
//! **Target Runtime:** ~10 seconds per benchmark
//! **Run Frequency:** Nightly CI / Perf Baselines
//!
//! Focus areas:
//! - Concurrent writer scaling (1-16 threads)
//! - Read/write contention patterns
//! - Compaction interference under sustained load
//! - Delete-heavy concurrent operations
//! - Column family scalability under concurrent access

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key size in bytes (14 = "key_" + 10 digit number)
const KEY_SIZE: usize = 14;

#[inline]
fn make_key(i: usize) -> Bytes {
    // Fixed-size key using direct byte manipulation (no format! allocations)
    let mut key = vec![0u8; KEY_SIZE];
    key[..4].copy_from_slice(b"key_");
    // Write i as 10-digit decimal directly
    let mut n = i;
    for j in (4..KEY_SIZE).rev() {
        key[j] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    Bytes::from(key)
}

#[inline]
fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

fn precompute_kv(n: usize, value_size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value(value_size));
    }
    (keys, vals)
}

/// Generate unique path for benchmark to avoid cross-iteration interference
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_t3_stress_{}_{}_{}", prefix, pid, counter))
}

/// Wrapper to ensure cleanup on drop
struct BenchDb {
    engine: MidgeEngine,
    path: PathBuf,
}

impl BenchDb {
    fn new(prefix: &str, compaction: bool) -> Self {
        let path = unique_bench_path(prefix);
        let _ = std::fs::remove_dir_all(&path);
        let opts = MidgeOptions {
            storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
            memtable_size: 4 * 1024 * 1024,
            enable_compaction: compaction,
            wal_sync: false, // Disable WAL sync for raw throughput measurement
            ..Default::default()
        };
        let engine = MidgeEngine::open(opts).expect("failed to open engine");
        Self { engine, path }
    }
}

impl Drop for BenchDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Create db for Arc usage (returns engine and path for cleanup)
fn setup_db_arc(prefix: &str, compaction: bool) -> (Arc<MidgeEngine>, PathBuf) {
    let path = unique_bench_path(prefix);
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path.clone() },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: compaction,
        wal_sync: false,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("failed to open engine");
    (Arc::new(engine), path)
}

fn cleanup_path(path: PathBuf) {
    let _ = std::fs::remove_dir_all(&path);
}

// ============================================================================
// Concurrent Writer Scaling
// ============================================================================

fn bench_concurrent_puts(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_concurrent_puts");
    group.sampling_mode(SamplingMode::Flat);

    let max_threads = 16;
    let n_ops = 5_000;
    let value_size = 128;
    let total_ops = max_threads * n_ops;
    let (keys, vals) = precompute_kv(total_ops, value_size);
    let keys = Arc::new(keys);
    let vals = Arc::new(vals);

    for &threads in &[1, 2, 4, 8, 16] {
        let ops_per_iter = threads * n_ops;
        let bytes_per_iter = ops_per_iter * (KEY_SIZE + value_size);
        group.throughput(Throughput::Bytes(bytes_per_iter as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}threads", threads)),
            &threads,
            |b, &tcount| {
                let keys = Arc::clone(&keys);
                let vals = Arc::clone(&vals);
                b.iter_batched(
                    || setup_db_arc("concurrent", false),
                    |(engine, path)| {
                        let cf = engine.default_column_family();
                        thread::scope(|scope| {
                            for tid in 0..tcount {
                                let engine = Arc::clone(&engine);
                                let cf = cf.clone();
                                let keys = Arc::clone(&keys);
                                let vals = Arc::clone(&vals);
                                scope.spawn(move || {
                                    let offset = tid * n_ops;
                                    for i in 0..n_ops {
                                        let idx = offset + i;
                                        engine
                                            .put(&cf, &keys[idx], &vals[idx])
                                            .expect("put failed");
                                    }
                                });
                            }
                        });
                        cleanup_path(path);
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Read/Write Contention
// ============================================================================

fn bench_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_read_write_contention");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute data to avoid allocations in hot path
    let prefill_keys: Vec<_> = (0..10_000).map(make_key).collect();
    let prefill_vals: Vec<_> = (0..10_000).map(|_| make_value(64)).collect();
    let writer_keys: Vec<_> = (0..4_000).map(|i| make_key(i + 20_000)).collect();
    let writer_vals: Vec<_> = (0..4_000).map(|_| make_value(128)).collect();
    let writer_keys = Arc::new(writer_keys);
    let writer_vals = Arc::new(writer_vals);
    let reader_keys: Vec<_> = (0..10_000).step_by(3).map(make_key).collect();
    let reader_keys = Arc::new(reader_keys);

    // Calculate throughput: 4 writers * 1000 ops + 4 readers * ~3333 ops
    let total_ops = 4 * 1_000 + 4 * reader_keys.len();
    group.throughput(Throughput::Elements(total_ops as u64));

    group.bench_function("4w4r_threads", |b| {
        let writer_keys = Arc::clone(&writer_keys);
        let writer_vals = Arc::clone(&writer_vals);
        let reader_keys = Arc::clone(&reader_keys);
        
        b.iter_batched(
            || setup_db_arc("mixed", false),
            |(engine, path)| {
                let cf = engine.default_column_family();
                let writer_keys = Arc::clone(&writer_keys);
                let writer_vals = Arc::clone(&writer_vals);
                let reader_keys = Arc::clone(&reader_keys);
                
                // Prefill (outside timed section ideally, but we measure full scenario)
                for i in 0..10_000 {
                    engine.put(&cf, &prefill_keys[i], &prefill_vals[i]).expect("prefill failed");
                }

                thread::scope(|scope| {
                    // Writers
                    for t in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf = cf.clone();
                        let writer_keys = Arc::clone(&writer_keys);
                        let writer_vals = Arc::clone(&writer_vals);
                        scope.spawn(move || {
                            for i in 0..1_000 {
                                let idx = t * 1_000 + i;
                                e.put(&cf, &writer_keys[idx], &writer_vals[idx]).expect("write failed");
                            }
                        });
                    }
                    // Readers
                    for _ in 0..4 {
                        let e = Arc::clone(&engine);
                        let cf = cf.clone();
                        let reader_keys = Arc::clone(&reader_keys);
                        scope.spawn(move || {
                            for j in 0..reader_keys.len() {
                                let _ = e.get(&cf, &reader_keys[j]);
                            }
                        });
                    }
                });
                cleanup_path(path);
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Compaction Stress
// ============================================================================

fn bench_compaction_pressure(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_compaction_pressure");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute data
    let compaction_keys: Vec<_> = (0..25_000).map(make_key).collect();
    let compaction_vals: Vec<_> = (0..25_000).map(|_| make_value(256)).collect();
    let verify_keys: Vec<_> = (0..1_000).step_by(50).map(make_key).collect();

    let total_bytes = 25_000 * (KEY_SIZE + 256);
    group.throughput(Throughput::Bytes(total_bytes as u64));

    group.bench_function("steady_write_with_compaction", |b| {
        b.iter_batched(
            || BenchDb::new("compacting", true),
            |db| {
                let cf = db.engine.default_column_family();
                for round in 0..5 {
                    for i in 0..5_000 {
                        let idx = round * 5_000 + i;
                        db.engine
                            .put(&cf, &compaction_keys[idx], &compaction_vals[idx])
                            .expect("write failed");
                    }
                    // Small yield to allow compaction progress (avoid blocking the whole benchmark)
                    std::thread::yield_now();
                }
                // Verify a few reads during/after compaction
                for key in &verify_keys {
                    let _ = db.engine.get(&cf, key);
                }
                black_box(());
                // BenchDb cleanup happens on drop
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Delete-Heavy Concurrent Operations
// ============================================================================

fn bench_concurrent_deletes(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_concurrent_deletes");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute data
    let prefill_keys: Vec<_> = (0..10_000).map(make_key).collect();
    let prefill_vals: Vec<_> = (0..10_000).map(|_| make_value(100)).collect();
    let delete_keys: Vec<_> = (0..10_000).map(make_key).collect();
    let delete_keys = Arc::new(delete_keys);

    for &threads in &[2, 4, 8] {
        group.throughput(Throughput::Elements(10_000));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}threads", threads)),
            &threads,
            |b, &tcount| {
                let delete_keys = Arc::clone(&delete_keys);
                b.iter_batched(
                    || {
                        let (engine, path) = setup_db_arc("delete_concurrent", false);
                        let cf = engine.default_column_family();
                        // Prefill with 10k keys
                        for i in 0..10_000 {
                            engine.put(&cf, &prefill_keys[i], &prefill_vals[i]).expect("prefill failed");
                        }
                        (engine, path)
                    },
                    |(engine, path)| {
                        let cf = engine.default_column_family();
                        let delete_keys = Arc::clone(&delete_keys);
                        thread::scope(|scope| {
                            for tid in 0..tcount {
                                let engine = Arc::clone(&engine);
                                let cf = cf.clone();
                                let delete_keys = Arc::clone(&delete_keys);
                                scope.spawn(move || {
                                    let offset = tid * (10_000 / tcount);
                                    let count = 10_000 / tcount;
                                    for i in 0..count {
                                        engine.delete(&cf, &delete_keys[offset + i]).ok();
                                    }
                                });
                            }
                        });
                        cleanup_path(path);
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Column Family Concurrent Operations
// ============================================================================

/// Benchmark concurrent writes across multiple column families
fn bench_concurrent_multi_cf(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_concurrent_multi_cf");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute for max pairs=8, 8*2*2500=40000
    let value_size = 150;
    let multi_cf_keys: Vec<_> = (0..40_000).map(make_key).collect();
    let multi_cf_vals: Vec<_> = (0..40_000).map(|_| make_value(value_size)).collect();
    let multi_cf_keys = Arc::new(multi_cf_keys);
    let multi_cf_vals = Arc::new(multi_cf_vals);

    for &thread_pairs in &[2, 4, 8] {
        let ops_per_iter = thread_pairs * 2 * 2_500; // pairs * threads_per_cf * ops_per_thread
        let bytes_per_iter = ops_per_iter * (KEY_SIZE + value_size);
        group.throughput(Throughput::Bytes(bytes_per_iter as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}cfs", thread_pairs)),
            &thread_pairs,
            |b, &pairs| {
                let multi_cf_keys = Arc::clone(&multi_cf_keys);
                let multi_cf_vals = Arc::clone(&multi_cf_vals);
                b.iter_batched(
                    || {
                        let (engine, path) = setup_db_arc("multi_cf", false);
                        // Create N column families
                        for i in 1..pairs {
                            engine
                                .create_column_family(&format!("cf{}", i), Default::default())
                                .ok();
                        }
                        (engine, path)
                    },
                    |(engine, path)| {
                        let cf_list = engine.list_column_families();
                        let multi_cf_keys = Arc::clone(&multi_cf_keys);
                        let multi_cf_vals = Arc::clone(&multi_cf_vals);
                        thread::scope(|scope| {
                            // 2 threads per CF
                            for (cf_idx, cf) in cf_list.iter().enumerate().take(pairs) {
                                for tid in 0..2 {
                                    let engine = Arc::clone(&engine);
                                    let cf = cf.clone();
                                    let multi_cf_keys = Arc::clone(&multi_cf_keys);
                                    let multi_cf_vals = Arc::clone(&multi_cf_vals);
                                    scope.spawn(move || {
                                        let base = cf_idx * 2 * 2_500 + tid * 2_500;
                                        for i in 0..2_500 {
                                            engine
                                                .put(
                                                    &cf,
                                                    &multi_cf_keys[base + i],
                                                    &multi_cf_vals[base + i],
                                                )
                                                .expect("write failed");
                                        }
                                    });
                                }
                            }
                        });
                        cleanup_path(path);
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_concurrency_stress;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_concurrent_puts,
        bench_mixed_read_write,
        bench_compaction_pressure,
        bench_concurrent_deletes,
        bench_concurrent_multi_cf
}
criterion_main!(tier3_system_concurrency_stress);
