//! Tier 3 — Basic Engine Benchmarks
//!
//! **Target Runtime:** ~60-120 seconds
//! **Run Frequency:** Nightly CI
//!
//! Covers fundamental engine operations with full engine setup:
//! - CRUD operations (put/get/delete, random vs sequential)
//! - Write strategies (sync modes, batch sizes)
//! - Memory mode performance
//!
//! ## Design Notes
//!
//! - Returns engine from timed closure to avoid engine Drop during timing
//! - Engine teardown can take 2+ seconds due to thread joins
//! - Throughput measured in bytes (KEY_SIZE + value_size per op)
//! - Uses SamplingMode::Flat for system benchmarks

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use cntryl_midge::api::column_family::ColumnFamilyConfig;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::hint::black_box;

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key size in bytes
const KEY_SIZE: usize = 14;
/// Default value size for benchmarks
const VALUE_SIZE: usize = 100;

/// Bytes per operation (key + value)
const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

/// Generate unique path for benchmark database
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_basic_{}_{}_{}", prefix, pid, counter))
}

fn setup_db(name: &str, enable_wal_sync: bool) -> MidgeEngine {
    let path = unique_bench_path(name);
    let _ = std::fs::remove_dir_all(&path);
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 4 * 1024 * 1024,
        enable_compaction: false,
        wal_sync: enable_wal_sync,
        ..Default::default()
    };
    MidgeEngine::open(opts).expect("failed to open engine")
}

fn setup_db_arc(name: &str) -> Arc<MidgeEngine> {
    Arc::new(setup_db(name, false))
}

#[inline]
fn make_key(i: usize) -> Bytes {
    // Fixed-size key using direct byte manipulation (no format! allocations)
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

// ============================================================================
// CRUD Operations - PUT
// ============================================================================

fn bench_put_variants(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_put_variants");
    group.sampling_mode(SamplingMode::Flat);

    for &op_count in &[100, 1000] {
        let (keys, vals) = precompute_kv(op_count, VALUE_SIZE);
        let bytes_total = (op_count as u64) * BYTES_PER_OP;
        group.throughput(Throughput::Bytes(bytes_total));

        group.bench_with_input(
            BenchmarkId::new("sequential", op_count),
            &op_count,
            |b, &n| {
                b.iter_batched(
                    || setup_db("sequential", false),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..n {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }
                        // Return engine to prevent Drop during timing
                        engine
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        // Random order writes
        let (keys_random, vals_random) = precompute_kv(op_count, VALUE_SIZE);
        let mut rng = StdRng::seed_from_u64(42);
        let mut indices: Vec<usize> = (0..op_count).collect();
        indices.shuffle(&mut rng);

        group.throughput(Throughput::Bytes(bytes_total));
        group.bench_with_input(BenchmarkId::new("random", op_count), &op_count, |b, &n| {
            b.iter_batched(
                || setup_db("random", false),
                |engine| {
                    let cf = engine.default_column_family();
                    for &i in indices.iter().take(n) {
                        engine.put(&cf, &keys_random[i], &vals_random[i]).unwrap();
                    }
                    engine
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

// ============================================================================
// Concurrent CF scaling (2-8 threads)
// Uses Arc for shared engine access across threads
// ============================================================================

fn bench_concurrent_cf_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_concurrent_cf_scaling");
    group.sampling_mode(SamplingMode::Flat);

    let thread_counts = [2usize, 4, 8];
    let ops_per_thread = 200usize;

    for &threads in &thread_counts {
        let total_ops = threads * ops_per_thread;
        let bytes_total = (total_ops as u64) * BYTES_PER_OP;
        group.throughput(Throughput::Bytes(bytes_total));

        // Precompute all KV pairs outside timing
        let all_kv: Vec<Vec<(Bytes, Bytes)>> = (0..threads)
            .map(|t| {
                (0..ops_per_thread)
                    .map(|i| {
                        let idx = i + t * ops_per_thread;
                        (make_key(idx), make_value_fixed(VALUE_SIZE))
                    })
                    .collect()
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter_custom(|iters| {
                    let mut total_elapsed = Duration::ZERO;

                    for _ in 0..iters {
                        let engine = setup_db_arc(&format!("concurrent_{}", threads));

                        // Create column families
                        let mut cfs = vec![engine.default_column_family()];
                        for i in 1..threads {
                            let name = format!("bench_cf_{}", i);
                            let cf = engine
                                .create_column_family(&name, ColumnFamilyConfig::default())
                                .unwrap();
                            cfs.push(cf);
                        }

                        let start = Instant::now();

                        // Use thread::scope with Arc for concurrent access
                        thread::scope(|s| {
                            for t in 0..threads {
                                let engine = Arc::clone(&engine);
                                let cf = cfs[t].clone();
                                let thread_kvs = &all_kv[t];
                                s.spawn(move || {
                                    for (k, v) in thread_kvs.iter() {
                                        engine.put(&cf, k, v).unwrap();
                                    }
                                });
                            }
                        });

                        total_elapsed += start.elapsed();
                        // Engine dropped outside timing
                    }

                    total_elapsed
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// GET Operations
// ============================================================================

fn bench_get_hit_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_get");
    group.sampling_mode(SamplingMode::Flat);

    // Precompute keys for gets
    let num_keys = 1000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);
    let get_indices: Vec<usize> = (0..num_keys).step_by(4).collect();
    let get_count = get_indices.len();

    group.throughput(Throughput::Bytes((get_count as u64) * BYTES_PER_OP));
    group.bench_function("hit_mixed", |b| {
        // Setup: create engine with data
        let engine = setup_db("get_hit", false);
        let cf = engine.default_column_family();
        for i in 0..num_keys {
            engine.put(&cf, &keys[i], &vals[i]).unwrap();
        }

        // Precompute lookup keys
        let lookup_keys: Vec<Bytes> = get_indices.iter().map(|&i| keys[i].clone()).collect();

        b.iter(|| {
            for k in &lookup_keys {
                black_box(engine.get(&cf, k).unwrap());
            }
        });
        // Engine lives until bench_function ends
    });

    // Miss benchmark
    let miss_count = 100usize;
    let miss_keys: Vec<Bytes> = (num_keys..num_keys + miss_count).map(make_key).collect();
    group.throughput(Throughput::Elements(miss_count as u64));

    group.bench_function("miss_random", |b| {
        let engine = setup_db("get_miss", false);
        let cf = engine.default_column_family();

        b.iter(|| {
            for k in &miss_keys {
                black_box(engine.get(&cf, k).unwrap());
            }
        });
    });

    group.finish();
}

// ============================================================================
// DELETE Operations
// ============================================================================

fn bench_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_delete");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 1000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    group.throughput(Throughput::Bytes((num_keys as u64) * BYTES_PER_OP));
    group.bench_function("delete_existing", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("delete", false);
                let cf = engine.default_column_family();
                for i in 0..num_keys {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                for k in &keys {
                    engine.delete(&cf, k).unwrap();
                }
                engine // prevent Drop during timing
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Write Modes - Compare sync vs async WAL
// ============================================================================

fn bench_write_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_write_modes");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 500usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));
    group.bench_function("nosync_batched", |b| {
        b.iter_batched(
            || setup_db("nosync", false),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..num_ops {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();
                }
                engine
            },
            BatchSize::SmallInput,
        )
    });

    group.throughput(Throughput::Bytes(bytes_total));
    group.bench_function("sync_every_write", |b| {
        b.iter_batched(
            || setup_db("sync", true),
            |engine| {
                let cf = engine.default_column_family();
                for i in 0..num_ops {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();
                }
                engine
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Memory Mode - Mixed read/write workload
// ============================================================================

fn bench_memory_mode(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_memory_mode");
    group.sampling_mode(SamplingMode::Flat);

    let write_count = 100usize;
    let read_count = 50usize;
    let (keys, vals) = precompute_kv(write_count, VALUE_SIZE);

    // Bytes for writes + reads
    let total_ops = write_count + read_count;
    group.throughput(Throughput::Bytes((total_ops as u64) * BYTES_PER_OP));

    group.bench_function("read_write_mix", |b| {
        b.iter_batched(
            || setup_db("memory_mode", false),
            |engine| {
                let cf = engine.default_column_family();

                // Writes
                for i in 0..write_count {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();
                }

                // Reads (every other key)
                for i in (0..write_count).step_by(2).take(read_count) {
                    black_box(engine.get(&cf, &keys[i]).unwrap());
                }

                engine
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// Full-stack end-to-end throughput
// ============================================================================

fn bench_full_stack_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_end_to_end");
    group.sampling_mode(SamplingMode::Flat);

    let cf_counts = [1usize, 4, 8];
    let n_ops = 5_000usize; // Reduced for faster runs

    for &n_cfs in &cf_counts {
        // Precompute all data outside timing
        let keys: Vec<Bytes> = (0..n_ops).map(make_key).collect();
        let vals: Vec<Bytes> = (0..n_ops).map(|_| make_value_fixed(VALUE_SIZE)).collect();
        let miss_keys: Vec<Bytes> = (n_ops..n_ops + 500).map(make_key).collect();

        // Total bytes: writes + reads + deletes
        let write_bytes = (n_ops as u64) * BYTES_PER_OP;
        let read_bytes = (4_500u64 + 500) * BYTES_PER_OP; // hits + misses
        let delete_bytes = 1_000u64 * BYTES_PER_OP;
        group.throughput(Throughput::Bytes(write_bytes + read_bytes + delete_bytes));

        group.bench_with_input(BenchmarkId::from_parameter(n_cfs), &n_cfs, |b, &n_cfs| {
            b.iter_batched(
                || {
                    let engine = setup_db(&format!("end_to_end_{}", n_cfs), false);
                    let mut cfs = vec![engine.default_column_family()];
                    for i in 1..n_cfs {
                        let name = format!("bench_cf_{}", i);
                        let cf = engine
                            .create_column_family(&name, ColumnFamilyConfig::default())
                            .unwrap();
                        cfs.push(cf);
                    }
                    (engine, cfs)
                },
                |(engine, cfs)| {
                    // 1) Writes spread across CFs
                    for (i, k) in keys.iter().enumerate() {
                        let cf = &cfs[i % n_cfs];
                        engine.put(cf, k, &vals[i]).unwrap();
                    }

                    // 2) Reads (hits)
                    for k in keys.iter().take(4_500) {
                        black_box(engine.get(&cfs[0], k).unwrap());
                    }

                    // 3) Reads (misses)
                    for k in &miss_keys {
                        black_box(engine.get(&cfs[0], k).unwrap());
                    }

                    // 4) Deletes
                    for (i, k) in keys.iter().take(1_000).enumerate() {
                        let cf = &cfs[i % n_cfs];
                        engine.delete(cf, k).unwrap();
                    }

                    // 5) Flush
                    for cf in &cfs {
                        engine.flush_cf(cf).unwrap();
                    }

                    engine // prevent Drop during timing
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group! {
    name = tier3_system_engine_basic;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_put_variants,
        bench_get_hit_miss,
        bench_delete,
        bench_write_modes,
        bench_memory_mode,
        bench_full_stack_throughput,
        bench_concurrent_cf_scaling
}
criterion_main!(tier3_system_engine_basic);
