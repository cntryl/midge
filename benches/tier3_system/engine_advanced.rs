//! Tier 3 — Advanced Engine Feature Benchmarks
//!
//! **Target Runtime:** ~30-60 seconds
//! **Run Frequency:** Nightly CI
//!
//! Covers advanced engine features with full engine setup:
//! - TTL expiration operations
//! - Column family scaling
//! - Large value handling (>100KB)
//! - Delete-heavy workloads
//!
//! ## Design Notes
//!
//! - Returns engine from timed closure to avoid engine Drop during timing
//! - Throughput measured in bytes where applicable
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

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::hint::black_box;

/// Global counter for unique benchmark directory names
static BENCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Key size in bytes
const KEY_SIZE: usize = 14;
/// Default value size
const VALUE_SIZE: usize = 100;
/// Bytes per operation (key + value)
const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

/// Generate unique path for benchmark database
fn unique_bench_path(prefix: &str) -> PathBuf {
    let counter = BENCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("midge_bench_advanced_{}_{}_{}", prefix, pid, counter))
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
    MidgeEngine::open(opts).unwrap()
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
// TTL Operations
// ============================================================================

fn bench_ttl(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_ttl");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 500usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    group.throughput(Throughput::Bytes(bytes_total));
    group.bench_function("put_with_ttl", |b| {
        b.iter_batched(
            || setup_db("ttl", false),
            |engine| {
                let cf = engine.default_column_family();
                let ttl_secs = 1200u64;
                for i in 0..num_ops {
                    engine
                        .put_with_ttl(&cf, &keys[i], &vals[i], ttl_secs)
                        .unwrap();
                }
                engine // prevent Drop during timing
            },
            BatchSize::SmallInput,
        )
    });

    // Read benchmark after TTL insert
    let read_count = num_ops / 4; // step_by(4) = 125 reads
    let read_indices: Vec<usize> = (0..num_ops).step_by(4).collect();

    group.throughput(Throughput::Bytes((read_count as u64) * BYTES_PER_OP));
    group.bench_function("ttl_read_after_insert", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("ttl_read", false);
                let cf = engine.default_column_family();
                let ttl_secs = 1200u64;
                for i in 0..num_ops {
                    engine.put_with_ttl(&cf, &keys[i], &vals[i], ttl_secs).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                for &i in &read_indices {
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
// Column Family Scaling
// ============================================================================

/// Benchmark multi-column family operations to measure CF routing overhead
fn bench_column_family_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_cf_scaling");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 1_000usize;
    let (keys, vals) = precompute_kv(num_ops, VALUE_SIZE);
    let bytes_total = (num_ops as u64) * BYTES_PER_OP;

    for &cf_count in &[1, 4, 8, 16] {
        group.throughput(Throughput::Bytes(bytes_total));
        group.bench_with_input(
            BenchmarkId::from_parameter(cf_count),
            &cf_count,
            |b, &num_cfs| {
                b.iter_batched(
                    || {
                        let engine = setup_db(&format!("cf_scale_{}", num_cfs), false);
                        // Create additional CFs
                        for i in 1..num_cfs {
                            let _ = engine
                                .create_column_family(&format!("cf{}", i), Default::default());
                        }
                        engine
                    },
                    |engine| {
                        let cf_list = engine.list_column_families();
                        for i in 0..num_ops {
                            let cf_idx = i % num_cfs;
                            let cf = &cf_list[cf_idx];
                            engine.put(cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

// ============================================================================
// Large Value Handling
// ============================================================================

/// Benchmark operations with large values (64KB-1MB) to test buffer handling
fn bench_large_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_large_values");
    group.sampling_mode(SamplingMode::Flat);

    let num_ops = 10usize;

    for &value_size in &[64 * 1024, 512 * 1024, 1024 * 1024] {
        // Precompute keys and large values
        let keys: Vec<Bytes> = (0..num_ops).map(make_key).collect();
        let vals: Vec<Bytes> = (0..num_ops).map(|_| make_value_fixed(value_size)).collect();
        let bytes_total = (num_ops as u64) * (KEY_SIZE as u64 + value_size as u64);

        group.throughput(Throughput::Bytes(bytes_total));
        group.bench_with_input(
            BenchmarkId::new("put", value_size),
            &value_size,
            |b, _size| {
                b.iter_batched(
                    || setup_db(&format!("large_put_{}", value_size), false),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..num_ops {
                            engine.put(&cf, &keys[i], &vals[i]).unwrap();
                        }
                        engine // prevent Drop during timing
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.throughput(Throughput::Bytes(bytes_total));
        group.bench_with_input(
            BenchmarkId::new("get", value_size),
            &value_size,
            |b, _size| {
                // Setup: engine with preloaded data
                let engine = setup_db(&format!("large_get_{}", value_size), false);
                let cf = engine.default_column_family();
                for i in 0..num_ops {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();
                }

                b.iter(|| {
                    for k in &keys {
                        black_box(engine.get(&cf, k).unwrap());
                    }
                });
                // engine lives until bench_with_input ends
            },
        );
    }

    group.finish();
}

// ============================================================================
// Delete-Heavy Workload
// ============================================================================

/// Benchmark delete-heavy scenarios to measure tombstone overhead
fn bench_delete_heavy(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_delete_heavy");
    group.sampling_mode(SamplingMode::Flat);

    let num_keys = 2_000usize;
    let (keys, vals) = precompute_kv(num_keys, VALUE_SIZE);

    // 50% delete (1000 deletes)
    let delete_50_count = num_keys / 2;
    let delete_50_indices: Vec<usize> = (0..num_keys).step_by(2).collect();

    group.throughput(Throughput::Bytes((delete_50_count as u64) * KEY_SIZE as u64));
    group.bench_function("delete_50pct", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("delete_heavy", false);
                let cf = engine.default_column_family();
                for i in 0..num_keys {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                // Delete every other key
                for &i in &delete_50_indices {
                    engine.delete(&cf, &keys[i]).unwrap();
                }
                engine // prevent Drop during timing
            },
            BatchSize::SmallInput,
        )
    });

    // 90% delete (1800 deletes)
    let delete_90_count = 1_800usize;

    group.throughput(Throughput::Bytes((delete_90_count as u64) * KEY_SIZE as u64));
    group.bench_function("delete_90pct", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("delete_heavy_90", false);
                let cf = engine.default_column_family();
                for i in 0..num_keys {
                    engine.put(&cf, &keys[i], &vals[i]).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                // Delete 90% of keys
                for i in 0..delete_90_count {
                    engine.delete(&cf, &keys[i]).unwrap();
                }
                engine // prevent Drop during timing
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier3_system_engine_advanced;
    config = criterion_config_for_tier(BenchTier::Tier3System);
    targets =
        bench_ttl,
        bench_column_family_scaling,
        bench_large_values,
        bench_delete_heavy
}
criterion_main!(tier3_system_engine_advanced);
