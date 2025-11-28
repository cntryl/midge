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

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::hint::black_box;
use std::time::Duration;

fn setup_db(name: &str, enable_wal_sync: bool) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_subsystem_advanced_{}", name));
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

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}
fn make_value(i: usize, base: usize) -> Bytes {
    // introduce slight variance in size
    let size = base + (i % 50);
    Bytes::from(vec![b'x'; size])
}

fn precompute_kv(n: usize, value_base: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(n);
    let mut vals = Vec::with_capacity(n);
    for i in 0..n {
        keys.push(make_key(i));
        vals.push(make_value(i, value_base));
    }
    (keys, vals)
}

// ============================================================================
// TTL Operations
// ============================================================================

fn bench_ttl(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_ttl");

    let (keys, vals) = precompute_kv(500, 80);
    group.bench_function("put_with_ttl", |b| {
        b.iter_batched(
            || setup_db("ttl", false),
            |engine| {
                let cf = engine.default_column_family();
                let ttl = Duration::from_secs(1200);
                for i in 0..500 {
                    engine
                        .put_with_ttl(&cf, &keys[i], &vals[i], ttl.as_secs())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    let (keys_read, vals_read) = precompute_kv(500, 100);
    group.bench_function("ttl_read_after_insert", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("ttl_read", false);
                let cf = engine.default_column_family();
                let ttl = Duration::from_secs(1200);
                for i in 0..500 {
                    engine
                        .put_with_ttl(&cf, &keys_read[i], &vals_read[i], ttl.as_secs())
                        .unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                for i in (0..500).step_by(4) {
                    let _ = engine.get(&cf, &keys_read[i]).unwrap();
                }
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

    for &cf_count in &[1, 4, 8, 16] {
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
                        for i in 0..1_000 {
                            let cf_idx = i % num_cfs;
                            let cf = &cf_list[cf_idx];
                            engine.put(cf, &make_key(i), &make_value(i, 100)).unwrap();
                        }
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
// Large Value Handling
// ============================================================================

/// Benchmark operations with large values (1MB+) to test buffer handling
fn bench_large_values(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_large_values");

    for &value_size in &[64 * 1024, 512 * 1024, 1024 * 1024] {
        group.bench_with_input(
            BenchmarkId::new("put", value_size),
            &value_size,
            |b, &size| {
                b.iter_batched(
                    || setup_db(&format!("large_put_{}", size), false),
                    |engine| {
                        let cf = engine.default_column_family();
                        for i in 0..10 {
                            engine.put(&cf, &make_key(i), &make_value(i, size)).unwrap();
                        }
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("get", value_size),
            &value_size,
            |b, &size| {
                let engine = setup_db(&format!("large_get_{}", size), false);
                let cf = engine.default_column_family();
                for i in 0..10 {
                    engine.put(&cf, &make_key(i), &make_value(i, size)).unwrap();
                }

                b.iter(|| {
                    for i in 0..10 {
                        let _ = engine.get(&cf, &make_key(i)).unwrap();
                    }
                })
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

    group.bench_function("delete_50pct", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("delete_heavy", false);
                let cf = engine.default_column_family();
                for i in 0..2_000 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                // Delete every other key
                for i in (0..2_000).step_by(2) {
                    engine.delete(&cf, &make_key(i)).unwrap();
                }
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("delete_90pct", |b| {
        b.iter_batched(
            || {
                let engine = setup_db("delete_heavy_90", false);
                let cf = engine.default_column_family();
                for i in 0..2_000 {
                    engine.put(&cf, &make_key(i), &make_value(i, 100)).unwrap();
                }
                engine
            },
            |engine| {
                let cf = engine.default_column_family();
                // Delete 90% of keys
                for i in 0..1_800 {
                    engine.delete(&cf, &make_key(i)).unwrap();
                }
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = tier3_system_engine_advanced;
    config = criterion_config();
    targets =
        bench_ttl,
        bench_column_family_scaling,
        bench_large_values,
        bench_delete_heavy
}
criterion_main!(tier3_system_engine_advanced);
