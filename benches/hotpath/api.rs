//! Tier 1 — Hot Path API Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical API hot paths:
//! - Batch writes (put/delete/upsert)
//! - Multi-get (batched point lookups)

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, WriteBatch};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn setup_db(name: &str) -> MidgeEngine {
    let path = std::env::temp_dir().join(format!("midge_bench_hotpath_api_{}", name));
    let _ = std::fs::remove_dir_all(&path);

    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 16 * 1024 * 1024,
        enable_compaction: false,
        ..Default::default()
    };

    MidgeEngine::open(opts).unwrap()
}

/// Benchmark batch put operations (hot path for write throughput)
fn bench_batch_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_batch_put");

    // Setup database once, reuse across iterations
    let engine = setup_db("batch_put");
    let cf = engine.default_column_family();
    let cf_id = cf.id();

    for &batch_size in &[100, 1_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        // Only prepare a WriteBatch in setup (not database creation)
                        let mut batch = WriteBatch::new();
                        for i in 0..size {
                            let key = format!("key_{:010}", i);
                            let value = format!("value_{:010}_data", i);
                            batch.put(cf_id, Bytes::from(key), Bytes::from(value));
                        }
                        batch
                    },
                    |batch| {
                        // Only measure the batch operation itself (writes to default CF)
                        engine.write_batch(&batch).unwrap();
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark single get operations (hot path for reads)
fn bench_single_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_single_get");

    let engine = setup_db("single_get");
    let cf = engine.default_column_family();

    // Pre-populate with data
    for i in 0..10_000 {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_padding_to_increase_size", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Hit rate benchmark
    let mut counter = 0;
    group.bench_function("single_get_hit", |b| {
        b.iter(|| {
            let key = format!("key_{:010}", counter % 10_000);
            counter += 1;
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            black_box(result);
        })
    });

    // Miss rate benchmark
    let mut miss_counter = 10_000;
    group.bench_function("single_get_miss", |b| {
        b.iter(|| {
            let key = format!("key_{:010}", miss_counter);
            miss_counter += 1;
            let result = engine.get(&cf, key.as_bytes()).unwrap();
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark single put operations (baseline for comparison)
fn bench_single_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_single_put");

    let engine = setup_db("single_put");
    let cf = engine.default_column_family();
    let mut counter = 0u64;

    group.bench_function("single_put", |b| {
        b.iter(|| {
            let key = format!("key_{:010}", counter);
            let value = format!("value_{:010}_data", counter);
            counter += 1;
            engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
            black_box(());
        })
    });

    group.finish();
}

/// Benchmark batch delete operations
fn bench_batch_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_batch_delete");

    let engine = setup_db("batch_delete");
    let cf = engine.default_column_family();
    let cf_id = cf.id();

    // Pre-populate with data to delete
    for i in 0..100_000 {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_data", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    let mut offset = 0;
    for &batch_size in &[100, 1_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut batch = WriteBatch::new();
                        for i in offset..offset + size {
                            let key = format!("key_{:010}", i);
                            batch.delete(cf_id, Bytes::from(key));
                        }
                        offset += size;
                        batch
                    },
                    |batch| {
                        engine.write_batch(&batch).unwrap();
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark mixed batch operations (put + delete)
fn bench_batch_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_batch_mixed");

    let engine = setup_db("batch_mixed");
    let cf = engine.default_column_family();
    let cf_id = cf.id();

    // Pre-populate with some data
    for i in 0..50_000 {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_data", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    let mut offset = 0;
    for &batch_size in &[100, 1_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut batch = WriteBatch::new();
                        for i in 0..size {
                            if i % 2 == 0 {
                                // Put new data
                                let key = format!("key_{:010}", offset + i);
                                let value = format!("value_{:010}_data", offset + i);
                                batch.put(cf_id, Bytes::from(key), Bytes::from(value));
                            } else {
                                // Delete existing data
                                let key = format!("key_{:010}", i / 2);
                                batch.delete(cf_id, Bytes::from(key));
                            }
                        }
                        offset += size;
                        batch
                    },
                    |batch| {
                        engine.write_batch(&batch).unwrap();
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark range scan operations
fn bench_range_scan(c: &mut Criterion) {
    use cntryl_midge::Query;

    let mut group = c.benchmark_group("hotpath_range_scan");

    let engine = setup_db("range_scan");
    let cf = engine.default_column_family();

    // Pre-populate with ordered keys
    for i in 0..10_000 {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_data", i);
        engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Scan 100 keys
    group.bench_function("scan_100_keys", |b| {
        b.iter(|| {
            let query = Query::new()
                .start_key(Bytes::from("key_0000000000"))
                .end_key(Bytes::from("key_0000000100"));
            let results = engine.scan(&cf, query).unwrap();
            black_box(results.len());
        })
    });

    // Scan 1000 keys
    group.bench_function("scan_1000_keys", |b| {
        b.iter(|| {
            let query = Query::new()
                .start_key(Bytes::from("key_0000000000"))
                .end_key(Bytes::from("key_0000001000"));
            let results = engine.scan(&cf, query).unwrap();
            black_box(results.len());
        })
    });

    group.finish();
}

criterion_group! {
    name = hotpath_api;
    config = criterion_config();
    targets = bench_batch_put, bench_single_get, bench_single_put, bench_batch_delete, bench_batch_mixed, bench_range_scan
}
criterion_main!(hotpath_api);
