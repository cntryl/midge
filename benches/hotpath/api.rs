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
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use criterion_helper::criterion_config;
use cntryl_midge::{MidgeEngine, MidgeOptions, Mutation, StorageMode};
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

    for &batch_size in &[100, 1_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        // Only prepare mutations in setup (not database creation)
                        let mutations: Vec<Mutation> = (0..size)
                            .map(|i| {
                                let key = format!("key_{:010}", i);
                                let value = format!("value_{:010}_data", i);
                                Mutation::put(Bytes::from(key), Bytes::from(value), None)
                            })
                            .collect();
                        mutations
                    },
                    |mutations| {
                        // Only measure the batch operation itself
                        engine.batch(mutations).unwrap();
                        black_box(());
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

/// Benchmark multi-get operations (hot path for batch reads)
fn bench_multi_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_multi_get");

    // Setup: pre-populate database
    let engine = setup_db("multi_get");
    for i in 0..10_000 {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_padding", i);
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
    }
    engine.flush().unwrap();

    for &batch_size in &[10, 100] {
        let keys: Vec<String> = (0..batch_size).map(|i| format!("key_{:010}", i)).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_bytes()).collect();

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, _| {
                b.iter(|| {
                    let results = engine.multi_get(&key_refs).unwrap();
                    black_box(results.len());
                })
            },
        );
    }

    group.finish();
}

/// Benchmark single put operations (baseline for comparison)
fn bench_single_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_single_put");

    let engine = setup_db("single_put");
    let mut counter = 0u64;

    group.bench_function("single_put", |b| {
        b.iter(|| {
            let key = format!("key_{:010}", counter);
            let value = format!("value_{:010}_data", counter);
            counter += 1;
            engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
            black_box(());
        })
    });

    group.finish();
}

/// Benchmark batch delete operations
fn bench_batch_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_batch_delete");

    let engine = setup_db("batch_delete");

    // Pre-populate with data to delete
    for i in 0..100_000 {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_data", i);
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
                        let mutations: Vec<Mutation> = (offset..offset + size)
                            .map(|i| {
                                let key = format!("key_{:010}", i);
                                Mutation::delete(Bytes::from(key))
                            })
                            .collect();
                        offset += size;
                        mutations
                    },
                    |mutations| {
                        engine.batch(mutations).unwrap();
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

    // Pre-populate with some data
    for i in 0..50_000 {
        let key = format!("key_{:010}", i);
        let value = format!("value_{:010}_data", i);
        engine.put(Bytes::from(key), Bytes::from(value)).unwrap();
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
                        let mutations: Vec<Mutation> = (0..size)
                            .map(|i| {
                                if i % 2 == 0 {
                                    // Put new data
                                    let key = format!("key_{:010}", offset + i);
                                    let value = format!("value_{:010}_data", offset + i);
                                    Mutation::put(Bytes::from(key), Bytes::from(value), None)
                                } else {
                                    // Delete existing data
                                    let key = format!("key_{:010}", i / 2);
                                    Mutation::delete(Bytes::from(key))
                                }
                            })
                            .collect();
                        offset += size;
                        mutations
                    },
                    |mutations| {
                        engine.batch(mutations).unwrap();
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
    name = hotpath_api;
    config = criterion_config();
    targets = bench_batch_put, bench_multi_get, bench_single_put, bench_batch_delete, bench_batch_mixed
}
criterion_main!(hotpath_api);
