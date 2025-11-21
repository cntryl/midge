//! Tier 2 — Flush Benchmarks
//!
//! **Target Runtime:** < 2 seconds total
//! **Run Frequency:** CI / Pre-commit
//!
//! Covers memtable flush operations

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use criterion_helper::criterion_config;
use std::hint::black_box;
use tempfile::TempDir;

fn make_key(i: usize) -> Bytes {
    Bytes::from(format!("key_{:010}", i))
}

fn make_value(size: usize) -> Bytes {
    Bytes::from(vec![b'v'; size])
}

fn setup_db(name: &str) -> (MidgeEngine, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let path = tmp.path().join(name);
    
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path: path },
        memtable_size: 16 * 1024 * 1024, // 16MB memtable
        enable_compaction: false,
        ..Default::default()
    };
    
    let engine = MidgeEngine::open(opts).expect("open engine");
    (engine, tmp)
}

/// Benchmark flush small memtable (~1000 keys, ~128KB)
fn bench_flush_small_memtable(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_small_memtable");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));
    group.measurement_time(std::time::Duration::from_millis(500));

    group.bench_function("flush_small", |b| {
        b.iter_batched(
            || {
                let (engine, _tmp) = setup_db("flush_small");
                let cf = engine.default_column_family();
                
                // Populate memtable with 1000 keys (~128 bytes each)
                for i in 0..1000 {
                    engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
                }
                
                (engine, _tmp)
            },
            |(engine, _tmp)| {
                // Measure only the flush operation
                engine.flush().unwrap();
                black_box(&engine);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark flush large memtable (~100k keys, ~12.8MB)
fn bench_flush_large_memtable(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_large_memtable");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100_000));
    group.measurement_time(std::time::Duration::from_secs(2));
    group.sample_size(10); // Fewer samples for large operation

    group.bench_function("flush_large", |b| {
        b.iter_batched(
            || {
                let (engine, _tmp) = setup_db("flush_large");
                let cf = engine.default_column_family();
                
                // Populate memtable with 100k keys (~128 bytes each = ~12.8MB)
                for i in 0..100_000 {
                    engine.put(&cf, &make_key(i), &make_value(128)).unwrap();
                }
                
                (engine, _tmp)
            },
            |(engine, _tmp)| {
                // Measure only the flush operation
                engine.flush().unwrap();
                black_box(&engine);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

/// Benchmark flush with sparse index building (many keys to trigger index entries)
fn bench_flush_sparse_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_flush_sparse_index_build");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10_000));
    group.measurement_time(std::time::Duration::from_millis(800));

    group.bench_function("build_sparse_index", |b| {
        b.iter_batched(
            || {
                let (engine, _tmp) = setup_db("flush_sparse");
                let cf = engine.default_column_family();
                
                // Populate with 10k keys - enough to build a meaningful sparse index
                for i in 0..10_000 {
                    engine.put(&cf, &make_key(i), &make_value(256)).unwrap();
                }
                
                (engine, _tmp)
            },
            |(engine, _tmp)| {
                // Flush creates SST with sparse index
                engine.flush().unwrap();
                black_box(&engine);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = flush_group;
    config = criterion_config();
    targets = bench_flush_small_memtable, bench_flush_large_memtable, bench_flush_sparse_index_build
}
criterion_main!(flush_group);