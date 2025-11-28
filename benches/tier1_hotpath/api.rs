//! Tier 1 — Hot Path API Benchmarks
//!
//! **Target Runtime:** < 1 second total
//! **Run Frequency:** Every PR (CI gate)
//!
//! Covers critical API hot paths:
//! - Batch writes (put/delete) - memtable operations only
//! - Single put/get operations
//!
//! Note: Heavy I/O operations (flush, scan) are in tier2/tier3.

#[path = "../criterion_helper.rs"]
mod criterion_helper;

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, WriteBatch};
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::criterion_config;
use std::hint::black_box;

fn make_fixed_kv(size: usize) -> (Vec<Bytes>, Vec<Bytes>) {
    let mut keys = Vec::with_capacity(size);
    let mut vals = Vec::with_capacity(size);
    for i in 0..size {
        let mut key = [0u8; 16];
        key[8..16].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(Bytes::copy_from_slice(&key));
        let mut val = [0u8; 32];
        val[24..32].copy_from_slice(&(i as u64).to_be_bytes());
        vals.push(Bytes::copy_from_slice(&val));
    }
    (keys, vals)
}

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
    group.sampling_mode(SamplingMode::Flat);

    // Setup database once, reuse across iterations
    let engine = setup_db("batch_put");
    let cf = engine.default_column_family();
    let cf_id = cf.id();

    for &batch_size in &[100, 1_000] {
        // Precompute keys and values outside the loop
        let (keys, vals) = make_fixed_kv(batch_size);
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter_batched(
                    || {
                        // Only prepare a WriteBatch in setup (no allocations)
                        let mut batch = WriteBatch::new();
                        for i in 0..size {
                            batch.put(cf_id, keys[i].clone(), vals[i].clone());
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
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let engine = setup_db("single_get");
    let cf = engine.default_column_family();

    // Precompute keys and values
    let num_keys = 10_000;
    let (keys, vals) = make_fixed_kv(num_keys);

    // Pre-populate with data
    for i in 0..num_keys {
        engine.put(&cf, &keys[i], &vals[i]).unwrap();
    }
    engine.flush().unwrap();

    // Hit rate benchmark - cycle through keys
    let mut counter = 0;
    group.bench_function("single_get_hit", |b| {
        b.iter(|| {
            let idx = counter % num_keys;
            counter += 1;
            let result = engine.get(&cf, &keys[idx]).unwrap();
            black_box(result);
        })
    });

    // Miss rate benchmark - use keys not in the populated set
    let mut miss_counter = 0;
    group.bench_function("single_get_miss", |b| {
        b.iter(|| {
            let idx = miss_counter % num_keys;
            miss_counter += 1;
            // Use miss_keys which are the same as keys, but since we didn't insert them with different values? Wait, no.
            // To make miss, I need different keys. Let's offset by num_keys.
            let mut miss_key = [0u8; 16];
            miss_key[8..16].copy_from_slice(&((idx + num_keys) as u64).to_be_bytes());
            let result = engine.get(&cf, &miss_key[..]).unwrap();
            black_box(result);
        })
    });

    group.finish();
}

/// Benchmark single put operations (baseline for comparison)
fn bench_single_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_single_put");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let engine = setup_db("single_put");
    let cf = engine.default_column_family();

    // Precompute keys and values
    let num_ops = 10_000;
    let (keys, vals) = make_fixed_kv(num_ops);
    let mut counter = 0;

    group.bench_function("single_put", |b| {
        b.iter(|| {
            let idx = counter % num_ops;
            counter += 1;
            engine.put(&cf, &keys[idx], &vals[idx]).unwrap();
            black_box(());
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_api;
    config = criterion_config();
    targets = bench_batch_put, bench_single_get, bench_single_put
}
criterion_main!(tier1_hotpath_api);
