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

#[path = "./criterion_helper.rs"]
mod criterion_helper;

use cntryl_midge::Bytes;
use cntryl_midge::{MidgeEngine, testkit::{MidgeOptions, StorageMode}};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_helper::{criterion_config_for_tier, BenchTier};
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
    let _ = name;

    // Tier 1 benches must be memtable-only: avoid filesystem/WAL I/O and avoid
    // background work triggered by frequent memtable flushes.
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        wal_sync: false,
        wal_batch_config: None,
        // Keep the memtable large enough that we do not trigger flush/compaction
        // during the measurement window.
        memtable_size: 1024 * 1024 * 1024, // 1 GiB
        compression: false,
        enable_compaction: false,
        // Explicit large budget to prevent WriteStall in CI where Auto budget
        // (based on available RAM) can be too small for thousands of iterations
        // with compaction disabled.
        memory_budget: Some(4 * 1024 * 1024 * 1024), // 4 GiB
    };

    MidgeEngine::open_with_options(opts).unwrap()
}

/// Benchmark batch put operations (hot path for write throughput)
///
/// Measures throughput of multiple puts in a single transaction + commit.
/// This is the canonical way to batch writes in Midge.
fn bench_batch_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_batch_put");
    group.sampling_mode(SamplingMode::Flat);

    // Setup database once, reuse across iterations
    let engine = setup_db("batch_put");
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Reuse WriteOptions across iterations (allowed optimization)
    let write_opts = cntryl_midge::WriteOptions::buffered();

    for &batch_size in &[100, 1_000] {
        // Precompute keys/values ONCE (outside measurement)
        let (keys, vals) = make_fixed_kv(batch_size);

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    // Measure: begin transaction, add all puts, commit
                    let mut tx = engine
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .unwrap();
                    for i in 0..size {
                        tx.put(keys[i].to_vec(), vals[i].to_vec(), None).unwrap();
                    }
                    engine.commit(tx, write_opts).unwrap();
                    black_box(())
                })
            },
        );
    }

    group.finish();
}

/// Benchmark single get operations (hot path for reads)
///
/// This benchmarks reads from the memtable (in-memory), which is the fastest
/// read path. SST reads are benchmarked separately in tier2.
fn bench_single_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_single_get");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let engine = setup_db("single_get");
    let cf = engine.create_column_family("cf1").unwrap();

    // Precompute keys and values
    let num_keys = 1_000;
    let (keys, vals) = make_fixed_kv(num_keys);

    // Pre-populate with data (NO flush - keep in memtable for hot path)
    for i in 0..num_keys {
        let mut tx = engine
            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(keys[i].to_vec(), vals[i].to_vec(), None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();
    }
    // Note: intentionally NOT flushing to keep data in memtable

    // Hit rate benchmark - cycle through keys (all in memtable)
    let mut counter = 0;
    let cf_id = cf.id();
    group.bench_function("single_get_hit_memtable", |b| {
        b.iter(|| {
            let idx = counter % num_keys;
            counter += 1;
            let tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin");
            let result = tx.get(black_box(&keys[idx]));
            black_box(result)
        })
    });

    // Miss rate benchmark - use keys not in the populated set
    // Pre-generate miss keys to avoid allocation in hot path
    let miss_keys: Vec<Bytes> = (0..num_keys)
        .map(|i| {
            let mut key = [0u8; 16];
            key[8..16].copy_from_slice(&((i + num_keys * 2) as u64).to_be_bytes());
            Bytes::copy_from_slice(&key)
        })
        .collect();

    let mut miss_counter = 0;
    group.bench_function("single_get_miss", |b| {
        b.iter(|| {
            let idx = miss_counter % num_keys;
            miss_counter += 1;
            let tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin");
            let result = tx.get(black_box(&miss_keys[idx]));
            black_box(result)
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
    let cf = engine.create_column_family("cf1").unwrap();

    // Precompute keys and values
    let num_ops = 10_000;
    let (keys, vals) = make_fixed_kv(num_ops);
    let mut counter = 0;
    let cf_id = cf.id();

    group.bench_function("single_put", |b| {
        b.iter(|| {
            let idx = counter % num_ops;
            counter += 1;
            let mut tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.put(keys[idx].to_vec(), vals[idx].to_vec(), None)
                .unwrap();
            engine
                .commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
            black_box(());
        })
    });

    group.finish();
}

criterion_group! {
    name = tier1_hotpath_api;
    config = criterion_config_for_tier(BenchTier::Tier1Hot);
    targets = bench_batch_put, bench_single_get, bench_single_put
}
criterion_main!(tier1_hotpath_api);
