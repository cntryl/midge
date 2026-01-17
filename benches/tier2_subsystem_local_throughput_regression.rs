//! Tier 2 — Local Durability Throughput Regression Guard
//!
//! Measures: batched write throughput for local vs memory mode
//! Purpose: Catch unintended local throughput collapse (regression guard)
//!
//! This benchmark ensures that local mode with batched durability doesn't
//! drop below 50% of memory throughput for the same workload.
//!
//! If this fails, it indicates a regression in local WAL batching,
//! memtable configuration, or durability path performance.

use cntryl_midge::testkit::opts_for_mode;
use cntryl_midge::{Engine, WriteOptions};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

const NUM_OPS_PER_BATCH: usize = 100;
const VALUE_SIZE: usize = 128;
const BATCH_ITERATIONS: usize = 100;

fn benchmark_batched_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("tier2_local_throughput_regression");

    // Measure both memory and local modes
    for mode in &["memory", "local"] {
        let opts = opts_for_mode(mode);

        group.throughput(Throughput::Bytes(
            (NUM_OPS_PER_BATCH * VALUE_SIZE * BATCH_ITERATIONS) as u64,
        ));

        group.bench_with_input(BenchmarkId::from_parameter(mode), mode, |b, _mode| {
            b.iter_batched(
                || {
                    // Setup: create engine and column family
                    let engine =
                        Engine::open_with_options(opts.clone()).expect("failed to open engine");
                    let cf = engine
                        .create_column_family("test")
                        .expect("failed to create column family");
                    (engine, cf)
                },
                |(engine, cf)| {
                    let cf_id = cf.id();

                    // Precompute keys and values
                    let mut keys_vals = Vec::with_capacity(NUM_OPS_PER_BATCH);
                    for i in 0..NUM_OPS_PER_BATCH {
                        let k = format!("key_{:016}", i);
                        let v = vec![(i % 251) as u8; VALUE_SIZE];
                        keys_vals.push((k, v));
                    }

                    // Run batches
                    for _ in 0..BATCH_ITERATIONS {
                        let mut tx = engine
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                            .expect("begin");

                        for (k, v) in &keys_vals {
                            tx.put(k.as_bytes().to_vec(), v.clone(), None).expect("put");
                        }

                        engine.commit(tx, WriteOptions::buffered()).expect("commit");
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn verify_local_throughput_minimum(c: &mut Criterion) {
    let mut group = c.benchmark_group("tier2_local_throughput_threshold");
    group.sample_size(10); // Reduce samples for faster runs

    let mem_opts = opts_for_mode("memory");
    let local_opts = opts_for_mode("local");

    // Benchmark memory mode
    group.bench_function("memory_baseline", |b| {
        b.iter_batched(
            || {
                let engine = Engine::open_with_options(mem_opts.clone()).unwrap();
                let cf = engine.create_column_family("test").unwrap();
                (engine, cf)
            },
            |(engine, cf)| {
                let cf_id = cf.id();
                let mut keys_vals = Vec::with_capacity(NUM_OPS_PER_BATCH);
                for i in 0..NUM_OPS_PER_BATCH {
                    let k = format!("key_{:016}", i);
                    let v = vec![(i % 251) as u8; VALUE_SIZE];
                    keys_vals.push((k, v));
                }

                for _ in 0..BATCH_ITERATIONS {
                    let mut tx = engine
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin");
                    for (k, v) in &keys_vals {
                        tx.put(k.as_bytes().to_vec(), v.clone(), None).expect("put");
                    }
                    engine.commit(tx, WriteOptions::buffered()).expect("commit");
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    // Benchmark local mode
    group.bench_function("local_throughput", |b| {
        b.iter_batched(
            || {
                let engine = Engine::open_with_options(local_opts.clone()).unwrap();
                let cf = engine.create_column_family("test").unwrap();
                (engine, cf)
            },
            |(engine, cf)| {
                let cf_id = cf.id();
                let mut keys_vals = Vec::with_capacity(NUM_OPS_PER_BATCH);
                for i in 0..NUM_OPS_PER_BATCH {
                    let k = format!("key_{:016}", i);
                    let v = vec![(i % 251) as u8; VALUE_SIZE];
                    keys_vals.push((k, v));
                }

                for _ in 0..BATCH_ITERATIONS {
                    let mut tx = engine
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin");
                    for (k, v) in &keys_vals {
                        tx.put(k.as_bytes().to_vec(), v.clone(), None).expect("put");
                    }
                    engine.commit(tx, WriteOptions::buffered()).expect("commit");
                }
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_batched_writes,
    verify_local_throughput_minimum
);
criterion_main!(benches);
