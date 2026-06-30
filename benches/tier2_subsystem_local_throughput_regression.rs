//! Tier 2 â€” Local Durability Throughput Regression Guard
//!
//! Measures: batched write throughput for local vs memory mode
//! Purpose: Catch unintended local throughput collapse (regression guard)
//!
//! This benchmark ensures that local mode with batched durability doesn't
//! drop below 50% of memory throughput for the same workload.
//!
//! If this fails, it indicates a regression in local WAL batching,
//! memtable configuration, or durability path performance.

#[path = "./criterion_config.rs"]
mod criterion_config;

use std::time::Duration;

use cntryl_midge::testkit::opts_for_mode;
use cntryl_midge::{Engine, WriteOptions};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_config::criterion_config_for_tier2;

const NUM_OPS_PER_BATCH: usize = 100;
const VALUE_SIZE: usize = 128;
const BATCH_ITERATIONS: usize = 100;

type KeyValueBatch = Vec<(Vec<u8>, Vec<u8>)>;

fn make_key_value_batch() -> KeyValueBatch {
    (0..NUM_OPS_PER_BATCH)
        .map(|i| {
            let key = format!("key_{i:016}").into_bytes();
            let value_byte = u8::try_from(i % 251).expect("value byte fits in u8");
            let value = vec![value_byte; VALUE_SIZE];
            (key, value)
        })
        .collect()
}

fn benchmark_batched_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("tier2_local_throughput_regression");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

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
                    let engine = Engine::open_with_options(&opts).expect("failed to open engine");
                    let cf = engine
                        .create_column_family("test")
                        .expect("failed to create column family");
                    let keys_vals = make_key_value_batch();
                    (engine, cf, keys_vals)
                },
                |(engine, cf, keys_vals)| {
                    let cf_id = cf.id();

                    // Run batches
                    for _ in 0..BATCH_ITERATIONS {
                        let mut tx = engine
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                            .expect("begin");

                        for (k, v) in &keys_vals {
                            tx.put(k.clone(), v.clone(), None).expect("put");
                        }

                        tx.commit(WriteOptions::buffered()).expect("commit");
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
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    let mem_opts = opts_for_mode("memory");
    let local_opts = opts_for_mode("local");

    // Benchmark memory mode
    group.bench_function("memory_baseline", |b| {
        b.iter_batched(
            || {
                let engine = Engine::open_with_options(&mem_opts).unwrap();
                let cf = engine.create_column_family("test").unwrap();
                let keys_vals = make_key_value_batch();
                (engine, cf, keys_vals)
            },
            |(engine, cf, keys_vals)| {
                let cf_id = cf.id();

                for _ in 0..BATCH_ITERATIONS {
                    let mut tx = engine
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin");
                    for (k, v) in &keys_vals {
                        tx.put(k.clone(), v.clone(), None).expect("put");
                    }
                    tx.commit(WriteOptions::buffered()).expect("commit");
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    // Benchmark local mode
    group.bench_function("local_throughput", |b| {
        b.iter_batched(
            || {
                let engine = Engine::open_with_options(&local_opts).unwrap();
                let cf = engine.create_column_family("test").unwrap();
                let keys_vals = make_key_value_batch();
                (engine, cf, keys_vals)
            },
            |(engine, cf, keys_vals)| {
                let cf_id = cf.id();

                for _ in 0..BATCH_ITERATIONS {
                    let mut tx = engine
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin");
                    for (k, v) in &keys_vals {
                        tx.put(k.clone(), v.clone(), None).expect("put");
                    }
                    tx.commit(WriteOptions::buffered()).expect("commit");
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config_for_tier2();
    targets = benchmark_batched_writes, verify_local_throughput_minimum
}
criterion_main!(benches);
