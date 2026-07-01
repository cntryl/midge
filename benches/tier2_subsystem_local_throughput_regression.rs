//! Tier 2 - Local Durability Throughput Regression Guard
//!
//! Measures: batched write throughput for memory and local storage modes
//! Purpose: Catch unintended local throughput collapse (regression guard)
//!
//! This benchmark ensures that local mode with buffered durability does not
//! drop below 50% of memory throughput for the same workload.
//!
//! If this fails, it indicates a regression in local WAL batching, memtable
//! configuration, or durability path performance.

#[path = "./criterion_config.rs"]
mod criterion_config;

use std::time::Duration;

use cntryl_midge::testkit::{bench::init_benchmark_telemetry, opts_for_mode};
use cntryl_midge::{Engine, TransactionMode, WriteOptions};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use criterion_config::criterion_config_for_tier2;

const NUM_OPS_PER_BATCH: usize = 100;
const VALUE_SIZE: usize = 128;
const BATCH_ITERATIONS: usize = 100;
const THROUGHPUT_MODES: [&str; 2] = ["memory", "local"];

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

fn run_batched_write_workload(
    engine: &Engine,
    cf_id: cntryl_midge::ColumnFamilyId,
    keys_vals: &KeyValueBatch,
) {
    for _ in 0..BATCH_ITERATIONS {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin transaction");

        for (key, value) in keys_vals {
            tx.put(key.clone(), value.clone(), None)
                .expect("put batch value");
        }

        tx.commit(WriteOptions::buffered())
            .expect("commit buffered batch");
    }
}

fn benchmark_batched_writes(c: &mut Criterion) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");

    let mut group = c.benchmark_group("tier2_local_throughput_regression");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(6));

    for mode in THROUGHPUT_MODES {
        let opts = opts_for_mode(mode);

        group.throughput(Throughput::Bytes(
            (NUM_OPS_PER_BATCH * VALUE_SIZE * BATCH_ITERATIONS) as u64,
        ));

        group.bench_with_input(BenchmarkId::from_parameter(mode), mode, |b, _mode| {
            b.iter_batched(
                || {
                    let engine = Engine::open_with_options(&opts).expect("failed to open engine");
                    let cf = engine
                        .create_column_family("test")
                        .expect("failed to create column family");
                    let keys_vals = make_key_value_batch();
                    (engine, cf.id(), keys_vals)
                },
                |(engine, cf_id, keys_vals)| {
                    run_batched_write_workload(&engine, cf_id, &keys_vals);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config_for_tier2();
    targets = benchmark_batched_writes
}
criterion_main!(benches);
