//! Tier 2 - Local Durability Throughput Regression Guard
//!
//! Measures batched write throughput for memory and local storage modes.

use cntryl_midge::testkit::{bench::init_benchmark_telemetry, opts_for_mode};
use cntryl_midge::{Engine, TransactionMode, WriteOptions};
use cntryl_stress::{stress_main, stress_test, StressContext};

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

fn run_mode(ctx: &mut StressContext, mode: &'static str) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let opts = opts_for_mode(mode);
    ctx.parameter("storage_profile", mode);
    ctx.parameter("batch_size", NUM_OPS_PER_BATCH);
    ctx.parameter("batch_iterations", BATCH_ITERATIONS);
    ctx.parameter(
        "logical_bytes",
        NUM_OPS_PER_BATCH * VALUE_SIZE * BATCH_ITERATIONS,
    );

    let engine = Engine::open_with_options(&opts).expect("failed to open engine");
    let cf = engine
        .create_column_family("test")
        .expect("failed to create column family");
    let keys_vals = make_key_value_batch();

    let _completed = ctx.measure_counted(|| {
        run_batched_write_workload(&engine, cf.id(), &keys_vals);
        (NUM_OPS_PER_BATCH * BATCH_ITERATIONS) as u64
    });
}

#[stress_test(
    tier = 2,
    metadata(component = "local_throughput", scenario = "memory")
)]
fn memory(ctx: &mut StressContext) {
    run_mode(ctx, "memory");
}

#[stress_test(tier = 2, metadata(component = "local_throughput", scenario = "local"))]
fn local(ctx: &mut StressContext) {
    run_mode(ctx, "local");
}

stress_main!();
