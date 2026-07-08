//! Tier 4 - Local Durability Throughput Regression Guard
//!
//! Measures end-to-end batched write throughput for memory and local storage modes.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::{Engine, TransactionMode, WriteOptions};
use cntryl_stress::{stress, stress_main, StressContext};
use stress_config::{init_benchmark_telemetry, write_coordination_opts_for_mode};

const NUM_OPS_PER_BATCH: usize = 100;
const VALUE_SIZE: usize = 128;
const BATCH_ITERATIONS: usize = 1_000;

type KeyValueBatch = Vec<(Vec<u8>, Vec<u8>)>;
type KeyValueBatches = Vec<KeyValueBatch>;

fn make_key_value_batches() -> KeyValueBatches {
    (0..BATCH_ITERATIONS)
        .map(|batch| {
            (0..NUM_OPS_PER_BATCH)
                .map(|index| {
                    let ordinal = batch * NUM_OPS_PER_BATCH + index;
                    let key = format!("key_{ordinal:016}").into_bytes();
                    let value_byte = u8::try_from(ordinal % 251).expect("value byte fits in u8");
                    let value = vec![value_byte; VALUE_SIZE];
                    (key, value)
                })
                .collect()
        })
        .collect()
}

fn run_batched_write_workload(
    engine: &Engine,
    cf_id: cntryl_midge::ColumnFamilyId,
    batches: &[KeyValueBatch],
) -> u64 {
    let mut completed = 0_u64;

    for keys_vals in batches {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin transaction");

        for (key, value) in keys_vals {
            tx.put(key.clone(), value.clone(), None)
                .expect("put batch value");
        }

        tx.commit(WriteOptions::buffered())
            .expect("commit buffered batch");
        completed += u64::try_from(keys_vals.len()).expect("batch length fits in u64");
    }

    completed
}

fn run_mode(ctx: &mut StressContext, mode: &'static str) {
    init_benchmark_telemetry().expect("initialize benchmark telemetry");
    let opts = write_coordination_opts_for_mode(mode);
    ctx.parameter("storage_profile", mode);
    ctx.parameter("batch_size", NUM_OPS_PER_BATCH);
    ctx.parameter("batch_iterations", BATCH_ITERATIONS);
    ctx.parameter("memtable_size_bytes", opts.memtable_size);
    ctx.parameter(
        "logical_bytes",
        NUM_OPS_PER_BATCH * VALUE_SIZE * BATCH_ITERATIONS,
    );
    stress_config::mark_capped_probe(ctx, "fixed_batch_inventory_regression_probe");

    let engine = Engine::open(opts.to_open_options()).expect("failed to open engine");
    let cf = engine
        .create_column_family("test")
        .expect("failed to create column family");
    let batches = make_key_value_batches();

    let measurement_name = format!("{mode}_batched_write_throughput");
    stress_config::measure_external_counted(ctx, measurement_name, || {
        let completed = run_batched_write_workload(&engine, cf.id(), &batches);
        ((), completed)
    });
}

#[stress(
    tier = 4,
    metadata(component = "engine_batch_throughput", scenario = "memory")
)]
fn memory(ctx: &mut StressContext) {
    run_mode(ctx, "memory");
}

#[stress(
    tier = 4,
    metadata(component = "engine_batch_throughput", scenario = "local")
)]
fn local(ctx: &mut StressContext) {
    run_mode(ctx, "local");
}

stress_main!();
