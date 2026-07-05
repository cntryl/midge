//! Tier 4 â€” Compaction Throughput & Scaling Behavior
//!
//! Measures: cascading compaction performance, overlap impact, and degradation
//! under realistic multi-level, multi-batch workloads.
//!
//! Tier 4 OWNS:
//! - Compaction cost scaled by overlap degree
//! - Multi-batch, multi-file cascading
//! - Throughput (keys/sec) vs file count
//! - End-to-end `compact_all()` across staged LSM states
//!
//! NOT measured:
//! - Single primitive cost (Tier 3)
//! - In-flight memory pressure or cache effects (Tier 4 integration)
//!
//! All setup outside measurement; measured body is one `compact_all()` call,
//! but the system state before it varies to show scaling.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use cntryl_midge::{testkit::MidgeOptions, MidgeEngine};

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const DEFAULT_VALUE_SIZE: usize = 100;
const TARGET_BATCH: usize = 1_000;
const MANY_SST_COMPACTION_KEYS_LOCAL: usize = 10_000;
const MANY_SST_COMPACTION_KEYS_CLOUD: usize = 20_000;
const MANY_SST_LOCAL_SAMPLE_ENGINES: usize = 1;
const MANY_SST_CLOUD_SAMPLE_ENGINES: usize = 64;

fn precompute_kv(num_keys: usize, value_size: usize) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    cntryl_midge::testkit::stress::precompute_kv16_u64_be(num_keys, value_size, u8::MAX)
}

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn setup_many_sst_engine(
    opts: MidgeOptions,
    num_keys: usize,
    value_size: usize,
) -> (MidgeEngine, usize) {
    let (keys, values) = precompute_kv(num_keys, value_size);

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // All setup outside measurement: create multiple flush outputs.
    let chunk = TARGET_BATCH.min(num_keys).max(1);
    let batches = num_keys.div_ceil(chunk);
    let write_opts = cntryl_midge::WriteOptions::best_effort(); // Fast setup: skip WAL I/O

    for i in 0..batches {
        let start = i * chunk;
        let end = if i + 1 == batches {
            num_keys
        } else {
            ((i + 1) * chunk).min(num_keys)
        };
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for idx in start..end {
            tx.put(keys[idx].to_vec(), values[idx].clone(), None)
                .expect("setup put");
        }
        tx.commit(write_opts).expect("setup commit");
        engine.flush_cf(&cf).expect("setup flush"); // Ensure durability after each batch
    }

    (engine, batches)
}

fn set_compaction_signal(
    ctx: &mut StressContext,
    case_name: &str,
    logical_keys: usize,
    value_size: usize,
    input_files: usize,
) {
    ctx.tag("case", case_name);
    ctx.tag("input_keys", logical_keys.to_string());
    ctx.tag("input_files", input_files.to_string());
    ctx.set_elements(logical_keys as u64);
    ctx.set_bytes((logical_keys * (KEY_SIZE + value_size)) as u64);
}

fn run_compact_all_many_sst_case(
    ctx: &mut StressContext,
    storage_profile: &'static str,
    num_keys: usize,
    value_size: usize,
    sample_engines: usize,
) {
    let mut engines = Vec::with_capacity(sample_engines);
    let mut input_files = 0usize;
    for _ in 0..sample_engines {
        let (engine, batches) = setup_many_sst_engine(
            cntryl_midge::testkit::opts_for_mode(storage_profile),
            num_keys,
            value_size,
        );
        input_files += batches;
        engines.push(engine);
    }

    // Measure compact_all() across independent pre-flushed engines. A single
    // compact_all call is sub-millisecond in simulated-cloud mode, so the cloud
    // row needs multiple prepared engines to avoid measuring timer jitter.
    let logical_keys = num_keys * sample_engines;
    set_compaction_signal(ctx, "many_sst", logical_keys, value_size, input_files);
    ctx.parameter("storage_profile", storage_profile);
    ctx.parameter("keys_per_engine", num_keys);
    ctx.parameter("files_per_engine", num_keys.div_ceil(TARGET_BATCH));
    ctx.parameter("sample_engines", sample_engines);

    stress_config::measure_external(ctx, logical_keys as u64, || {
        for engine in &engines {
            engine.compact_all().expect("compact_all failed");
        }
    });

    drop(engines);
}

fn run_many_overlapping_l0_files_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    num_keys_per_batch: usize,
    num_batches: usize,
    value_size: usize,
) {
    let total_keys = num_keys_per_batch * num_batches;
    let (base_keys, base_values) = precompute_kv(total_keys, value_size);

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::buffered();

    // All setup outside measurement: create multiple L0 files with overlapping keyspace.
    for batch in 0..num_batches {
        let start = batch * num_keys_per_batch;
        let end = start + num_keys_per_batch;

        let mut offset = start;
        while offset < end {
            let tx_end = (offset + TARGET_BATCH).min(end);
            let mut tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            for idx in offset..tx_end {
                let mut k = base_keys[idx];
                k[0] = u8::try_from(batch % 10).expect("batch prefix fits in u8");
                tx.put(k.to_vec(), base_values[idx].clone(), None)
                    .expect("setup put");
            }
            tx.commit(write_opts).expect("setup commit");
            offset = tx_end;
        }
        engine.flush_cf(&cf).expect("setup flush");
    }

    // Measure compact_all() under pathological overlap.
    set_compaction_signal(
        ctx,
        "overlapping_l0_files",
        total_keys,
        value_size,
        num_batches,
    );

    stress_config::measure_external(ctx, total_keys as u64, || {
        engine.compact_all().expect("compact_all failed");
    });

    drop(engine);
}

fn run_overlap_pressure_compact_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    num_keys_per_batch: usize,
    num_batches: usize,
    value_size: usize,
) {
    let total_keys = num_keys_per_batch * num_batches;
    let (base_keys, base_values) = precompute_kv(total_keys, value_size);

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::buffered();

    // All setup outside measurement: create many overlapping flush outputs by repeatedly writing the same keyspace.
    for batch in 0..num_batches {
        let batch_start = batch * num_keys_per_batch;
        let batch_end = batch_start + num_keys_per_batch;

        let mut offset = batch_start;
        while offset < batch_end {
            let tx_end = (offset + TARGET_BATCH).min(batch_end);
            let mut tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            for idx in offset..tx_end {
                let k = base_keys[idx];
                tx.put(k.to_vec(), base_values[idx].clone(), None)
                    .expect("setup put");
            }
            tx.commit(write_opts).expect("setup commit");
            offset = tx_end;
        }
        engine.flush_cf(&cf).expect("setup flush");
    }

    // Measure compact_all() under full overlap pressure.
    set_compaction_signal(ctx, "overlap_pressure", total_keys, value_size, num_batches);

    stress_config::measure_external(ctx, total_keys as u64, || {
        engine.compact_all().expect("compact_all failed");
    });

    drop(engine);
}

// ---------------------------------------------------------------------------
// Stress tests (Tier 4: system behavior under varying conditions)
// ---------------------------------------------------------------------------

#[stress_test(tier = 4)]
fn tier4_compaction_compact_all_many_sst_local(ctx: &mut StressContext) {
    run_compact_all_many_sst_case(
        ctx,
        "local",
        MANY_SST_COMPACTION_KEYS_LOCAL,
        DEFAULT_VALUE_SIZE,
        MANY_SST_LOCAL_SAMPLE_ENGINES,
    );
}

#[stress_test(tier = 4)]
fn tier4_compaction_compact_all_many_sst_cloud(ctx: &mut StressContext) {
    run_compact_all_many_sst_case(
        ctx,
        "cloud",
        MANY_SST_COMPACTION_KEYS_CLOUD,
        DEFAULT_VALUE_SIZE,
        MANY_SST_CLOUD_SAMPLE_ENGINES,
    );
}

#[stress_test(tier = 4)]
fn tier4_compaction_many_overlapping_l0_files_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_many_overlapping_l0_files_case(ctx, opts, 2_500, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test(tier = 4)]
fn tier4_compaction_many_overlapping_l0_files_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_many_overlapping_l0_files_case(ctx, opts, 2_500, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test(tier = 4)]
fn tier4_compaction_overlap_pressure_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_overlap_pressure_compact_case(ctx, opts, 2_500, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test(tier = 4)]
fn tier4_compaction_overlap_pressure_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_overlap_pressure_compact_case(ctx, opts, 2_500, 4, DEFAULT_VALUE_SIZE);
}

stress_main!();
