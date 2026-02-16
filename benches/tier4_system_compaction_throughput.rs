//! Tier 4 — Compaction Throughput & Scaling Behavior
//!
//! Measures: cascading compaction performance, overlap impact, and degradation
//! under realistic multi-level, multi-batch workloads.
//!
//! Tier 4 OWNS:
//! - Compaction cost scaled by overlap degree
//! - Multi-batch, multi-file cascading
//! - Throughput (keys/sec) vs file count
//! - End-to-end compact_all() across staged LSM states
//!
//! NOT measured:
//! - Single primitive cost (Tier 3)
//! - In-flight memory pressure or cache effects (Tier 4 integration)
//!
//! All setup outside measurement; measured body is one compact_all() call,
//! but the system state before it varies to show scaling.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{MidgeEngine, testkit::MidgeOptions};

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const DEFAULT_VALUE_SIZE: usize = 100;
const TARGET_BATCH: usize = 1_000;
const DEFAULT_COMPACTION_KEYS: usize = 1_000;

fn precompute_kv(num_keys: usize, value_size: usize) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    cntryl_midge::testkit::stress::precompute_kv16_u64_be(num_keys, value_size, u8::MAX)
}

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn run_compact_all_many_sst_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    num_keys: usize,
    value_size: usize,
) {
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
        engine.commit(tx, write_opts).expect("setup commit");
        engine.flush_cf(&cf).expect("setup flush"); // Ensure durability after each batch
    }

    // Measure compact_all() across many pre-flushed SST files
    ctx.set_elements(1); // one compaction cycle

    ctx.measure_ref(&engine, |e| e.compact_all().expect("compact_all failed"));

    drop(engine);
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
                k[0] = (batch % 10) as u8;
                tx.put(k.to_vec(), base_values[idx].clone(), None)
                    .expect("setup put");
            }
            engine.commit(tx, write_opts).expect("setup commit");
            offset = tx_end;
        }
        engine.flush_cf(&cf).expect("setup flush");
    }

    // Measure compact_all() under pathological overlap
    ctx.set_elements(1); // one compaction cycle with high overlap

    ctx.measure_ref(&engine, |e| e.compact_all().expect("compact_all failed"));

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
            engine.commit(tx, write_opts).expect("setup commit");
            offset = tx_end;
        }
        engine.flush_cf(&cf).expect("setup flush");
    }

    // Measure compact_all() under full overlap pressure
    ctx.set_elements(1); // one compaction cycle under maximum overlap

    ctx.measure_ref(&engine, |e| e.compact_all().expect("compact_all failed"));

    drop(engine);
}

// ---------------------------------------------------------------------------
// Stress tests (Tier 4: system behavior under varying conditions)
// ---------------------------------------------------------------------------

#[stress_test]
fn tier4_compaction_compact_all_many_sst_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_compact_all_many_sst_case(ctx, opts, DEFAULT_COMPACTION_KEYS, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier4_compaction_compact_all_many_sst_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_compact_all_many_sst_case(ctx, opts, DEFAULT_COMPACTION_KEYS, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier4_compaction_many_overlapping_l0_files_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_many_overlapping_l0_files_case(ctx, opts, 250, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier4_compaction_many_overlapping_l0_files_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_many_overlapping_l0_files_case(ctx, opts, 250, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier4_compaction_overlap_pressure_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_overlap_pressure_compact_case(ctx, opts, 250, 4, DEFAULT_VALUE_SIZE);
}

#[stress_test]
fn tier4_compaction_overlap_pressure_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_overlap_pressure_compact_case(ctx, opts, 250, 4, DEFAULT_VALUE_SIZE);
}

stress_main!();
