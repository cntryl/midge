//! Tier 3 â€” MVCC primitives (single version operation measurement)
//!
//! Measures: cost of version checks and single version lookups
//! NOT: sustained overwrites, version chain length scaling

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use cntryl_midge::{testkit::MidgeOptions, MidgeEngine};

const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn run_single_version_write_case(ctx: &mut StressContext, opts: MidgeOptions) {
    ctx.set_elements(50_000); // cheap (Âµs-scale)

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Precompute one key outside measurement
    let k = cntryl_midge::testkit::stress::key16_u64_be(0);
    let v = vec![1u8; VALUE_SIZE];

    // Measure ONLY one single overwrite call
    ctx.measure_ref(&engine, |e| {
        let mut tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(k.to_vec(), v.clone(), None).unwrap();
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    });

    drop(engine);
}

fn run_read_old_version_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize) {
    ctx.set_elements(10_000); // moderate (compaction + lookup)

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // Setup (not measured): write in TARGET_BATCH-sized transactions
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::buffered();
    let total = num_keys;

    let mut keys = Vec::with_capacity(num_keys);
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for i in start..end {
            let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
            keys.push(k);
            tx.put(k.to_vec(), vec![1u8; VALUE_SIZE], None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    // Create snapshot via transaction
    let snap_tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");

    // Write ONE newer version to demonstrate old-version visibility
    {
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(keys[0].to_vec(), vec![2u8; VALUE_SIZE], None)
            .unwrap();
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();
    engine.compact_all().unwrap();

    // Measure reading ONE old version via snapshot transaction
    ctx.measure_ref(&snap_tx, |s| {
        let v = s.get(&keys[0][..]).unwrap();
        if let Some(bytes) = v {
            if bytes.as_ref() == vec![1u8; VALUE_SIZE].as_slice() {
                1
            } else {
                0
            }
        } else {
            0
        }
    });

    drop(engine);
}

#[stress_test]
fn tier3_mvcc_single_version_write_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_single_version_write_case(ctx, opts);
}

#[stress_test]
fn tier3_mvcc_single_version_write_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_single_version_write_case(ctx, opts);
}

#[stress_test]
fn tier3_mvcc_single_version_write_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_single_version_write_case(ctx, opts);
}

#[stress_test]
fn tier3_mvcc_read_old_version_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_read_old_version_case(ctx, opts, 1_000);
}

#[stress_test]
fn tier3_mvcc_read_old_version_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_read_old_version_case(ctx, opts, 1_000);
}

stress_main!();
