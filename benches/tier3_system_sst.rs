//! Tier 3 — SST primitives (single operation measurement)
//!
//! Measures: cost of point seek, iterator construction, first advance
//! NOT: full scans, iteration, payload processing

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use cntryl_midge::{testkit::MidgeOptions, MidgeEngine};

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn precompute_keys(num: usize) -> Vec<[u8; KEY_SIZE]> {
    cntryl_midge::testkit::stress::precompute_keys16_u64_be(num)
}

fn run_sst_point_seek_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize) {
    ctx.set_elements(10_000); // moderate (seek has I/O)

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup outside measurement: create an SST
    let keys = precompute_keys(num_keys);
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::buffered();
    let total = keys.len();
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for (i, k) in keys[start..end].iter().enumerate() {
            let idx = start + i;
            let v = vec![(idx % 251) as u8; VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        engine.commit(tx, write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    let k = keys[num_keys / 2];

    // Measure ONLY one point get call
    ctx.measure_ref(&engine, |e| {
        let tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let _ = tx.get(&k[..]).unwrap();
    });

    drop(engine);
}

fn run_sst_range_seek_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize) {
    ctx.set_elements(10_000); // moderate (seek has I/O)

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup outside measurement: create an SST
    let keys = precompute_keys(num_keys);
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::buffered();
    let total = keys.len();
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for (i, k) in keys[start..end].iter().enumerate() {
            let idx = start + i;
            let v = vec![(idx % 251) as u8; VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        engine.commit(tx, write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    let start = keys[0];
    let end = keys[keys.len() - 1];

    // Measure ONLY iterator construction and first advance (seek)
    ctx.measure_ref(&engine, |e| {
        let tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let query = cntryl_midge::Query::new()
            .start_key(cntryl_midge::Bytes::copy_from_slice(&start[..]))
            .end_key(cntryl_midge::Bytes::copy_from_slice(&end[..]));
        let mut it = tx.scan(&query).expect("scan failed");
        let _ = it.next();
    });

    drop(engine);
}

fn run_sst_sparse_keyspace_seek_case(ctx: &mut StressContext, opts: MidgeOptions) {
    ctx.set_elements(10_000); // moderate (seek has I/O)

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup outside measurement: sparse keys (large gaps)
    let mut keys = Vec::with_capacity(2_000);
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::buffered();
    let total = 2_000usize;
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for i in start..end {
            let mut k = [0u8; KEY_SIZE];
            let spaced = (i as u64) << 20;
            k[..8].copy_from_slice(&spaced.to_be_bytes());
            keys.push(k);
            let v = vec![(i % 251) as u8; VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        engine.commit(tx, write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    let start = keys[0];
    let end = keys[keys.len() - 1];

    // Measure ONLY iterator construction and first advance across sparse keyspace
    ctx.measure_ref(&engine, |e| {
        let tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let query = cntryl_midge::Query::new()
            .start_key(cntryl_midge::Bytes::copy_from_slice(&start[..]))
            .end_key(cntryl_midge::Bytes::copy_from_slice(&end[..]));
        let mut it = tx.scan(&query).expect("scan failed");
        let _ = it.next();
    });

    drop(engine);
}

#[stress_test]
fn tier3_sst_point_seek_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_sst_point_seek_case(ctx, opts, 5_000);
}

#[stress_test]
fn tier3_sst_point_seek_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_sst_point_seek_case(ctx, opts, 5_000);
}

#[stress_test]
fn tier3_sst_range_seek_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_sst_range_seek_case(ctx, opts, 10_000);
}

#[stress_test]
fn tier3_sst_range_seek_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_sst_range_seek_case(ctx, opts, 10_000);
}

#[stress_test]
fn tier3_sst_sparse_keyspace_seek_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_sst_sparse_keyspace_seek_case(ctx, opts);
}

stress_main!();
