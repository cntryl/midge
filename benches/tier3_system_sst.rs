//! Tier 3 — SST / index behavior scenarios (stress harness)

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{MidgeEngine, MidgeOptions};

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn precompute_keys(num: usize) -> Vec<[u8; KEY_SIZE]> {
    cntryl_midge::testkit::stress::precompute_keys16_u64_be(num)
}

fn run_sst_point_lookup_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    num_keys: usize,
    num_gets: usize,
) {
    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured): create an SST
    let keys = precompute_keys(num_keys);
    let cf_id = cf.id();
    for (i, k) in keys.iter().enumerate() {
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(k.to_vec(), v, None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();
    }
    engine.flush().unwrap();

    ctx.set_elements(num_gets as u64);

    // Measure point-lookup workload
    ctx.measure_ref(&engine, |e| {
        let mut found = 0usize;
        for i in 0..num_gets {
            let k = &keys[i % keys.len()];
            let tx = e
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin");
            if tx.get(&k[..]).unwrap().is_some() {
                found += 1;
            }
        }
        found
    });

    drop(engine);
}

fn run_sst_range_scan_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize) {
    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured): create an SST
    let keys = precompute_keys(num_keys);
    let cf_id = cf.id();
    for (i, k) in keys.iter().enumerate() {
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(k.to_vec(), v, None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();
    }
    engine.flush().unwrap();

    let start = keys[0];
    let end = keys[keys.len() - 1];

    ctx.set_elements(1);

    ctx.measure_ref(&engine, |e| {
        let tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let query = cntryl_midge::Query::new()
            .start_key(bytes::Bytes::copy_from_slice(&start[..]))
            .end_key(bytes::Bytes::copy_from_slice(&end[..]));
        let results = tx.scan(&query).expect("range failed");
    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured): sparse keys (large gaps)
    let mut keys = Vec::with_capacity(2_000);
    let cf_id = cf.id();
    for i in 0..2_000usize {
        let mut k = [0u8; KEY_SIZE];
        // Space keys out by 1<<20 in the high bits.
        let spaced = (i as u64) << 20;
        k[..8].copy_from_slice(&spaced.to_be_bytes());
        keys.push(k);
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(k.to_vec(), v, None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();
    }
    engine.flush().unwrap();

    ctx.set_elements(1);

    // Measure a wide range scan across sparse keyspace
    ctx.measure_ref(&engine, |e| {
        let start = keys[0];
        let end = keys[keys.len() - 1];
        let tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let query = cntryl_midge::Query::new()
            .start_key(bytes::Bytes::copy_from_slice(&start[..]))
            .end_key(bytes::Bytes::copy_from_slice(&end[..]));
        let results = tx.scan(&query).expect("range failed");
    run_sst_point_lookup_case(ctx, opts, 5_000, 10_000);
}

#[stress_test]
fn tier3_sst_point_lookup_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_sst_point_lookup_case(ctx, opts, 5_000, 10_000);
}

#[stress_test]
fn tier3_sst_point_lookup_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_sst_point_lookup_case(ctx, opts, 5_000, 10_000);
}

#[stress_test]
fn tier3_sst_range_scan_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_sst_range_scan_case(ctx, opts, 10_000);
}

#[stress_test]
fn tier3_sst_range_scan_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_sst_range_scan_case(ctx, opts, 10_000);
}

#[stress_test]
fn tier3_sst_sparse_keyspace_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_sst_sparse_keyspace_cloud_case(ctx, opts);
}

stress_main!();
