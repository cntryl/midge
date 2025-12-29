//! Tier 3 — SST / index behavior scenarios (stress harness)

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{MidgeEngine, MidgeOptions};

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 64;

fn setup_engine(mut opts: MidgeOptions) -> MidgeEngine {
    opts.enable_compaction = false;
    MidgeEngine::open_with_options(opts).unwrap()
}

fn precompute_keys(num: usize) -> Vec<[u8; KEY_SIZE]> {
    let mut keys = Vec::with_capacity(num);
    for i in 0..num {
        let mut k = [0u8; KEY_SIZE];
        k[..8].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(k);
    }
    keys
}

fn run_sst_point_lookup_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize, num_gets: usize) {
    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured): create an SST
    let keys = precompute_keys(num_keys);
    for (i, k) in keys.iter().enumerate() {
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        engine.put(&cf, &k[..], &v).unwrap();
    }
    engine.flush().unwrap();

    ctx.set_elements(num_gets as u64);

    // Measure point-lookup workload
    ctx.measure_ref(&engine, |e| {
        let mut found = 0usize;
        for i in 0..num_gets {
            let k = &keys[i % keys.len()];
            if e.get(&cf, &k[..]).unwrap().is_some() {
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
    for (i, k) in keys.iter().enumerate() {
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        engine.put(&cf, &k[..], &v).unwrap();
    }
    engine.flush().unwrap();

    let start = keys[0];
    let end = keys[keys.len() - 1];

    ctx.set_elements(1);

    ctx.measure_ref(&engine, |e| {
        let results = e.range(&cf, &start[..], &end[..]).expect("range failed");
        results.len()
    });

    drop(engine);
}

fn run_sst_sparse_keyspace_cloud_case(ctx: &mut StressContext, opts: MidgeOptions) {
    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured): sparse keys (large gaps)
    let mut keys = Vec::with_capacity(2_000);
    for i in 0..2_000usize {
        let mut k = [0u8; KEY_SIZE];
        // Space keys out by 1<<20 in the high bits.
        let spaced = (i as u64) << 20;
        k[..8].copy_from_slice(&spaced.to_be_bytes());
        keys.push(k);
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        engine.put(&cf, &k[..], &v).unwrap();
    }
    engine.flush().unwrap();

    ctx.set_elements(1);

    // Measure a wide range scan across sparse keyspace
    ctx.measure_ref(&engine, |e| {
        let start = keys[0];
        let end = keys[keys.len() - 1];
        let results = e.range(&cf, &start[..], &end[..]).expect("range failed");
        results.len()
    });

    drop(engine);
}

#[stress_test]
fn tier3_sst_point_lookup_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
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
