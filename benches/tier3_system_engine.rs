//! Tier 3 — Engine basics scenarios (stress harness)
//!
//! This file intentionally avoids Criterion.
//! Each scenario is a **single-shot** stress test with an explicit name.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::MidgeOptions;

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 128;

fn precompute_kv(num_keys: usize) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    cntryl_midge::testkit::stress::precompute_kv16_u64_be(num_keys, VALUE_SIZE, 251)
}

fn run_put_get_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize) {
    let (keys, values) = precompute_kv(num_keys);

    ctx.set_elements((num_keys * 2) as u64);
    ctx.set_bytes((num_keys * (KEY_SIZE + VALUE_SIZE)) as u64);

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.default_column_family();
    let cf_id = cf.id();

    ctx.measure_ref(&engine, |e| {
        for (k, v) in keys.iter().zip(values.iter()) {
            let mut tx = e.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).expect("begin");
            tx.put(k.to_vec(), v.clone(), None).unwrap();
            e.commit(tx, cntryl_midge::WriteOptions::buffered()).unwrap();
        }

        let mut found = 0usize;
        for k in keys.iter() {
            let tx = e.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).expect("begin");
            if tx.get(&k[..]).unwrap().is_some() {
                found += 1;
            }
        }
        found
    });

    // Quick correctness smoke (not timed)
    let tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).expect("begin");
    assert!(tx.get(&keys[0][..]).unwrap().is_some());

    drop(engine);
}

fn run_write_batch_case(ctx: &mut StressContext, opts: MidgeOptions, num_ops: usize) {
    ctx.set_elements(num_ops as u64);
    ctx.set_bytes((num_ops * (KEY_SIZE + VALUE_SIZE)) as u64);

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.default_column_family();
    let cf_id = cf.id();

    // Setup (not measured): prepare keys and values
    let mut keys_vals = Vec::with_capacity(num_ops);
    for i in 0..num_ops {
        let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        keys_vals.push((k, v));
    }

    // Measure exactly one transaction with multiple puts
    ctx.measure_ref(&engine, |e| {
        let mut tx = e.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).expect("begin");
        for (k, v) in &keys_vals {
            tx.put(k.to_vec(), v.clone(), None).expect("put");
        }
        e.commit(tx, cntryl_midge::WriteOptions::buffered()).expect("commit")
    });

    drop(engine);
}

// ---------------------------------------------------------------------------
// Stress tests (same system question × different storage = different function)
// ---------------------------------------------------------------------------

#[stress_test]
fn tier3_engine_put_get_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_put_get_case(ctx, opts, 2_000);
}

#[stress_test]
fn tier3_engine_put_get_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_put_get_case(ctx, opts, 2_000);
}

#[stress_test]
fn tier3_engine_put_get_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_put_get_case(ctx, opts, 2_000);
}

#[stress_test]
fn tier3_engine_write_batch_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_write_batch_case(ctx, opts, 2_000);
}

#[stress_test]
fn tier3_engine_write_batch_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_write_batch_case(ctx, opts, 2_000);
}

#[stress_test]
fn tier3_engine_write_batch_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_write_batch_case(ctx, opts, 2_000);
}

stress_main!();
