//! Tier 3 — Engine basics scenarios (stress harness)
//!
//! This file intentionally avoids Criterion.
//! Each scenario is a **single-shot** stress test with an explicit name.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{Key, MidgeEngine, MidgeOptions, Value, WriteBatch};

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 128;

fn precompute_kv(num_keys: usize) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    let mut keys = Vec::with_capacity(num_keys);
    let mut values = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        let mut k = [0u8; KEY_SIZE];
        k[..8].copy_from_slice(&(i as u64).to_be_bytes());
        keys.push(k);
        values.push(vec![(i % 251) as u8; VALUE_SIZE]);
    }

    (keys, values)
}

fn setup_engine(mut opts: MidgeOptions) -> MidgeEngine {
    // Engine basics scenarios should not run background compaction.
    opts.enable_compaction = false;
    MidgeEngine::open_with_options(opts).unwrap()
}

fn run_put_get_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize) {
    let (keys, values) = precompute_kv(num_keys);

    ctx.set_elements((num_keys * 2) as u64);
    ctx.set_bytes((num_keys * (KEY_SIZE + VALUE_SIZE)) as u64);

    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    ctx.measure_ref(&engine, |e| {
        for (k, v) in keys.iter().zip(values.iter()) {
            e.put(&cf, &k[..], v).unwrap();
        }

        let mut found = 0usize;
        for k in keys.iter() {
            if e.get(&cf, &k[..]).unwrap().is_some() {
                found += 1;
            }
        }
        found
    });

    // Quick correctness smoke (not timed)
    assert!(engine.get(&cf, &keys[0][..]).unwrap().is_some());

    drop(engine);
}

fn run_write_batch_case(ctx: &mut StressContext, opts: MidgeOptions, num_ops: usize) {
    ctx.set_elements(num_ops as u64);
    ctx.set_bytes((num_ops * (KEY_SIZE + VALUE_SIZE)) as u64);

    let engine = setup_engine(opts);

    // Setup (not measured)
    let mut batch = WriteBatch::new();
    for i in 0..num_ops {
        let mut k = [0u8; KEY_SIZE];
        k[..8].copy_from_slice(&(i as u64).to_be_bytes());
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        batch.put(Key::copy_from_slice(&k[..]), Value::from(v));
    }

    // Measure exactly one write_batch
    ctx.measure_ref(&engine, |e| e.write_batch(&batch).expect("write_batch failed"));

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
