//! Tier 3 — MVCC / version pressure scenarios (stress harness)

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{MidgeEngine, MidgeOptions};

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn run_overwrite_hot_keys_case(ctx: &mut StressContext, opts: MidgeOptions, num_hot: usize, rounds: usize) {
    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured)
    let mut keys = Vec::with_capacity(num_hot);
    for i in 0..num_hot {
        keys.push(cntryl_midge::testkit::stress::key16_u64_be(i as u64));
    }

    let total_ops = num_hot * rounds;
    ctx.set_elements(total_ops as u64);
    ctx.set_bytes((total_ops * (KEY_SIZE + VALUE_SIZE)) as u64);

    // Measure overwrite pressure
    ctx.measure_ref(&engine, |e| {
        for r in 0..rounds {
            let fill = (r % 251) as u8;
            let v = vec![fill; VALUE_SIZE];
            for k in keys.iter() {
                e.put(&cf, &k[..], &v).unwrap();
            }
        }
    });

    // Not timed
    assert!(engine.get(&cf, &keys[0][..]).unwrap().is_some());

    drop(engine);
}

fn run_read_old_versions_case(ctx: &mut StressContext, opts: MidgeOptions, num_keys: usize) {
    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    // Setup (not measured)
    let mut keys = Vec::with_capacity(num_keys);
    for i in 0..num_keys {
        let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
        keys.push(k);
        engine.put(&cf, &k[..], &vec![1u8; VALUE_SIZE]).unwrap();
    }
    engine.flush().unwrap();

    let snap = engine.snapshot_cf(&cf);

    for k in keys.iter() {
        engine.put(&cf, &k[..], &vec![2u8; VALUE_SIZE]).unwrap();
    }
    engine.flush().unwrap();
    engine.compact_all().unwrap();

    ctx.set_elements(num_keys as u64);

    // Measure reading old versions via snapshot
    ctx.measure_ref(&snap, |s| {
        let mut ok = 0usize;
        for k in keys.iter() {
            let v = s.get(&cf, &k[..]).unwrap();
            if let Some(bytes) = v {
                if bytes.as_ref() == vec![1u8; VALUE_SIZE].as_slice() {
                    ok += 1;
                }
            }
        }
        ok
    });

    // Not timed
    let v0 = snap.get(&cf, &keys[0][..]).unwrap().unwrap();
    assert_eq!(v0.as_ref(), vec![1u8; VALUE_SIZE].as_slice());

    drop(engine);
}

#[stress_test]
fn tier3_mvcc_overwrite_hot_keys_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_overwrite_hot_keys_case(ctx, opts, 128, 64);
}

#[stress_test]
fn tier3_mvcc_overwrite_hot_keys_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_overwrite_hot_keys_case(ctx, opts, 128, 64);
}

#[stress_test]
fn tier3_mvcc_overwrite_hot_keys_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_overwrite_hot_keys_case(ctx, opts, 128, 64);
}

#[stress_test]
fn tier3_mvcc_read_old_versions_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_read_old_versions_case(ctx, opts, 1_000);
}

#[stress_test]
fn tier3_mvcc_read_old_versions_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_read_old_versions_case(ctx, opts, 1_000);
}

stress_main!();
