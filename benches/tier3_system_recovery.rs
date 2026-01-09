//! Tier 3 — Recovery / reopen paths (stress harness)
//!
//! Not meaningful for pure memory; only local and cloud.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{MidgeEngine, MidgeOptions};

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 64;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn write_some(engine: &MidgeEngine, num_keys: usize) {
    let cf = engine.default_column_family();
    let cf_id = cf.id();
    for i in 0..num_keys {
        let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        let mut tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).expect("begin");
        tx.put(k.to_vec(), v, None).unwrap();
        engine.commit(tx, cntryl_midge::WriteOptions::default()).unwrap();
    }
}

fn run_reopen_clean_case(ctx: &mut StressContext, opts: MidgeOptions) {
    // Setup (not measured): create initial metadata, then close.
    {
        let engine = setup_engine(opts.clone());
        drop(engine);
    }

    ctx.set_elements(1);

    // Measure reopen
    ctx.measure(|| {
        let engine = setup_engine(opts);
        drop(engine);
    });
}

fn run_reopen_after_flush_case(ctx: &mut StressContext, opts: MidgeOptions) {
    // Setup (not measured)
    {
        let engine = setup_engine(opts.clone());
        write_some(&engine, 5_000);
        engine.flush().unwrap();
        drop(engine);
    }

    ctx.set_elements(1);

    ctx.measure(|| {
        let engine = setup_engine(opts);
        drop(engine);
    });
}

fn run_reopen_after_compaction_case(ctx: &mut StressContext, opts: MidgeOptions) {
    // Setup (not measured)
    {
        let engine = setup_engine(opts.clone());
        write_some(&engine, 3_000);
        engine.flush().unwrap();
        write_some(&engine, 3_000);
        engine.flush().unwrap();
        engine.compact_all().unwrap();
        drop(engine);
    }

    ctx.set_elements(1);

    ctx.measure(|| {
        let engine = setup_engine(opts);
        drop(engine);
    });
}

fn run_wal_replay_case(ctx: &mut StressContext, opts: MidgeOptions) {
    // Setup (not measured): writes without flush
    {
        let engine = setup_engine(opts.clone());
        write_some(&engine, 5_000);
        drop(engine);
    }

    ctx.set_elements(1);

    // Measure reopen (includes WAL replay)
    ctx.measure(|| {
        let engine = setup_engine(opts.clone());
        let cf = engine.default_column_family();
        let cf_id = cf.id();
        let k0 = [0u8; KEY_SIZE];
        let tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).expect("begin");
        let _ = tx.get(&k0[..]).unwrap();
        drop(engine);
    });
}

#[stress_test]
fn tier3_recovery_reopen_clean_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_reopen_clean_case(ctx, opts);
}

#[stress_test]
fn tier3_recovery_reopen_clean_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_reopen_clean_case(ctx, opts);
}

#[stress_test]
fn tier3_recovery_reopen_after_flush_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_reopen_after_flush_case(ctx, opts);
}

#[stress_test]
fn tier3_recovery_reopen_after_flush_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_reopen_after_flush_case(ctx, opts);
}

#[stress_test]
fn tier3_recovery_reopen_after_compaction_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_reopen_after_compaction_case(ctx, opts);
}

#[stress_test]
fn tier3_recovery_reopen_after_compaction_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_reopen_after_compaction_case(ctx, opts);
}

#[stress_test]
fn tier3_recovery_wal_replay_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_wal_replay_case(ctx, opts);
}

#[stress_test]
fn tier3_recovery_wal_replay_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_wal_replay_case(ctx, opts);
}

stress_main!();
