//! Tier 3 — Durability semantics scenarios (stress harness)
//!
//! mem skips durability by definition.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::MidgeOptions;

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 128;

fn run_durability_puts_case(ctx: &mut StressContext, opts: MidgeOptions, num_ops: usize) {
    ctx.set_elements(num_ops as u64);
    ctx.set_bytes((num_ops * (KEY_SIZE + VALUE_SIZE)) as u64);

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.default_column_family();

    let cf_id = cf.id();
    ctx.measure_ref(&engine, |e| {
        for i in 0..num_ops {
            let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
            let v = vec![(i % 251) as u8; VALUE_SIZE];
            let mut tx = e
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.put(k.to_vec(), v, None).unwrap();
            e.commit(tx, cntryl_midge::WriteOptions::buffered())
                .unwrap();
        }
    });

    // Not timed
    let tx = engine
        .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");
    assert!(tx.get(&[0u8; KEY_SIZE]).is_ok());

    drop(engine);
}

#[stress_test]
fn tier3_durability_async_local(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    run_durability_puts_case(ctx, opts, 10);
}

#[stress_test]
fn tier3_durability_async_local_100(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    run_durability_puts_case(ctx, opts, 100);
}

#[stress_test]
fn tier3_durability_async_local_1000(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    run_durability_puts_case(ctx, opts, 1_000);
}

#[stress_test]
fn tier3_durability_sync_local(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = true;
    run_durability_puts_case(ctx, opts, 10);
}

#[stress_test]
fn tier3_durability_sync_local_100(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = true;
    run_durability_puts_case(ctx, opts, 100);
}

#[stress_test]
fn tier3_durability_sync_local_1000(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = true;
    run_durability_puts_case(ctx, opts, 1_000);
}

stress_main!();
