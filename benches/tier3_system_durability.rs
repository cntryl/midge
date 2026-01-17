//! Tier 3 — Durability sync call cost (single commit measurement)
//!
//! Measures: cost of sync vs async commit call (wal sync only, not data volume)
//! mem skips durability by definition.
//!
//! Tier 3 measures: single put/commit call cost
//! Tier 4 measures: sustained throughput under batching/concurrency

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::MidgeOptions;

const VALUE_SIZE: usize = 128;

fn run_single_durability_call(ctx: &mut StressContext, opts: MidgeOptions) {
    ctx.set_elements(10_000); // moderate (WAL sync/async)

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Precompute one key-value pair outside measurement
    let k = cntryl_midge::testkit::stress::key16_u64_be(0);
    let v = vec![1u8; VALUE_SIZE];

    // Measure ONLY one put/commit call
    ctx.measure_ref(&engine, |e| {
        let mut tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(k.to_vec(), v.clone(), None).unwrap();
        e.commit(tx, cntryl_midge::WriteOptions::buffered())
            .unwrap();
    });

    drop(engine);
}

#[stress_test]
fn tier3_durability_sync_call_local(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = true;
    run_single_durability_call(ctx, opts);
}

#[stress_test]
fn tier3_durability_async_call_local(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    run_single_durability_call(ctx, opts);
}

stress_main!();
