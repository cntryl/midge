//! Tier 3 — Engine primitives (single operation measurement)
//!
//! Measures: cost of individual put/get/commit calls
//! NOT: bulk operations, batch throughput, or volume scaling
//!
//! **Measurement Notes:**
//! - Memory mode: reads from in-memory skiplist (memtable)
//! - Local mode: reads from flushed SST via block cache
//! - Cloud mode: reads from cloud-backed SST via block cache
//!
//! Different storage modes may show different latencies because they exercise
//! different code paths. This is expected and informative, not a bug.
//! Memory mode hits memtable, while local/cloud modes hit the block cache
//! after the setup flush.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::testkit::MidgeOptions;

const VALUE_SIZE: usize = 128;

fn run_single_put_case(ctx: &mut StressContext, opts: MidgeOptions) {
    ctx.set_elements(50_000); // cheap (µs-scale)

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

fn run_single_get_case(ctx: &mut StressContext, opts: MidgeOptions) {
    ctx.set_elements(50_000); // cheap (µs-scale)

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Setup (not measured): write one key
    {
        let k = cntryl_midge::testkit::stress::key16_u64_be(0);
        let v = vec![1u8; VALUE_SIZE];
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(k.to_vec(), v, None).unwrap();
        engine
            .commit(tx, cntryl_midge::WriteOptions::best_effort())
            .unwrap();
        engine.flush_cf(&cf).unwrap(); // Ensure durability before measurement
    }

    let k = cntryl_midge::testkit::stress::key16_u64_be(0);

    // Measure ONLY one get call
    ctx.measure_ref(&engine, |e| {
        let tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin");
        let _ = tx.get(&k[..]).unwrap();
    });

    drop(engine);
}

// MOVED TO TIER 4: batch throughput testing belongs in tier4_system_engine.rs
// This was a Tier 3 violation: loop inside measured body violates Rule 3.

// ---------------------------------------------------------------------------
// Stress tests
// ---------------------------------------------------------------------------

#[stress_test]
fn tier3_engine_put_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_single_put_case(ctx, opts);
}

#[stress_test]
fn tier3_engine_put_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_single_put_case(ctx, opts);
}

#[stress_test]
fn tier3_engine_put_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_single_put_case(ctx, opts);
}

#[stress_test]
fn tier3_engine_get_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_single_get_case(ctx, opts);
}

#[stress_test]
fn tier3_engine_get_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_single_get_case(ctx, opts);
}

#[stress_test]
fn tier3_engine_get_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_single_get_case(ctx, opts);
}

stress_main!();
