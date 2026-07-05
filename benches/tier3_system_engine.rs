//! Tier 3 â€” Engine primitives (single operation measurement)
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

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use cntryl_midge::testkit::MidgeOptions;

const VALUE_SIZE: usize = 128;
const PUT_BATCH_SIZE: usize = 64;

fn run_single_put_case(ctx: &mut StressContext, opts: MidgeOptions) {
    ctx.set_elements(50_000); // cheap (Âµs-scale)
    ctx.parameter("put_batch_size", PUT_BATCH_SIZE);

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    let keys: Vec<[u8; 16]> = (0..4096)
        .map(cntryl_midge::testkit::stress::key16_u64_be)
        .collect();
    let v = vec![1u8; VALUE_SIZE];
    let mut key_index = 0usize;

    // Measure repeated logical put/commit calls per framework iteration.
    let _ = ctx.measure_batch(PUT_BATCH_SIZE as u64, || {
        for _ in 0..PUT_BATCH_SIZE {
            let k = keys[key_index % keys.len()];
            key_index = key_index.wrapping_add(1);
            let e = &engine;
            let mut tx = e
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.put(k.to_vec(), v.clone(), None).unwrap();
            tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
        }
    });

    drop(engine);
}

fn run_single_get_case(ctx: &mut StressContext, opts: MidgeOptions) {
    ctx.set_elements(50_000); // cheap (Âµs-scale)

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
        tx.commit(cntryl_midge::WriteOptions::best_effort())
            .unwrap();
        engine.flush_cf(&cf).unwrap(); // Ensure durability before measurement
    }

    let k = cntryl_midge::testkit::stress::key16_u64_be(0);

    // Measure ONLY one get call
    let _ = ctx.measure_batch(1, || {
        let e = &engine;
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

#[stress_test(tier = 3)]
fn tier3_engine_put_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_single_put_case(ctx, opts);
}

#[stress_test(tier = 3)]
fn tier3_engine_put_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_single_put_case(ctx, opts);
}

#[stress_test(tier = 3)]
fn tier3_engine_put_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_single_put_case(ctx, opts);
}

#[stress_test(tier = 3)]
fn tier3_engine_get_mem(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_single_get_case(ctx, opts);
}

#[stress_test(tier = 3)]
fn tier3_engine_get_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_single_get_case(ctx, opts);
}

#[stress_test(tier = 3)]
fn tier3_engine_get_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_single_get_case(ctx, opts);
}

stress_main!();
