//! Tier 3 â€” Durability sync call cost (single commit measurement)
//!
//! Measures: cost of sync vs async commit call (wal sync only, not data volume)
//! mem skips durability by definition.
//!
//! Tier 3 measures: single put/commit call cost
//! Tier 4 measures: sustained throughput under batching/concurrency

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use cntryl_midge::testkit::MidgeOptions;

const VALUE_SIZE: usize = 128;

fn run_single_durability_call(ctx: &mut StressContext, scenario: &'static str, opts: MidgeOptions) {
    ctx.set_elements(10_000); // moderate (WAL sync/async)

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Precompute one key-value pair outside measurement
    let k = cntryl_midge::testkit::stress::key16_u64_be(0);
    let v = vec![1u8; VALUE_SIZE];

    // Measure ONLY one put/commit call
    let _ = ctx.measure_batch(scenario, 1, || {
        let e = &engine;
        let mut tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        tx.put(k.to_vec(), v.clone(), None).unwrap();
        tx.commit(cntryl_midge::WriteOptions::buffered()).unwrap();
    });

    drop(engine);
}

#[stress(tier = 3)]
fn tier3_durability_sync_call_local(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = true;
    run_single_durability_call(ctx, "tier3_durability_sync_call_local", opts);
}

#[stress(tier = 3)]
fn tier3_durability_async_call_local(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    run_single_durability_call(ctx, "tier3_durability_async_call_local", opts);
}

stress_main!();
