//! Tier 4 — Cloud durability semantics scenarios (stress harness)
//!
//! Cloud runs are dominated by network/object-store latency and are inherently
//! slower/less deterministic than local-only durability. Keeping these in Tier 4
//! avoids making Tier 3 runs long-running.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{AckPolicy, MidgeOptions};

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
            let mut tx = e.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).expect("begin");
            tx.put(k.to_vec(), v, None).unwrap();
            e.commit(tx, cntryl_midge::WriteOptions::buffered()).unwrap();
        }
    });

    // Not timed
    let tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).expect("begin");
    assert!(tx.get(&[0u8; KEY_SIZE]).is_ok());

    drop(engine);
}

#[stress_test]
fn tier4_durability_async_cloud(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    // "Async" in cloud mode = do not wait for cloud durability.
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    run_durability_puts_case(ctx, opts, 10);
}

#[stress_test]
fn tier4_durability_async_cloud_100(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    // "Async" in cloud mode = do not wait for cloud durability.
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    run_durability_puts_case(ctx, opts, 100);
}

#[stress_test]
fn tier4_durability_async_cloud_1000(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    // "Async" in cloud mode = do not wait for cloud durability.
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    run_durability_puts_case(ctx, opts, 1_000);
}

#[stress_test]
fn tier4_durability_sync_cloud(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    opts.ack_policy = AckPolicy::AfterCloudDurable;
    run_durability_puts_case(ctx, opts, 10);
}

#[stress_test]
fn tier4_durability_sync_cloud_100(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    opts.ack_policy = AckPolicy::AfterCloudDurable;
    run_durability_puts_case(ctx, opts, 100);
}

#[stress_test]
fn tier4_durability_sync_cloud_1000(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    opts.ack_policy = AckPolicy::AfterCloudDurable;
    run_durability_puts_case(ctx, opts, 1_000);
}

stress_main!();
