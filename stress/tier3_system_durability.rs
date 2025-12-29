//! Tier 3 — Durability semantics scenarios (stress harness)
//!
//! mem skips durability by definition.

use cntryl_stress::{stress_main, stress_test, StressContext};

use cntryl_midge::{AckPolicy, MidgeEngine, MidgeOptions};

const KEY_SIZE: usize = 16;
const VALUE_SIZE: usize = 128;

fn setup_engine(mut opts: MidgeOptions) -> MidgeEngine {
    // Durability scenarios should not run background compaction.
    opts.enable_compaction = false;
    MidgeEngine::open_with_options(opts).unwrap()
}

fn run_durability_puts_case(ctx: &mut StressContext, opts: MidgeOptions, num_ops: usize) {
    ctx.set_elements(num_ops as u64);
    ctx.set_bytes((num_ops * (KEY_SIZE + VALUE_SIZE)) as u64);

    let engine = setup_engine(opts);
    let cf = engine.default_column_family();

    ctx.measure_ref(&engine, |e| {
        for i in 0..num_ops {
            let mut k = [0u8; KEY_SIZE];
            k[..8].copy_from_slice(&(i as u64).to_be_bytes());
            let v = vec![(i % 251) as u8; VALUE_SIZE];
            e.put(&cf, &k[..], &v).unwrap();
        }
    });

    // Not timed
    assert!(engine.get(&cf, &[0u8; KEY_SIZE]).is_ok());

    drop(engine);
}

#[stress_test]
fn tier3_durability_async_local(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = false;
    opts.ack_policy = AckPolicy::Immediate;
    run_durability_puts_case(ctx, opts, 2_000);
}

#[stress_test]
fn tier3_durability_sync_local(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("local");
    opts.wal_sync = true;
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    run_durability_puts_case(ctx, opts, 2_000);
}

#[stress_test]
fn tier3_durability_async_cloud(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    // "Async" in cloud mode = do not wait for cloud durability.
    opts.ack_policy = AckPolicy::AfterLocalDurable;
    run_durability_puts_case(ctx, opts, 2_000);
}

#[stress_test]
fn tier3_durability_sync_cloud(ctx: &mut StressContext) {
    let mut opts = cntryl_midge::testkit::opts_for_mode("cloud");
    opts.ack_policy = AckPolicy::AfterCloudDurable;
    run_durability_puts_case(ctx, opts, 2_000);
}

stress_main!();
