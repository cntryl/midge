//! Tier 4 â€” Cloud durability semantics scenarios (stress harness)
//!
//! Cloud runs are dominated by network/object-store latency and are inherently
//! slower/less deterministic than local-only durability. Keeping these in Tier 4
//! avoids making Tier 3 runs long-running.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use cntryl_midge::testkit::MidgeOptions;
use cntryl_midge::WriteOptions;

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const VALUE_SIZE: usize = 128;

fn run_durability_puts_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    num_ops: usize,
    mode_name: &str,
    write_opts: WriteOptions,
) {
    ctx.tag("durability_mode", mode_name);
    ctx.set_elements(num_ops as u64);
    ctx.set_bytes((num_ops * (KEY_SIZE + VALUE_SIZE)) as u64);

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    ctx.measure_ref(&engine, |e| {
        for i in 0..num_ops {
            let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
            let value_byte = u8::try_from(i % 251).expect("value byte fits in u8");
            let v = vec![value_byte; VALUE_SIZE];
            let mut tx = e
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                .expect("begin");
            tx.put(k.to_vec(), v, None).unwrap();
            tx.commit(write_opts).unwrap();
        }
    });

    // Not timed
    let tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");
    assert!(tx.get(&[0u8; KEY_SIZE]).is_ok());

    drop(engine);
}

#[stress_test]
fn tier4_durability_async_cloud_1000(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_durability_puts_case(
        ctx,
        opts,
        1_000,
        "cloud_buffered_async",
        WriteOptions::buffered(),
    );
}

#[stress_test]
fn tier4_durability_sync_seal_cloud_250(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_durability_puts_case(ctx, opts, 250, "cloud_sync_seal", WriteOptions::sync());
}

#[stress_test]
fn tier4_durability_cloud_strict_100(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_durability_puts_case(
        ctx,
        opts,
        100,
        "cloud_strict_ack",
        WriteOptions::cloud_strict(),
    );
}

stress_main!();
