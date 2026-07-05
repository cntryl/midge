//! Tier 4 â€” Engine Batch Throughput
//!
//! Measures: throughput of batched writes (multiple ops per tx)
//! NOT: single primitive cost (Tier 3)
//!
//! Tier 4 OWNS:
//! - Batching effects: how throughput scales with batch size
//! - Transaction lifecycle under pressure
//! - End-to-end write throughput (ops/sec)
//!
//! NOT measured:
//! - Single put cost (Tier 3: `tier3_system_engine.rs`)
//! - Single get cost (Tier 3: `tier3_system_engine.rs`)

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use cntryl_midge::testkit::MidgeOptions;

const VALUE_SIZE: usize = 128;
const BATCH_COMMITS: usize = 1_000;

fn run_batch_commit_case(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    storage_profile: &str,
    num_ops: usize,
) {
    ctx.tag("storage_profile", storage_profile);
    ctx.tag("batch_size", num_ops.to_string());
    ctx.tag("batch_commits", BATCH_COMMITS.to_string());
    ctx.set_elements(
        u64::try_from(BATCH_COMMITS * num_ops).expect("logical record count fits in u64"),
    );
    ctx.set_bytes(
        u64::try_from(BATCH_COMMITS * num_ops * VALUE_SIZE).expect("logical bytes fit in u64"),
    );

    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Setup (not measured): prepare keys and values
    let mut keys_vals = Vec::with_capacity(num_ops);
    for i in 0..num_ops {
        let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
        let v = vec![u8::try_from(i % 251).expect("value byte fits in u8"); VALUE_SIZE];
        keys_vals.push((k, v));
    }

    // Measure batch throughput: num_ops writes in a single commit
    let measurement_name =
        format!("tier4_engine_batch_commit_throughput_{num_ops}_{storage_profile}");
    stress_config::measure_external(
        ctx,
        measurement_name,
        u64::try_from(num_ops * BATCH_COMMITS).expect("operation count fits"),
        || {
            for _ in 0..BATCH_COMMITS {
                let mut tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin");
                for (k, v) in &keys_vals {
                    tx.put(k.to_vec(), v.clone(), None).expect("put");
                }
                tx.commit(cntryl_midge::WriteOptions::buffered())
                    .expect("commit");
            }
        },
    );

    drop(engine);
}

#[stress(tier = 4)]
fn tier4_engine_batch_commit_throughput_1_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_batch_commit_case(ctx, opts, "local", 1);
}

#[stress(tier = 4)]
fn tier4_engine_batch_commit_throughput_10_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_batch_commit_case(ctx, opts, "local", 10);
}

#[stress(tier = 4)]
fn tier4_engine_batch_commit_throughput_100_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_batch_commit_case(ctx, opts, "local", 100);
}

#[stress(tier = 4)]
fn tier4_engine_batch_commit_throughput_1000_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_batch_commit_case(ctx, opts, "local", 1_000);
}

#[stress(tier = 4)]
fn tier4_engine_batch_commit_throughput_100_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_batch_commit_case(ctx, opts, "cloud", 100);
}

stress_main!();
