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
//! - Single put cost (Tier 3: tier3_system_engine.rs)
//! - Single get cost (Tier 3: tier3_system_engine.rs)

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use cntryl_midge::testkit::MidgeOptions;

const VALUE_SIZE: usize = 128;

fn run_batch_commit_case(ctx: &mut StressContext, opts: MidgeOptions, num_ops: usize) {
    let engine = cntryl_midge::testkit::stress::open_engine_no_compaction(opts);
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Setup (not measured): prepare keys and values
    let mut keys_vals = Vec::with_capacity(num_ops);
    for i in 0..num_ops {
        let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
        let v = vec![(i % 251) as u8; VALUE_SIZE];
        keys_vals.push((k, v));
    }

    // Measure batch throughput: num_ops writes in a single commit
    // Amortized over 1000 batches to measure stable throughput
    ctx.set_elements(1_000);

    ctx.measure_ref(&engine, |e| {
        let mut tx = e
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for (k, v) in &keys_vals {
            tx.put(k.to_vec(), v.clone(), None).expect("put");
        }
        tx.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit")
    });

    drop(engine);
}

#[stress_test]
fn tier4_engine_batch_commit_throughput_100_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_batch_commit_case(ctx, opts, 100);
}

#[stress_test]
fn tier4_engine_batch_commit_throughput_100_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_batch_commit_case(ctx, opts, 100);
}

stress_main!();
