//! Tier 4 â€” Recovery & Reopen Behavior
//!
//! Measures: engine reopen latency after state-dependent lifecycle events.
//! NOT: single primitive cost (Tier 3)
//!
//! Tier 4 OWNS:
//! - Reopen after flush (manifest replay cost)
//! - Reopen after compaction (multi-level state complexity)
//! - Replay throughput under complex states
//! - State-dependent recovery cost (recovery depends on manifests, WALs, levels)
//!
//! NOT measured:
//! - Clean reopen (Tier 3: tier3_system_recovery.rs)

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use cntryl_midge::{testkit::MidgeOptions, MidgeEngine};

const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn write_some(engine: &MidgeEngine, num_keys: usize) {
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::best_effort(); // Fast setup: skip WAL I/O
    let total = num_keys;

    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for i in start..end {
            let k = cntryl_midge::testkit::stress::key16_u64_be(i as u64);
            let v = vec![(i % 251) as u8; VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
}

fn run_reopen_after_flush_case(ctx: &mut StressContext, opts: MidgeOptions) {
    // All setup outside measurement: create flushed state
    {
        let e = setup_engine(opts.clone());
        let cf = e.create_column_family("cf1").unwrap();
        write_some(&e, 5_000);
        e.flush_cf(&cf).unwrap(); // Ensure durability before measurement
        drop(e);
    }

    // Measure reopen latency under flushed manifest state
    ctx.set_elements(100);

    ctx.measure(|| {
        let engine = setup_engine(opts.clone());
        drop(engine);
    });
}

fn run_reopen_after_compaction_case(ctx: &mut StressContext, opts: MidgeOptions) {
    // All setup outside measurement: create multi-level compacted state
    {
        let e = setup_engine(opts.clone());
        let cf = e.create_column_family("cf1").unwrap();
        write_some(&e, 3_000);
        e.flush_cf(&cf).unwrap();
        write_some(&e, 3_000);
        e.flush_cf(&cf).unwrap(); // Ensure durability before compaction
        e.compact_all().unwrap();
        drop(e);
    }

    // Measure reopen latency under compacted multi-level state
    ctx.set_elements(100);

    ctx.measure(|| {
        let engine = setup_engine(opts.clone());
        drop(engine);
    });
}

#[stress_test]
fn tier4_recovery_reopen_after_flush_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_reopen_after_flush_case(ctx, opts);
}

#[stress_test]
fn tier4_recovery_reopen_after_flush_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_reopen_after_flush_case(ctx, opts);
}

#[stress_test]
fn tier4_recovery_reopen_after_compaction_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_reopen_after_compaction_case(ctx, opts);
}

#[stress_test]
fn tier4_recovery_reopen_after_compaction_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_reopen_after_compaction_case(ctx, opts);
}

stress_main!();
