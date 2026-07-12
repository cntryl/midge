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
//! - Clean reopen (Tier 3: `tier3_system_lifecycle.rs`)

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
use std::time::Duration;
use stress_config::MidgeStressContextExt as _;

use cntryl_midge::MidgeEngine;
use stress_config::MidgeOptions;

const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    stress_config::bench_stress::open_engine_no_compaction(opts)
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
            let k = stress_config::bench_stress::key16_u64_be(i as u64);
            let v = vec![u8::try_from(i % 251).expect("value byte fits in u8"); VALUE_SIZE];
            tx.put(k.to_vec(), v, None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
}

fn run_reopen_after_flush_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: &MidgeOptions,
) {
    // All setup outside measurement: create flushed state
    {
        let e = setup_engine(opts.clone());
        let cf = e.create_column_family("cf1").unwrap();
        write_some(&e, 5_000);
        e.flush_cf(&cf).unwrap(); // Ensure durability before measurement
        let mut e = e;
        e.shutdown(Duration::from_secs(10))
            .expect("prepare flushed recovery benchmark");
    }

    // Measure reopen latency under flushed manifest state
    ctx.set_elements(100);
    stress_config::mark_capped_probe(ctx, "fixed_reopen_count_latency_probe");

    stress_config::measure_external(ctx, scenario, 100, || {
        let mut engine = setup_engine(opts.clone());
        engine
            .shutdown(Duration::from_secs(10))
            .expect("complete flushed recovery measurement");
    });
}

fn run_reopen_after_compaction_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: &MidgeOptions,
) {
    // All setup outside measurement: create multi-level compacted state
    {
        let e = setup_engine(opts.clone());
        let cf = e.create_column_family("cf1").unwrap();
        write_some(&e, 3_000);
        e.flush_cf(&cf).unwrap();
        write_some(&e, 3_000);
        e.flush_cf(&cf).unwrap(); // Ensure durability before compaction
        e.compact_all().unwrap();
        let mut e = e;
        e.shutdown(Duration::from_secs(10))
            .expect("prepare compacted recovery benchmark");
    }

    // Measure reopen latency under compacted multi-level state
    ctx.set_elements(100);
    stress_config::mark_capped_probe(ctx, "fixed_reopen_count_latency_probe");

    stress_config::measure_external(ctx, scenario, 100, || {
        let mut engine = setup_engine(opts.clone());
        engine
            .shutdown(Duration::from_secs(10))
            .expect("complete compacted recovery measurement");
    });
}

#[stress(tier = 4)]
fn tier4_recovery_reopen_after_flush_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_reopen_after_flush_case(ctx, "tier4_recovery_reopen_after_flush_local", &opts);
}

#[stress(tier = 4)]
fn tier4_recovery_reopen_after_flush_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_reopen_after_flush_case(ctx, "tier4_recovery_reopen_after_flush_cloud", &opts);
}

#[stress(tier = 4)]
fn tier4_recovery_reopen_after_compaction_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_reopen_after_compaction_case(ctx, "tier4_recovery_reopen_after_compaction_local", &opts);
}

#[stress(tier = 4)]
fn tier4_recovery_reopen_after_compaction_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_reopen_after_compaction_case(ctx, "tier4_recovery_reopen_after_compaction_cloud", &opts);
}

stress_main!();
