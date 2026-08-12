//! Tier 3 — clean durable lifecycle boundaries.
//!
//! Clean reopen belongs here; recovery from flushed or compacted state remains
//! Tier 4 because it is state-dependent recovery work.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::{TransactionMode, WriteOptions};
use cntryl_stress::{stress, stress_main, StressContext};
use std::time::Duration;

const FLUSH_BATCH_SIZE: usize = 32;
const FLUSH_CYCLES_PER_SAMPLE: u64 = 3;

fn row_metadata(
    ctx: &mut StressContext,
    unit: &'static str,
    mode: &'static str,
    logical_batch_size: u64,
) {
    ctx.parameter("logical_batch_size", logical_batch_size);
    ctx.parameter("logical_unit", unit);
    ctx.parameter("storage_mode", mode);
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");
    ctx.parameter("local_gate_rsd_limit_pct", 5);
}

fn run_flush_cycle(ctx: &mut StressContext, scenario: &'static str, mode: &'static str) {
    row_metadata(ctx, "write_and_flush_cycle", mode, FLUSH_CYCLES_PER_SAMPLE);
    let engine =
        stress_config::bench_stress::open_engine_no_compaction(stress_config::opts_for_mode(mode));
    let cf = engine
        .create_column_family("lifecycle")
        .expect("create lifecycle CF");
    let mut batch = 0_u64;
    let mut failures = 0_u64;

    let _ = ctx.measure_batch(scenario, FLUSH_CYCLES_PER_SAMPLE, || {
        for _ in 0..FLUSH_CYCLES_PER_SAMPLE {
            let Ok(mut tx) = engine.begin_tx(cf.id(), TransactionMode::ReadWrite) else {
                failures += 1;
                return;
            };
            for offset in 0..FLUSH_BATCH_SIZE {
                let key = stress_config::bench_stress::key16_u64_be(
                    batch * FLUSH_BATCH_SIZE as u64 + offset as u64,
                );
                if tx
                    .put(
                        key.to_vec(),
                        vec![u8::try_from(offset).expect("byte fits"); 64],
                        None,
                    )
                    .is_err()
                {
                    failures += 1;
                    return;
                }
            }
            batch = batch.wrapping_add(1);
            let write_options = if mode == "cloud" {
                WriteOptions::cloud_async()
            } else {
                WriteOptions::buffered()
            };
            if tx.commit(write_options).is_err() || engine.flush_cf(&cf).is_err() {
                failures += 1;
                return;
            }
        }
    });

    assert_eq!(failures, 0, "measured write-and-flush cycles must succeed");
    drop(engine);
}

fn run_clean_reopen(ctx: &mut StressContext, scenario: &'static str, mode: &'static str) {
    row_metadata(ctx, "clean_reopen", mode, 1);
    let opts = stress_config::opts_for_mode(mode);
    {
        let mut engine = stress_config::bench_stress::open_engine_no_compaction(opts.clone());
        engine
            .shutdown(Duration::from_secs(10))
            .expect("prepare clean reopen benchmark");
    }
    let mut failures = 0_u64;

    let _ = ctx.measure_batch(scenario, 1, || {
        match cntryl_midge::Engine::open(opts.to_open_options()) {
            Ok(mut engine) => {
                if engine.shutdown(Duration::from_secs(10)).is_err() {
                    failures += 1;
                }
            }
            Err(_) => failures += 1,
        }
    });

    assert_eq!(failures, 0, "measured clean reopen cycles must succeed");
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_lifecycle_flush_cycle_local(ctx: &mut StressContext) {
    run_flush_cycle(ctx, "tier3_lifecycle_flush_cycle_local", "local");
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_lifecycle_flush_cycle_cloud(ctx: &mut StressContext) {
    run_flush_cycle(ctx, "tier3_lifecycle_flush_cycle_cloud", "cloud");
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_lifecycle_clean_reopen_local(ctx: &mut StressContext) {
    run_clean_reopen(ctx, "tier3_lifecycle_clean_reopen_local", "local");
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_lifecycle_clean_reopen_cloud(ctx: &mut StressContext) {
    run_clean_reopen(ctx, "tier3_lifecycle_clean_reopen_cloud", "cloud");
}

stress_main!();
