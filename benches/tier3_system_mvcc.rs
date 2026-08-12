//! Tier 3 — MVCC snapshot reads
//!
//! Measures: old-version visibility reads through snapshot transactions.
//! NOT: single commit call cost or sustained overwrite throughput.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};

use cntryl_midge::MidgeEngine;
use stress_config::MidgeOptions;

const VALUE_SIZE: usize = 64;
const TARGET_BATCH: usize = 1_000;
const OLD_VERSION_READ_BATCH_SIZE: usize = 64;

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    stress_config::bench_stress::open_engine_no_compaction(opts)
}

fn run_read_old_version_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: MidgeOptions,
    num_keys: usize,
) {
    ctx.parameter("logical_batch_size", OLD_VERSION_READ_BATCH_SIZE);
    ctx.parameter("logical_unit", "snapshot_read");
    ctx.parameter("operation_surface", "mvcc_old_version_read");
    ctx.parameter("begin_tx_included", "false");
    ctx.parameter("rotating_key_count", num_keys);
    ctx.metadata("diagnostic_reason", "pending_three_clean_baselines");
    ctx.parameter("local_gate_rsd_limit_pct", 5);

    let write_opts = stress_config::measured_write_options(&opts);
    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // Setup (not measured): write in TARGET_BATCH-sized transactions
    let cf_id = cf.id();
    let total = num_keys;

    let mut keys = Vec::with_capacity(num_keys);
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for i in start..end {
            let k = stress_config::bench_stress::key16_u64_be(i as u64);
            keys.push(k);
            tx.put(k.to_vec(), vec![1u8; VALUE_SIZE], None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();

    // Create snapshot via transaction
    let snap_tx = engine
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin");

    // Write newer versions to demonstrate old-version visibility for all rotating keys.
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for key in &keys[start..end] {
            tx.put(key.to_vec(), vec![2u8; VALUE_SIZE], None).unwrap();
        }
        tx.commit(write_opts).unwrap();
    }
    engine.flush_cf(&cf).unwrap();
    engine.compact_all().unwrap();

    let expected = vec![1u8; VALUE_SIZE];
    let read_path_before = engine.read_path_diagnostics_snapshot_for_benchmarks();
    let mut key_index = 0usize;
    let mut validation_failures = 0_u64;

    let _ = ctx.measure_batch(scenario, OLD_VERSION_READ_BATCH_SIZE as u64, || {
        for _ in 0..OLD_VERSION_READ_BATCH_SIZE {
            let key = keys[key_index % keys.len()];
            key_index = key_index.wrapping_add(1);
            let v = snap_tx.get(&key[..]).unwrap();
            let visible = if let Some(bytes) = v {
                bytes.as_ref() == expected.as_slice()
            } else {
                false
            };
            if !visible {
                validation_failures += 1;
            }
        }
    });

    let read_path_after = engine.read_path_diagnostics_snapshot_for_benchmarks();
    assert_eq!(validation_failures, 0, "measured MVCC reads must validate");
    assert!(
        read_path_after.candidate_sst_files_checked > read_path_before.candidate_sst_files_checked
            && read_path_after.candidate_blocks_checked > read_path_before.candidate_blocks_checked,
        "MVCC old-version row must exercise candidate SST and block work"
    );

    // Engine shutdown waits for active transaction guards so it cannot release
    // its lease while a snapshot still references the runtime. End the
    // snapshot before dropping the engine to avoid waiting on our own guard.
    drop(snap_tx);
    drop(engine);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_mvcc_read_old_version_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_read_old_version_case(ctx, "tier3_mvcc_read_old_version_local", opts, 1_000);
}

#[stress(tier = 3, role = "diagnostic")]
fn tier3_mvcc_read_old_version_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_read_old_version_case(ctx, "tier3_mvcc_read_old_version_cloud", opts, 1_000);
}

stress_main!();
