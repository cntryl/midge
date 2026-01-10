//! Tier 4 — YCSB Workload E (Scan heavy)
//!
//! Workload E: 95% scans, 5% inserts.

use cntryl_stress::{stress_main, stress_test, StressContext};

use std::sync::Arc;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::MidgeOptions;

const INITIAL_KEYS: usize = 100_000;
const WARMUP: Duration = Duration::from_secs(10);
const MEASURED: Duration = Duration::from_secs(30);

const SCAN_LEN: u64 = 64;

const CLIENTS_1: usize = 1;
const CLIENTS_4: usize = 4;
const CLIENTS_8: usize = 8;

const WORKLOAD_SEED: u64 = 0xE0E0_EA5E_5678_9ABC;

fn run_workload_e(ctx: &mut StressContext, opts: MidgeOptions, clients: usize) {
    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.default_column_family();
    ycsb::load_initial_dataset(engine.as_ref(), cf, INITIAL_KEYS);

    // Workload E: 95% scans, 5% inserts. Use a deterministic, stochastic mix.
    // Scans target the dense initial keyspace for stable scan lengths.

    // Phase 2: Warm-up (not measured)
    {
        let write_opts = cntryl_midge::WriteOptions::buffered();
        let _warmup_ops = ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            WARMUP,
            |client_id| {
                move |e, cf, op_index| {
                    let r0 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let is_insert = (r0 % 100) >= 95;
                    let cf_id = cf.id();

                    if is_insert {
                        let key_id = INITIAL_KEYS as u64 + ((client_id as u64) << 32) + op_index;
                        let k = ycsb::make_key(key_id);
                        let v = ycsb::make_value((op_index % 251) as u8);
                        let mut tx = e
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                            .expect("begin");
                        tx.put(k.to_vec(), v.to_vec(), None).expect("warmup insert");
                        e.commit(tx, write_opts).expect("commit");
                        return;
                    }

                    let max_start = (INITIAL_KEYS as u64).saturating_sub(SCAN_LEN + 1).max(1);
                    let start_id =
                        ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0) % max_start;
                    let start = ycsb::make_key(start_id);
                    let end = ycsb::make_key(start_id.saturating_add(SCAN_LEN));

                    let tx = e
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .expect("begin");
                    let _scanned = tx.scan(&start[..], &end[..]).expect("warmup range").len();
                }
            },
        );
    }

    // Phase 3: Measured (duration-based; multi-client)
    let measured_ops = ctx.measure_ref(engine.as_ref(), |_e| {
        let write_opts = cntryl_midge::WriteOptions::buffered();
        ycsb::run_multi_client_for_duration(Arc::clone(&engine), clients, MEASURED, |client_id| {
            move |e, cf, op_index| {
                let r0 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                let is_insert = (r0 % 100) >= 95;
                let cf_id = cf.id();

                if is_insert {
                    let key_id = INITIAL_KEYS as u64 + ((client_id as u64) << 32) + op_index;
                    let k = ycsb::make_key(key_id);
                    let v = ycsb::make_value((op_index % 251) as u8);
                    let mut tx = e
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("measured begin");
                    tx.put(k.to_vec(), v.to_vec(), None)
                        .expect("measured insert");
                    e.commit(tx, write_opts).expect("measured commit");
                    return;
                }

                let max_start = (INITIAL_KEYS as u64).saturating_sub(SCAN_LEN + 1).max(1);
                let start_id =
                    ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0) % max_start;
                let start = ycsb::make_key(start_id);
                let end = ycsb::make_key(start_id.saturating_add(SCAN_LEN));

                let tx = e
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .expect("measured begin");
                let _scanned = tx.scan(&start[..], &end[..]).expect("measured range").len();
            }
        })
    });

    // Approximate bytes touched: 95% scans of length SCAN_LEN, 5% inserts.
    let bytes_per_kv = (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64;
    let est_inserts = measured_ops / 20;
    let est_scans = measured_ops.saturating_sub(est_inserts);
    let est_bytes = est_inserts * bytes_per_kv + est_scans * (SCAN_LEN * bytes_per_kv);

    ctx.set_elements(measured_ops);
    ctx.set_bytes(est_bytes);
}

#[stress_test]
fn tier4_ycsb_e_mem_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_e(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_e_mem_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_e(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_e_mem_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_e(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_e_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_e(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_e_local_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_e(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_e_local_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_e(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_e_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_e(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_e_cloud_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_e(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_e_cloud_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_e(ctx, opts, CLIENTS_8);
}

stress_main!();
