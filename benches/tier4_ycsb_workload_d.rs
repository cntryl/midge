//! Tier 4 — YCSB Workload D (Read latest)
//!
//! Workload D: 95% reads, 5% inserts; reads bias toward the most recent keys.

use cntryl_stress::{stress_main, stress_test, StressContext};

use std::sync::Arc;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::MidgeOptions;

const INITIAL_KEYS: usize = 100_000;
const WARMUP: Duration = Duration::from_secs(10);
const MEASURED: Duration = Duration::from_secs(30);

const CLIENTS_1: usize = 1;
const CLIENTS_4: usize = 4;
const CLIENTS_8: usize = 8;

const WORKLOAD_SEED: u64 = 0xD0D0_EA5E_5678_9ABC;

fn run_workload_d(ctx: &mut StressContext, opts: MidgeOptions, clients: usize) {
    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.default_column_family();
    ycsb::load_initial_dataset(engine.as_ref(), cf, INITIAL_KEYS);

    // Workload D: 95% reads, 5% inserts; read-latest bias.
    // Use a deterministic, stochastic mix (avoid periodic scheduling artifacts).

    // Phase 2: Warm-up (not measured)
    {
        let cf_id = cf.id();
        let _warmup_ops = ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            WARMUP,
            |client_id| {
                let mut inserts_so_far: u64 = 0;
                move |e, _cf, op_index| {
                    let r0 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let is_insert = (r0 % 100) >= 95;

                    if is_insert {
                        inserts_so_far = inserts_so_far.wrapping_add(1);
                        let key_id = (INITIAL_KEYS as u64)
                            .wrapping_add((client_id as u64) << 32)
                            .wrapping_add(inserts_so_far);
                        let k = ycsb::make_key(key_id);
                        let v = ycsb::make_value((op_index % 251) as u8);
                        let mut tx = e.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite).expect("begin");
                        tx.put(k.to_vec(), v.to_vec(), None).expect("warmup insert");
                        e.commit(tx, cntryl_midge::WriteOptions::default()).expect("commit");
                        return;
                    }

                    let latest = (INITIAL_KEYS as u64)
                        .wrapping_add((client_id as u64) << 32)
                        .wrapping_add(inserts_so_far);

                    let recent_window = (latest / 10).max(1);
                    let pick = if (r0 % 100) < 90 {
                        let r1 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 1);
                        latest.saturating_sub(1).saturating_sub(r1 % recent_window)
                    } else {
                        let r2 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 2);
                        r2 % latest.max(1)
                    };

                    let k = ycsb::make_key(pick);
                    let tx = e.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly).expect("begin");
                    let _ = tx.get(&k[..]).expect("warmup get");
                }
            },
        );
    }

    // Phase 3: Measured (duration-based; multi-client)
    let measured_ops = ctx.measure_ref(engine.as_ref(), |_e| {
        ycsb::run_multi_client_for_duration(Arc::clone(&engine), clients, MEASURED, |client_id| {
            let mut inserts_so_far: u64 = 0;
            move |e, cf, op_index| {
                let r0 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                let is_insert = (r0 % 100) >= 95;

                if is_insert {
                    inserts_so_far = inserts_so_far.wrapping_add(1);
                    let key_id = (INITIAL_KEYS as u64)
                        .wrapping_add((client_id as u64) << 32)
                        .wrapping_add(inserts_so_far);
                    let k = ycsb::make_key(key_id);
                    let v = ycsb::make_value((op_index % 251) as u8);
                    e.put(cf, &k[..], &v[..]).expect("measured insert");
                    return;
                }

                let latest = (INITIAL_KEYS as u64)
                    .wrapping_add((client_id as u64) << 32)
                    .wrapping_add(inserts_so_far);

                let recent_window = (latest / 10).max(1);
                let pick = if (r0 % 100) < 90 {
                    let r1 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 1);
                    latest.saturating_sub(1).saturating_sub(r1 % recent_window)
                } else {
                    let r2 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 2);
                    r2 % latest.max(1)
                };

                let k = ycsb::make_key(pick);
                let _ = e.get(cf, &k[..]).expect("measured get");
            }
        })
    });

    ctx.set_elements(measured_ops);
    ctx.set_bytes(measured_ops * (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64);
}

#[stress_test]
fn tier4_ycsb_d_mem_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_d(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_d_mem_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_d(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_d_mem_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_d(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_d_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_d(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_d_local_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_d(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_d_local_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_d(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_d_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_d(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_d_cloud_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_d(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_d_cloud_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_d(ctx, opts, CLIENTS_8);
}

stress_main!();
