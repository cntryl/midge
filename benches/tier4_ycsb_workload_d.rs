//! Tier 4 — YCSB Workload D (Read latest)
//!
//! Workload D: 95% reads, 5% inserts; reads bias toward the most recent keys.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use std::sync::Arc;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::testkit::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000; // Overridable for larger-than-RAM nightly runs
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xD0D0_EA5E_5678_9ABC;

fn run_workload_d(ctx: &mut StressContext, opts: MidgeOptions, clients: usize) {
    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);

    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, initial_keys);

    // Workload D: 95% reads, 5% inserts; read-latest bias.
    // Use a deterministic, stochastic mix (avoid periodic scheduling artifacts).

    // Phase 2: Warm-up (not measured)
    {
        let cf_id = cf.id();
        let _warmup_ops = ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            WARMUP,
            |client_id, stop| {
                let mut inserts_so_far: u64 = 0;
                move |e, _cf, op_index| {
                    let r0 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let is_insert = (r0 % 100) >= 95;

                    if is_insert {
                        inserts_so_far = inserts_so_far.wrapping_add(1);
                        let key_id = (initial_keys as u64)
                            .wrapping_add((client_id as u64) << 32)
                            .wrapping_add(inserts_so_far);
                        let k = ycsb::make_key(key_id);
                        let v = ycsb::make_value((op_index % 251) as u8);
                        ycsb::retry_write_stall(e, cf_id, stop.as_ref(), || {
                            let mut tx = e
                                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                                .expect("begin");
                            tx.put(k.to_vec(), v.clone(), None).expect("warmup insert");
                            e.commit(tx, cntryl_midge::WriteOptions::best_effort())
                            // Fast warmup
                        })
                        .expect("commit");
                        return;
                    }

                    let latest = (initial_keys as u64)
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
                    let tx = e
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .expect("begin");
                    let _ = tx.get(&k[..]).expect("warmup get");
                }
            },
        );
    }

    // Flush to ensure warmup data is durable before measured phase
    engine.flush_cf(&cf).unwrap();

    // Phase 3: Measured (duration-based; multi-client)
    let measured_ops = ctx.measure_ref(engine.as_ref(), |_e| {
        let write_opts = cntryl_midge::WriteOptions::buffered(); // Back to buffered for measured phase
        ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            MEASURED,
            |client_id, stop| {
                let mut inserts_so_far: u64 = 0;
                move |e, cf, op_index| {
                    let r0 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let is_insert = (r0 % 100) >= 95;
                    let cf_id = cf.id();

                    if is_insert {
                        inserts_so_far = inserts_so_far.wrapping_add(1);
                        let key_id = (initial_keys as u64)
                            .wrapping_add((client_id as u64) << 32)
                            .wrapping_add(inserts_so_far);
                        let k = ycsb::make_key(key_id);
                        let v = ycsb::make_value((op_index % 251) as u8);
                        ycsb::retry_write_stall(e, cf_id, stop.as_ref(), || {
                            let mut tx = e
                                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                                .expect("measured begin");
                            tx.put(k.to_vec(), v.clone(), None)
                                .expect("measured insert");
                            e.commit(tx, write_opts)
                        })
                        .expect("measured commit");
                        return;
                    }

                    let latest = (initial_keys as u64)
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
                    let tx = e
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .expect("measured begin");
                    let _ = tx.get(&k[..]).expect("measured get");
                }
            },
        )
    });

    ctx.set_elements(measured_ops);
    ctx.set_bytes(measured_ops * ycsb::logical_entry_size_bytes() as u64);
}

#[stress_test]
fn tier4_ycsb_d_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_d(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_d_local_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_d(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_d_local_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_d(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_d_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_d(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_d_cloud_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_d(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_d_cloud_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_d(ctx, opts, CLIENTS_64);
}

stress_main!();
