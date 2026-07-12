//! Tier 4 â€” YCSB Workload B (Read mostly)
//!
//! Workload B: 95% reads, 5% updates.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
use stress_config::MidgeStressContextExt as _;

use std::sync::Arc;
use std::time::Duration;

use stress_config::ycsb;
use stress_config::zipfian::ZipfianGenerator;
use stress_config::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000; // Overridable for larger-than-RAM nightly runs
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);
const LOCAL_16_MEASURED: Duration = Duration::from_secs(12);

const ZIPFIAN_THETA: f64 = 0.99;

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xB0B0_EA5E_5678_9ABC;

fn measured_duration(profile: &str, clients: usize) -> Duration {
    if profile == "local" && clients == CLIENTS_16 {
        LOCAL_16_MEASURED
    } else {
        MEASURED
    }
}

fn run_workload_b(ctx: &mut StressContext, opts: MidgeOptions, profile: &str, clients: usize) {
    let measured_window = measured_duration(profile, clients);
    ctx.tag("storage_profile", profile);
    ctx.parameter("clients", clients);
    ctx.parameter("measured_secs", measured_window.as_secs());
    ctx.parameter("logical_unit", "ycsb_operation");
    if profile == "local" && matches!(clients, CLIENTS_16 | CLIENTS_64) {
        stress_config::mark_local_rsd_diagnostic(ctx);
    }

    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);
    let measured_write_opts = stress_config::measured_write_options(&opts);

    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, initial_keys);

    // Phase 2: Warm-up (not measured)
    {
        let zipf = Arc::new(ZipfianGenerator::new(initial_keys, ZIPFIAN_THETA));
        let _warmup_ops =
            ycsb::run_multi_client_for_duration(&engine, clients, WARMUP, |client_id, stop| {
                let zipf = Arc::clone(&zipf);
                move |e, cf, op_index| {
                    let op_r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);

                    // Reserve draw=0 for op selection; use draw>=1 for key selection.
                    let mut draw: u64 = 1;
                    let key_idx = zipf.next_from_u64(&mut || {
                        let r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, draw);
                        draw = draw.wrapping_add(1);
                        r
                    }) as u64;
                    let k = ycsb::make_key(key_idx);

                    // Deterministic, stochastic 95/5 mix (avoids periodic scheduling).
                    if (op_r % 100) < 95 {
                        let tx = e
                            .begin_tx(cf.id(), cntryl_midge::TransactionMode::ReadOnly)
                            .expect("begin");
                        let _ = tx.get(&k[..]).expect("warmup get");
                    } else {
                        let v = ycsb::make_value((op_index % 251) as u8);
                        let cf_id = cf.id();
                        ycsb::retry_write_stall(e, cf_id, stop.as_ref(), || {
                            let mut tx = e
                                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                                .expect("begin");
                            tx.put(k.to_vec(), v.clone(), None).expect("warmup put");
                            tx.commit(cntryl_midge::WriteOptions::best_effort())
                            // Fast warmup
                        })
                        .expect("warmup commit");
                    }
                }
            });
    }

    // Flush to ensure warmup data is durable before measured phase
    engine.flush_cf(&cf).unwrap();

    // Phase 3: Measured (duration-based; multi-client)
    let client_suffix = if clients == 1 { "client" } else { "clients" };
    let measurement_name = format!("tier4_ycsb_b_{profile}_{clients}_{client_suffix}");
    let measured = stress_config::measure_external_counted(ctx, measurement_name, || {
        let measured = {
            let zipf = Arc::new(ZipfianGenerator::new(initial_keys, ZIPFIAN_THETA));
            let write_opts = measured_write_opts;
            ycsb::run_multi_client_for_duration_with_stats(
                &engine,
                clients,
                measured_window,
                |client_id, stop| {
                    let zipf = Arc::clone(&zipf);
                    move |e, cf, op_index| {
                        let op_r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                        let cf_id = cf.id();

                        // Reserve draw=0 for op selection; use draw>=1 for key selection.
                        let mut draw: u64 = 1;
                        let key_idx = zipf.next_from_u64(&mut || {
                            let r =
                                ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, draw);
                            draw = draw.wrapping_add(1);
                            r
                        }) as u64;
                        let k = ycsb::make_key(key_idx);

                        if (op_r % 100) < 95 {
                            let tx = e
                                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                                .expect("measured begin");
                            let _ = tx.get(&k[..]).expect("measured get");
                        } else {
                            let v = ycsb::make_value((op_index % 251) as u8);
                            ycsb::retry_write_stall(e, cf_id, stop.as_ref(), || {
                                let mut tx = e
                                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                                    .expect("measured begin");
                                tx.put(k.to_vec(), v.clone(), None).expect("measured put");
                                tx.commit(write_opts)
                            })
                            .expect("measured commit");
                        }
                    }
                },
            )
        };
        let operations = measured.operations;
        (measured, operations)
    });

    ctx.set_elements(measured.operations);
    ctx.set_bytes(measured.operations * ycsb::logical_entry_size_bytes() as u64);
    for (name, value) in measured.latency_tags() {
        ctx.tag(name, value.to_string());
    }
}

#[stress(tier = 4)]
fn tier4_ycsb_b_local_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_b(ctx, opts, "local", CLIENTS_1);
}

#[stress(tier = 4)]
fn tier4_ycsb_b_local_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_b(ctx, opts, "local", CLIENTS_16);
}

#[stress(tier = 4)]
fn tier4_ycsb_b_local_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_b(ctx, opts, "local", CLIENTS_64);
}

#[stress(tier = 4)]
fn tier4_ycsb_b_cloud_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_b(ctx, opts, "cloud", CLIENTS_1);
}

#[stress(tier = 4)]
fn tier4_ycsb_b_cloud_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_b(ctx, opts, "cloud", CLIENTS_16);
}

#[stress(tier = 4)]
fn tier4_ycsb_b_cloud_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_b(ctx, opts, "cloud", CLIENTS_64);
}

stress_main!();
