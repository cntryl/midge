//! Tier 4 — YCSB Workload C (Read only)
//!
//! Workload C: 100% reads.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use std::sync::Arc;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::testkit::zipfian::ZipfianGenerator;
use cntryl_midge::testkit::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000; // Overridable for larger-than-RAM nightly runs
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

const ZIPFIAN_THETA: f64 = 0.99;

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xC0C0_EA5E_5678_9ABC;

fn run_workload_c(ctx: &mut StressContext, opts: MidgeOptions, profile: &str, clients: usize) {
    ctx.tag("storage_profile", profile);
    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);

    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, initial_keys);

    // Phase 2: Warm-up (not measured)
    {
        let zipf = Arc::new(ZipfianGenerator::new(initial_keys, ZIPFIAN_THETA));
        let cf_id = cf.id();
        let _warmup_ops =
            ycsb::run_multi_client_for_duration(&engine, clients, WARMUP, |client_id, _stop| {
                let zipf = Arc::clone(&zipf);
                move |e, _cf, op_index| {
                    let mut draw: u64 = 0;
                    let key_idx = zipf.next_from_u64(&mut || {
                        let r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, draw);
                        draw = draw.wrapping_add(1);
                        r
                    }) as u64;
                    let k = ycsb::make_key(key_idx);
                    let tx = e
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .expect("begin");
                    let _ = tx.get(&k[..]).expect("warmup get");
                }
            });
    }

    let perf_start = ycsb::capture_runtime_perf_snapshot(engine.as_ref());

    // Phase 3: Measured (duration-based; multi-client)
    let cf_id = cf.id();
    let measured = stress_config::measure_external_counted(ctx, || {
        let measured = {
            let zipf = Arc::new(ZipfianGenerator::new(initial_keys, ZIPFIAN_THETA));
            ycsb::run_multi_client_for_duration_with_stats(
                &engine,
                clients,
                MEASURED,
                |client_id, _stop| {
                    let zipf = Arc::clone(&zipf);
                    move |e, _cf, op_index| {
                        let mut draw: u64 = 0;
                        let key_idx = zipf.next_from_u64(&mut || {
                            let r =
                                ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, draw);
                            draw = draw.wrapping_add(1);
                            r
                        }) as u64;
                        let k = ycsb::make_key(key_idx);
                        let tx = e
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                            .expect("begin");
                        let _ = tx.get(&k[..]).expect("measured get");
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
    for (name, value) in ycsb::runtime_perf_report(engine.as_ref(), perf_start).tags() {
        ctx.tag(name, value.to_string());
    }
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_memory_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_1);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_memory_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_16);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_memory_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_64);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_1);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_local_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_16);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_local_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_64);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_1);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_cloud_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_16);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_cloud_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_64);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_hybrid_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_1);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_hybrid_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_16);
}

#[stress_test(tier = 4)]
fn tier4_ycsb_c_hybrid_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_64);
}

stress_main!();
