//! Tier 4 â€” YCSB Workload E (Scan heavy)
//!
//! Workload E: 95% scans, 5% inserts.
//!
//! **Benchmark Methodology:**
//! - Measures full scan throughput (iterates through all results)
//! - Uses deterministic key selection for reproducibility
//! - Scan length: 64 keys per scan operation
//! - Initial dataset: 50K keys by default (overridable for nightly larger-than-RAM runs)
//!
//! **Important:** This benchmark was fixed on 2026-02-14 to actually consume
//! iterator results. Previously it only measured iterator setup overhead by
//! calling `remaining()` without iterating. Expect 10-40x higher throughput
//! in results after this fix.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};

use std::sync::Arc;
use std::time::Duration;

use stress_config::ycsb;
use stress_config::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED_DURATION_DEFAULT: Duration = Duration::from_secs(5);
const MEASURED_DURATION_PLATEAU_BASE: Duration = Duration::from_secs(12);
const MEASURED_DURATION_PLATEAU_LONG: Duration = Duration::from_secs(16);
const MEASURED_DURATION_CLOUD_16: Duration = Duration::from_secs(20);

const SCAN_LEN: u64 = 64;

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xE0E0_EA5E_5678_9ABC;

fn run_workload_e_warmup(
    engine: &Arc<cntryl_midge::MidgeEngine>,
    clients: usize,
    initial_keys: usize,
) {
    let write_opts = cntryl_midge::WriteOptions::best_effort();
    let _warmup_ops =
        ycsb::run_multi_client_for_duration(engine, clients, WARMUP, |client_id, stop| {
            move |e, cf, op_index| {
                let r0 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                let is_insert = (r0 % 100) >= 95;
                let cf_id = cf.id();

                if is_insert {
                    let key_id = initial_keys as u64 + ((client_id as u64) << 32) + op_index;
                    let k = ycsb::make_key(key_id);
                    let v = ycsb::make_value((op_index % 251) as u8);
                    ycsb::retry_write_stall(e, cf_id, stop.as_ref(), || {
                        let mut tx = e
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                            .expect("begin");
                        tx.put(k.to_vec(), v.clone(), None).expect("warmup insert");
                        tx.commit(write_opts)
                    })
                    .expect("commit");
                    return;
                }

                let max_start = (initial_keys as u64).saturating_sub(SCAN_LEN + 1).max(1);
                let start_id =
                    ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0) % max_start;
                let start = ycsb::make_key(start_id);
                let end = ycsb::make_key(start_id.saturating_add(SCAN_LEN));

                let tx = e
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin");
                let query = cntryl_midge::Query::new()
                    .start_key(cntryl_midge::Bytes::copy_from_slice(&start[..]))
                    .end_key(cntryl_midge::Bytes::copy_from_slice(&end[..]));
                let mut iter = tx.scan(&query).expect("warmup range");
                let mut count = 0;
                for row in &mut iter {
                    row.expect("warmup scan row");
                    count += 1;
                }
                std::hint::black_box(count);
            }
        });
}

fn run_workload_e_measured(
    ctx: &mut StressContext,
    engine: &Arc<cntryl_midge::MidgeEngine>,
    clients: usize,
    initial_keys: usize,
    profile: &str,
    duration: Duration,
    write_opts: cntryl_midge::WriteOptions,
) -> ycsb::MultiClientRunStats {
    let client_suffix = if clients == 1 { "client" } else { "clients" };
    let measurement_name = format!("tier4_ycsb_e_{profile}_{clients}_{client_suffix}");
    stress_config::measure_counted(ctx, measurement_name, "ycsb_operation", || {
        let measured = ycsb::run_multi_client_for_duration_with_stats(
            engine,
            clients,
            duration,
            |client_id, stop| {
                move |e, cf, op_index| {
                    let r0 = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let is_insert = (r0 % 100) >= 95;
                    let cf_id = cf.id();

                    if is_insert {
                        let key_id = initial_keys as u64 + ((client_id as u64) << 32) + op_index;
                        let k = ycsb::make_key(key_id);
                        let v = ycsb::make_value((op_index % 251) as u8);
                        ycsb::retry_write_stall(e, cf_id, stop.as_ref(), || {
                            let mut tx = e
                                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                                .expect("measured begin");
                            tx.put(k.to_vec(), v.clone(), None)
                                .expect("measured insert");
                            tx.commit(write_opts)
                        })
                        .expect("measured commit");
                        return;
                    }

                    let max_start = (initial_keys as u64).saturating_sub(SCAN_LEN + 1).max(1);
                    let start_id =
                        ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0) % max_start;
                    let start = ycsb::make_key(start_id);
                    let end = ycsb::make_key(start_id.saturating_add(SCAN_LEN));

                    let tx = e
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .expect("measured begin");
                    let query = cntryl_midge::Query::new()
                        .start_key(cntryl_midge::Bytes::copy_from_slice(&start[..]))
                        .end_key(cntryl_midge::Bytes::copy_from_slice(&end[..]));
                    let mut iter = tx.scan(&query).expect("measured range");
                    let mut count = 0;
                    for row in &mut iter {
                        row.expect("measured scan row");
                        count += 1;
                    }
                    std::hint::black_box(count);
                }
            },
        );
        let operations = measured.operations;
        (measured, operations)
    })
}

fn measured_duration(profile: &str, clients: usize) -> Duration {
    match (profile, clients) {
        ("cloud", CLIENTS_1) => MEASURED_DURATION_PLATEAU_BASE,
        ("cloud", CLIENTS_16) => MEASURED_DURATION_CLOUD_16,
        ("local", CLIENTS_16 | CLIENTS_64) => MEASURED_DURATION_PLATEAU_LONG,
        _ => MEASURED_DURATION_DEFAULT,
    }
}

fn run_workload_e(ctx: &mut StressContext, opts: MidgeOptions, profile: &str, clients: usize) {
    let measured = measured_duration(profile, clients);
    ycsb::configure_workload_parameters(ctx, profile, clients, measured);
    ctx.parameter("scan_length", SCAN_LEN);
    ctx.parameter("key_size_bytes", ycsb::KEY_SIZE);
    ctx.parameter("value_size_bytes", ycsb::DEFAULT_VALUE_SIZE);
    if matches!(
        (profile, clients),
        ("cloud", CLIENTS_1) | ("local", CLIENTS_16 | CLIENTS_64)
    ) {
        stress_config::mark_duration_plateau_probe(
            ctx,
            "deterministic_ycsb_e_duration_window_plateau",
        );
    } else if matches!((profile, clients), ("cloud", CLIENTS_16 | CLIENTS_64)) {
        stress_config::mark_local_rsd_diagnostic(ctx);
    }

    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);
    let measured_write_opts = stress_config::measured_write_options(&opts);

    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, initial_keys);

    // Phase 2: Warm-up (not measured)
    run_workload_e_warmup(&engine, clients, initial_keys);

    // Flush to ensure warmup data is durable before measured phase
    engine.flush_cf(&cf).unwrap();

    let perf_start = ycsb::capture_runtime_perf_snapshot(engine.as_ref());

    // Phase 3: Measured (duration-based; multi-client)
    let measured = run_workload_e_measured(
        ctx,
        &engine,
        clients,
        initial_keys,
        profile,
        measured,
        measured_write_opts,
    );

    measured.record_latencies(ctx);
    let perf = ycsb::runtime_perf_report(engine.as_ref(), perf_start);
    ycsb::record_runtime_correctness(ctx, &perf);
}

#[stress(tier = 4)]
fn tier4_ycsb_e_local_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_e(ctx, opts, "local", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_e_local_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_e(ctx, opts, "local", CLIENTS_16);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_e_local_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_e(ctx, opts, "local", CLIENTS_64);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_e_cloud_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_e(ctx, opts, "cloud", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_e_cloud_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_e(ctx, opts, "cloud", CLIENTS_16);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_e_cloud_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_e(ctx, opts, "cloud", CLIENTS_64);
}

stress_main!();
