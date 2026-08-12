//! Tier 4 â€” YCSB Workload A (Update heavy)
//!
//! Workload A: 50% reads, 50% updates on an existing keyspace.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};

use std::sync::Arc;
use std::time::Duration;

use stress_config::ycsb;
use stress_config::zipfian::ZipfianGenerator;
use stress_config::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

const ZIPFIAN_THETA: f64 = 0.99;

#[derive(Clone, Copy)]
enum KeyDistribution {
    Zipf { theta: f64 },
}

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xA0A0_EA5E_5678_9ABC;

fn run_workload_a_warmup(
    engine: &Arc<cntryl_midge::MidgeEngine>,
    clients: usize,
    initial_keys: usize,
    distribution: KeyDistribution,
) {
    let zipf = match distribution {
        KeyDistribution::Zipf { theta } => {
            Some(Arc::new(ZipfianGenerator::new(initial_keys, theta)))
        }
    };
    let write_opts = cntryl_midge::WriteOptions::best_effort();
    let _warmup_ops =
        ycsb::run_multi_client_for_duration(engine, clients, WARMUP, |client_id, stop| {
            let zipf = zipf.clone();
            move |e, cf, op_index| {
                let op_r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                let cf_id = cf.id();
                let key_idx = match &zipf {
                    Some(zipf) => {
                        let mut draw = 1_u64;
                        zipf.next_from_u64(&mut || {
                            let r =
                                ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, draw);
                            draw = draw.wrapping_add(1);
                            r
                        }) as u64
                    }
                    None => 0,
                };
                let k = ycsb::make_key(key_idx);

                if (op_r & 1) == 0 {
                    let tx = e
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                        .expect("begin");
                    let _ = tx.get(&k[..]).expect("warmup get");
                } else {
                    let v = ycsb::make_value((op_index % 251) as u8);
                    ycsb::retry_write_stall(e, cf_id, stop.as_ref(), || {
                        let mut tx = e
                            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                            .expect("begin");
                        tx.put(k.to_vec(), v.clone(), None).expect("warmup put");
                        tx.commit(write_opts)
                    })
                    .expect("warmup commit");
                }
            }
        });
}

fn run_workload_a_measured(
    ctx: &mut StressContext,
    engine: &Arc<cntryl_midge::MidgeEngine>,
    clients: usize,
    initial_keys: usize,
    profile: &str,
    distribution: KeyDistribution,
    write_opts: cntryl_midge::WriteOptions,
) -> ycsb::MultiClientRunStats {
    let client_suffix = if clients == 1 { "client" } else { "clients" };
    let measurement_name = format!("tier4_ycsb_a_{profile}_{clients}_{client_suffix}");
    stress_config::measure_counted(ctx, measurement_name, "ycsb_operation", || {
        let zipf = match distribution {
            KeyDistribution::Zipf { theta } => {
                Some(Arc::new(ZipfianGenerator::new(initial_keys, theta)))
            }
        };
        let measured = ycsb::run_multi_client_for_duration_with_stats(
            engine,
            clients,
            MEASURED,
            |client_id, stop| {
                let zipf = zipf.clone();
                move |e, cf, op_index| {
                    let op_r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let cf_id = cf.id();
                    let key_idx = match &zipf {
                        Some(zipf) => {
                            let mut draw = 1_u64;
                            zipf.next_from_u64(&mut || {
                                let r = ycsb::deterministic_u64(
                                    WORKLOAD_SEED,
                                    client_id,
                                    op_index,
                                    draw,
                                );
                                draw = draw.wrapping_add(1);
                                r
                            }) as u64
                        }
                        None => 0,
                    };
                    let k = ycsb::make_key(key_idx);

                    if (op_r & 1) == 0 {
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
        );
        let operations = measured.operations;
        (measured, operations)
    })
}

fn run_workload_a_with_distribution(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    profile: &str,
    clients: usize,
    distribution: KeyDistribution,
) {
    ycsb::configure_workload_parameters(ctx, profile, clients, MEASURED);
    ctx.parameter(
        "logical_bytes_per_operation",
        ycsb::logical_entry_size_bytes(),
    );
    if matches!(
        (profile, clients),
        ("memory", CLIENTS_1 | CLIENTS_16 | CLIENTS_64)
            | ("cloud" | "local", CLIENTS_64)
            | ("hybrid", CLIENTS_16)
    ) {
        stress_config::mark_duration_plateau_probe(
            ctx,
            "deterministic_ycsb_a_duration_window_plateau",
        );
    }
    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);
    let measured_write_opts = stress_config::measured_write_options(&opts);

    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, initial_keys);

    // Phase 2: Warm-up (not measured)
    run_workload_a_warmup(&engine, clients, initial_keys, distribution);

    // Flush to ensure warmup data is durable before measured phase
    engine.flush_cf(&cf).unwrap();
    let perf_start = ycsb::capture_runtime_perf_snapshot(engine.as_ref());

    // Phase 3: Measured (duration-based; multi-client)
    let measured = run_workload_a_measured(
        ctx,
        &engine,
        clients,
        initial_keys,
        profile,
        distribution,
        measured_write_opts,
    );

    measured.record_latencies(ctx);
    let perf = ycsb::runtime_perf_report(engine.as_ref(), perf_start);
    ycsb::record_runtime_correctness(ctx, &perf);
}

fn run_workload_a(ctx: &mut StressContext, opts: MidgeOptions, profile: &str, clients: usize) {
    run_workload_a_with_distribution(
        ctx,
        opts,
        profile,
        clients,
        KeyDistribution::Zipf {
            theta: ZIPFIAN_THETA,
        },
    );
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_a_memory_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("memory");
    run_workload_a(ctx, opts, "memory", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_a_memory_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("memory");
    run_workload_a(ctx, opts, "memory", CLIENTS_16);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_a_memory_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("memory");
    run_workload_a(ctx, opts, "memory", CLIENTS_64);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_a_local_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_a(ctx, opts, "local", CLIENTS_64);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_a_cloud_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_a(ctx, opts, "cloud", CLIENTS_64);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_a_hybrid_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("hybrid");
    run_workload_a(ctx, opts, "hybrid", CLIENTS_16);
}
stress_main!();
