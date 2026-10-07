//! Tier 4 â€” YCSB Workload D (Read latest)
//!
//! Workload D: 95% reads, 5% inserts; reads bias toward the most recent keys.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};

use std::sync::Arc;
use std::time::Duration;

use stress_config::ycsb;
use stress_config::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000; // Overridable for larger-than-RAM nightly runs
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED_DEFAULT: Duration = Duration::from_secs(5);
const MEASURED_CLOUD_16: Duration = Duration::from_secs(15);

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xD0D0_EA5E_5678_9ABC;

fn run_workload_d_warmup(
    engine: &Arc<cntryl_midge::Engine>,
    inventories: &[ycsb::inventory::InsertInventory],
) {
    ycsb::run_multi_client_for_duration_observed_with_stats(
        engine,
        inventories.len(),
        WARMUP,
        |client_id, stop| {
            let inventory = inventories[client_id].clone();
            move |e, cf, op_index| {
                inventory
                    .read_latest_step(
                        e,
                        cf.id(),
                        stop.as_ref(),
                        WORKLOAD_SEED,
                        op_index,
                        cntryl_midge::WriteOptions::best_effort(),
                    )
                    .expect("warmup D operation")
            }
        },
    );
}

fn run_workload_d(ctx: &mut StressContext, opts: MidgeOptions, profile: &str, clients: usize) {
    let measured_duration = if profile == "cloud" && clients == CLIENTS_16 {
        MEASURED_CLOUD_16
    } else {
        MEASURED_DEFAULT
    };
    let measured_duration = stress_config::tier4_measured_duration(measured_duration);
    ycsb::configure_workload_parameters(ctx, profile, clients, measured_duration);
    ctx.parameter("measurement_window_shape", "continuous_same_owner");
    ctx.parameter(
        "logical_bytes_per_operation",
        ycsb::logical_entry_size_bytes(),
    );
    if matches!((profile, clients), ("cloud", CLIENTS_1)) {
        stress_config::mark_duration_plateau_probe(
            ctx,
            "deterministic_ycsb_d_duration_window_plateau",
        );
    } else if matches!(
        (profile, clients),
        ("local", CLIENTS_1 | CLIENTS_16 | CLIENTS_64) | ("cloud", CLIENTS_16 | CLIENTS_64)
    ) {
        stress_config::mark_local_rsd_diagnostic(ctx);
    }

    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);
    let measured_write_opts = stress_config::measured_write_options(&opts);

    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, initial_keys);

    // Workload D: 95% reads, 5% inserts; read-latest bias.
    // Use a deterministic, stochastic mix (avoid periodic scheduling artifacts).

    // Phase 2: Warm-up (not measured)
    let inventories: Vec<_> = (0..clients)
        .map(|client| ycsb::inventory::InsertInventory::new(initial_keys, client))
        .collect();
    run_workload_d_warmup(&engine, &inventories);
    let warmup_inserts: u64 = inventories
        .iter()
        .map(ycsb::inventory::InsertInventory::committed)
        .sum();
    let warmup_reads: u64 = inventories
        .iter()
        .map(ycsb::inventory::InsertInventory::read_hits)
        .sum();

    // Flush to ensure warmup data is durable before measured phase
    ycsb::flush_after_phase(engine.as_ref(), &cf).expect("flush warmup phase");

    let perf_start = ycsb::capture_runtime_perf_snapshot(engine.as_ref());

    // Phase 3: Measured (duration-based; multi-client)
    let client_suffix = if clients == 1 { "client" } else { "clients" };
    let measurement_name = format!("tier4_ycsb_d_{profile}_{clients}_{client_suffix}");
    let measured = stress_config::measure_counted(ctx, measurement_name, "ycsb_operation", || {
        let measured = {
            let write_opts = measured_write_opts;
            ycsb::run_multi_client_for_duration_observed_with_stats(
                &engine,
                clients,
                measured_duration,
                |client_id, stop| {
                    let inventory = inventories[client_id].clone();
                    move |e, cf, op_index| {
                        inventory
                            .read_latest_step(
                                e,
                                cf.id(),
                                stop.as_ref(),
                                WORKLOAD_SEED,
                                op_index,
                                write_opts,
                            )
                            .expect("measured D operation")
                    }
                },
            )
        };
        let operations = measured.operations;
        (measured, operations)
    });

    measured.record_latencies(ctx);
    let perf = ycsb::runtime_perf_report(engine.as_ref(), perf_start);
    ycsb::record_runtime_report(ctx, &perf);
    let rows =
        ycsb::inventory::verify_inventory(engine.as_ref(), cf.id(), initial_keys, &inventories)
            .expect("D fresh insert cardinality");
    let read_hits: u64 = inventories
        .iter()
        .map(ycsb::inventory::InsertInventory::read_hits)
        .sum();
    ctx.parameter("verified_read_hits", read_hits - warmup_reads);
    ctx.parameter(
        "verified_fresh_inserts",
        rows - initial_keys as u64 - warmup_inserts,
    );
    ctx.parameter("verified_rows", rows);
    let mut engine = Arc::try_unwrap(engine).unwrap_or_else(|_| panic!("D workers must be joined"));
    engine
        .shutdown(Duration::from_secs(30))
        .expect("D shutdown");
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_d_local_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_d(ctx, opts, "local", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_d_local_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_d(ctx, opts, "local", CLIENTS_16);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_d_local_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_d(ctx, opts, "local", CLIENTS_64);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_d_cloud_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_d(ctx, opts, "cloud", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_d_cloud_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_d(ctx, opts, "cloud", CLIENTS_16);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_d_cloud_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_d(ctx, opts, "cloud", CLIENTS_64);
}

stress_main!();
