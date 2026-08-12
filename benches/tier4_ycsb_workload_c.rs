//! Tier 4 — YCSB Workload C (Read only)
//!
//! Workload C: 100% reads.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};

use cntryl_midge::{ColumnFamilyId, MidgeEngine};
use std::sync::Arc;
use std::time::Duration;

use stress_config::ycsb;
use stress_config::zipfian::ZipfianGenerator;
use stress_config::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000; // Overridable for larger-than-RAM nightly runs
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

const ZIPFIAN_THETA: f64 = 0.99;

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xC0C0_EA5E_5678_9ABC;

#[must_use]
fn workload_c_read_key(
    zipf: &ZipfianGenerator,
    client_id: usize,
    op_index: u64,
) -> [u8; ycsb::KEY_SIZE] {
    let mut draw: u64 = 0;
    let key_idx = zipf.next_from_u64(&mut || {
        let r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, draw);
        draw = draw.wrapping_add(1);
        r
    });
    ycsb::make_key(u64::try_from(key_idx).expect("zipfian key index fits in u64"))
}

fn measure_duration(
    ctx: &mut StressContext,
    engine: &Arc<MidgeEngine>,
    cf_id: ColumnFamilyId,
    initial_keys: usize,
    clients: usize,
    measurement_name: String,
) -> ycsb::MultiClientRunStats {
    stress_config::measure_counted(ctx, measurement_name, "ycsb_operation", || {
        let measured = {
            let zipf = Arc::new(ZipfianGenerator::new(initial_keys, ZIPFIAN_THETA));
            ycsb::run_multi_client_for_duration_with_stats(
                engine,
                clients,
                MEASURED,
                |client_id, _stop| {
                    let zipf = Arc::clone(&zipf);
                    move |e, _cf, op_index| {
                        let k = workload_c_read_key(&zipf, client_id, op_index);
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
    })
}

fn run_workload_c(ctx: &mut StressContext, opts: MidgeOptions, profile: &str, clients: usize) {
    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);
    ycsb::configure_workload_parameters(ctx, profile, clients, MEASURED);
    ctx.parameter(
        "logical_bytes_per_operation",
        ycsb::logical_entry_size_bytes(),
    );
    if matches!(
        (profile, clients),
        ("local", CLIENTS_1) | ("memory", CLIENTS_16)
    ) {
        stress_config::mark_duration_plateau_probe(
            ctx,
            "deterministic_ycsb_c_duration_window_plateau",
        );
    } else if matches!(
        (profile, clients),
        ("hybrid" | "cloud", CLIENTS_1 | CLIENTS_16 | CLIENTS_64)
            | ("memory", CLIENTS_1 | CLIENTS_64)
            | ("local", CLIENTS_16)
    ) {
        stress_config::mark_local_rsd_diagnostic(ctx);
    }

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
                    let k = workload_c_read_key(&zipf, client_id, op_index);
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
    let client_suffix = if clients == 1 { "client" } else { "clients" };
    let measurement_name = format!("tier4_ycsb_c_{profile}_{clients}_{client_suffix}");
    let measured = measure_duration(ctx, &engine, cf_id, initial_keys, clients, measurement_name);

    measured.record_latencies(ctx);
    let perf = ycsb::runtime_perf_report(engine.as_ref(), perf_start);
    ycsb::record_runtime_correctness(ctx, &perf);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_memory_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_memory_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_16);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_memory_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_64);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_local_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_local_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_16);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_local_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_64);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_cloud_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_cloud_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_16);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_cloud_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_64);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_hybrid_1_client(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_1);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_hybrid_16_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_16);
}

#[stress(tier = 4, role = "diagnostic")]
fn tier4_ycsb_c_hybrid_64_clients(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_64);
}

stress_main!();
