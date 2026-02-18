//! Tier 4 — YCSB Workload C (Read only)
//!
//! Workload C: 100% reads.

use cntryl_stress::{stress_main, stress_test, StressContext};

use std::sync::Arc;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::testkit::zipfian::ZipfianGenerator;
use cntryl_midge::testkit::MidgeOptions;

const INITIAL_KEYS: usize = 50_000; // Reduced from 100k: still exercises LSM multi-level reads
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

const ZIPFIAN_THETA: f64 = 0.99;

const CLIENTS_1: usize = 1;
const CLIENTS_2: usize = 2;
const CLIENTS_4: usize = 4;
const CLIENTS_8: usize = 8;
const CLIENTS_16: usize = 16;
const CLIENTS_32: usize = 32;
const CLIENTS_64: usize = 64;

// C = number of logical CPUs (realistic scaling)
fn clients_c() -> usize {
    num_cpus::get()
}

// 2C = 2× CPUs (oversubscription stress)
fn clients_2c() -> usize {
    num_cpus::get() * 2
}

// 4C = 4× CPUs (extreme oversubscription to find saturation point)
fn clients_4c() -> usize {
    num_cpus::get() * 4
}

const WORKLOAD_SEED: u64 = 0xC0C0_EA5E_5678_9ABC;

fn run_workload_c(ctx: &mut StressContext, opts: MidgeOptions, clients: usize) {
    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, INITIAL_KEYS);

    // Phase 2: Warm-up (not measured)
    {
        let zipf = Arc::new(ZipfianGenerator::new(INITIAL_KEYS, ZIPFIAN_THETA));
        let cf_id = cf.id();
        let _warmup_ops = ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            WARMUP,
            |client_id, _stop| {
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
            },
        );
    }

    // Phase 3: Measured (duration-based; multi-client)
    let cf_id = cf.id();
    let measured_ops = ctx.measure_ref(engine.as_ref(), |_e| {
        let zipf = Arc::new(ZipfianGenerator::new(INITIAL_KEYS, ZIPFIAN_THETA));
        ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            MEASURED,
            |client_id, _stop| {
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
                    let _ = tx.get(&k[..]).expect("measured get");
                }
            },
        )
    });

    ctx.set_elements(measured_ops);
    ctx.set_bytes(measured_ops * (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64);
}

#[stress_test]
fn tier4_ycsb_c_mem_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_c_mem_2_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, CLIENTS_2);
}

#[stress_test]
fn tier4_ycsb_c_mem_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_c_mem_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_c_mem_c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, clients_c());
}

#[stress_test]
fn tier4_ycsb_c_mem_2c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, clients_2c());
}

#[stress_test]
fn tier4_ycsb_c_mem_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_c_mem_32_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, CLIENTS_32);
}

#[stress_test]
fn tier4_ycsb_c_mem_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_c_mem_4c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, clients_4c());
}

#[stress_test]
fn tier4_ycsb_c_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_c_local_2_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_2);
}

#[stress_test]
fn tier4_ycsb_c_local_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_c_local_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_c_local_c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, clients_c());
}

#[stress_test]
fn tier4_ycsb_c_local_2c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, clients_2c());
}

#[stress_test]
fn tier4_ycsb_c_local_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_c_local_32_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_32);
}

#[stress_test]
fn tier4_ycsb_c_local_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_c_local_4c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, clients_4c());
}

#[stress_test]
fn tier4_ycsb_c_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_c_cloud_2_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_2);
}

#[stress_test]
fn tier4_ycsb_c_cloud_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_c_cloud_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_c_cloud_c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, clients_c());
}

#[stress_test]
fn tier4_ycsb_c_cloud_2c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, clients_2c());
}

#[stress_test]
fn tier4_ycsb_c_cloud_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_c_cloud_32_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_32);
}

#[stress_test]
fn tier4_ycsb_c_cloud_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_c_cloud_4c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, clients_4c());
}

stress_main!();
