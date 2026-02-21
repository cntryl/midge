//! Tier 4 — YCSB Workload C (Read only)
//!
//! Workload C: 100% reads.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

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
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

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
fn tier4_ycsb_c_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_c_local_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_c_local_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_c_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_c_cloud_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_c_cloud_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, CLIENTS_64);
}

stress_main!();
