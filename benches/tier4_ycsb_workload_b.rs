//! Tier 4 — YCSB Workload B (Read mostly)
//!
//! Workload B: 95% reads, 5% updates.

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

const DEFAULT_INITIAL_KEYS: usize = 50_000; // Overridable for larger-than-RAM nightly runs
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

const ZIPFIAN_THETA: f64 = 0.99;

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xB0B0_EA5E_5678_9ABC;

fn run_workload_b(ctx: &mut StressContext, opts: MidgeOptions, clients: usize) {
    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);

    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, initial_keys);

    // Phase 2: Warm-up (not measured)
    {
        let zipf = Arc::new(ZipfianGenerator::new(initial_keys, ZIPFIAN_THETA));
        let _warmup_ops = ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            WARMUP,
            |client_id, stop| {
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
                            e.commit(tx, cntryl_midge::WriteOptions::best_effort())
                            // Fast warmup
                        })
                        .expect("warmup commit");
                    }
                }
            },
        );
    }

    // Flush to ensure warmup data is durable before measured phase
    engine.flush_cf(&cf).unwrap();

    // Phase 3: Measured (duration-based; multi-client)
    let measured_ops = ctx.measure_ref(engine.as_ref(), |_e| {
        let zipf = Arc::new(ZipfianGenerator::new(initial_keys, ZIPFIAN_THETA));
        let write_opts = cntryl_midge::WriteOptions::buffered(); // Back to buffered for measured phase
        ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            MEASURED,
            |client_id, stop| {
                let zipf = Arc::clone(&zipf);
                move |e, cf, op_index| {
                    let op_r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let cf_id = cf.id();

                    // Reserve draw=0 for op selection; use draw>=1 for key selection.
                    let mut draw: u64 = 1;
                    let key_idx = zipf.next_from_u64(&mut || {
                        let r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, draw);
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
                            e.commit(tx, write_opts)
                        })
                        .expect("measured commit");
                    }
                }
            },
        )
    });

    ctx.set_elements(measured_ops);
    ctx.set_bytes(measured_ops * ycsb::logical_entry_size_bytes() as u64);
}

#[stress_test]
fn tier4_ycsb_b_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_b(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_b_local_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_b(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_b_local_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_b(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_b_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_b(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_b_cloud_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_b(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_b_cloud_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_b(ctx, opts, CLIENTS_64);
}

stress_main!();
