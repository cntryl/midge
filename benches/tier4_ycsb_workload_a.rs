//! Tier 4 — YCSB Workload A (Update heavy)
//!
//! Workload A: 50% reads, 50% updates on an existing keyspace.

use cntryl_stress::{stress_main, stress_test, StressContext};

use std::sync::Arc;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::testkit::zipfian::ZipfianGenerator;
use cntryl_midge::testkit::MidgeOptions;

const INITIAL_KEYS: usize = 50_000;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

const ZIPFIAN_THETA: f64 = 0.99;

enum KeyDistribution {
    Zipf { theta: f64 },
    HotKey,
}

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

const WORKLOAD_SEED: u64 = 0xA0A0_EA5E_5678_9ABC;

fn run_workload_a_with_distribution(
    ctx: &mut StressContext,
    opts: MidgeOptions,
    clients: usize,
    distribution: KeyDistribution,
) {
    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, INITIAL_KEYS);

    // Phase 2: Warm-up (not measured)
    {
        let zipf = match distribution {
            KeyDistribution::Zipf { theta } => {
                Some(Arc::new(ZipfianGenerator::new(INITIAL_KEYS, theta)))
            }
            KeyDistribution::HotKey => None,
        };
        let write_opts = cntryl_midge::WriteOptions::best_effort(); // Fast warmup: skip WAL I/O
        let _warmup_ops = ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            WARMUP,
            |client_id, stop| {
                let zipf = zipf.clone();
                move |e, cf, op_index| {
                    let op_r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let cf_id = cf.id();

                    let key_idx = match &zipf {
                        Some(zipf) => {
                            // Reserve draw=0 for op selection; use draw>=1 for key selection.
                            let mut draw: u64 = 1;
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

                    // Deterministic, stochastic 50/50 mix (avoids perfect alternation).
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
                            tx.put(k.to_vec(), v.to_vec(), None).expect("warmup put");
                            e.commit(tx, write_opts)
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
        let zipf = match distribution {
            KeyDistribution::Zipf { theta } => {
                Some(Arc::new(ZipfianGenerator::new(INITIAL_KEYS, theta)))
            }
            KeyDistribution::HotKey => None,
        };
        let write_opts = cntryl_midge::WriteOptions::buffered(); // Back to buffered for measured phase
        ycsb::run_multi_client_for_duration(
            Arc::clone(&engine),
            clients,
            MEASURED,
            |client_id, stop| {
                let zipf = zipf.clone();
                move |e, cf, op_index| {
                    let op_r = ycsb::deterministic_u64(WORKLOAD_SEED, client_id, op_index, 0);
                    let cf_id = cf.id();

                    let key_idx = match &zipf {
                        Some(zipf) => {
                            // Reserve draw=0 for op selection; use draw>=1 for key selection.
                            let mut draw: u64 = 1;
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

                    // Deterministic, stochastic 50/50 mix (avoids perfect alternation).
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
                            tx.put(k.to_vec(), v.to_vec(), None).expect("measured put");
                            e.commit(tx, write_opts)
                        })
                        .expect("measured commit");
                    }
                }
            },
        )
    });

    ctx.set_elements(measured_ops);
    ctx.set_bytes(measured_ops * (ycsb::KEY_SIZE + ycsb::VALUE_SIZE) as u64);
}

fn run_workload_a(ctx: &mut StressContext, opts: MidgeOptions, clients: usize) {
    run_workload_a_with_distribution(
        ctx,
        opts,
        clients,
        KeyDistribution::Zipf {
            theta: ZIPFIAN_THETA,
        },
    );
}

#[stress_test]
fn tier4_ycsb_a_mem_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_a_mem_2_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, CLIENTS_2);
}

#[stress_test]
fn tier4_ycsb_a_mem_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_a_mem_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_a_mem_c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, clients_c());
}

#[stress_test]
fn tier4_ycsb_a_mem_c_clients_uniform(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a_with_distribution(
        ctx,
        opts,
        clients_c(),
        KeyDistribution::Zipf { theta: 0.0 },
    );
}

#[stress_test]
fn tier4_ycsb_a_mem_c_clients_hot_key(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a_with_distribution(ctx, opts, clients_c(), KeyDistribution::HotKey);
}

#[stress_test]
fn tier4_ycsb_a_mem_2c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, clients_2c());
}

#[stress_test]
fn tier4_ycsb_a_mem_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_a_mem_32_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, CLIENTS_32);
}

#[stress_test]
fn tier4_ycsb_a_mem_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_a_mem_4c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_a(ctx, opts, clients_4c());
}

#[stress_test]
fn tier4_ycsb_a_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_a_local_2_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, CLIENTS_2);
}

#[stress_test]
fn tier4_ycsb_a_local_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_a_local_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_a_local_c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, clients_c());
}

#[stress_test]
fn tier4_ycsb_a_local_2c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, clients_2c());
}

#[stress_test]
fn tier4_ycsb_a_local_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_a_local_32_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, CLIENTS_32);
}

#[stress_test]
fn tier4_ycsb_a_local_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_a_local_4c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_a(ctx, opts, clients_4c());
}

#[stress_test]
fn tier4_ycsb_a_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_a_cloud_2_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, CLIENTS_2);
}

#[stress_test]
fn tier4_ycsb_a_cloud_4_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, CLIENTS_4);
}

#[stress_test]
fn tier4_ycsb_a_cloud_8_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, CLIENTS_8);
}

#[stress_test]
fn tier4_ycsb_a_cloud_c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, clients_c());
}

#[stress_test]
fn tier4_ycsb_a_cloud_2c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, clients_2c());
}

#[stress_test]
fn tier4_ycsb_a_cloud_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_a_cloud_32_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, CLIENTS_32);
}

#[stress_test]
fn tier4_ycsb_a_cloud_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_a_cloud_4c_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_a(ctx, opts, clients_4c());
}
stress_main!();
