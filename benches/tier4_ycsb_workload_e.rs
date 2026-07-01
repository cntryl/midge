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

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::BenchConfig;

use std::sync::Arc;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::testkit::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

const SCAN_LEN: u64 = 64;

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xE0E0_EA5E_5678_9ABC;

#[allow(clippy::too_many_lines)]
fn run_workload_e(ctx: &mut StressContext, opts: MidgeOptions, clients: usize) {
    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);

    // Phase 1: Load (not measured)
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    ycsb::load_initial_dataset(engine.as_ref(), &cf, initial_keys);

    // Workload E: 95% scans, 5% inserts. Use a deterministic, stochastic mix.
    // Scans target the dense initial keyspace for stable scan lengths.

    // Phase 2: Warm-up (not measured)
    {
        let write_opts = cntryl_midge::WriteOptions::best_effort(); // Fast warmup: skip WAL I/O
        let _warmup_ops =
            ycsb::run_multi_client_for_duration(&engine, clients, WARMUP, |client_id, stop| {
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
                    // Actually consume the iterator to measure scan throughput
                    let mut iter = tx.scan(&query).expect("warmup range");
                    let mut count = 0;
                    while iter.next().is_some() {
                        count += 1;
                    }
                    std::hint::black_box(count);
                }
            });
    }

    // Flush to ensure warmup data is durable before measured phase
    engine.flush_cf(&cf).unwrap();

    // Phase 3: Measured (duration-based; multi-client)
    let measured = ctx.measure_ref(engine.as_ref(), |_e| {
        let write_opts = cntryl_midge::WriteOptions::buffered(); // Back to buffered for measured phase
        ycsb::run_multi_client_for_duration_with_stats(
            &engine,
            clients,
            MEASURED,
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
                    // Actually consume the iterator to measure scan throughput
                    let mut iter = tx.scan(&query).expect("measured range");
                    let mut count = 0;
                    while iter.next().is_some() {
                        count += 1;
                    }
                    std::hint::black_box(count);
                }
            },
        )
    });

    // Approximate bytes touched: 95% scans of length SCAN_LEN, 5% inserts.
    let bytes_per_kv = ycsb::logical_entry_size_bytes() as u64;
    let est_inserts = measured.operations / 20;
    let est_scans = measured.operations.saturating_sub(est_inserts);
    let est_bytes = est_inserts * bytes_per_kv + est_scans * (SCAN_LEN * bytes_per_kv);

    ctx.set_elements(measured.operations);
    ctx.set_bytes(est_bytes);
    for (name, value) in measured.latency_tags() {
        ctx.tag(name, value.to_string());
    }
}

#[stress_test]
fn tier4_ycsb_e_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_e(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_e_local_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_e(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_e_local_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_e(ctx, opts, CLIENTS_64);
}

#[stress_test]
fn tier4_ycsb_e_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_e(ctx, opts, CLIENTS_1);
}

#[stress_test]
fn tier4_ycsb_e_cloud_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_e(ctx, opts, CLIENTS_16);
}

#[stress_test]
fn tier4_ycsb_e_cloud_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_e(ctx, opts, CLIENTS_64);
}

stress_main!();
