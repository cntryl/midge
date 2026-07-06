//! Tier 4 — YCSB Workload C (Read only)
//!
//! Workload C: 100% reads.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use cntryl_midge::{ColumnFamilyId, MidgeEngine};
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use cntryl_midge::testkit::ycsb;
use cntryl_midge::testkit::zipfian::ZipfianGenerator;
use cntryl_midge::testkit::MidgeOptions;

const DEFAULT_INITIAL_KEYS: usize = 50_000; // Overridable for larger-than-RAM nightly runs
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);
const MEMORY_16_FIXED_OPS_PER_CLIENT: u64 = 600_000;
const MEMORY_16_WORKER_THREADS: usize = 4;

const ZIPFIAN_THETA: f64 = 0.99;

const CLIENTS_1: usize = 1;
const CLIENTS_16: usize = 16;
const CLIENTS_64: usize = 64;

const WORKLOAD_SEED: u64 = 0xC0C0_EA5E_5678_9ABC;

fn duration_to_micros(value: Duration) -> u64 {
    u64::try_from(value.as_micros().max(1)).unwrap_or(u64::MAX)
}

fn run_logical_clients_for_operations_with_stats<MakeClient, Step>(
    engine: &Arc<cntryl_midge::MidgeEngine>,
    logical_clients: usize,
    worker_threads: usize,
    operations_per_client: u64,
    make_client: MakeClient,
) -> (ycsb::MultiClientRunStats, Duration)
where
    MakeClient: Fn(usize) -> Step,
    Step: FnMut(&cntryl_midge::MidgeEngine, u64) + Send + 'static,
{
    let barrier = Arc::new(Barrier::new(worker_threads + 1));
    let mut handles = Vec::with_capacity(worker_threads);
    let mut grouped_steps: Vec<Vec<Step>> = (0..worker_threads).map(|_| Vec::new()).collect();

    for client_id in 0..logical_clients {
        grouped_steps[client_id % worker_threads].push(make_client(client_id));
    }

    for (worker_id, mut client_steps) in grouped_steps.into_iter().enumerate() {
        let engine = Arc::clone(engine);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier.wait();
            if worker_id > 0 {
                thread::sleep(Duration::from_micros(worker_id as u64 * 50));
            }

            let mut latency_us = Histogram::<u64>::new(3).expect("create client latency histogram");
            for op_index in 0..operations_per_client {
                for client_step in &mut client_steps {
                    let started_at = Instant::now();
                    client_step(engine.as_ref(), op_index);
                    latency_us
                        .record(duration_to_micros(started_at.elapsed()))
                        .expect("record client latency");
                }
            }
            latency_us
        }));
    }

    barrier.wait();
    let started_at = Instant::now();

    let mut latency_us = Histogram::<u64>::new(3).expect("create aggregate latency histogram");
    for handle in handles {
        let client_latency = handle.join().expect("YCSB client thread should not panic");
        latency_us
            .add(&client_latency)
            .expect("merge compatible latency histograms");
    }
    let elapsed = started_at.elapsed();
    let operations = operations_per_client.saturating_mul(logical_clients as u64);

    (
        ycsb::MultiClientRunStats {
            operations,
            latency_p50_us: latency_us.value_at_percentile(50.0),
            latency_p95_us: latency_us.value_at_percentile(95.0),
            latency_p99_us: latency_us.value_at_percentile(99.0),
            latency_max_us: latency_us.max(),
        },
        elapsed,
    )
}

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

fn measure_fixed_operations(
    ctx: &mut StressContext,
    engine: &Arc<MidgeEngine>,
    cf_id: ColumnFamilyId,
    initial_keys: usize,
    clients: usize,
    measurement_name: String,
) -> ycsb::MultiClientRunStats {
    ctx.parameter("operations_per_client", MEMORY_16_FIXED_OPS_PER_CLIENT);
    ctx.parameter("worker_threads", MEMORY_16_WORKER_THREADS);
    ctx.parameter("logical_clients", clients);

    let zipf = Arc::new(ZipfianGenerator::new(initial_keys, ZIPFIAN_THETA));
    let (measured, elapsed) = run_logical_clients_for_operations_with_stats(
        engine,
        clients,
        MEMORY_16_WORKER_THREADS,
        MEMORY_16_FIXED_OPS_PER_CLIENT,
        |client_id| {
            let zipf = Arc::clone(&zipf);
            move |e, op_index| {
                let k = workload_c_read_key(&zipf, client_id, op_index);
                let tx = e
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin");
                let _ = tx.get(&k[..]).expect("measured get");
            }
        },
    );
    ctx.record_external(measurement_name, elapsed, measured.operations);
    measured
}

fn measure_duration(
    ctx: &mut StressContext,
    engine: &Arc<MidgeEngine>,
    cf_id: ColumnFamilyId,
    initial_keys: usize,
    clients: usize,
    measurement_name: String,
) -> ycsb::MultiClientRunStats {
    stress_config::measure_external_counted(ctx, measurement_name, || {
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
    ctx.tag("storage_profile", profile);
    let initial_keys = ycsb::configured_initial_keys(DEFAULT_INITIAL_KEYS);
    let use_fixed_operations = profile == "memory" && clients == CLIENTS_16;
    ctx.parameter(
        "measurement_mode",
        if use_fixed_operations {
            "fixed_ops"
        } else {
            "duration"
        },
    );
    ctx.parameter("measured_secs", MEASURED.as_secs());

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
    let measured = if use_fixed_operations {
        measure_fixed_operations(ctx, &engine, cf_id, initial_keys, clients, measurement_name)
    } else {
        measure_duration(ctx, &engine, cf_id, initial_keys, clients, measurement_name)
    };

    ctx.set_elements(measured.operations);
    ctx.set_bytes(measured.operations * ycsb::logical_entry_size_bytes() as u64);
    for (name, value) in measured.latency_tags() {
        ctx.tag(name, value.to_string());
    }
    for (name, value) in ycsb::runtime_perf_report(engine.as_ref(), perf_start).tags() {
        ctx.tag(name, value.to_string());
    }
}

#[stress(tier = 4)]
fn tier4_ycsb_c_memory_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_1);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_memory_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_16);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_memory_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("memory");
    run_workload_c(ctx, opts, "memory", CLIENTS_64);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_local_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_1);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_local_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_16);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_local_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_workload_c(ctx, opts, "local", CLIENTS_64);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_cloud_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_1);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_cloud_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_16);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_cloud_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_workload_c(ctx, opts, "cloud", CLIENTS_64);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_hybrid_1_client(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_1);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_hybrid_16_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_16);
}

#[stress(tier = 4)]
fn tier4_ycsb_c_hybrid_64_clients(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("hybrid");
    run_workload_c(ctx, opts, "hybrid", CLIENTS_64);
}

stress_main!();
