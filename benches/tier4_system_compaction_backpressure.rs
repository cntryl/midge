//! Tier 4 - Compaction write backpressure
//!
//! Measures sustained local and simulated-cloud write throughput with compaction enabled.
//! The reported throughput is write throughput under compaction pressure, not raw bytes compacted.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_midge::{Engine, MidgeError, WriteOptions};
use cntryl_stress::{
    stress, stress_main, LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome,
    StressContext,
};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use stress_config::{ycsb, MidgeOptions};

const VALUE_SIZE: usize = 512;
const WRITE_BATCH_SIZE: usize = 64;
const COMPACTION_MEMTABLE_SIZE_BYTES: usize = 512 * 1024;

#[derive(Default)]
struct PhaseResult {
    elapsed: Duration,
    writes: u64,
    transactions: u64,
    hard_stalls: u64,
    commit_latencies: Vec<Duration>,
}

fn make_value(fill: u8) -> [u8; VALUE_SIZE] {
    [fill; VALUE_SIZE]
}

const WRITERS: usize = 1;
const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(8);
const COMPACTION_CONCURRENCY_ENV: &str = "MIDGE_COMPACTION_CONCURRENCY";

fn compaction_concurrency() -> usize {
    match std::env::var(COMPACTION_CONCURRENCY_ENV) {
        Ok(value) => value.parse().unwrap_or_else(|error| {
            panic!("{COMPACTION_CONCURRENCY_ENV} must be an integer: {error}")
        }),
        Err(_) => 1,
    }
}

fn counter_as_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).expect("upper counter bits fit u32");
    let low = u32::try_from(value & u64::from(u32::MAX)).expect("lower counter bits fit u32");
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}

fn open_compaction_engine(mut opts: MidgeOptions, compaction_concurrency: usize) -> Engine {
    opts.enable_compaction = true;
    opts.memtable_size = COMPACTION_MEMTABLE_SIZE_BYTES;
    Engine::open(opts.to_open_options_with_compaction_concurrency(compaction_concurrency))
        .expect("open compaction backpressure engine")
}

fn run_write_phase(
    engine: &Arc<Engine>,
    cf_id: cntryl_midge::ColumnFamilyId,
    write_options: fn() -> WriteOptions,
    next_key: &Arc<AtomicU64>,
    writers: usize,
    duration: Duration,
    count: bool,
) -> PhaseResult {
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(writers + 1));
    let mut handles = Vec::with_capacity(writers);

    for _ in 0..writers {
        let engine = Arc::clone(engine);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let next_key = Arc::clone(next_key);

        handles.push(thread::spawn(move || {
            barrier.wait();

            let mut local_writes = 0u64;
            let mut local_transactions = 0u64;
            let mut hard_stalls = 0u64;
            let mut commit_latencies = Vec::new();

            while !stop.load(Ordering::Acquire) {
                let transaction_started_at = Instant::now();
                loop {
                    let mut tx = engine
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                        .expect("begin compaction pressure transaction");

                    for _ in 0..WRITE_BATCH_SIZE {
                        let seq = next_key.fetch_add(1, Ordering::Relaxed);
                        let key = ycsb::make_key(seq);
                        let value = make_value((seq & 0xFF) as u8);
                        tx.put(key.to_vec(), value.to_vec(), None)
                            .expect("put compaction pressure value");
                    }

                    match tx.commit(write_options()) {
                        Ok(()) => {
                            if count {
                                local_writes = local_writes.wrapping_add(WRITE_BATCH_SIZE as u64);
                                local_transactions = local_transactions.wrapping_add(1);
                                commit_latencies.push(transaction_started_at.elapsed());
                            }
                            break;
                        }
                        Err(MidgeError::WriteStall(_)) => {
                            if count {
                                hard_stalls = hard_stalls.saturating_add(1);
                            }
                            let _ = engine
                                .wait_for_write_stall_clear(cf_id, Duration::from_millis(500))
                                .expect("wait for compaction pressure stall");
                        }
                        Err(error) => panic!("commit compaction pressure transaction: {error}"),
                    }
                }
            }

            PhaseResult {
                writes: local_writes,
                transactions: local_transactions,
                hard_stalls,
                commit_latencies,
                ..PhaseResult::default()
            }
        }));
    }

    barrier.wait();
    let started_at = Instant::now();
    thread::sleep(duration);
    let elapsed = started_at.elapsed();
    stop.store(true, Ordering::Release);

    let mut out = PhaseResult {
        elapsed,
        ..PhaseResult::default()
    };
    for handle in handles {
        let result = handle
            .join()
            .expect("compaction pressure worker should finish");
        out.writes = out.writes.wrapping_add(result.writes);
        out.transactions = out.transactions.wrapping_add(result.transactions);
        out.hard_stalls = out.hard_stalls.saturating_add(result.hard_stalls);
        out.commit_latencies.extend(result.commit_latencies);
    }

    out
}

fn run_compaction_backpressure_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    mode: &'static str,
    write_options: fn() -> WriteOptions,
) {
    stress_config::init_benchmark_telemetry().expect("initialize benchmark telemetry");

    let measured_duration = stress_config::tier4_measured_duration(MEASURED);
    #[cfg(feature = "candidate-write-backpressure-bench")]
    let opts = {
        let opts = stress_config::opts_for_mode(mode);
        if mode == "local" {
            opts.with_write_backpressure(cntryl_midge::WriteBackpressureConfig {
                l0_soft_limit: Some(2),
                max_admission_delay: Duration::from_micros(100),
                ..Default::default()
            })
        } else {
            opts
        }
    };
    #[cfg(not(feature = "candidate-write-backpressure-bench"))]
    let opts = stress_config::opts_for_mode(mode);
    let compaction_concurrency = compaction_concurrency();
    let engine = Arc::new(open_compaction_engine(opts, compaction_concurrency));
    let cf = engine
        .create_column_family("cf1")
        .expect("create compaction pressure column family");
    let cf_id = cf.id();
    let next_key = Arc::new(AtomicU64::new(0));
    let _warmup = run_write_phase(
        &engine,
        cf_id,
        write_options,
        &next_key,
        WRITERS,
        WARMUP,
        false,
    );

    let start = ycsb::capture_runtime_perf_snapshot(&engine);
    let metrics_before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture backpressure metrics before measurement");
    let gate_before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture publication deferrals before measurement")
        .flush_publication_deferred_by_gate;
    let measured = run_write_phase(
        &engine,
        cf_id,
        write_options,
        &next_key,
        WRITERS,
        measured_duration,
        true,
    );
    let perf = ycsb::runtime_perf_report(&engine, start);
    let metrics_after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture backpressure metrics after measurement");
    let gate_after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("capture publication deferrals after measurement")
        .flush_publication_deferred_by_gate;

    record_backpressure_parameters(ctx, mode, compaction_concurrency, measured_duration);
    assert_eq!(
        measured.writes,
        measured
            .transactions
            .saturating_mul(WRITE_BATCH_SIZE as u64),
        "each completed transaction must account for one full write batch"
    );

    ctx.record_external_outcome(
        scenario,
        measured.elapsed,
        LogicalUnit::new("write"),
        OperationOutcome::success(measured.writes),
    );
    ycsb::record_runtime_correctness(ctx, &perf);
    for latency in measured.commit_latencies.iter().copied() {
        ctx.record_latency(latency);
    }
    ctx.parameter(
        "end_to_end_commit_latency_sample_count",
        measured.transactions,
    );
    let slowdown_ns = metrics_after
        .write_slowdown_ns_total
        .saturating_sub(metrics_before.write_slowdown_ns_total);
    let slowdown_count = metrics_after
        .write_slowdown_count
        .saturating_sub(metrics_before.write_slowdown_count);
    let deferrals = gate_after.saturating_sub(gate_before);
    record_backpressure_observations(
        ctx,
        slowdown_ns,
        slowdown_count,
        measured.hard_stalls,
        deferrals,
    );
}

fn record_backpressure_parameters(
    ctx: &mut StressContext,
    mode: &'static str,
    compaction_concurrency: usize,
    measured_duration: Duration,
) {
    ctx.parameter("storage_profile", mode);
    ctx.parameter(
        "provider_evidence",
        if mode == "cloud" {
            "simulated_cloud_no_live_provider"
        } else {
            "local_filesystem"
        },
    );
    ctx.parameter("measurement_window_shape", "continuous_same_owner");
    #[cfg(feature = "candidate-write-backpressure-bench")]
    ctx.parameter("candidate_soft_limits_enabled", mode == "local");
    #[cfg(not(feature = "candidate-write-backpressure-bench"))]
    ctx.parameter("candidate_soft_limits_enabled", false);
    #[cfg(feature = "candidate-write-backpressure-bench")]
    if mode == "local" {
        ctx.parameter("l0_soft_limit", 2);
        ctx.parameter("max_admission_delay_us", 100);
    }
    ctx.parameter("writers", WRITERS);
    ctx.parameter("compaction_concurrency", compaction_concurrency);
    ctx.parameter("write_batch_size", WRITE_BATCH_SIZE);
    ctx.parameter("value_size_bytes", VALUE_SIZE);
    ctx.parameter("memtable_size_bytes", COMPACTION_MEMTABLE_SIZE_BYTES);
    ctx.parameter("logical_bytes_per_operation", ycsb::KEY_SIZE + VALUE_SIZE);
    ctx.parameter("warmup_secs", WARMUP.as_secs_f64());
    ctx.parameter("measured_secs", measured_duration.as_secs_f64());
}

fn record_backpressure_observations(
    ctx: &mut StressContext,
    slowdown_ns: u64,
    slowdown_count: u64,
    hard_stalls: u64,
    deferrals: u64,
) {
    ctx.record_observation(
        "admission_delay_total_ns",
        counter_as_f64(slowdown_ns),
        ObservationUnit::Nanoseconds,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "admission_delay_submission_count",
        counter_as_f64(slowdown_count),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    if slowdown_count > 0 {
        ctx.record_observation(
            "admission_delay_mean_ns_per_submission",
            counter_as_f64(slowdown_ns) / counter_as_f64(slowdown_count),
            ObservationUnit::Nanoseconds,
            ObservationDirection::Informational,
        );
    }
    ctx.record_observation(
        "hard_stall_count",
        counter_as_f64(hard_stalls),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
    ctx.record_observation(
        "flush_publication_deferred_by_gate",
        counter_as_f64(deferrals),
        ObservationUnit::Count,
        ObservationDirection::Informational,
    );
}

#[stress(tier = 4)]
fn tier4_compaction_write_backpressure_local(ctx: &mut StressContext) {
    run_compaction_backpressure_case(
        ctx,
        "tier4_compaction_write_backpressure_local",
        "local",
        WriteOptions::buffered,
    );
}

#[stress(tier = 4)]
fn tier4_compaction_write_backpressure_cloud_simulated(ctx: &mut StressContext) {
    run_compaction_backpressure_case(
        ctx,
        "tier4_compaction_write_backpressure_cloud_simulated",
        "cloud",
        WriteOptions::cloud_async,
    );
}

stress_main!();
