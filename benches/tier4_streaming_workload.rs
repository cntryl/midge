//! Tier 4 â€” Streaming Workload
//!
//! Models append-heavy streaming with tail-follow reads.
//! Focuses on stability, lag, and interference â€” not peak throughput.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, LogicalUnit, OperationOutcome, StressContext};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use cntryl_midge::MidgeEngine;
use stress_config::{ycsb, MidgeOptions};

const VALUE_SIZE: usize = 256;

// -----------------------------------------------------------------------------
// Streaming model parameters (tune intentionally)
// -----------------------------------------------------------------------------

const WRITERS: usize = 2;
const READERS: usize = 2;

const WARMUP: Duration = Duration::from_secs(3);
const MEASURED: Duration = Duration::from_secs(15);

// Readers follow within this many keys of the head
const TAIL_WINDOW: u64 = 1_000;

fn make_value(fill: u8) -> [u8; VALUE_SIZE] {
    [fill; VALUE_SIZE]
}

#[derive(Default, Clone, Copy)]
struct StreamingStats {
    reads: u64,
    read_misses: u64,
    lag_sum: u64,
    lag_max: u64,
}

#[derive(Default, Clone, Copy)]
struct PhaseResult {
    elapsed: Duration,
    write_attempts: u64,
    writes: u64,
    write_failures: u64,
    read_attempts: u64,
    reads: u64,
    read_failures: u64,
    read_misses: u64,
    lag_sum: u64,
    lag_max: u64,
}

fn spawn_writer(
    engine: Arc<MidgeEngine>,
    cf_id: cntryl_midge::ColumnFamilyId,
    write_opts: cntryl_midge::WriteOptions,
    head: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    count: bool,
) -> thread::JoinHandle<PhaseResult> {
    thread::spawn(move || {
        barrier.wait();

        let mut writes = 0_u64;
        let mut attempts = 0_u64;
        let mut failures = 0_u64;
        while !stop.load(Ordering::Acquire) {
            let seq = head.fetch_add(1, Ordering::Relaxed);
            let key = ycsb::make_key(seq);
            let value = make_value((seq & 0xFF) as u8);
            let result = ycsb::retry_write_stall_observed(&engine, cf_id, &stop, || {
                let mut tx = engine.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)?;
                tx.put(key.to_vec(), value.to_vec(), None)?;
                tx.commit(write_opts)
            });

            if count {
                match result {
                    Ok(true) => {
                        attempts = attempts.wrapping_add(1);
                        writes = writes.wrapping_add(1);
                    }
                    Ok(false) => {}
                    Err(_) => {
                        attempts = attempts.wrapping_add(1);
                        failures = failures.wrapping_add(1);
                    }
                }
            }
        }

        PhaseResult {
            write_attempts: attempts,
            writes,
            write_failures: failures,
            ..PhaseResult::default()
        }
    })
}

fn spawn_reader(
    engine: Arc<MidgeEngine>,
    cf_id: cntryl_midge::ColumnFamilyId,
    head: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    count: bool,
) -> thread::JoinHandle<PhaseResult> {
    thread::spawn(move || {
        barrier.wait();

        let mut next = 0_u64;
        let mut local = StreamingStats::default();
        let mut attempts = 0_u64;
        let mut failures = 0_u64;

        while !stop.load(Ordering::Acquire) {
            let current_head = head.load(Ordering::Relaxed);
            if next + TAIL_WINDOW < current_head {
                next = current_head.saturating_sub(TAIL_WINDOW);
            }

            let key = ycsb::make_key(next);
            let tx = engine
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                .expect("begin");
            let read = tx.get(&key[..]);

            if count {
                attempts = attempts.wrapping_add(1);
                match read {
                    Ok(value) => {
                        local.reads = local.reads.wrapping_add(1);
                        local.read_misses += u64::from(value.is_none());
                        let lag = current_head.saturating_sub(next);
                        local.lag_sum = local.lag_sum.wrapping_add(lag);
                        local.lag_max = local.lag_max.max(lag);
                    }
                    Err(_) => failures = failures.wrapping_add(1),
                }
            }

            next = next.wrapping_add(1);
        }

        PhaseResult {
            read_attempts: attempts,
            reads: local.reads,
            read_failures: failures,
            read_misses: local.read_misses,
            lag_sum: local.lag_sum,
            lag_max: local.lag_max,
            ..PhaseResult::default()
        }
    })
}

fn run_streaming_phase(
    engine: &Arc<MidgeEngine>,
    cf_id: cntryl_midge::ColumnFamilyId,
    write_opts: cntryl_midge::WriteOptions,
    head: &Arc<AtomicU64>,
    duration: Duration,
    count: bool,
) -> PhaseResult {
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(WRITERS + READERS + 1));

    let mut handles = Vec::with_capacity(WRITERS + READERS);

    for _ in 0..WRITERS {
        handles.push(spawn_writer(
            Arc::clone(engine),
            cf_id,
            write_opts,
            Arc::clone(head),
            Arc::clone(&stop),
            Arc::clone(&barrier),
            count,
        ));
    }

    for _ in 0..READERS {
        handles.push(spawn_reader(
            Arc::clone(engine),
            cf_id,
            Arc::clone(head),
            Arc::clone(&stop),
            Arc::clone(&barrier),
            count,
        ));
    }

    // Release all workers at the same time, then start the phase window.
    barrier.wait();
    let started_at = Instant::now();
    thread::sleep(duration);
    let elapsed = started_at.elapsed();
    stop.store(true, Ordering::Release);

    let mut out = PhaseResult::default();
    for h in handles {
        let r = h.join().unwrap_or_default();
        out.write_attempts = out.write_attempts.wrapping_add(r.write_attempts);
        out.writes = out.writes.wrapping_add(r.writes);
        out.write_failures = out.write_failures.wrapping_add(r.write_failures);
        out.read_attempts = out.read_attempts.wrapping_add(r.read_attempts);
        out.reads = out.reads.wrapping_add(r.reads);
        out.read_failures = out.read_failures.wrapping_add(r.read_failures);
        out.read_misses = out.read_misses.wrapping_add(r.read_misses);
        out.lag_sum = out.lag_sum.wrapping_add(r.lag_sum);
        out.lag_max = out.lag_max.max(r.lag_max);
    }

    out.elapsed = elapsed;
    out
}

fn run_streaming(ctx: &mut StressContext, scenario: &'static str, opts: MidgeOptions) {
    ctx.parameter("writers", WRITERS);
    ctx.parameter("readers", READERS);
    ctx.parameter("warmup_secs", WARMUP.as_secs());
    ctx.parameter("measured_secs", MEASURED.as_secs());
    ctx.parameter("tail_window_keys", TAIL_WINDOW);
    ctx.parameter("logical_bytes_per_operation", ycsb::KEY_SIZE + VALUE_SIZE);

    let write_opts = stress_config::measured_write_options(&opts);
    let engine = Arc::new(ycsb::open_tier4_engine(opts));
    let cf = engine.create_column_family("cf1").unwrap();
    let cf_id = cf.id();

    // Shared stream head across warmup + measured (represents the append log).
    let head = Arc::new(AtomicU64::new(0));

    // -------------------------------------------------------------------------
    // Warmup (unmeasured)
    // -------------------------------------------------------------------------

    let _warmup = run_streaming_phase(&engine, cf_id, write_opts, &head, WARMUP, false);

    // -------------------------------------------------------------------------
    // Measured phase
    // -------------------------------------------------------------------------

    let measured_phase = run_streaming_phase(&engine, cf_id, write_opts, &head, MEASURED, true);

    let PhaseResult {
        elapsed,
        write_attempts,
        writes,
        write_failures,
        read_attempts,
        reads,
        read_failures,
        read_misses: misses,
        lag_sum,
        lag_max,
    } = measured_phase;

    // -------------------------------------------------------------------------
    // StressContext reporting
    // -------------------------------------------------------------------------

    let attempted = write_attempts.saturating_add(read_attempts);
    let completed = writes.saturating_add(reads);
    let failures = write_failures.saturating_add(read_failures);
    assert_eq!(
        attempted,
        completed.saturating_add(failures),
        "every streaming operation must be classified as completed or failed"
    );
    ctx.record_external_outcome(
        scenario,
        elapsed,
        LogicalUnit::new("stream_operation"),
        OperationOutcome::new(attempted, completed).failures(failures),
    );

    // These shape checks remain local invariants: sample-varying observations
    // are not workload parameters in the v2 stress artifact contract.
    let avg_lag_milli_keys = lag_sum
        .saturating_mul(1_000)
        .checked_div(reads)
        .unwrap_or(0);
    let miss_rate_ppm = misses
        .saturating_mul(1_000_000)
        .checked_div(reads)
        .unwrap_or(0);

    assert!(misses <= reads, "read misses cannot exceed completed reads");
    assert!(
        reads == 0 || lag_sum >= lag_max,
        "aggregate reader lag must include the maximum observed lag"
    );
    std::hint::black_box((miss_rate_ppm, avg_lag_milli_keys, lag_max));
}

#[stress(tier = 4)]
fn tier4_streaming_local(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("local");
    run_streaming(ctx, "tier4_streaming_local", opts);
}

#[stress(tier = 4)]
fn tier4_streaming_cloud(ctx: &mut StressContext) {
    let opts = stress_config::opts_for_mode("cloud");
    run_streaming(ctx, "tier4_streaming_cloud", opts);
}

stress_main!();
