//! Tier 4 â€” Streaming Workload
//!
//! Models append-heavy streaming with tail-follow reads.
//! Focuses on stability, lag, and interference â€” not peak throughput.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress_main, stress_test, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::{testkit::MidgeOptions, MidgeEngine};

const VALUE_SIZE: usize = 256;

// -----------------------------------------------------------------------------
// Streaming model parameters (tune intentionally)
// -----------------------------------------------------------------------------

const WRITERS: usize = 2;
const READERS: usize = 2;

const WARMUP: Duration = Duration::from_secs(1);
const MEASURED: Duration = Duration::from_secs(5);

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
    writes: u64,
    reads: u64,
    read_misses: u64,
    lag_sum: u64,
    lag_max: u64,
}

fn run_streaming_phase(
    engine: &Arc<MidgeEngine>,
    head: &Arc<AtomicU64>,
    duration: Duration,
    count: bool,
) -> PhaseResult {
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(WRITERS + READERS + 1));

    let mut handles = Vec::with_capacity(WRITERS + READERS);

    for _ in 0..WRITERS {
        let engine = Arc::clone(engine);
        let head = Arc::clone(head);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            let cf = engine.create_column_family("cf1").unwrap();
            let cf_id = cf.id();
            barrier.wait();

            let mut local_writes: u64 = 0;
            while !stop.load(Ordering::Acquire) {
                let seq = head.fetch_add(1, Ordering::Relaxed);
                let key = ycsb::make_key(seq);
                let value = make_value((seq & 0xFF) as u8);

                let mut tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    .expect("begin");
                tx.put(key.to_vec(), value.to_vec(), None).ok();
                let _ = tx.commit(cntryl_midge::WriteOptions::buffered());

                if count {
                    local_writes = local_writes.wrapping_add(1);
                }
            }

            PhaseResult {
                writes: local_writes,
                ..PhaseResult::default()
            }
        }));
    }

    for _ in 0..READERS {
        let engine = Arc::clone(engine);
        let head = Arc::clone(head);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            let cf = engine.create_column_family("cf1").unwrap();
            let cf_id = cf.id();
            barrier.wait();

            let mut next: u64 = 0;
            let mut local = StreamingStats::default();

            while !stop.load(Ordering::Acquire) {
                let current_head = head.load(Ordering::Relaxed);

                if next + TAIL_WINDOW < current_head {
                    next = current_head.saturating_sub(TAIL_WINDOW);
                }

                let key = ycsb::make_key(next);
                let tx = engine
                    .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
                    .expect("begin");
                let hit = tx.get(&key[..]).ok().flatten().is_some();

                if count {
                    local.reads = local.reads.wrapping_add(1);
                    if !hit {
                        local.read_misses = local.read_misses.wrapping_add(1);
                    }

                    let lag = current_head.saturating_sub(next);
                    local.lag_sum = local.lag_sum.wrapping_add(lag);
                    local.lag_max = local.lag_max.max(lag);
                }

                next = next.wrapping_add(1);
            }

            PhaseResult {
                reads: local.reads,
                read_misses: local.read_misses,
                lag_sum: local.lag_sum,
                lag_max: local.lag_max,
                ..PhaseResult::default()
            }
        }));
    }

    // Release all workers at the same time, then start the phase window.
    barrier.wait();
    thread::sleep(duration);
    stop.store(true, Ordering::Release);

    let mut out = PhaseResult::default();
    for h in handles {
        let r = h.join().unwrap_or_default();
        out.writes = out.writes.wrapping_add(r.writes);
        out.reads = out.reads.wrapping_add(r.reads);
        out.read_misses = out.read_misses.wrapping_add(r.read_misses);
        out.lag_sum = out.lag_sum.wrapping_add(r.lag_sum);
        out.lag_max = out.lag_max.max(r.lag_max);
    }

    out
}

fn run_streaming(ctx: &mut StressContext, opts: MidgeOptions) {
    let engine = Arc::new(ycsb::open_tier4_engine(opts));

    // Shared stream head across warmup + measured (represents the append log).
    let head = Arc::new(AtomicU64::new(0));

    // -------------------------------------------------------------------------
    // Warmup (unmeasured)
    // -------------------------------------------------------------------------

    let _warmup = run_streaming_phase(&engine, &head, WARMUP, false);

    // -------------------------------------------------------------------------
    // Measured phase
    // -------------------------------------------------------------------------

    let measured_phase = stress_config::measure_external_counted(ctx, || {
        let phase = run_streaming_phase(&engine, &head, MEASURED, true);
        let total_ops = phase.writes.saturating_add(phase.reads);
        (phase, total_ops)
    });

    let PhaseResult {
        writes,
        reads,
        read_misses: misses,
        lag_sum,
        lag_max,
    } = measured_phase;

    // -------------------------------------------------------------------------
    // StressContext reporting
    // -------------------------------------------------------------------------

    let total_ops = writes.saturating_add(reads);
    ctx.set_elements(total_ops);

    // Approximate bytes: reads and writes both touch key+value.
    let bytes_per_op = (ycsb::KEY_SIZE + VALUE_SIZE) as u64;
    ctx.set_bytes(total_ops.saturating_mul(bytes_per_op));

    // Extra shape diagnostics (not used for throughput math).
    let avg_lag_milli_keys = lag_sum
        .saturating_mul(1_000)
        .checked_div(reads)
        .unwrap_or(0);
    let miss_rate_ppm = misses
        .saturating_mul(1_000_000)
        .checked_div(reads)
        .unwrap_or(0);

    ctx.tag("writers", WRITERS.to_string());
    ctx.tag("readers", READERS.to_string());
    ctx.tag("tail_window_keys", TAIL_WINDOW.to_string());
    ctx.tag("writes", writes.to_string());
    ctx.tag("reads", reads.to_string());
    ctx.tag("read_misses", misses.to_string());
    ctx.tag("read_miss_rate_ppm", miss_rate_ppm.to_string());
    ctx.tag("avg_lag_milli_keys", avg_lag_milli_keys.to_string());
    ctx.tag("max_lag_keys", lag_max.to_string());
}

#[stress_test(tier = 4)]
fn tier4_streaming_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_streaming(ctx, opts);
}

#[stress_test(tier = 4)]
fn tier4_streaming_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_streaming(ctx, opts);
}

stress_main!();
