//! Tier 4 — Streaming Workload
//!
//! Models append-heavy streaming with tail-follow reads.
//! Focuses on stability, lag, and interference — not peak throughput.

use cntryl_stress::{stress_main, stress_test, StressContext};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use cntryl_midge::testkit::ycsb;
use cntryl_midge::{MidgeEngine, MidgeOptions};

const VALUE_SIZE: usize = 256;

// -----------------------------------------------------------------------------
// Streaming model parameters (tune intentionally)
// -----------------------------------------------------------------------------

const WRITERS: usize = 2;
const READERS: usize = 2;

const WARMUP: Duration = Duration::from_secs(10);
const MEASURED: Duration = Duration::from_secs(30);

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
    engine: Arc<MidgeEngine>,
    head: Arc<AtomicU64>,
    duration: Duration,
    count: bool,
) -> PhaseResult {
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(WRITERS + READERS + 1));

    let mut handles = Vec::with_capacity(WRITERS + READERS);

    for _ in 0..WRITERS {
        let engine = Arc::clone(&engine);
        let head = Arc::clone(&head);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            let cf = engine.default_column_family();
            barrier.wait();

            let mut local_writes: u64 = 0;
            while !stop.load(Ordering::Acquire) {
                let seq = head.fetch_add(1, Ordering::Relaxed);
                let key = ycsb::make_key(seq);
                let value = make_value((seq & 0xFF) as u8);

                let _ = engine.put(cf, &key[..], &value[..]);

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
        let engine = Arc::clone(&engine);
        let head = Arc::clone(&head);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            let cf = engine.default_column_family();
            barrier.wait();

            let mut next: u64 = 0;
            let mut local = StreamingStats::default();

            while !stop.load(Ordering::Acquire) {
                let current_head = head.load(Ordering::Relaxed);

                if next + TAIL_WINDOW < current_head {
                    next = current_head.saturating_sub(TAIL_WINDOW);
                }

                let key = ycsb::make_key(next);
                let hit = engine.get(cf, &key[..]).ok().flatten().is_some();

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

    let _warmup = run_streaming_phase(Arc::clone(&engine), Arc::clone(&head), WARMUP, false);

    // -------------------------------------------------------------------------
    // Measured phase
    // -------------------------------------------------------------------------

    let PhaseResult {
        writes,
        reads,
        read_misses: misses,
        lag_sum,
        lag_max,
    } = ctx.measure_ref(engine.as_ref(), |_e| {
        run_streaming_phase(Arc::clone(&engine), Arc::clone(&head), MEASURED, true)
    });

    // -------------------------------------------------------------------------
    // StressContext reporting
    // -------------------------------------------------------------------------

    let total_ops = writes.saturating_add(reads);
    ctx.set_elements(total_ops);

    // Approximate bytes: reads and writes both touch key+value.
    let bytes_per_op = (ycsb::KEY_SIZE + VALUE_SIZE) as u64;
    ctx.set_bytes(total_ops.saturating_mul(bytes_per_op));

    // Extra shape diagnostics (not used for throughput math).
    let avg_lag = if reads == 0 {
        0.0
    } else {
        (lag_sum as f64) / (reads as f64)
    };
    let miss_rate = if reads == 0 {
        0.0
    } else {
        (misses as f64) / (reads as f64)
    };

    eprintln!(
        "streaming: writers={} readers={} tail_window={} writes={} reads={} miss_rate={:.3} avg_lag={:.1} max_lag={} ",
        WRITERS, READERS, TAIL_WINDOW, writes, reads, miss_rate, avg_lag, lag_max
    );
}

#[stress_test]
fn tier4_streaming_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_streaming(ctx, opts);
}

#[stress_test]
fn tier4_streaming_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_streaming(ctx, opts);
}

stress_main!();
