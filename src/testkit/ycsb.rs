//! Helpers for Tier-4 (YCSB) stress workloads.
//!
//! These helpers are intentionally deterministic and "boring": Tier-4 aims to
//! measure steady-state behavior over time (load → warm-up → measured).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

use crate::engine::api;
use crate::{ColumnFamilyHandle, Engine, MidgeEngine, MidgeError, MidgeResult};

use super::config::MidgeOptions;

pub const KEY_SIZE: usize = 16;
pub const DEFAULT_VALUE_SIZE: usize = 128;

pub const TIER4_MEMTABLE_SIZE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

pub fn configured_initial_keys(default: usize) -> usize {
    std::env::var("MIDGE_YCSB_INITIAL_KEYS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

pub fn configured_value_size() -> usize {
    std::env::var("MIDGE_YCSB_VALUE_SIZE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_VALUE_SIZE)
}

pub fn logical_entry_size_bytes() -> usize {
    KEY_SIZE + configured_value_size()
}

pub fn logical_dataset_bytes(initial_keys: usize) -> u64 {
    initial_keys as u64 * logical_entry_size_bytes() as u64
}

pub fn make_key(id: u64) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[..8].copy_from_slice(&id.to_be_bytes());
    k
}

pub fn make_value(fill: u8) -> Vec<u8> {
    vec![fill; configured_value_size()]
}

pub fn open_tier4_engine(mut opts: MidgeOptions) -> Engine {
    // Tier-4 workloads should exercise the full system shape.
    opts.enable_compaction = true;
    // Avoid tiny testkit memtables causing constant flush.
    opts.memtable_size = std::env::var("MIDGE_BENCH_MEMTABLE_SIZE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(TIER4_MEMTABLE_SIZE_BYTES);
    opts.memory_budget = std::env::var("MIDGE_BENCH_MEMORY_BUDGET_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0);

    Engine::open_with_options(opts).expect("open tier4 engine")
}

pub fn load_initial_dataset(engine: &Engine, cf: &ColumnFamilyHandle, initial_keys: usize) {
    // Load is not measured; optimize aggressively to keep Tier-4 runs practical.
    // Use WriteOptions::best_effort() for fastest loading of initial dataset:
    //
    // SAFETY: No durability is needed during load because:
    // - Load phase is not measured (setup phase only)
    // - On engine crash, re-running load_initial_dataset reloads the data
    // - Measured workload uses buffered() for proper durability
    // - flush_cf() at end ensures data reaches storage before warm-up begins
    //
    // This skip of WAL commits speeds up 100k key loads by 3-5x compared to buffered().
    //
    // Use larger batches to amortize commit overhead during load.

    // Optional trace: set MIDGE_TRACE_YCSB=1 to print progress during load.
    let trace = std::env::var_os("MIDGE_TRACE_YCSB").is_some();

    // Hard-coded sensible defaults (no env lookups):
    // - Batch size: increased from 20k to 50k to amortize commit overhead.
    // - Threads: use available CPU cores, but never exceed the number of keys.
    const DEFAULT_BATCH_OPS: usize = 50_000;
    let batch_ops: usize = DEFAULT_BATCH_OPS;

    let threads: usize = std::cmp::min(initial_keys.max(1), num_cpus::get().max(1));

    let cf_id = cf.id();
    if trace {
        eprintln!(
            "[midge][ycsb] starting load: initial_keys={} batch_ops={} threads={}",
            initial_keys, batch_ops, threads
        );
    }

    if threads <= 1 {
        // Single-threaded (original behavior) but with configurable batch size.
        let mut tx = engine
            .begin_tx(cf_id, api::TransactionMode::ReadWrite)
            .expect("begin_tx failed");
        let mut count = 0;

        for i in 0..initial_keys as u64 {
            let k = make_key(i);
            let v = make_value((i as usize % 251) as u8);

            tx.put(k.to_vec(), v, None).expect("put failed");
            count += 1;

            if count >= batch_ops {
                tx.commit(api::WriteOptions::best_effort())
                    .expect("commit failed");
                if trace {
                    eprintln!("[midge][ycsb] loaded {} keys", i + 1);
                }
                tx = engine
                    .begin_tx(cf_id, api::TransactionMode::ReadWrite)
                    .expect("begin_tx failed");
                count = 0;
            }
        }

        if count > 0 {
            tx.commit(api::WriteOptions::best_effort())
                .expect("commit failed");
            if trace {
                eprintln!("[midge][ycsb] loaded {} keys (final)", initial_keys);
            }
        }
    } else {
        // Parallel load: split keyspace into contiguous ranges per worker and
        // let each thread drive its own transactions. Use a scoped thread
        // pool so we can borrow &Engine safely.
        let per_worker = initial_keys.div_ceil(threads); // ceil div

        thread::scope(|s| {
            for worker in 0..threads {
                let start = worker * per_worker;
                let end = ((worker + 1) * per_worker).min(initial_keys);

                if start >= end {
                    continue;
                }

                s.spawn(move || {
                    let mut tx = engine
                        .begin_tx(cf_id, api::TransactionMode::ReadWrite)
                        .expect("begin_tx failed");
                    let mut count = 0usize;
                    for i in start..end {
                        let i = i as u64;
                        let k = make_key(i);
                        let v = make_value((i as usize % 251) as u8);
                        tx.put(k.to_vec(), v, None).expect("put failed");
                        count += 1;
                        if count >= batch_ops {
                            tx.commit(api::WriteOptions::best_effort())
                                .expect("commit failed");
                            if trace {
                                eprintln!(
                                    "[midge][ycsb] worker={} loaded {}..{}",
                                    worker,
                                    start,
                                    i + 1
                                );
                            }
                            tx = engine
                                .begin_tx(cf_id, api::TransactionMode::ReadWrite)
                                .expect("begin_tx failed");
                            count = 0;
                        }
                    }
                    if count > 0 {
                        tx.commit(api::WriteOptions::best_effort())
                            .expect("commit failed");
                        if trace {
                            eprintln!(
                                "[midge][ycsb] worker={} loaded {}..{} (final)",
                                worker, start, end
                            );
                        }
                    }
                });
            }
        });
    }

    engine.flush_cf(cf).expect("load phase flush");
    if trace {
        eprintln!("[midge][ycsb] load complete");
    }
}

/// Run a duration-based loop, returning `(operations, bytes)`.
///
/// The `step` closure executes one logical workload operation and returns the
/// number of bytes logically touched by that operation.
pub fn run_for_duration<F>(duration: Duration, mut step: F) -> (u64, u64)
where
    F: FnMut(u64) -> u64,
{
    let deadline = Instant::now() + duration;

    let mut ops: u64 = 0;
    let mut bytes: u64 = 0;

    loop {
        // Reduce the cost of time checks in tight loops.
        if (ops & 0xFF) == 0 && Instant::now() >= deadline {
            break;
        }

        bytes = bytes.wrapping_add(step(ops));
        ops = ops.wrapping_add(1);
    }

    (ops, bytes)
}

fn splitmix64(mut x: u64) -> u64 {
    // Deterministic, fast mixing function.
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Deterministically derive a pseudo-random u64 from `(seed, client_id, op_index, draw_index)`.
///
/// Use this to feed Zipfian generators without introducing true randomness.
pub fn deterministic_u64(seed: u64, client_id: usize, op_index: u64, draw_index: u64) -> u64 {
    let base = seed
        ^ ((client_id as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93))
        ^ op_index
        ^ draw_index.rotate_left(17);
    splitmix64(base)
}

/// Retry an operation on `MidgeError::WriteStall` by waiting for the engine to
/// signal that backpressure has cleared.
///
/// This is designed for Tier-4 stress workloads:
/// - No sleeps
/// - No panics on expected backpressure
/// - Cancellation-aware via the shared `stop` flag
pub fn retry_write_stall<F>(
    engine: &MidgeEngine,
    cf_id: crate::engine::ColumnFamilyId,
    stop: &AtomicBool,
    mut op: F,
) -> MidgeResult<()>
where
    F: FnMut() -> MidgeResult<()>,
{
    loop {
        if stop.load(Ordering::Acquire) {
            // Stress harness is ending; don't block shutdown.
            return Ok(());
        }

        match op() {
            Ok(()) => return Ok(()),
            Err(MidgeError::WriteStall(_)) => {
                // Block waiting for stall to clear, but use a timeout so we can
                // observe stop and avoid hanging on pathological stalls.
                while !stop.load(Ordering::Acquire) {
                    let cleared =
                        engine.wait_for_write_stall_clear(cf_id, Duration::from_millis(50))?;
                    if cleared {
                        break;
                    }
                }
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Run `clients` independent client loops concurrently for `duration` and return total ops.
///
/// Core contract:
/// - One shared engine instance (passed by `Arc`)
/// - Each client runs a tight loop with no sleeps/pacing
/// - The only shared state between clients is the engine and the stop flag
pub fn run_multi_client_for_duration<MakeClient, Step>(
    engine: Arc<MidgeEngine>,
    clients: usize,
    duration: Duration,
    make_client: MakeClient,
) -> u64
where
    MakeClient: Fn(usize, Arc<AtomicBool>) -> Step,
    Step: FnMut(&MidgeEngine, &ColumnFamilyHandle, u64) + Send + 'static,
{
    use std::sync::atomic::AtomicU64;
    use std::time::SystemTime;

    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);

    // Optional watchdog to detect stalls. Enable by setting MIDGE_YCSB_WATCHDOG=1.
    let watchdog_enabled = std::env::var_os("MIDGE_YCSB_WATCHDOG").is_some();
    let watchdog_secs: u64 = std::env::var("MIDGE_YCSB_WATCHDOG_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    // Shared last-op timestamp (milliseconds since UNIX_EPOCH).
    let last_op_ts = Arc::new(AtomicU64::new(
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
    ));

    for client_id in 0..clients {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let mut step = make_client(client_id, Arc::clone(&stop));
        let last_op_ts = Arc::clone(&last_op_ts);

        // Get the CF (it was created in load phase)
        // Note: The actual CF name ("cf1", "data", etc.) varies by benchmark,
        // so we try "cf1" first (YCSB convention), then fall back to "data"
        let cf = engine
            .get_column_family("cf1")
            .or_else(|| engine.get_column_family("data"))
            .expect("CF should exist (tried 'cf1' and 'data')");

        handles.push(thread::spawn(move || {
            // Start all clients together to reduce launch skew.
            barrier.wait();

            // Stagger thread starts to enable write grouping: threads with
            // overlapping commits will be batched together automatically.
            // Without this, all threads hit their first commit simultaneously,
            // preventing the write group coordinator from batching.
            if client_id > 0 {
                std::thread::sleep(Duration::from_micros(client_id as u64 * 50));
            }

            let mut op_index: u64 = 0;
            // Optional slow-op threshold (enable with MIDGE_YCSB_SLOW_OP_MS)
            let slow_op_ms = std::env::var("MIDGE_YCSB_SLOW_OP_MS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok());

            while !stop.load(Ordering::Acquire) {
                let start = Instant::now();
                step(&engine, &cf, op_index);
                let elapsed = start.elapsed();

                // Update heartbeat timestamp after each logical operation.
                let now_ms = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                last_op_ts.store(now_ms, Ordering::Release);

                if let Some(threshold) = slow_op_ms {
                    let el_ms = elapsed.as_millis() as u64;
                    if el_ms >= threshold {
                        eprintln!(
                            "[midge][ycsb][slow_op] client={} op_index={} elapsed_ms={} threshold_ms={}",
                            client_id, op_index, el_ms, threshold
                        );
                    }
                }

                op_index = op_index.wrapping_add(1);
            }
            op_index
        }));
    }

    // Release all clients at the same time, then start the measurement window.
    barrier.wait();

    // Start watchdog thread if enabled. It will panic with diagnostics when a stall is detected.
    let watchdog_handle = if watchdog_enabled {
        let stop = Arc::clone(&stop);
        let last_op_ts = Arc::clone(&last_op_ts);
        Some(thread::spawn(move || {
            let poll_interval = std::time::Duration::from_secs(1);
            loop {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                let now_ms = SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let last_ms = last_op_ts.load(Ordering::Acquire);
                let elapsed = now_ms.saturating_sub(last_ms);
                if elapsed >= watchdog_secs.saturating_mul(1000) {
                    eprintln!(
                        "[midge][ycsb][watchdog] No progress for {} ms (threshold {}s). Panicking to capture diagnostics.",
                        elapsed,
                        watchdog_secs
                    );
                    // Suggest the user run with RUST_BACKTRACE=1 for stack traces.
                    panic!("YCSB watchdog detected stall: elapsed={} ms", elapsed);
                }
                thread::sleep(poll_interval);
            }
        }))
    } else {
        None
    };

    thread::sleep(duration);
    stop.store(true, Ordering::Release);

    if let Some(h) = watchdog_handle {
        let _ = h.join();
    }

    let mut total_ops: u64 = 0;
    for h in handles {
        total_ops = total_ops.wrapping_add(h.join().unwrap_or(0));
    }

    total_ops
}
