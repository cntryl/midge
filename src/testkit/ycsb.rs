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
use crate::{ColumnFamily, MidgeEngine};

use super::MidgeOptions;

pub const KEY_SIZE: usize = 16;
pub const VALUE_SIZE: usize = 128;

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

pub fn make_key(id: u64) -> [u8; KEY_SIZE] {
    let mut k = [0u8; KEY_SIZE];
    k[..8].copy_from_slice(&id.to_be_bytes());
    k
}

pub fn make_value(fill: u8) -> [u8; VALUE_SIZE] {
    [fill; VALUE_SIZE]
}

pub fn open_tier4_engine(mut opts: MidgeOptions) -> MidgeEngine {
    // Tier-4 workloads should exercise the full system shape.
    opts.enable_compaction = true;
    // Avoid tiny testkit memtables causing constant flush.
    opts.memtable_size = TIER4_MEMTABLE_SIZE_BYTES;

    MidgeEngine::open_with_options(opts).expect("open tier4 engine")
}

pub fn load_initial_dataset(engine: &MidgeEngine, cf: &ColumnFamily, initial_keys: usize) {
    // Load is not measured; optimize aggressively to keep Tier-4 runs practical.
    // Use transactions with batched commits to amortize WAL overhead.
    const BATCH_OPS: usize = 1024;

    let cf_id = cf.id();
    let mut tx = engine
        .begin_tx(cf_id, api::TransactionMode::ReadWrite)
        .expect("begin_tx failed");
    let mut count = 0;

    for i in 0..initial_keys as u64 {
        let k = make_key(i);
        let v = make_value((i as usize % 251) as u8);

        tx.put(k.to_vec(), v.to_vec(), None)
            .expect("put failed");
        count += 1;

        if count >= BATCH_OPS {
            engine
                .commit(tx, api::WriteOptions::default())
                .expect("commit failed");
            tx = engine
                .begin_tx(cf_id, api::TransactionMode::ReadWrite)
                .expect("begin_tx failed");
            count = 0;
        }
    }

    if count > 0 {
        engine
            .commit(tx, api::WriteOptions::default())
            .expect("commit failed");
    }

    engine.flush().expect("load phase flush");
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
    MakeClient: Fn(usize) -> Step,
    Step: FnMut(&MidgeEngine, &ColumnFamily, u64) + Send + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);

    for client_id in 0..clients {
        let engine = Arc::clone(&engine);
        let stop = Arc::clone(&stop);
        let barrier = Arc::clone(&barrier);
        let mut step = make_client(client_id);

        handles.push(thread::spawn(move || {
            let cf: &ColumnFamily = engine.default_column_family();

            // Start all clients together to reduce launch skew.
            barrier.wait();

            let mut op_index: u64 = 0;
            while !stop.load(Ordering::Acquire) {
                step(&engine, cf, op_index);
                op_index = op_index.wrapping_add(1);
            }
            op_index
        }));
    }

    // Release all clients at the same time, then start the measurement window.
    barrier.wait();
    thread::sleep(duration);
    stop.store(true, Ordering::Release);

    let mut total_ops: u64 = 0;
    for h in handles {
        total_ops = total_ops.wrapping_add(h.join().unwrap_or(0));
    }

    total_ops
}
