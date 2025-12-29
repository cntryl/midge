//! Helpers for Tier-4 (YCSB) stress workloads.
//!
//! These helpers are intentionally deterministic and "boring": Tier-4 aims to
//! measure steady-state behavior over time (load → warm-up → measured).

use std::time::{Duration, Instant};

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

pub fn load_initial_dataset(
    engine: &MidgeEngine,
    cf: &ColumnFamily,
    initial_keys: usize,
) {
    for i in 0..initial_keys as u64 {
        let k = make_key(i);
        let v = make_value((i as usize % 251) as u8);
        engine
            .put(cf, &k[..], &v[..])
            .expect("load phase put");
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
