//! Test helpers for backpressure and memory-related tests

use crate::testkit::MidgeOptions;
use crate::Engine;

/// Open an engine with a tiny memory budget (bytes) and return (engine, `cf_id`)
#[must_use]
pub fn open_engine_with_memory_budget_bytes(opts: MidgeOptions, bytes: usize) -> Engine {
    let mut opts = opts;
    opts = opts.memory_budget(bytes);
    Engine::open_with_options(opts).expect("open engine")
}

// More helpers can be added as needed
