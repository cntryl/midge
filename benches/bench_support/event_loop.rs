//! Benchmark-only event-loop support types.

/// Tiny benchmark message kinds used for dispatch/match overhead measurements.
#[derive(Clone, Copy, Debug)]
pub enum MessageKind {
    Noop,
    StartupPing,
    GetRuntimeConfig,
}

/// Benchmark handler with near-zero work.
#[inline(never)]
pub fn handle(counter: &mut u64) {
    *counter = counter.wrapping_add(1);
}
