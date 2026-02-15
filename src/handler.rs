//! Minimal handler used by benchmark suites.

/// Benchmark handler with near-zero work.
#[inline]
pub fn handle(counter: &mut u64) {
    *counter = counter.wrapping_add(1);
}
