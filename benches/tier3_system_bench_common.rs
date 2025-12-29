//! Common utilities for benchmarks across all tiers.
//!
//! This module provides:
//! - Storage mode configurations for testing Memory, LocalDisk, and CloudBacked
//! - Unique path generation for isolated benchmark directories
//! - Precomputed key/value generation (no allocations in hot loops)
//! - Standard constants for consistent benchmarking
//!
//! ## Usage
//!
//! Include in benchmark files via:
//! ```ignore
//! #[path = "../bench_common.rs"]
//! mod bench_common;
//! use bench_common::*;
//! ```

#[allow(unused_imports)]
pub use cntryl_midge::testkit::bench::*;

#[allow(unused_imports)]
pub use cntryl_midge::testkit::kv::{
    make_key, make_value_fixed, precompute_kv, precompute_read_indices, KEY_SIZE,
};

// ============================================================================
// Constants
// ============================================================================

/// Default value size for benchmarks
#[allow(dead_code)]
pub const VALUE_SIZE: usize = 128;

/// Bytes per operation (key + value)
#[allow(dead_code)]
pub const BYTES_PER_OP: u64 = (KEY_SIZE + VALUE_SIZE) as u64;

/// Default memtable size for benchmarks (4MB)
#[allow(dead_code)]
pub const BENCH_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

/// Whether to use BenchFast mode for cloud backends (no filesystem IO).
/// Set to false to test with real filesystem for debugging.
#[allow(dead_code)]
pub const USE_BENCH_FAST_CLOUD: bool = true;

// ============================================================================
// Benchmark Group Helpers
// ============================================================================

/// Helper macro for iterating over storage modes in benchmarks.
///
/// Usage:
/// ```ignore
/// for_each_storage_mode!(group, FAST_STORAGE_MODES, |mode| {
///     group.bench_with_input(
///         BenchmarkId::new("operation", mode),
///         &mode,
///         |b, &mode| { ... }
///     );
/// });
/// ```
#[macro_export]
macro_rules! for_each_storage_mode {
    ($modes:expr, $closure:expr) => {
        for mode in $modes {
            $closure(mode);
        }
    };
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    // NOTE: Non-Criterion harness helpers (seed/restore/single-shot) live under
    // `cntryl_midge::testkit::bench` and are tested in the library.
}
