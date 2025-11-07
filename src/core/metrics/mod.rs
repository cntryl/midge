//! Performance and operational metrics for Midge engine
//!
//! This module provides comprehensive metrics collection for monitoring
//! database performance, resource usage, and operational health.
//!
//! # Architecture
//!
//! The metrics system is divided into two main components:
//!
//! - [`PerformanceMetrics`]: Component-level metrics organized by subsystem
//!   (WAL, memtable, SST, compaction, cache). Each subsystem has its own
//!   dedicated metrics struct with relevant counters and gauges.
//!
//! - [`Metrics`]: Engine-level metrics tracking high-level operations
//!   (gets, puts, deletes), snapshots, tombstones, errors, and autotuning.
//!
//! # Usage
//!
//! ```rust,ignore
//! use midge::core::metrics::{PerformanceMetrics, Metrics};
//!
//! // Component-level metrics
//! let perf = PerformanceMetrics::new();
//! perf.wal.record_write(1024);
//! perf.cache.record_hit();
//!
//! // Engine-level metrics
//! let metrics = Metrics::new();
//! metrics.record_get();
//! metrics.record_put();
//! ```

pub mod engine;
pub mod performance;
pub mod timer;

// Re-export primary types
pub use engine::{Metrics, MetricsSnapshot};
pub use performance::{
    CacheMetrics, CompactionMetrics, MemtableMetrics, PerformanceMetrics, SstMetrics, WalMetrics,
};
pub use timer::Timer;

use once_cell::sync::OnceCell;

static GLOBAL_PERF: OnceCell<PerformanceMetrics> = OnceCell::new();

/// Get a global PerformanceMetrics instance. This creates a default instance
/// on first use. Tests or higher-level components may choose to construct and
/// manage their own `PerformanceMetrics` instances instead of relying on the
/// global singleton.
pub fn global_performance_metrics() -> &'static PerformanceMetrics {
    GLOBAL_PERF.get_or_init(PerformanceMetrics::new)
}
