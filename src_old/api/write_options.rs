//! Write options for controlling durability and sync behavior.
//!
//! Provides per-transaction control over WAL sync behavior, allowing applications
//! to make fine-grained durability vs. performance tradeoffs.

/// Options for controlling write behavior at transaction commit time.
///
/// This allows per-transaction control over durability guarantees. Some transactions
/// (e.g., financial ledgers, critical state) require immediate fsync for durability,
/// while others (e.g., caches, metrics) can accept eventual consistency for better
/// performance.
///
/// # Examples
///
/// ```rust
/// use cntryl_midge::WriteOptions;
///
/// // Critical transaction - sync immediately
/// let sync_opts = WriteOptions { sync: true };
///
/// // Non-critical transaction - amortize sync cost
/// let async_opts = WriteOptions { sync: false };
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriteOptions {
    /// If true, call fsync after writing to WAL before returning from commit.
    ///
    /// **sync: true** - Guarantees durability (survives process crash and power loss)
    /// but incurs fsync latency (~1-10ms depending on storage).
    ///
    /// **sync: false** - Faster commits but data may be lost if process crashes
    /// before the OS flushes buffers to disk. Still recoverable from WAL if only
    /// the process crashes (not power loss).
    ///
    /// Default: `false` (matches database-level `wal_sync` setting)
    pub sync: bool,
}

// Default is derived

impl WriteOptions {
    /// Create write options with sync enabled (strict durability).
    pub fn sync() -> Self {
        Self { sync: true }
    }

    /// Create write options with sync disabled (performance over durability).
    pub fn no_sync() -> Self {
        Self { sync: false }
    }
}
