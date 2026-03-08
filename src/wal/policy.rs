//! WAL durability policies.
//!
//! This module defines *how the WAL achieves durability* (e.g., per-write fsync vs batched fsync).
//!
//! Important: this is **not** an API-level acknowledgment policy.
//! Acknowledgment answers “when does `put()` return to the caller?”, while durability answers
//! “when is the write guaranteed durable?”. Those are related but distinct concerns.
//!
//! Policies are pure strategy definitions—they contain no I/O logic.
//! Invariants and implementation details are enforced via unit tests.

/// Durability policy for WAL operations.
///
/// Determines when the WAL actor performs `sync()`/fsync on the underlying writer
/// and whether cloud persistence participates in the durability guarantee.
///
/// This enum intentionally does **not** define when a write is *acknowledged* to a caller.
/// If a caller-visible API waits for local/cloud durability before returning, that behavior
/// must be defined by a separate acknowledgment/commit policy at the API boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DurabilityPolicy {
    /// Fsync after every single write operation.
    /// Maximum durability, highest latency.
    /// Use for: financial transactions, critical metadata.
    Strict,

    /// Batch writes and fsync periodically (e.g., every 100ms or 64KB).
    /// Balanced durability/performance tradeoff.
    /// Use for: most production workloads.
    #[default]
    Batched,

    /// Write to local WAL + async persist to cloud.
    /// Durability = local fsync (cloud upload is background optimization).
    /// Use for: cloud-native deployments with geo-persistence.
    CloudMirrored,

    /// Write to local WAL, make the write visible after the local append barrier,
    /// and upload sealed WAL segments to cloud asynchronously.
    /// Durability = local append visibility now, cloud durability later via
    /// `cloud_durable_seq`.
    /// Use for: cloud-backed deployments that want ordinary commits to remain
    /// low-latency while allowing callers to opt into `cloud_strict()` when they
    /// must wait for cloud durability explicitly.
    CloudAsync,

    /// Best-effort persistence - write directly to memtable only.
    /// Behavior: data visible for reads and flushes to SST, but NO durability guarantee on crash before flush.
    /// Use case: bulk loads, initialization, test data where re-load is acceptable.
    /// Safe pattern: load with best_effort() → flush_cf() → switch to buffered() or sync() for measured workload.
    /// WARNING: data loss on crash before flush_cf() is called.
    BestEffort,
}

/// Configuration for batched sync policy.
#[derive(Debug, Clone, Copy)]
pub struct BatchConfig {
    /// Maximum time between fsyncs (milliseconds)
    pub max_delay_ms: u64,
    /// Maximum bytes buffered before fsync
    pub max_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_delay_ms: 100,    // 100ms
            max_bytes: 64 * 1024, // 64KB
        }
    }
}
