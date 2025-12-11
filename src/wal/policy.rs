//! WAL durability policies.
//!
//! Policies are pure strategy definitions—they contain no I/O logic.
//! Invariants and implementation details are enforced via unit tests.

/// Durability policy for WAL operations.
///
/// This determines when the WalActor calls fsync() on the underlying writer
/// and whether cloud replication is required for durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Default)]
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

    /// Write to local WAL + async replicate to cloud.
    /// Durability = local fsync (cloud upload is background optimization).
    /// Use for: cloud-native deployments with geo-replication.
    CloudMirrored,

    /// Write to local WAL + WAIT for cloud acknowledgment.
    /// Durability = cloud upload complete (local is just a cache).
    /// Use for: true cloud-first deployments where local disk is ephemeral.
    /// WalActor tracks cloud_durable_seq and blocks responses until cloud confirms.
    CloudFirst,
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
