//! WAL durability policies
//!
//! This module defines durability policies used by the WAL actor.
//! These are *pure strategy definitions* — they contain NO I/O logic.
//!
//! Architectural rules:
//! - Policies are chosen by the runtime based on user configuration
//! - The WalActor implements policy behavior via message handling
//! - FsWalWriter remains policy-agnostic (just writes bytes)
//! - CloudActor handles replication (not WalActor)
//!
//! COPILOT: CLOUD DURABILITY REQUIREMENT
//!
//! Implement cloud-first durability exactly as follows:
//! 
//! 1. The WAL writer is ALWAYS local-only (FsWalWriter). Do NOT create new WAL
//!    backends. Do NOT embed cloud logic in the writer.
//!
//! 2. WalActor owns ALL durability semantics. It must:
//!      • assign global sequence numbers
//!      • append to FsWalWriter
//!      • rotate segments {segment_id}.wal
//!      • issue CloudUpload requests to CloudActor
//!      • track two durability frontiers:
//!            - local_durable_seq
//!            - cloud_durable_seq
//!
//! 3. Cloud-first durability rule:
//!      A write is considered DURABLE only after cloud ack IF the mode is
//!      Durability::CloudFirst. Otherwise, fallback to Steady/Batched or Strict.
//! 
//! 4. CloudActor is responsible for:
//!      • uploading WAL segments to cloud storage
//!      • retrying on failure (no blocking writes)
//!      • reporting CloudUploadComplete(request_id, segment_id)
//!
//! 5. WalActor must update cloud_durable_seq when CloudUploadComplete arrives.
//!    After updating, respond to any pending requests waiting on cloud durability.
//!
//! 6. On WAL append:
//!      • always append locally
//!      • schedule upload for the active segment
//!      • if mode == CloudFirst:
//!              do NOT return Ok until cloud_durable_seq >= record.sequence
//!        else if mode == Strict:
//!              fsync before returning
//!        else if mode == Steady/Batched:
//!              batch sync but return immediately
//!
//! 7. On startup recovery:
//!      • recovery returns RecoveryStats { max_sequence, wal_segments }
//!      • engine must restore the global sequence counter from stats.max_sequence
//!      • recovery MUST replay cloud segments first, then local segments
//!
//! 8. DO NOT invent new channels or readers.
//!    All request/response routing uses RuntimeMsg + ResponseRouter.
//!
//! 9. DO NOT block the event loop while waiting for cloud durability.
//!    Instead:
//!      • store pending request_ids in a queue
//!      • complete them asynchronously when cloud_durable_seq advances.
//!
//! Implement modifications ONLY inside WalActor and CloudActor.
//! No new WAL writer types.

/// Durability policy for WAL operations.
///
/// This determines when the WalActor calls fsync() on the underlying writer
/// and whether cloud replication is required for durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityPolicy {
    /// Fsync after every single write operation.
    /// Maximum durability, highest latency.
    /// Use for: financial transactions, critical metadata.
    Strict,

    /// Batch writes and fsync periodically (e.g., every 100ms or 64KB).
    /// Balanced durability/performance tradeoff.
    /// Use for: most production workloads.
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

impl Default for DurabilityPolicy {
    fn default() -> Self {
        Self::Batched
    }
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
            max_delay_ms: 100,  // 100ms
            max_bytes: 64 * 1024, // 64KB
        }
    }
}
