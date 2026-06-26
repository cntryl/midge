//! Runtime - Actor-based background task execution
//!
//! Deterministic actor framework for compaction, flushing, WAL, cloud ops, GC, and manifest.
//! All engine state mutations flow through actors via message passing.
//!
//! # Architecture
//!
//! - **EventLoop**: Receives messages and dispatches to actors
//! - **State**: Centralized mutable state owned by runtime
//! - **Actors**: Stateless handlers that process messages and return state updates
//! - **Actors**: Stateless handlers that process messages and return state updates

pub mod actors;
pub mod durability;
pub mod event_loop;
pub mod intent_persistence;
pub mod read_snapshot;
pub mod snapshot_cache;
pub mod state;

pub use event_loop::EventLoop;
pub use intent_persistence::IntentPersistence;
pub use read_snapshot::ReadSnapshot;

pub use state::RuntimeState;

use crate::common::{MidgeError, MidgeResult};
use crate::wal::policy::BatchConfig;
use crate::wal::DurabilityPolicy;
use bytes::Bytes;
use crossbeam::channel::{self, Receiver, Sender};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct CloudWalSealPolicy {
    pub min_segment_bytes: usize,
    pub max_flush_delay: Duration,
    pub max_pending_writes: usize,
}

impl Default for CloudWalSealPolicy {
    fn default() -> Self {
        Self {
            min_segment_bytes: 16 * 1024 * 1024,
            max_flush_delay: Duration::from_millis(500),
            max_pending_writes: 10_000,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CloudRuntimePolicy {
    pub eventual_flush_segment_gap: u64,
    pub wal_seal: CloudWalSealPolicy,
}

impl Default for CloudRuntimePolicy {
    fn default() -> Self {
        Self {
            eventual_flush_segment_gap: 128,
            wal_seal: CloudWalSealPolicy::default(),
        }
    }
}

/// Runtime configuration for wiring durability/storage behavior.
#[derive(Clone)]
pub struct RuntimeConfig {
    pub wal_durability_policy: DurabilityPolicy,
    pub wal_batch_config: BatchConfig,
    pub cloud_runtime_policy: CloudRuntimePolicy,
    pub hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub hybrid_storage_events: Option<crossbeam::channel::Receiver<crate::storage::StorageEvent>>,
    pub cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    pub compression_policy: crate::sst::compression::CompressionPolicy,
    /// Fencing epoch from leader election.  Stamped on every WAL record.
    pub writer_epoch: u64,
    /// Shared lease-health flag.  Set to `false` by the heartbeat thread when
    /// renewal fails.  The event loop checks this before accepting writes.
    pub lease_healthy: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// Leader store for epoch validation at WAL sync boundaries.
    /// Injected into the WAL actor so it can verify the epoch is still
    /// valid before flushing to durable storage.
    pub leader_store: Option<Arc<dyn crate::lease::LeaderStore>>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            wal_durability_policy: DurabilityPolicy::Batched,
            wal_batch_config: BatchConfig::default(),
            cloud_runtime_policy: CloudRuntimePolicy::default(),
            hybrid_storage: None,
            hybrid_storage_events: None,
            cloud_metadata_storage: None,
            compression_policy: crate::sst::compression::CompressionPolicy::default(),
            writer_epoch: 0,
            lease_healthy: None,
            leader_store: None,
        }
    }
}

/// Global request ID counter for routing responses to correct requesters.
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a new, globally unique request ID.
///
/// Callers should always use this when constructing `RuntimeMsg` values
/// that expect a response.
///
/// Uses `Relaxed` ordering because request IDs only need uniqueness,
/// not cross-thread ordering guarantees.
/// Returns an error if the counter would wrap (ID space exhausted).
pub(crate) fn next_request_id() -> MidgeResult<u64> {
    let id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    if id == 0 {
        // Wrapped; reset counter and refuse to hand out duplicate IDs.
        NEXT_REQUEST_ID.store(1, Ordering::Relaxed);
        return Err(MidgeError::Internal(
            "request ID space exhausted (u64 wrap)".into(),
        ));
    }
    Ok(id)
}

use serde::{Deserialize, Serialize};

/// Crash-recovery phase marker for publish workflows.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PublicationPhase {
    /// Output files are durable, but the manifest journal has not been made authoritative yet.
    OutputDurable,
    /// The manifest journal now reflects the new state; replay may finalize cleanup idempotently.
    ManifestPublished,
}

/// Simplified compaction plan for message passing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionPlan {
    pub input_files: Vec<String>,
    pub source_level: u32,
    pub target_level: u32,
    pub cf_id: crate::engine::ColumnFamilyId,
}

/// Simplified file metadata for message passing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub level: u32,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_crc32c: Option<u32>,
    pub cf_id: crate::engine::ColumnFamilyId,
    pub smallest_key: Option<Vec<u8>>,
    pub largest_key: Option<Vec<u8>>,
    pub smallest_seq: Option<u64>,
    pub largest_seq: Option<u64>,
}

/// A single operation within an atomic transaction apply.
///
/// This type lives in the runtime layer so higher layers can submit a
/// transaction without depending on engine API types.
///
/// Uses `Bytes` for zero-copy transfer through the write pipeline:
/// API → ingest coordinator → event loop → WAL actor → memtable.
#[derive(Debug, Clone)]
pub enum TransactionOp {
    Put {
        cf_id: crate::engine::ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl_seconds: Option<u64>,
        insert_only: bool,
    },
    Delete {
        cf_id: crate::engine::ColumnFamilyId,
        key: Bytes,
    },
    DeleteRange {
        cf_id: crate::engine::ColumnFamilyId,
        start_key: Bytes,
        end_key: Bytes,
    },
}

/// Runtime conflict policy used when applying transaction batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionIsolationPolicy {
    /// Preserve current behavior: last commit wins for overlapping writes.
    LastWriteWins,
    /// Abort when a write-set key changed after transaction start sequence.
    AbortOnWriteConflict,
}

/// Intent log entry - records all state transitions for deterministic replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntentLogEntry {
    /// Seqno allocated
    SeqnoAllocated {
        seqno: u64,
        cf_id: crate::engine::ColumnFamilyId,
    },
    /// Flush plan created
    FlushPlanned {
        cf_id: crate::engine::ColumnFamilyId,
        seqno_range: (u64, u64),
    },
    /// Compaction plan created
    CompactionPlanned {
        input_files: Vec<String>,
        output_level: u32,
    },
    /// Flush output SST is durable and awaiting publication cleanup.
    FlushPublish {
        phase: PublicationPhase,
        cf_id: crate::engine::ColumnFamilyId,
        sequence: u64,
        file_meta: FileMeta,
    },
    /// Compaction output SSTs are durable and awaiting publication cleanup.
    CompactionPublish {
        phase: PublicationPhase,
        cf_id: crate::engine::ColumnFamilyId,
        removed: Vec<String>,
        added: Vec<FileMeta>,
    },
    /// Manifest updated with new SST
    SstAdded { file_meta: FileMeta },
    /// Manifest updated after compaction
    CompactionApplied {
        removed: Vec<String>,
        added: Vec<FileMeta>,
    },
    /// WAL segment synced
    WalSynced { segment_id: u64, seqno: u64 },
    /// Data uploaded to cloud
    CloudUploadComplete { resource: String, seqno: u64 },
}

/// Messages that can be sent to the runtime.
///
/// Copilot: each variant that expects a response MUST carry a `request_id: u64`.
#[derive(Debug)]
pub enum RuntimeMsg {
    // === Flush Actor ===
    /// Request memtable flush for a column family.
    FlushMemtable {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
    },
    /// Memtable flush completed.
    FlushComplete {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        sst_name: String,
        sequence: u64,
    },

    // === Compaction Actor ===
    /// Trigger compaction check.
    CheckCompaction { request_id: u64 },
    /// Execute a specific compaction plan.
    RunCompaction {
        request_id: u64,
        plan: CompactionPlan,
    },
    /// Compaction completed.
    CompactionComplete {
        request_id: u64,
        input_ssts: Vec<String>,
        output_ssts: Vec<String>,
        cf_id: crate::engine::ColumnFamilyId,
        target_level: u32,
        succeeded: bool,
    },

    // === WAL Actor ===
    /// Append record to WAL.
    WalAppend {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        ttl_seconds: Option<u64>, // TTL in seconds, None means no expiration
        insert_only: bool,        // When true, fail if key already exists
    },

    /// Append delete range tombstone to WAL.
    WalAppendDeleteRange {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        durability_policy: Option<DurabilityPolicy>,
    },

    /// Apply a transaction as a single atomic unit.
    ///
    /// Sequence numbers are allocated in-order inside the runtime.
    /// The response returns the last allocated sequence for the transaction.
    /// The durability_policy parameter allows per-request durability control
    /// (e.g., BestEffort for bulk loads to skip WAL writes entirely).
    ApplyTransaction {
        request_id: u64,
        ops: Vec<TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: Option<u64>,
        isolation_policy: TransactionIsolationPolicy,
    },
    /// Sync WAL to disk.
    WalSync { request_id: u64 },
    /// Rotate WAL segment.
    WalRotate { request_id: u64 },
    /// Force-seal the current WAL segment for cloud upload and optionally wait for cloud durability.
    SealWalForCloud {
        request_id: u64,
        sequence: u64,
        wait_for_ack: bool,
    },
    /// WAL sync completed.
    WalSyncComplete { request_id: u64, segment_id: u64 },

    // === Cloud Actor ===
    /// Upload SST to cloud.
    CloudUploadSst { request_id: u64, sst_name: String },
    /// Upload WAL segment to cloud.
    CloudUploadWal { request_id: u64, segment_id: u64 },
    /// Cloud upload completed.
    CloudUploadComplete { request_id: u64, resource: String },

    // === GC Actor ===
    /// Check for garbage collection opportunities.
    CheckGc { request_id: u64 },
    /// Delete obsolete SST files.
    DeleteObsoleteSsts {
        request_id: u64,
        sst_names: Vec<String>,
    },

    // === Manifest Actor ===
    /// Update manifest with new SST.
    ManifestAddSst {
        request_id: u64,
        file_meta: FileMeta,
    },
    /// Update manifest after compaction.
    ManifestCompactionComplete {
        request_id: u64,
        removed: Vec<String>,
        added: Vec<FileMeta>,
    },
    /// Persist manifest to disk.
    ManifestPersist { request_id: u64 },

    // === Column Family Lifecycle ===
    /// Create a new column family.
    ManifestCreateColumnFamily { request_id: u64, name: String },
    /// Drop a column family (soft delete).
    ManifestDropColumnFamily {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
    },

    /// Begin an ingest barrier: prevent new compactions, bump ingest epoch,
    /// and wait until in-flight compactions drain.
    BeginIngest { request_id: u64 },

    /// End an ingest barrier: flush outstanding memtables, bump epoch and
    /// re-enable scheduling.
    EndIngest { request_id: u64 },

    /// Query whether an ingest barrier is currently active.
    GetIngestState { request_id: u64 },

    /// Set runtime configuration atomically. Any field set to `None` will be left unchanged.
    SetRuntimeConfig {
        request_id: u64,
        memtable_size_limit: Option<usize>,
        memtable_flush_threshold: Option<usize>,
        enable_compaction: Option<bool>,
        l0_compaction_trigger: Option<usize>,
        wal_durability_policy: Option<DurabilityPolicy>,
        wal_batch_config: Option<crate::wal::policy::BatchConfig>,
    },

    /// Get runtime configuration snapshot
    GetRuntimeConfig { request_id: u64 },

    // === Read Path ===
    /// Query a value from memtables and SST files.
    ///
    /// INVARIANT: Reads must respect the durability frontier.
    /// If requested_durability is Strict/Steady, the read must not return data
    /// with seqno > local_durable_seq. Reads at higher seqnos are queued in
    /// durability_waiters until the frontier advances.
    Read {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64, // Read at this sequence number or earlier.
        requested_durability: crate::engine::api::Durability, // Durability level requested
    },
    /// Scan a range of keys from memtables and SST files.
    ///
    /// INVARIANT: Range scans must respect the durability frontier.
    /// Same semantics as Read: if requested_durability is Strict/Steady,
    /// the scan must not return data with seqno > local_durable_seq.
    RangeScan {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        start: Vec<u8>,
        end: Vec<u8>,
        sequence: u64, // Read at this sequence number or earlier.
        requested_durability: crate::engine::api::Durability, // Durability level requested
    },

    // === Observability ===
    /// Get read amplification metrics snapshot.
    GetReadAmpMetrics { request_id: u64 },

    /// Get startup recovery metrics snapshot.
    GetRecoveryMetrics { request_id: u64 },

    /// Get a stable runtime metrics snapshot for operators.
    GetRuntimeMetrics { request_id: u64 },

    /// Get a stable storage layout snapshot for operators.
    GetStorageLayout { request_id: u64 },

    // === Sequencing ===
    /// Get the runtime's authoritative current sequence number.
    ///
    /// This is the sequence maintained by the runtime state and advanced at
    /// WAL append time.
    GetCurrentSequence { request_id: u64 },

    /// Capture an immutable read snapshot for transaction execution.
    ///
    /// Returns a snapshot containing references to current memtables and SST metadata,
    /// allowing transactions to execute reads directly without message passing.
    CaptureReadSnapshot {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
        sequence: u64,
    },

    /// Combined begin-transaction: atomically fetch current sequence AND capture
    /// a read snapshot in a single event-loop round-trip.
    ///
    /// Replaces the previous two-message pattern (GetCurrentSequence + CaptureReadSnapshot)
    /// to halve the message-passing overhead of `begin_tx`.
    BeginTransaction {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
    },

    /// Register a transaction snapshot so compaction/GC can respect active readers.
    RegisterSnapshot {
        request_id: u64,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    },

    /// Unregister a previously tracked transaction snapshot.
    ///
    /// Fire-and-forget cleanup used on transaction completion/drop.
    UnregisterSnapshot { snapshot_id: u64 },

    // === Control ===
    /// Shutdown the runtime (no request_id; fire-and-forget).
    Shutdown,
    /// Trigger a full compaction sweep and wait for completion.
    CompactAll { request_id: u64 },
    /// No-op for testing.
    Noop { request_id: u64 },
    /// Startup handshake to verify event loop is running.
    StartupPing { request_id: u64 },

    /// Check if writes should be stalled for a column family.
    /// Used by Engine::commit() to expose backpressure before accepting writes.
    CheckWriteStall {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
    },

    /// Block until writes are no longer stalled for `cf_id`.
    ///
    /// Responds immediately if not stalled; otherwise the response is held
    /// until a stall-clearing event occurs (e.g. flush completion).
    WaitForWriteStallClear {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
    },

    /// Best-effort cancel for a previous `WaitForWriteStallClear` request.
    ///
    /// Used by timeout/cancellation-aware callers (e.g. stress harness).
    /// Fire-and-forget.
    CancelWaitForWriteStallClear { wait_request_id: u64 },
}

impl RuntimeMsg {
    /// Extract the request_id for messages that expect a response.
    ///
    /// Returns `None` for messages that do not participate in request/response
    /// routing (e.g., `Shutdown`).
    pub fn request_id(&self) -> Option<u64> {
        use RuntimeMsg::*;
        match self {
            FlushMemtable { request_id, .. }
            | FlushComplete { request_id, .. }
            | CheckCompaction { request_id }
            | RunCompaction { request_id, .. }
            | CompactionComplete { request_id, .. }
            | WalAppend { request_id, .. }
            | WalAppendDeleteRange { request_id, .. }
            | ApplyTransaction { request_id, .. }
            | WalSync { request_id }
            | WalRotate { request_id }
            | SealWalForCloud { request_id, .. }
            | WalSyncComplete { request_id, .. }
            | CloudUploadSst { request_id, .. }
            | CloudUploadWal { request_id, .. }
            | CloudUploadComplete { request_id, .. }
            | CheckGc { request_id }
            | DeleteObsoleteSsts { request_id, .. }
            | ManifestAddSst { request_id, .. }
            | ManifestCompactionComplete { request_id, .. }
            | ManifestPersist { request_id }
            | ManifestCreateColumnFamily { request_id, .. }
            | ManifestDropColumnFamily { request_id, .. }
            | Read { request_id, .. }
            | RangeScan { request_id, .. }
            | GetReadAmpMetrics { request_id }
            | GetRecoveryMetrics { request_id }
            | GetRuntimeMetrics { request_id }
            | GetStorageLayout { request_id }
            | GetCurrentSequence { request_id }
            | CaptureReadSnapshot { request_id, .. }
            | BeginTransaction { request_id, .. }
            | RegisterSnapshot { request_id, .. }
            | SetRuntimeConfig { request_id, .. }
            | GetRuntimeConfig { request_id }
            | GetIngestState { request_id }
            | BeginIngest { request_id }
            | EndIngest { request_id }
            | CompactAll { request_id }
            | Noop { request_id }
            | StartupPing { request_id }
            | CheckWriteStall { request_id, .. }
            | WaitForWriteStallClear { request_id, .. } => Some(*request_id),

            CancelWaitForWriteStallClear { .. } | UnregisterSnapshot { .. } => None,

            Shutdown => None,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        use RuntimeMsg::*;
        match self {
            FlushMemtable { .. } => "FlushMemtable",
            FlushComplete { .. } => "FlushComplete",
            CheckCompaction { .. } => "CheckCompaction",
            RunCompaction { .. } => "RunCompaction",
            CompactionComplete { .. } => "CompactionComplete",
            WalAppend { .. } => "WalAppend",
            WalAppendDeleteRange { .. } => "WalAppendDeleteRange",
            ApplyTransaction { .. } => "ApplyTransaction",
            WalSync { .. } => "WalSync",
            WalRotate { .. } => "WalRotate",
            SealWalForCloud { .. } => "SealWalForCloud",
            WalSyncComplete { .. } => "WalSyncComplete",
            CloudUploadSst { .. } => "CloudUploadSst",
            CloudUploadWal { .. } => "CloudUploadWal",
            CloudUploadComplete { .. } => "CloudUploadComplete",
            CheckGc { .. } => "CheckGc",
            DeleteObsoleteSsts { .. } => "DeleteObsoleteSsts",
            ManifestAddSst { .. } => "ManifestAddSst",
            ManifestCompactionComplete { .. } => "ManifestCompactionComplete",
            ManifestPersist { .. } => "ManifestPersist",
            ManifestCreateColumnFamily { .. } => "ManifestCreateColumnFamily",
            ManifestDropColumnFamily { .. } => "ManifestDropColumnFamily",
            Read { .. } => "Read",
            RangeScan { .. } => "RangeScan",
            GetReadAmpMetrics { .. } => "GetReadAmpMetrics",
            GetRecoveryMetrics { .. } => "GetRecoveryMetrics",
            GetRuntimeMetrics { .. } => "GetRuntimeMetrics",
            GetStorageLayout { .. } => "GetStorageLayout",
            GetCurrentSequence { .. } => "GetCurrentSequence",
            CaptureReadSnapshot { .. } => "CaptureReadSnapshot",
            BeginTransaction { .. } => "BeginTransaction",
            RegisterSnapshot { .. } => "RegisterSnapshot",
            UnregisterSnapshot { .. } => "UnregisterSnapshot",
            SetRuntimeConfig { .. } => "SetRuntimeConfig",
            GetRuntimeConfig { .. } => "GetRuntimeConfig",
            GetIngestState { .. } => "GetIngestState",
            BeginIngest { .. } => "BeginIngest",
            EndIngest { .. } => "EndIngest",
            CompactAll { .. } => "CompactAll",
            Shutdown => "Shutdown",
            Noop { .. } => "Noop",
            CheckWriteStall { .. } => "CheckWriteStall",
            WaitForWriteStallClear { .. } => "WaitForWriteStallClear",
            CancelWaitForWriteStallClear { .. } => "CancelWaitForWriteStallClear",
            StartupPing { .. } => "StartupPing",
        }
    }
}

/// Response from runtime operations.
///
/// Copilot: every response variant MUST carry the originating request_id.
#[derive(Debug)]
pub enum RuntimeResponse {
    Ok {
        request_id: u64,
    },
    /// WAL write accepted and assigned a sequence number.
    ///
    /// Note: Sequence numbers are allocated inside the runtime at append time
    /// to preserve a total order under concurrency.
    WalAppended {
        request_id: u64,
        sequence: u64,
    },

    /// Transaction accepted and assigned a contiguous sequence range.
    ///
    /// `last_sequence` is the last (highest) sequence allocated for the transaction.
    /// `write_stall_hint` indicates if writes should be stalled (memory pressure).
    TransactionApplied {
        request_id: u64,
        last_sequence: u64,
        op_count: usize,
        write_stall_hint: bool,
    },
    Error {
        request_id: u64,
        error: crate::common::MidgeError,
    },
    ReadValue {
        request_id: u64,
        value: Option<Vec<u8>>,
    },
    RangeScanResults {
        request_id: u64,
        results: Vec<(Vec<u8>, Vec<u8>)>,
    },
    FlushComplete {
        request_id: u64,
        sst_name: String,
    },
    CompactionComplete {
        request_id: u64,
        output_ssts: Vec<String>,
    },
    ColumnFamilyCreated {
        request_id: u64,
        cf_id: crate::engine::ColumnFamilyId,
    },
    ReadAmpMetricsSnapshot {
        request_id: u64,
        reads_total: u64,
        ssts_touched_total: u64,
        l0_ssts_touched_total: u64,
        blocks_read_total: u64,
        avg_ssts_per_read: f64,
        avg_l0_ssts_per_read: f64,
        avg_blocks_per_read: f64,
        l0_overlap_rate: f64,
        sst_budget_violation_rate: f64,
        block_budget_violation_rate: f64,
    },

    RecoveryMetricsSnapshot {
        request_id: u64,
        wal_recovery_records_replayed: u64,
        wal_recovery_bytes_replayed: u64,
        intent_log_replay_runs: u64,
        intent_log_entries_replayed: u64,
    },

    /// Stable operator-facing runtime metrics snapshot.
    RuntimeMetricsSnapshot {
        request_id: u64,
        snapshot: Box<crate::engine::RuntimeMetricsSnapshot>,
    },

    /// Stable operator-facing storage layout snapshot.
    StorageLayoutSnapshot {
        request_id: u64,
        snapshot: crate::engine::StorageLayoutSnapshot,
    },

    /// Current authoritative runtime sequence.
    CurrentSequence {
        request_id: u64,
        sequence: u64,
    },

    /// Immutable read snapshot for direct transaction execution.
    ReadSnapshot {
        request_id: u64,
        snapshot: Arc<super::runtime::read_snapshot::ReadSnapshot>,
    },

    /// Combined response for BeginTransaction: sequence + snapshot in one round-trip.
    BeginTransactionResult {
        request_id: u64,
        /// Start sequence for the transaction (current committed sequence).
        start_sequence: u64,
        /// Immutable read snapshot (None if the CF doesn't exist).
        snapshot: Option<Arc<super::runtime::read_snapshot::ReadSnapshot>>,
    },

    /// Snapshot of runtime configuration for diagnostics and tooling
    RuntimeConfigSnapshot {
        request_id: u64,
        memtable_size_limit: usize,
        memtable_flush_threshold: usize,
        enable_compaction: bool,
        l0_compaction_trigger: usize,
        wal_durability_policy: DurabilityPolicy,
        wal_batch_config: crate::wal::policy::BatchConfig,
    },
    /// Simple ingest state response
    IngestState {
        request_id: u64,
        ingest_active: bool,
    },

    /// Write stall status response
    WriteStallStatus {
        request_id: u64,
        is_stalled: bool,
    },
}

impl RuntimeResponse {
    pub fn request_id(&self) -> u64 {
        match self {
            RuntimeResponse::Ok { request_id }
            | RuntimeResponse::WalAppended { request_id, .. }
            | RuntimeResponse::TransactionApplied { request_id, .. }
            | RuntimeResponse::Error { request_id, .. }
            | RuntimeResponse::ReadValue { request_id, .. }
            | RuntimeResponse::RangeScanResults { request_id, .. }
            | RuntimeResponse::FlushComplete { request_id, .. }
            | RuntimeResponse::CompactionComplete { request_id, .. }
            | RuntimeResponse::ColumnFamilyCreated { request_id, .. }
            | RuntimeResponse::ReadAmpMetricsSnapshot { request_id, .. }
            | RuntimeResponse::RecoveryMetricsSnapshot { request_id, .. }
            | RuntimeResponse::RuntimeMetricsSnapshot { request_id, .. }
            | RuntimeResponse::StorageLayoutSnapshot { request_id, .. }
            | RuntimeResponse::CurrentSequence { request_id, .. }
            | RuntimeResponse::ReadSnapshot { request_id, .. }
            | RuntimeResponse::BeginTransactionResult { request_id, .. }
            | RuntimeResponse::RuntimeConfigSnapshot { request_id, .. }
            | RuntimeResponse::IngestState { request_id, .. }
            | RuntimeResponse::WriteStallStatus { request_id, .. } => *request_id,
        }
    }
}

/// ResponseRouter - per-request routing using oneshot-style channels.
///
/// Uses `DashMap` for lock-free concurrent access, eliminating the
/// `Mutex<HashMap>` contention point between caller threads (register)
/// and the event loop thread (complete).
#[derive(Debug)]
pub(crate) struct ResponseRouter {
    pending: DashMap<u64, Sender<RuntimeResponse>>,
}

impl ResponseRouter {
    pub fn new() -> Self {
        Self {
            pending: DashMap::new(),
        }
    }

    /// Register a new pending response for a given request_id.
    ///
    /// Returns a receiver that will yield exactly one `RuntimeResponse`.
    pub fn register(&self, request_id: u64) -> Receiver<RuntimeResponse> {
        let (tx, rx) = channel::bounded(1);
        self.pending.insert(request_id, tx);
        rx
    }

    /// Complete a request by delivering its response to the waiting receiver.
    ///
    /// If no pending entry exists, logs a warning and drops the response.
    pub fn complete(&self, response: RuntimeResponse) {
        let request_id = response.request_id();
        if let Some((_, tx)) = self.pending.remove(&request_id) {
            let _ = tx.send(response);
        } else {
            tracing::warn!(
                request_id,
                "response received with no matching pending request"
            );
        }
    }

    /// Remove a pending request without delivering a response.
    ///
    /// Used by timeout-based waits so callers can abandon a request cleanly.
    pub fn unregister(&self, request_id: u64) {
        let _ = self.pending.remove(&request_id);
    }
}

/// Handle for submitting work to the runtime.
///
/// Copilot:
/// - Route responses by request_id using ResponseRouter.
/// - Use per-request channels (bounded(1)) created via ResponseRouter::register.
/// - Never use a single shared response_rx.
/// - RuntimeHandle MUST be thread-safe and support concurrent callers.
#[derive(Clone)]
pub struct RuntimeHandle {
    msg_tx: Sender<RuntimeMsg>,
    router: Arc<ResponseRouter>,
    /// Lock-free snapshot cache for read-path bypass.
    ///
    /// Allows `begin_tx` to capture a read snapshot without event loop round-trip.
    pub(crate) snapshot_cache: Arc<snapshot_cache::SnapshotCache>,
}

impl RuntimeHandle {
    /// Submit a message to the runtime (fire-and-forget).
    ///
    /// For messages that expect a response, prefer `send_and_wait`.
    pub fn send(&self, msg: RuntimeMsg) -> MidgeResult<()> {
        self.msg_tx
            .send(msg)
            .map_err(|_| MidgeError::Internal("Runtime channel closed".to_string()))
    }

    /// Register a response channel for a request_id.
    ///
    /// Intended for benchmarks to pre-register receivers and avoid
    /// per-iteration allocations in hot loops.
    pub fn register_response(&self, request_id: u64) -> Receiver<RuntimeResponse> {
        self.router.register(request_id)
    }

    /// Submit a message and wait synchronously for its response.
    ///
    /// The `RuntimeMsg` MUST carry a `request_id`. Use `next_request_id()` when
    /// constructing such messages.
    pub fn send_and_wait(&self, msg: RuntimeMsg) -> MidgeResult<RuntimeResponse> {
        let request_id = msg.request_id().ok_or_else(|| {
            MidgeError::Internal(
                "send_and_wait called with message that has no request_id (e.g. Shutdown)"
                    .to_string(),
            )
        })?;

        let msg_kind = msg.kind_name();

        // Register for the response before sending the request.
        let rx = self.router.register(request_id);

        self.msg_tx
            .send(msg)
            .map_err(|_| MidgeError::Internal("Runtime channel closed".to_string()))?;

        // Block waiting for the single response.
        // If debug-wait mode is enabled, emit a periodic warning to help
        // diagnose hangs (e.g., CloudAsync waiting on CloudAck).
        if std::env::var_os("MIDGE_DEBUG_WAIT").is_some() {
            let mut waited = std::time::Duration::from_secs(0);
            loop {
                match rx.recv_timeout(std::time::Duration::from_secs(2)) {
                    Ok(resp) => return Ok(resp),
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                        waited += std::time::Duration::from_secs(2);
                        eprintln!(
                            "[midge] still waiting for response: request_id={request_id} kind={msg_kind} waited={:?}",
                            waited
                        );
                    }
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        return Err(MidgeError::Internal("Response channel closed".to_string()));
                    }
                }
            }
        }

        rx.recv()
            .map_err(|_| MidgeError::Internal("Response channel closed".to_string()))
    }

    /// Submit a message and wait up to `timeout` for its response.
    ///
    /// Returns `Ok(None)` on timeout. In that case, the pending response slot
    /// is unregistered to avoid leaking the router map.
    pub fn send_and_wait_timeout(
        &self,
        msg: RuntimeMsg,
        timeout: std::time::Duration,
    ) -> MidgeResult<Option<RuntimeResponse>> {
        let request_id = msg.request_id().ok_or_else(|| {
            MidgeError::Internal(
                "send_and_wait_timeout called with message that has no request_id".to_string(),
            )
        })?;

        // Register for the response before sending the request.
        let rx = self.router.register(request_id);

        self.msg_tx
            .send(msg)
            .map_err(|_| MidgeError::Internal("Runtime channel closed".to_string()))?;

        match rx.recv_timeout(timeout) {
            Ok(resp) => Ok(Some(resp)),
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {
                self.router.unregister(request_id);
                Ok(None)
            }
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                Err(MidgeError::Internal("Response channel closed".to_string()))
            }
        }
    }

    /// Submit a message and wait for a response that matches a predicate.
    ///
    /// Since each request_id yields exactly one response, this is mainly
    /// useful for callers that want to validate the response shape.
    pub fn send_and_wait_filtered<F>(
        &self,
        msg: RuntimeMsg,
        mut predicate: F,
    ) -> MidgeResult<RuntimeResponse>
    where
        F: FnMut(&RuntimeResponse) -> bool,
    {
        let resp = self.send_and_wait(msg)?;
        if predicate(&resp) {
            Ok(resp)
        } else {
            Err(MidgeError::Internal(
                "Response did not satisfy predicate".to_string(),
            ))
        }
    }

    /// Request runtime shutdown (fire-and-forget).
    pub fn shutdown(&self) -> MidgeResult<()> {
        self.send(RuntimeMsg::Shutdown)
    }

    /// Check if writes should be stalled for the given column family.
    ///
    /// Returns `true` if memory budget is exceeded (immutable memtable queue full
    /// or total memtable memory over threshold).
    ///
    /// Used by Engine::commit() to expose backpressure to clients before
    /// accepting new write transactions.
    pub fn check_write_stall(&self, cf_id: crate::engine::ColumnFamilyId) -> MidgeResult<bool> {
        let response = self.send_and_wait(RuntimeMsg::CheckWriteStall {
            request_id: next_request_id()?,
            cf_id,
        })?;

        match response {
            RuntimeResponse::WriteStallStatus { is_stalled, .. } => Ok(is_stalled),
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to CheckWriteStall".to_string(),
            )),
        }
    }
}

/// Main runtime for background operations.
///
/// Owns all mutable engine state and coordinates actors via message passing.
/// All background work flows through this runtime and its event loop thread.
pub struct Runtime {
    /// Message channel sender (for handle).
    msg_tx: Sender<RuntimeMsg>,
    /// Message channel receiver (for event loop).
    msg_rx: Receiver<RuntimeMsg>,
    /// Event loop thread handle.
    event_loop_handle: Option<JoinHandle<()>>,
    /// Whether tracing is enabled.
    trace_enabled: bool,
    /// Response router shared between handle and event loop.
    router: Arc<ResponseRouter>,
}

impl Runtime {
    /// Create a new runtime and a corresponding handle for submitting work.
    pub fn new() -> MidgeResult<(Self, RuntimeHandle)> {
        let (msg_tx, msg_rx) = channel::bounded(1000);
        let router = Arc::new(ResponseRouter::new());

        let trace_enabled = std::env::var("MIDGE_TRACE_RUNTIME")
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);

        let snapshot_cache = Arc::new(snapshot_cache::SnapshotCache::new());

        let handle = RuntimeHandle {
            msg_tx: msg_tx.clone(),
            router: router.clone(),
            snapshot_cache,
        };

        let runtime = Self {
            msg_tx,
            msg_rx,
            event_loop_handle: None,
            trace_enabled,
            router,
        };

        Ok((runtime, handle))
    }

    /// Start the runtime event loop in a background thread.
    ///
    /// Returns the Runtime (which owns the thread) and a handle for submitting work.
    pub fn start(self, state: RuntimeState) -> MidgeResult<(Self, RuntimeHandle)> {
        self.start_with_config(state, RuntimeConfig::default())
    }

    /// Start the runtime event loop with explicit configuration.
    ///
    /// Returns the Runtime (which owns the thread) and a handle for submitting work.
    pub fn start_with_config(
        mut self,
        state: RuntimeState,
        config: RuntimeConfig,
    ) -> MidgeResult<(Self, RuntimeHandle)> {
        let trace_enabled = self.trace_enabled;
        let router = self.router.clone();

        // Channel to signal successful event loop initialization
        let (init_tx, init_rx) = channel::bounded::<Result<(), String>>(1);

        let snapshot_cache = Arc::new(snapshot_cache::SnapshotCache::new());

        // Handle for callers to use.
        let handle = RuntimeHandle {
            msg_tx: self.msg_tx.clone(),
            router: router.clone(),
            snapshot_cache: snapshot_cache.clone(),
        };

        let msg_tx_for_loop = self.msg_tx.clone();
        let msg_rx = std::mem::replace(&mut self.msg_rx, channel::bounded(1000).1);

        let event_loop_handle = thread::Builder::new()
            .name("midge-runtime".to_string())
            .spawn(move || {
                match EventLoop::new(state, trace_enabled, router, config, Some(msg_tx_for_loop)) {
                    Ok(mut event_loop) => {
                        // Share the snapshot cache with the event loop
                        event_loop.set_snapshot_cache(snapshot_cache);
                        // Signal successful initialization
                        let _ = init_tx.send(Ok(()));
                        event_loop.run(msg_rx);
                    }
                    Err(e) => {
                        let msg = format!("Failed to create event loop: {}", e);
                        tracing::error!("{}", msg);
                        // Signal initialization failure
                        let _ = init_tx.send(Err(msg));
                    }
                }
            })
            .map_err(|e| MidgeError::Internal(format!("Failed to spawn runtime thread: {}", e)))?;

        self.event_loop_handle = Some(event_loop_handle);

        // Wait for event loop initialization to complete
        match init_rx.recv() {
            Ok(Ok(())) => Ok((self, handle)),
            Ok(Err(e)) => Err(MidgeError::Internal(e)),
            Err(_) => Err(MidgeError::Internal(
                "Runtime initialization channel closed unexpectedly".to_string(),
            )),
        }
    }

    fn shutdown_inner(&mut self, context: &str) {
        if let Some(handle) = self.event_loop_handle.take() {
            if let Err(e) = self.msg_tx.send(RuntimeMsg::Shutdown) {
                tracing::debug!("Runtime {}: shutdown message send failed: {}", context, e);
            }

            match handle.join() {
                Ok(()) => {
                    tracing::debug!("Runtime {} completed cleanly", context);
                }
                Err(_) => {
                    tracing::warn!("Runtime thread panicked during {}", context);
                }
            }
        }
    }

    /// Shutdown the runtime and wait for completion.
    pub fn shutdown(mut self) {
        self.shutdown_inner("shutdown");
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        self.shutdown_inner("drop");
    }
}

// Note: Runtime does not implement Default because it returns (Runtime, RuntimeHandle).
// Use Runtime::new() directly.

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    // =========== next_request_id Tests ===========

    #[test]
    fn should_generate_unique_request_ids() {
        // Arrange
        // (no setup)

        // Act
        let id1 = next_request_id().expect("id");
        let id2 = next_request_id().expect("id");
        let id3 = next_request_id().expect("id");

        // Assert
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn should_increment_request_ids_monotonically() {
        // Arrange
        // (no setup)

        // Act
        let id1 = next_request_id().expect("id");
        let id2 = next_request_id().expect("id");
        let id3 = next_request_id().expect("id");

        // Assert
        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    #[test]
    fn should_allocate_request_ids_atomically_across_threads() {
        // Arrange
        let handles: Vec<_> = (0..5)
            .map(|_| {
                thread::spawn(|| {
                    let mut ids = vec![];
                    for _ in 0..20 {
                        ids.push(next_request_id().expect("id"));
                    }
                    ids
                })
            })
            .collect();

        // Act
        let all_ids: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // Assert - All IDs should be unique
        for i in 0..all_ids.len() {
            for j in (i + 1)..all_ids.len() {
                assert_ne!(all_ids[i], all_ids[j]);
            }
        }

        // Should have 100 IDs from 5 threads
        assert_eq!(all_ids.len(), 100);
    }

    #[test]
    fn should_start_from_nonzero() {
        // Arrange
        // (no setup)

        // Act
        let id = next_request_id().expect("id");

        // Assert - Should never be 0
        assert!(id > 0);
    }

    // =========== RuntimeMsg Tests ===========

    #[test]
    fn should_extract_request_id_from_message() {
        // Arrange
        let msg = RuntimeMsg::Noop { request_id: 42 };

        // Act
        let req_id = msg.request_id();

        // Assert
        assert_eq!(req_id, Some(42));
    }

    #[test]
    fn should_return_none_for_shutdown_message() {
        // Arrange
        let msg = RuntimeMsg::Shutdown;

        // Act
        let req_id = msg.request_id();

        // Assert
        assert_eq!(req_id, None);
    }

    #[test]
    fn should_extract_request_id_from_all_request_response_messages() {
        // Arrange
        // (no setup)

        // Act
        // (none)

        // Assert
        assert!(RuntimeMsg::FlushMemtable {
            request_id: 1,
            cf_id: 0
        }
        .request_id()
        .is_some());

        assert!(RuntimeMsg::CheckCompaction { request_id: 2 }
            .request_id()
            .is_some());
        assert!(RuntimeMsg::CompactAll { request_id: 3 }
            .request_id()
            .is_some());
        assert!(RuntimeMsg::CompactAll { request_id: 4 }.kind_name() == "CompactAll");

        assert!(RuntimeMsg::WalAppend {
            request_id: 3,
            cf_id: 0,
            key: vec![],
            value: None,
            ttl_seconds: None,
            insert_only: false
        }
        .request_id()
        .is_some());

        assert!(RuntimeMsg::CheckGc { request_id: 4 }.request_id().is_some());

        assert!(RuntimeMsg::Noop { request_id: 5 }.request_id().is_some());

        assert!(RuntimeMsg::StartupPing { request_id: 6 }
            .request_id()
            .is_some());
        // CompactAll should be a recognized message kind
        assert!(RuntimeMsg::CompactAll { request_id: 7 }
            .request_id()
            .is_some());
        assert!(RuntimeMsg::CompactAll { request_id: 8 }.kind_name() == "CompactAll");

        // Verify that CompactAll responds correctly when runtime is healthy
        let (runtime, _handle) = Runtime::new().expect("create runtime");
        let state = RuntimeState::new("/tmp/test_compact_all".into(), true);
        let (_runtime, h) = runtime
            .start_with_config(state, RuntimeConfig::default())
            .expect("start runtime");
        let resp = h
            .send_and_wait(RuntimeMsg::CompactAll {
                request_id: next_request_id().expect("id"),
            })
            .expect("compact_all call");
        match resp {
            RuntimeResponse::Ok { .. } => (),
            _ => panic!("CompactAll did not return Ok"),
        }

        // Signal runtime to shut down cleanly so Drop won't block waiting for thread join
        h.send(RuntimeMsg::Shutdown).expect("send shutdown");

        assert!(RuntimeMsg::GetCurrentSequence { request_id: 7 }
            .request_id()
            .is_some());
    }

    // =========== RuntimeResponse Tests ===========

    #[test]
    fn should_extract_request_id_from_response() {
        // Arrange
        let response = RuntimeResponse::Ok { request_id: 42 };

        // Act
        let req_id = response.request_id();

        // Assert
        assert_eq!(req_id, 42);
    }

    #[test]
    fn should_extract_request_id_from_all_responses() {
        // Arrange
        // (no setup)

        // Act
        // (none)

        // Assert
        assert_eq!(RuntimeResponse::Ok { request_id: 1 }.request_id(), 1);

        assert_eq!(
            RuntimeResponse::Error {
                request_id: 2,
                error: crate::common::MidgeError::Internal("error".to_string())
            }
            .request_id(),
            2
        );

        assert_eq!(
            RuntimeResponse::ReadValue {
                request_id: 3,
                value: None
            }
            .request_id(),
            3
        );

        assert_eq!(
            RuntimeResponse::RangeScanResults {
                request_id: 4,
                results: vec![]
            }
            .request_id(),
            4
        );

        assert_eq!(
            RuntimeResponse::FlushComplete {
                request_id: 5,
                sst_name: "sst".to_string()
            }
            .request_id(),
            5
        );

        assert_eq!(
            RuntimeResponse::CompactionComplete {
                request_id: 6,
                output_ssts: vec![]
            }
            .request_id(),
            6
        );

        assert_eq!(
            RuntimeResponse::ColumnFamilyCreated {
                request_id: 7,
                cf_id: 0
            }
            .request_id(),
            7
        );

        assert_eq!(
            RuntimeResponse::CurrentSequence {
                request_id: 8,
                sequence: 123
            }
            .request_id(),
            8
        );

        assert_eq!(
            RuntimeResponse::TransactionApplied {
                request_id: 9,
                last_sequence: 200,
                op_count: 2,
                write_stall_hint: false,
            }
            .request_id(),
            9
        );
    }

    // =========== ResponseRouter Tests ===========

    #[test]
    fn should_create_response_router() {
        // Arrange
        // (no setup)

        // Act
        let router = ResponseRouter::new();

        // Assert - Should be usable, register should return a receiver
        let rx = router.register(1);
        // Just verify we got a receiver back (doesn't block, nonblocking channel)
        drop(rx);
    }

    #[test]
    fn should_register_then_complete_response() {
        // Arrange
        let router = ResponseRouter::new();

        // Act - Register and deliver response
        let rx = router.register(42);
        router.complete(RuntimeResponse::Ok { request_id: 42 });

        // Assert - Should receive response
        let received = rx.recv().unwrap();
        assert_eq!(received.request_id(), 42);
    }

    #[test]
    fn should_handle_multiple_pending_requests() {
        // Arrange
        let router = Arc::new(ResponseRouter::new());
        let rx1 = router.register(1);
        let rx2 = router.register(2);
        let rx3 = router.register(3);

        // Act - Complete in different order
        router.complete(RuntimeResponse::Ok { request_id: 2 });
        router.complete(RuntimeResponse::Ok { request_id: 1 });
        router.complete(RuntimeResponse::Ok { request_id: 3 });

        // Assert - Should receive correct responses
        assert_eq!(rx1.recv().unwrap().request_id(), 1);
        assert_eq!(rx2.recv().unwrap().request_id(), 2);
        assert_eq!(rx3.recv().unwrap().request_id(), 3);
    }

    #[test]
    fn should_handle_orphaned_response() {
        // Arrange
        let router = ResponseRouter::new();

        // Act - Try to complete response with no matching request
        // (Should not panic, logs warning)
        router.complete(RuntimeResponse::Ok { request_id: 999 });

        // Assert - Should not crash
    }

    // =========== RuntimeHandle Tests ===========

    #[test]
    fn should_create_runtime_handle() {
        // Arrange
        // (no setup)

        // Act
        let (runtime, handle) = Runtime::new().expect("Should create runtime");

        // Assert
        // Handle should be cloneable
        let handle2 = handle.clone();
        drop(runtime);
        drop(handle2);
    }

    #[test]
    fn should_handle_send_noop_message() {
        // Arrange
        let (runtime, handle) = Runtime::new().expect("Should create runtime");
        let msg = RuntimeMsg::Noop { request_id: 1 };

        // Act
        let result = handle.send(msg);

        // Assert
        assert!(result.is_ok());
        drop(runtime);
    }

    #[test]
    fn should_detect_closed_channel_on_send() {
        // Arrange
        let (runtime, handle) = Runtime::new().expect("Should create runtime");

        // Act - Drop runtime to close channel
        drop(runtime);

        // Wait a moment for channel to close
        thread::sleep(std::time::Duration::from_millis(10));

        // Assert
        let result = handle.send(RuntimeMsg::Noop { request_id: 1 });
        assert!(result.is_err());
    }

    #[test]
    fn should_require_request_id_for_send_wait() {
        // Arrange
        let (runtime, handle) = Runtime::new().expect("Should create runtime");
        let msg = RuntimeMsg::Shutdown;

        // Act
        let result = handle.send_and_wait(msg);

        // Assert
        assert!(result.is_err());
        drop(runtime);
    }

    // =========== Runtime Tests ===========

    #[test]
    fn should_create_runtime() {
        // Arrange
        // (no setup)

        // Act
        let result = Runtime::new();

        // Assert
        assert!(result.is_ok());
        let (runtime, _handle) = result.unwrap();
        drop(runtime);
    }

    #[test]
    fn should_create_with_trace_disabled_by_default() {
        // Arrange
        // (no setup)

        // Act
        let (_runtime, _handle) = Runtime::new().expect("Should create runtime");

        // Assert - Default should have tracing disabled
        // (Verified by not panicking)
    }

    #[test]
    fn should_shutdown_runtime() {
        // Arrange
        let (runtime, _handle) = Runtime::new().expect("Should create runtime");

        // Act - Shutdown should not panic
        runtime.shutdown();

        // Assert - Just checking no panic
    }

    // =========== CompactionPlan Tests ===========

    #[test]
    fn should_create_compaction_plan() {
        // Arrange
        // (no setup)

        // Act
        let plan = CompactionPlan {
            input_files: vec![crate::sst::file_name(0, 0, 1)],
            source_level: 0,
            target_level: 1,
            cf_id: 0,
        };

        // Assert
        assert_eq!(plan.input_files.len(), 1);
        assert_eq!(plan.source_level, 0);
        assert_eq!(plan.target_level, 1);
        assert_eq!(plan.cf_id, 0);
    }

    #[test]
    fn should_clone_compaction_plan() {
        // Arrange
        let plan = CompactionPlan {
            input_files: vec!["sst.sst".to_string()],
            source_level: 0,
            target_level: 1,
            cf_id: 0,
        };

        // Act
        let cloned = plan.clone();

        // Assert
        assert_eq!(plan.input_files, cloned.input_files);
    }

    // =========== FileMeta Tests ===========

    #[test]
    fn should_create_file_meta() {
        // Arrange
        // (no setup)

        // Act
        let sst_name = crate::sst::file_name(0, 0, 1);
        let meta = FileMeta {
            name: sst_name.clone(),
            level: 0,
            size_bytes: 1024,
            content_crc32c: None,
            cf_id: 0,
            smallest_key: None,
            largest_key: None,
            smallest_seq: None,
            largest_seq: None,
        };

        // Assert
        assert_eq!(meta.name, sst_name);
        assert_eq!(meta.level, 0);
        assert_eq!(meta.size_bytes, 1024);
        assert_eq!(meta.cf_id, 0);
    }

    #[test]
    fn should_clone_file_meta() {
        // Arrange
        let meta = FileMeta {
            name: "sst.sst".to_string(),
            level: 0,
            size_bytes: 100,
            content_crc32c: None,
            cf_id: 0,
            smallest_key: Some(b"a".to_vec()),
            largest_key: Some(b"z".to_vec()),
            smallest_seq: Some(1),
            largest_seq: Some(100),
        };

        // Act
        let cloned = meta.clone();

        // Assert
        assert_eq!(meta.name, cloned.name);
        assert_eq!(meta.smallest_key, cloned.smallest_key);
    }
}
