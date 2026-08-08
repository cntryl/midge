//! Runtime messages, responses, and wire-neutral protocol DTOs.

use crate::common::{MidgeError, MidgeResult};
use crate::types::ConflictPolicy;
use crate::wal::DurabilityPolicy;
use bytes::Bytes;
use crossbeam::channel::Sender;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a new, globally unique request ID.
pub(crate) fn next_request_id() -> MidgeResult<u64> {
    let id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    if id == 0 {
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
#[cfg(test)]
pub struct CompactionPlan {
    pub input_files: Vec<String>,
    pub source_level: u32,
    pub target_level: u32,
    pub cf_id: crate::types::ColumnFamilyId,
}

/// Simplified file metadata for message passing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub level: u32,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_crc32c: Option<u32>,
    pub cf_id: crate::types::ColumnFamilyId,
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
        cf_id: crate::types::ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl_seconds: Option<u64>,
        insert_only: bool,
    },
    Delete {
        cf_id: crate::types::ColumnFamilyId,
        key: Bytes,
    },
    DeleteRange {
        cf_id: crate::types::ColumnFamilyId,
        start_key: Bytes,
        end_key: Bytes,
    },
}

/// A key whose value the client observed (and validated) at the
/// transaction's start snapshot, which must not have changed by commit time.
///
/// This carries only the key, never the expected value: value equality is
/// already checked client-side, against the frozen snapshot, before the
/// transaction is submitted (see `Transaction::validate_assertions`). What
/// the runtime checks here is narrower and cheaper — and, critically,
/// ABA-safe — a *sequence* comparison: has any point mutation or covering
/// range deletion committed a sequence higher than the transaction's
/// `start_sequence`? A write that restores the original value still bumps
/// the sequence, so this correctly rejects the ABA case a value re-read
/// would silently accept.
///
/// Assertions ride alongside `ops` through the commit pipeline but are
/// never written to the WAL or applied to a memtable — they are a read-only
/// check performed at the same pre-sequence-allocation serialization point
/// as ordinary write-conflict detection, and enforced unconditionally
/// (regardless of `ConflictPolicy`): an explicit assertion is a stronger
/// guarantee than the ambient conflict policy.
#[derive(Debug, Clone)]
pub struct KeyAssertion {
    pub cf_id: crate::types::ColumnFamilyId,
    pub key: Bytes,
}

/// Bundled inputs for one in-memory `ApplyTransaction` submission, grouped
/// so `RuntimeHandle`/`IngestCoordinator` call signatures stay readable now
/// that assertions ride alongside the write set.
pub(crate) struct TransactionSubmission {
    pub ops: Vec<TransactionOp>,
    pub assertions: Vec<KeyAssertion>,
    pub durability_policy: Option<DurabilityPolicy>,
    pub start_sequence: Option<u64>,
    pub conflict_policy: ConflictPolicy,
}

/// Bundled inputs for one spilled `ApplySpilledTransaction` submission.
/// Mirrors `TransactionSubmission` for the streamed-source variant, whose
/// `start_sequence` is mandatory rather than optional.
pub(crate) struct SpilledTransactionSubmission {
    pub source: crate::runtime::transaction_spill::TransactionOpSource,
    pub assertions: Vec<KeyAssertion>,
    pub durability_policy: Option<DurabilityPolicy>,
    pub start_sequence: u64,
    pub conflict_policy: ConflictPolicy,
}

/// Intent log entry - records all state transitions for deterministic replay
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntentLogEntry {
    /// Seqno allocated
    SeqnoAllocated {
        seqno: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
    /// Flush plan created
    FlushPlanned {
        cf_id: crate::types::ColumnFamilyId,
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
        cf_id: crate::types::ColumnFamilyId,
        sequence: u64,
        file_meta: FileMeta,
    },
    /// Compaction output SSTs are durable and awaiting publication cleanup.
    CompactionPublish {
        phase: PublicationPhase,
        cf_id: crate::types::ColumnFamilyId,
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
/// Maintainer: each variant that expects a response MUST carry a `request_id: u64`.
#[derive(Debug)]
pub enum RuntimeMsg {
    // === Flush Actor ===
    /// Request memtable flush for a column family.
    FlushMemtable {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },
    /// Memtable flush completed.
    #[cfg(test)]
    FlushComplete {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        sst_name: String,
        sequence: u64,
    },

    // === Compaction Actor ===
    /// Trigger compaction check.
    #[cfg(test)]
    CheckCompaction { request_id: u64 },
    /// Execute a specific compaction plan.
    #[cfg(test)]
    RunCompaction {
        request_id: u64,
        plan: CompactionPlan,
    },
    /// Compaction completed.
    CompactionComplete {
        request_id: u64,
        input_ssts: Vec<String>,
        output_ssts: Vec<String>,
        cf_id: crate::types::ColumnFamilyId,
        target_level: u32,
        succeeded: bool,
    },

    // === WAL Actor ===
    /// Append record to WAL.
    #[cfg(test)]
    WalAppend {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        ttl_seconds: Option<u64>, // TTL in seconds, None means no expiration
        insert_only: bool,        // When true, fail if key already exists
    },

    /// Append delete range tombstone to WAL.
    #[cfg(test)]
    WalAppendDeleteRange {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        durability_policy: Option<DurabilityPolicy>,
    },

    /// Apply a transaction as a single atomic unit.
    ///
    /// Sequence numbers are allocated in-order inside the runtime.
    /// The response returns the last allocated sequence for the transaction.
    /// The `durability_policy` parameter allows per-request durability control
    /// (e.g., `BestEffort` for bulk loads to skip WAL writes entirely).
    ApplyTransaction {
        request_id: u64,
        ops: Vec<TransactionOp>,
        assertions: Vec<KeyAssertion>,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: Option<u64>,
        conflict_policy: ConflictPolicy,
        response_tx: Option<Sender<RuntimeResponse>>,
    },
    /// Apply a transaction from bounded engine-private spill runs.
    ///
    /// The source is reopenable so validation, WAL append, and memtable apply
    /// can each stream it without materializing the complete write set.
    ApplySpilledTransaction {
        request_id: u64,
        source: crate::runtime::transaction_spill::TransactionOpSource,
        assertions: Vec<KeyAssertion>,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: u64,
        conflict_policy: ConflictPolicy,
        response_tx: Option<Sender<RuntimeResponse>>,
    },
    /// Sync WAL to disk.
    WalSync { request_id: u64 },
    /// Rotate WAL segment.
    #[cfg(test)]
    WalRotate { request_id: u64 },
    /// Force-seal the current WAL segment for cloud upload and optionally wait for cloud durability.
    SealWalForCloud {
        request_id: u64,
        sequence: u64,
        wait_for_ack: bool,
    },
    /// WAL sync completed.
    #[cfg(test)]
    WalSyncComplete { request_id: u64, segment_id: u64 },

    // === Cloud Actor ===
    /// Upload SST to cloud.
    #[cfg(test)]
    CloudUploadSst { request_id: u64, sst_name: String },
    /// Upload WAL segment to cloud.
    #[cfg(test)]
    CloudUploadWal { request_id: u64, segment_id: u64 },
    /// Cloud upload completed.
    #[cfg(test)]
    CloudUploadComplete { request_id: u64, resource: String },

    // === GC Actor ===
    /// Check for garbage collection opportunities.
    #[cfg(test)]
    CheckGc { request_id: u64 },
    /// Delete obsolete SST files.
    #[cfg(test)]
    DeleteObsoleteSsts {
        request_id: u64,
        sst_names: Vec<String>,
    },

    // === Manifest Actor ===
    /// Update manifest with new SST.
    #[cfg(test)]
    ManifestAddSst {
        request_id: u64,
        file_meta: FileMeta,
    },
    /// Update manifest after compaction.
    #[cfg(test)]
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
        cf_id: crate::types::ColumnFamilyId,
        /// Explicitly allow committed data still resident in the active
        /// memtable to be discarded. Publication workers are still quiesced
        /// before either safe or destructive drop executes.
        discard_unflushed: bool,
    },

    /// Begin an ingest barrier: prevent new compactions, bump ingest epoch,
    /// and wait until in-flight compactions drain.
    #[cfg(test)]
    BeginIngest { request_id: u64 },

    /// End an ingest barrier: flush outstanding memtables, bump epoch and
    /// re-enable scheduling.
    #[cfg(test)]
    EndIngest { request_id: u64 },

    /// Query whether an ingest barrier is currently active.
    #[cfg(test)]
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
    #[cfg(test)]
    GetRuntimeConfig { request_id: u64 },

    // === Read Path ===
    /// Query a value from memtables and SST files.
    ///
    /// INVARIANT: Reads must respect the durability frontier.
    /// If `requested_durability` is Strict/Steady, the read must not return data
    /// with seqno > `local_durable_seq`. Reads at higher seqnos are queued in
    /// `durability_waiters` until the frontier advances.
    #[cfg(test)]
    Read {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        sequence: u64, // Read at this sequence number or earlier.
        requested_durability: crate::types::ReadDurability, // Durability level requested
    },
    /// Scan a range of keys from memtables and SST files.
    ///
    /// INVARIANT: Range scans must respect the durability frontier.
    /// Same semantics as Read: if `requested_durability` is Strict/Steady,
    /// the scan must not return data with seqno > `local_durable_seq`.
    #[cfg(test)]
    RangeScan {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        start: Vec<u8>,
        end: Vec<u8>,
        sequence: u64, // Read at this sequence number or earlier.
        requested_durability: crate::types::ReadDurability, // Durability level requested
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

    /// Acquire the runtime-wide publication barrier used by online verification.
    BeginStorageVerification { request_id: u64 },

    /// Release a previously acquired online-verification barrier.
    EndStorageVerification { request_id: u64, token: u64 },

    // === Sequencing ===
    /// Get the runtime's authoritative current sequence number.
    ///
    /// This is the sequence maintained by the runtime state and advanced at
    /// WAL append time.
    #[cfg(test)]
    GetCurrentSequence { request_id: u64 },

    /// Capture an immutable read snapshot for transaction execution.
    ///
    /// Returns a snapshot containing references to current memtables and SST metadata,
    /// allowing transactions to execute reads directly without message passing.
    #[cfg(test)]
    CaptureReadSnapshot {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        sequence: u64,
    },

    /// Combined begin-transaction: atomically fetch current sequence AND capture
    /// a read snapshot in a single event-loop round-trip.
    ///
    /// Replaces the previous two-message pattern (`GetCurrentSequence` + `CaptureReadSnapshot`)
    /// to halve the message-passing overhead of `begin_tx`.
    BeginTransaction {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },

    /// Register a transaction snapshot so compaction/GC can respect active readers.
    #[cfg(test)]
    RegisterSnapshot {
        request_id: u64,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    },

    /// Unregister a previously tracked transaction snapshot.
    ///
    /// Fire-and-forget cleanup used on transaction completion/drop.
    #[cfg(test)]
    UnregisterSnapshot { snapshot_id: u64 },

    // === Control ===
    /// Shutdown the runtime (no `request_id`; fire-and-forget).
    Shutdown,
    /// Shutdown the runtime and report final durability/upload failures.
    ShutdownWithResponse { request_id: u64 },
    /// Trigger a full compaction sweep and wait for completion.
    CompactAll { request_id: u64 },
    /// No-op for testing.
    #[cfg(test)]
    Noop { request_id: u64 },
    /// Startup handshake to verify event loop is running.
    #[cfg(test)]
    StartupPing { request_id: u64 },

    /// Check if writes should be stalled for a column family.
    /// Used by `Engine::commit()` to expose backpressure before accepting writes.
    CheckWriteStall {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },

    /// Block until writes are no longer stalled for `cf_id`.
    ///
    /// Responds immediately if not stalled; otherwise the response is held
    /// until a stall-clearing event occurs (e.g. flush completion).
    WaitForWriteStallClear {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    },

    /// Best-effort cancel for a previous `WaitForWriteStallClear` request.
    ///
    /// Used by timeout/cancellation-aware callers (e.g. stress harness).
    /// Fire-and-forget.
    CancelWaitForWriteStallClear { wait_request_id: u64 },
    /// Retry obsolete SST deletion after a snapshot pin is released.
    RetryGc,
}

impl RuntimeMsg {
    /// Extract the `request_id` for messages that expect a response.
    ///
    /// Returns `None` for messages that do not participate in request/response
    /// routing (e.g., `Shutdown`).
    pub fn request_id(&self) -> Option<u64> {
        match self {
            RuntimeMsg::FlushMemtable { request_id, .. }
            | RuntimeMsg::CompactionComplete { request_id, .. }
            | RuntimeMsg::ApplyTransaction { request_id, .. }
            | RuntimeMsg::ApplySpilledTransaction { request_id, .. }
            | RuntimeMsg::WalSync { request_id }
            | RuntimeMsg::SealWalForCloud { request_id, .. }
            | RuntimeMsg::ManifestPersist { request_id }
            | RuntimeMsg::ManifestCreateColumnFamily { request_id, .. }
            | RuntimeMsg::ManifestDropColumnFamily { request_id, .. }
            | RuntimeMsg::GetReadAmpMetrics { request_id }
            | RuntimeMsg::GetRecoveryMetrics { request_id }
            | RuntimeMsg::GetRuntimeMetrics { request_id }
            | RuntimeMsg::GetStorageLayout { request_id }
            | RuntimeMsg::BeginStorageVerification { request_id }
            | RuntimeMsg::EndStorageVerification { request_id, .. }
            | RuntimeMsg::BeginTransaction { request_id, .. }
            | RuntimeMsg::SetRuntimeConfig { request_id, .. }
            | RuntimeMsg::CompactAll { request_id }
            | RuntimeMsg::CheckWriteStall { request_id, .. }
            | RuntimeMsg::WaitForWriteStallClear { request_id, .. } => Some(*request_id),

            RuntimeMsg::CancelWaitForWriteStallClear { .. }
            | RuntimeMsg::Shutdown
            | RuntimeMsg::RetryGc => None,
            RuntimeMsg::ShutdownWithResponse { request_id } => Some(*request_id),

            #[cfg(test)]
            RuntimeMsg::FlushComplete { request_id, .. }
            | RuntimeMsg::CheckCompaction { request_id }
            | RuntimeMsg::RunCompaction { request_id, .. }
            | RuntimeMsg::WalAppend { request_id, .. }
            | RuntimeMsg::WalAppendDeleteRange { request_id, .. }
            | RuntimeMsg::WalRotate { request_id }
            | RuntimeMsg::WalSyncComplete { request_id, .. }
            | RuntimeMsg::CloudUploadSst { request_id, .. }
            | RuntimeMsg::CloudUploadWal { request_id, .. }
            | RuntimeMsg::CloudUploadComplete { request_id, .. }
            | RuntimeMsg::CheckGc { request_id }
            | RuntimeMsg::DeleteObsoleteSsts { request_id, .. }
            | RuntimeMsg::ManifestAddSst { request_id, .. }
            | RuntimeMsg::ManifestCompactionComplete { request_id, .. }
            | RuntimeMsg::Read { request_id, .. }
            | RuntimeMsg::RangeScan { request_id, .. }
            | RuntimeMsg::GetCurrentSequence { request_id }
            | RuntimeMsg::CaptureReadSnapshot { request_id, .. }
            | RuntimeMsg::RegisterSnapshot { request_id, .. }
            | RuntimeMsg::GetRuntimeConfig { request_id }
            | RuntimeMsg::GetIngestState { request_id }
            | RuntimeMsg::BeginIngest { request_id }
            | RuntimeMsg::EndIngest { request_id }
            | RuntimeMsg::Noop { request_id }
            | RuntimeMsg::StartupPing { request_id } => Some(*request_id),

            #[cfg(test)]
            RuntimeMsg::UnregisterSnapshot { .. } => None,
        }
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            RuntimeMsg::FlushMemtable { .. } => "FlushMemtable",
            RuntimeMsg::CompactionComplete { .. } => "CompactionComplete",
            RuntimeMsg::ApplyTransaction { .. } => "ApplyTransaction",
            RuntimeMsg::ApplySpilledTransaction { .. } => "ApplySpilledTransaction",
            RuntimeMsg::WalSync { .. } => "WalSync",
            RuntimeMsg::SealWalForCloud { .. } => "SealWalForCloud",
            RuntimeMsg::ManifestPersist { .. } => "ManifestPersist",
            RuntimeMsg::ManifestCreateColumnFamily { .. } => "ManifestCreateColumnFamily",
            RuntimeMsg::ManifestDropColumnFamily { .. } => "ManifestDropColumnFamily",
            RuntimeMsg::GetReadAmpMetrics { .. } => "GetReadAmpMetrics",
            RuntimeMsg::GetRecoveryMetrics { .. } => "GetRecoveryMetrics",
            RuntimeMsg::GetRuntimeMetrics { .. } => "GetRuntimeMetrics",
            RuntimeMsg::GetStorageLayout { .. } => "GetStorageLayout",
            RuntimeMsg::BeginStorageVerification { .. } => "BeginStorageVerification",
            RuntimeMsg::EndStorageVerification { .. } => "EndStorageVerification",
            RuntimeMsg::BeginTransaction { .. } => "BeginTransaction",
            RuntimeMsg::SetRuntimeConfig { .. } => "SetRuntimeConfig",
            RuntimeMsg::CompactAll { .. } => "CompactAll",
            RuntimeMsg::Shutdown => "Shutdown",
            RuntimeMsg::ShutdownWithResponse { .. } => "ShutdownWithResponse",
            RuntimeMsg::CheckWriteStall { .. } => "CheckWriteStall",
            RuntimeMsg::WaitForWriteStallClear { .. } => "WaitForWriteStallClear",
            RuntimeMsg::CancelWaitForWriteStallClear { .. } => "CancelWaitForWriteStallClear",
            RuntimeMsg::RetryGc => "RetryGc",

            #[cfg(test)]
            RuntimeMsg::FlushComplete { .. } => "FlushComplete",
            #[cfg(test)]
            RuntimeMsg::CheckCompaction { .. } => "CheckCompaction",
            #[cfg(test)]
            RuntimeMsg::RunCompaction { .. } => "RunCompaction",
            #[cfg(test)]
            RuntimeMsg::WalAppend { .. } => "WalAppend",
            #[cfg(test)]
            RuntimeMsg::WalAppendDeleteRange { .. } => "WalAppendDeleteRange",
            #[cfg(test)]
            RuntimeMsg::WalRotate { .. } => "WalRotate",
            #[cfg(test)]
            RuntimeMsg::WalSyncComplete { .. } => "WalSyncComplete",
            #[cfg(test)]
            RuntimeMsg::CloudUploadSst { .. } => "CloudUploadSst",
            #[cfg(test)]
            RuntimeMsg::CloudUploadWal { .. } => "CloudUploadWal",
            #[cfg(test)]
            RuntimeMsg::CloudUploadComplete { .. } => "CloudUploadComplete",
            #[cfg(test)]
            RuntimeMsg::CheckGc { .. } => "CheckGc",
            #[cfg(test)]
            RuntimeMsg::DeleteObsoleteSsts { .. } => "DeleteObsoleteSsts",
            #[cfg(test)]
            RuntimeMsg::ManifestAddSst { .. } => "ManifestAddSst",
            #[cfg(test)]
            RuntimeMsg::ManifestCompactionComplete { .. } => "ManifestCompactionComplete",
            #[cfg(test)]
            RuntimeMsg::Read { .. } => "Read",
            #[cfg(test)]
            RuntimeMsg::RangeScan { .. } => "RangeScan",
            #[cfg(test)]
            RuntimeMsg::GetCurrentSequence { .. } => "GetCurrentSequence",
            #[cfg(test)]
            RuntimeMsg::CaptureReadSnapshot { .. } => "CaptureReadSnapshot",
            #[cfg(test)]
            RuntimeMsg::RegisterSnapshot { .. } => "RegisterSnapshot",
            #[cfg(test)]
            RuntimeMsg::UnregisterSnapshot { .. } => "UnregisterSnapshot",
            #[cfg(test)]
            RuntimeMsg::GetRuntimeConfig { .. } => "GetRuntimeConfig",
            #[cfg(test)]
            RuntimeMsg::GetIngestState { .. } => "GetIngestState",
            #[cfg(test)]
            RuntimeMsg::BeginIngest { .. } => "BeginIngest",
            #[cfg(test)]
            RuntimeMsg::EndIngest { .. } => "EndIngest",
            #[cfg(test)]
            RuntimeMsg::Noop { .. } => "Noop",
            #[cfg(test)]
            RuntimeMsg::StartupPing { .. } => "StartupPing",
        }
    }
}

/// Response from runtime operations.
///
/// Maintainer: every response variant MUST carry the originating `request_id`.
#[derive(Debug)]
pub enum RuntimeResponse {
    Ok {
        request_id: u64,
    },
    /// WAL write accepted and assigned a sequence number.
    ///
    /// Note: Sequence numbers are allocated inside the runtime at append time
    /// to preserve a total order under concurrency.
    #[cfg(test)]
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
    #[cfg(test)]
    ReadValue {
        request_id: u64,
        value: Option<Vec<u8>>,
    },
    #[cfg(test)]
    RangeScanResults {
        request_id: u64,
        results: Vec<(Vec<u8>, Vec<u8>)>,
    },
    #[cfg(test)]
    FlushComplete {
        request_id: u64,
        sst_name: String,
    },
    #[cfg(test)]
    CompactionComplete {
        request_id: u64,
        output_ssts: Vec<String>,
    },
    ColumnFamilyCreated {
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
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
        snapshot: Box<crate::types::RuntimeMetricsSnapshot>,
    },

    /// Stable operator-facing storage layout snapshot.
    StorageLayoutSnapshot {
        request_id: u64,
        snapshot: crate::types::StorageLayoutSnapshot,
    },

    /// Online verification barrier acquisition acknowledgement.
    StorageVerificationBarrier {
        request_id: u64,
        token: u64,
    },

    /// Current authoritative runtime sequence.
    #[cfg(test)]
    CurrentSequence {
        request_id: u64,
        sequence: u64,
    },

    /// Immutable read snapshot for direct transaction execution.
    #[cfg(test)]
    ReadSnapshot {
        request_id: u64,
        snapshot: Arc<super::read_snapshot::ReadSnapshot>,
    },

    /// Combined response for `BeginTransaction`: sequence + snapshot in one round-trip.
    BeginTransactionResult {
        request_id: u64,
        /// Start sequence for the transaction (current committed sequence).
        start_sequence: u64,
        /// Immutable read snapshot (None if the CF doesn't exist).
        snapshot: Option<Arc<super::read_snapshot::ReadSnapshot>>,
    },

    /// Snapshot of runtime configuration for diagnostics and tooling
    #[cfg(test)]
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
    #[cfg(test)]
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
            | RuntimeResponse::TransactionApplied { request_id, .. }
            | RuntimeResponse::Error { request_id, .. }
            | RuntimeResponse::ColumnFamilyCreated { request_id, .. }
            | RuntimeResponse::ReadAmpMetricsSnapshot { request_id, .. }
            | RuntimeResponse::RecoveryMetricsSnapshot { request_id, .. }
            | RuntimeResponse::RuntimeMetricsSnapshot { request_id, .. }
            | RuntimeResponse::StorageLayoutSnapshot { request_id, .. }
            | RuntimeResponse::StorageVerificationBarrier { request_id, .. }
            | RuntimeResponse::BeginTransactionResult { request_id, .. }
            | RuntimeResponse::WriteStallStatus { request_id, .. } => *request_id,
            #[cfg(test)]
            RuntimeResponse::WalAppended { request_id, .. }
            | RuntimeResponse::ReadValue { request_id, .. }
            | RuntimeResponse::RangeScanResults { request_id, .. }
            | RuntimeResponse::FlushComplete { request_id, .. }
            | RuntimeResponse::CompactionComplete { request_id, .. }
            | RuntimeResponse::CurrentSequence { request_id, .. }
            | RuntimeResponse::ReadSnapshot { request_id, .. }
            | RuntimeResponse::RuntimeConfigSnapshot { request_id, .. }
            | RuntimeResponse::IngestState { request_id, .. } => *request_id,
        }
    }
}
