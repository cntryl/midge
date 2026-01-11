#![allow(dead_code)]

// == COPILOT MASTER RULES FOR STORAGE SUBSYSTEM =====================================
// These rules define the *correct* architecture of the Midge storage layer. You MUST
// follow them in all completions that touch storage, providers, WAL durability, or
// cloud persistence.
//
// 1. HYBRID STORAGE ARCHITECTURE
//    HybridStorage has TWO independent responsibilities:
//    a) Object Storage: SSTs use submit_read/write/delete/list.
//    b) WAL Durability: WAL segments ONLY use enqueue_wal_segment + process_uploads.
//    NEVER mix these two paths. WAL NEVER calls submit_write(). SSTs ALWAYS call it.
//
// 2. CLOUD-FIRST WAL DURABILITY
//    - Memtables MUST NOT apply updates until CloudAck is received.
//    - CloudAck is emitted ONLY by HybridStorage when cloud upload succeeds.
//    - CloudFail MUST also be emitted if cloud upload fails.
//    - WAL ordering guarantee: local write → cloud upload → CloudAck → memtable update.
//
// 3. CLOUD EXECUTOR REQUIREMENTS
//    - CloudExecutor MUST embed its own single-threaded tokio runtime:
//          tokio::runtime::Builder::new_current_thread().enable_all().build()
//    - spawn_request MUST use rt.spawn(), NOT tokio::spawn.
//    - Every cloud request MUST eventually produce a CloudEvent, no dropped futures.
//
// 4. S3 PROVIDER RULES
//    - S3Provider MUST support AWS, Wasabi, MinIO, and generic S3-compatible vendors.
//    - AWS uses full SigV4 signing. Others use access key + secret only.
//    - All object keys MUST be normalized to "sst/<name>" or "wal/<segment_id>.wal".
//    - LIST operations MUST use prefix semantics.
//
// 5. FILESYSTEM BACKEND RULES
//    - FileSystem MUST be synchronous and callback-driven.
//    - submit_write MUST create parent dirs before writing.
//    - Paths MUST be sanitized to avoid directory traversal.
//    - Used for SSTs + local WAL segments before cloud persistence.
//
// 6. HYBRID STORAGE UPLOAD PIPELINE
//    - enqueue_wal_segment() inserts UploadState { segment_id, local_path, max_sequence }.
//    - process_uploads() advances Pending → InProgress → Completed/Failed.
//    - initiate_cloud_upload() MUST run cloud write asynchronously.
//    - Retry logic: max 3 retries, then emit CloudFail.
//    - Upload completion MUST push CloudAck{segment_id, max_sequence} to event_queue.
//
// 7. STORAGE BUDGET ACTOR INTEGRATION
//    - HybridStorage MUST call budget_actor for:
//         reserve_for_flush, flush_completed, compaction_planned, compaction_completed.
//    - Backpressure events MUST be surfaced to the runtime (TODO but expected).
//
// 8. CORRECTNESS GUARANTEES
//    - Local writes MUST complete before cloud uploads begin.
//    - Memtable state MUST reflect only sequences with CloudAck.
//    - No write becomes visible unless fully durable in cloud.
//    - WAL NEVER becomes inconsistent due to partial upload.
//    - SST writes may be persisted to cloud, but do NOT block memtables.
//
// 9. TESTING REQUIREMENTS FOR COPILOT
//    When generating tests, enforce:
//      - HybridStorage WAL pipeline: Pending → InProgress → CloudAck.
//      - CloudExecutor event delivery.
//      - S3 path correctness.
//      - Retry logic.
//      - Budget actor watermark behaviors.
//    Avoid sleeps or timing assumptions; use manual state triggers.
//
// 10. WHAT COPILOT MUST NEVER DO
//    - Never send WAL through submit_write().
//    - Never update a memtable before CloudAck.
//    - Never spawn async tasks directly without going through CloudExecutor.rt.
//    - Never block the main runtime thread.
//    - Never drop cloud upload results.
//
// Follow these rules EXACTLY. They reflect the authoritative storage architecture.
// ====================================================================================

//! # Storage Subsystem
//!
//! Provides durability abstractions for SSTs (synchronous local/cloud) and WAL segments
//! (cloud-first with local fallback).
//!
//! ## Architecture: Two Abstraction Layers
//!
//! **Layer 1: `StorageBackend` Trait (Callback-based)**
//! - Used by `HybridStorage` and cloud orchestration
//! - Callback-driven, non-blocking I/O via `StorageCallback` channels
//! - Implementations: `FileSystem` (local), `CloudStorage` (cloud)
//! - Modules: [`filesystem`], [`cloud`], [`hybrid`]
//!
//! **Layer 2: `Storage` Trait (File handle-based, in `abstraction`)**
//! - Used by WAL recovery and legacy internal APIs
//! - Full POSIX-like file interface with handles and explicit sync
//! - Implementation: `LocalFsStorage` (local filesystem only)
//! - Module: [`local_fs_storage`]
//!
//! ## Module Overview
//!
//! - **[`abstraction`]**: High-level `Storage` trait and error types
//!   - Portable filesystem abstraction (POSIX-like semantics)
//!   - Not used by hot path; kept for WAL recovery contracts
//!
//! - **[`filesystem`]** (`StorageBackend`): Local filesystem via callbacks
//!   - Synchronous, callback-based operations
//!   - Parent directory creation, path sanitization
//!   - Used for local SST cache, WAL fallback, and test backends
//!
//! - **[`cloud`]**: Cloud storage abstractions
//!   - `CloudBackend` trait for non-blocking I/O
//!   - `CloudStorage` namespace-aware dispatcher
//!   - `CloudExecutor` embedded tokio runtime for async HTTP
//!   - `MockCloudBackend` for deterministic testing
//!
//! - **[`hybrid`]**: Orchestration layer combining filesystem + cloud
//!   - `HybridStorage`: WAL durability (local → cloud upload pipeline)
//!   - SST management: local cache + cloud persistence
//!   - Retry logic, backpressure, state tracking
//!
//! - **[`providers`]**: Cloud provider implementations
//!   - Generic S3 (base implementation)
//!   - AWS S3, Wasabi, MinIO (S3-compatible wrappers)
//!   - Azure Blob Storage, Google Cloud Storage, OCI stubs
//!
//! - **[`local_fs_storage`]**: Legacy `Storage` trait implementation
//!   - Full file handle API with per-handle mutexes
//!   - Used exclusively by WAL recovery tests
//!   - Not on hot path; keep for contract compatibility
//!
//! - **[`test_support`]**: Test harnesses
//!   - Pre-configured `HybridStorage` with mocks
//!
//! ## Data Flow
//!
//! ### SST Write Path (Hot Path)
//! ```text
//! Engine → HybridStorage::submit_write()
//!   → FileSystem (local cache) → CloudStorage (background)
//!   → StorageEvent::WriteComplete
//! ```
//!
//! ### WAL Durability Path (Cloud-First)
//! ```text
//! Memtable → WAL segment (local)
//!   → HybridStorage::enqueue_wal_segment()
//!   → [Pending] → [InProgress] → [Completed]
//!   → CloudStorage (upload via CloudExecutor)
//!   → StorageEvent::CloudAck(segment_id)
//!   → WAL Actor (memtable commit)
//! ```
//!
//! ## Key Guarantees
//!
//! 1. **No futures in engine thread**: All async work happens in `CloudExecutor`'s embedded tokio runtime
//! 2. **Callback-driven hot path**: No blocking or waiting; results sent via mpsc channels
//! 3. **WAL ordering**: Local write → cloud upload → CloudAck → memtable update
//! 4. **Deterministic testing**: `MockCloudBackend` for synchronous test execution

pub(crate) mod filesystem;
pub(crate) mod hybrid;
pub(crate) mod local_fs_storage;
pub(crate) mod test_support;

/// Stable, filesystem-oriented storage abstraction.
///
/// This is the long-lived API contract intended for WAL recovery compatibility.
/// Not on the hot path; use `StorageBackend` trait for actual I/O.
pub mod abstraction;

pub(crate) use local_fs_storage::LocalFsStorage;

pub use hybrid::backend::HybridStorage;

use crate::common::MidgeResult;

// COPILOT: CLOUD-DURABLE STORAGE RULES
//
// Storage subsystem must support CloudFirst durability for WAL and SST.
//
// In CloudFirst mode:
//
//   1. Local filesystem is ephemeral durability.
//      Local writes must NOT mark data visible.
//
//   2. Cloud storage is authoritative durability.
//      A write becomes visible only after CloudBackend acknowledges upload.
//
//   3. HybridStorage orchestrates:
//        - local write
//        - cloud upload
//        - retry on failure
//        - disk watermark backpressure
//        - emitting StorageEvent::CloudAck(segment_id)
//
//   4. HybridStorage MUST maintain:
//        struct UploadState { segment_id, local_path, retries, status }
//
//   5. HybridStorage MUST expose:
//        fn enqueue_wal_segment(segment_id: u64, path: Path)
//        fn poll() -> Vec<StorageEvent>
//      Called by StorageBudgetActor or Runtime.
//
//   6. StorageEvent variants must include:
//        CloudAck(segment_id)
//        CloudFail(segment_id)
//        BackpressureOn
//        BackpressureOff
//
//   7. CloudBackend::upload(...) MUST be async and return strong durability.
//
//   8. HybridStorage MAY stream the WAL segment rather than load into memory.
//
//   9. On CloudAck:
//        - HybridStorage deletes local WAL segment if configured
//        - HybridStorage notifies WalActor via StorageEvent
//
//  10. WAL Actor:
//        must NOT apply writes to memtable until CloudAck is received.
//
// Implementations MUST NOT modify memtables directly, only send StorageEvent.
// Implement only coordination logic here; WAL ordering logic stays in WalActor.

/// Storage events sent back to the runtime after async I/O.
///
/// These events are sent via StorageCallback channels when operations complete.
/// This unified event type works for both filesystem and cloud backends.
#[derive(Debug, Clone)]
pub enum StorageEvent {
    /// Read operation completed
    ReadComplete {
        key: String,
        result: StorageOutcome<Vec<u8>>,
    },
    /// Write operation completed
    WriteComplete {
        key: String,
        result: StorageOutcome<()>,
    },
    /// Delete operation completed
    DeleteComplete {
        key: String,
        result: StorageOutcome<()>,
    },
    /// List operation completed
    ListComplete {
        prefix: String,
        result: StorageOutcome<Vec<String>>,
    },
    /// Cloud upload acknowledged - segment is now durable
    /// WAL Actor MUST apply pending writes to memtable on receipt
    CloudAck { segment_id: u64, max_sequence: u64 },
    /// Cloud upload failed - segment NOT durable
    /// Runtime should retry or handle failure
    CloudFail { segment_id: u64, error: String },
    /// Backpressure activated - disk watermark exceeded
    /// Runtime should pause flushes until BackpressureOff
    BackpressureOn,
    /// Backpressure released - disk usage below threshold
    /// Runtime can resume normal operations
    BackpressureOff,
}

/// Serializable result type for storage operations.
///
/// Can be converted to/from MidgeResult for compatibility.
#[derive(Debug, Clone)]
pub enum StorageOutcome<T: Clone> {
    Ok(T),
    Err(String),
}

impl<T: Clone> StorageOutcome<T> {
    /// Convert from MidgeResult to StorageOutcome
    pub fn from_result(r: MidgeResult<T>) -> Self {
        match r {
            Ok(v) => StorageOutcome::Ok(v),
            Err(e) => StorageOutcome::Err(format!("{:?}", e)),
        }
    }

    /// Convert StorageOutcome to MidgeResult
    pub fn to_result(self) -> MidgeResult<T> {
        match self {
            StorageOutcome::Ok(v) => Ok(v),
            StorageOutcome::Err(e) => Err(crate::common::MidgeError::Internal(e)),
        }
    }

    /// Check if this is an Ok outcome
    pub fn is_ok(&self) -> bool {
        matches!(self, StorageOutcome::Ok(_))
    }

    /// Check if this is an Err outcome
    pub fn is_err(&self) -> bool {
        matches!(self, StorageOutcome::Err(_))
    }
}

/// Callback type: a sync channel to send StorageEvent back to runtime
pub type StorageCallback = std::sync::mpsc::Sender<StorageEvent>;

/// NEW async-compatible storage backend trait.
///
/// CRITICAL DESIGN:
/// - All operations return immediately (non-blocking)
/// - Real I/O happens asynchronously (in thread pools or tokio tasks)
/// - Results are reported back via StorageCallback
/// - Same trait for both filesystem and cloud backends
///
/// This allows:
/// - Synchronous engine with async I/O workers
/// - Deterministic runtime (events consumed in event loop)
/// - Unified hybrid storage (same interface for local + cloud)
/// - No mutable references (works with Arc)
/// - Ready for batching and pipelining
pub trait StorageBackend: Send + Sync + 'static {
    /// Submit a read operation. Returns immediately.
    fn submit_read(&self, key: String, callback: StorageCallback);

    /// Submit a write operation. Returns immediately.
    fn submit_write(&self, key: String, data: Vec<u8>, callback: StorageCallback);

    /// Submit a delete operation. Returns immediately.
    fn submit_delete(&self, key: String, callback: StorageCallback);

    /// Submit a prefix list operation. Returns immediately.
    fn submit_list(&self, prefix: String, callback: StorageCallback);
}
