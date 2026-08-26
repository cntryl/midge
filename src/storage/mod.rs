//! # Storage Subsystem
//!
//! Provides durability abstractions for SSTs (synchronous local/cloud) and WAL
//! segments (cloud-backed async with local staging).
//!
//! ## Architecture: Callback Storage Orchestration
//!
//! **`StorageBackend` Trait (Callback-based)**
//! - Used by `HybridStorage` and cloud orchestration
//! - Callback-driven, non-blocking I/O via `StorageCallback` channels
//! - Implementations: `FileSystem` (local), `CloudStorage` (cloud)
//! - Modules: [`filesystem`], [`cloud`], [`hybrid`]
//!
//! ## Module Overview
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
//!   - AWS S3 and generic S3-compatible endpoints
//!   - Azure Blob Storage and Google Cloud Storage
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
//! ### WAL Durability Path (Cloud-Backed Async)
//! ```text
//! WAL append barrier (local)
//!   → memtable visibility
//!   → runtime::hybrid_persistence maps the WAL object key
//!   → HybridStorage::enqueue_object_upload()
//!   → [Pending] → [InProgress] → [Completed]
//!   → CloudStorage (upload via CloudExecutor)
//!   → StorageEvent::CloudAck(segment_id)
//!   → cloud durability frontier advance
//! ```
//!
//! ## Key Guarantees
//!
//! 1. **No futures in engine thread**: All async work happens in `CloudExecutor`'s embedded tokio runtime
//! 2. **Callback-driven hot path**: No blocking or waiting; results sent via mpsc channels
//! 3. **WAL ordering**: Local write → memtable visibility → cloud upload → `CloudAck`
//! 4. **Deterministic testing**: `MockCloudBackend` for synchronous test execution

pub(crate) mod cloud;
pub(crate) mod filesystem;
pub(crate) mod hybrid;
pub(crate) mod providers;
pub(crate) mod test_support;

pub use hybrid::backend::HybridStorage;

// ARCHITECTURE: CLOUD-DURABLE STORAGE RULES
//
// Storage subsystem must support CloudAsync durability for WAL and SST.
//
// In CloudAsync mode:
//
//   1. Local WAL append is the ordinary commit visibility barrier.
//      Cloud upload is a separate durability frontier.
//
//   2. Cloud storage is the cloud durability target.
//      A write becomes cloud-durable only after CloudBackend acknowledges upload.
//
//   3. Runtime owns WAL/SST/manifest meaning. HybridStorage owns:
//        - raw local and remote object I/O
//        - bounded cloud upload
//        - retry on failure
//        - disk watermark backpressure
//        - emitting StorageEvent::CloudAck(segment_id)
//
//   4. HybridStorage MUST maintain:
//        struct UploadState { segment_id, local_path, retries, status }
//
//   5. HybridStorage MUST expose format-neutral operations:
//        fn enqueue_object_upload(request_id: u64, key: String, path: Path)
//        fn poll() -> Vec<StorageEvent>
//      Called through runtime-owned format orchestration.
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
//        - HybridStorage may delete local WAL segment if configured
//        - HybridStorage notifies WalActor via StorageEvent so cloud frontiers advance
//
//  10. WAL Actor:
//        applies writes after the local append barrier and uses CloudAck only for
//        cloud durability bookkeeping or explicit `cloud_strict()` waits.
//
// Implementations MUST NOT modify memtables directly, only send StorageEvent.
// Implement only coordination logic here; WAL ordering logic stays in WalActor.

/// Basic object metadata used to revalidate cached cloud object proofs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageObjectMetadata {
    pub size: u64,
    pub etag: String,
    pub generation: Option<String>,
}

/// Public error category retained across the asynchronous cloud WAL pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudUploadFailureKind {
    Other,
    Timeout,
}

impl StorageObjectMetadata {
    pub fn content_crc(size: u64, data: &[u8]) -> Self {
        Self {
            size,
            etag: format!("crc32c:{:08x}", crc32c::crc32c(data)),
            generation: None,
        }
    }
}

/// Storage events sent back to the runtime after async I/O.
///
/// These events are sent via `StorageCallback` channels when operations complete.
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
    #[cfg(test)]
    ListComplete {
        prefix: String,
        result: StorageOutcome<Vec<String>>,
    },
    /// Metadata lookup completed
    HeadComplete {
        key: String,
        result: StorageOutcome<StorageObjectMetadata>,
    },
    /// Cloud upload acknowledged - segment is now durable
    /// WAL Actor MUST apply pending writes to memtable on receipt
    CloudAck { segment_id: u64, max_sequence: u64 },
    /// Cloud upload attempt failed - segment NOT durable.
    ///
    /// While `terminal` is false, `HybridStorage` still owns an internal retry.
    /// A terminal failure transfers the accepted local WAL obligation back to
    /// the runtime for delayed callerless retry.
    CloudFail {
        segment_id: u64,
        error: String,
        terminal: bool,
        failure_kind: CloudUploadFailureKind,
    },
    /// Remote WAL pruning completed after the segment became covered by
    /// cloud-published SST and metadata state.
    CloudWalPruneComplete {
        segment_id: u64,
        result: StorageOutcome<()>,
    },
    /// A background prune attempt failed before catalog authority retirement.
    /// The runtime clears inflight state but retains the segment for retry.
    CloudWalPruneAttemptFailed { segment_id: u64, error: String },
    /// Backpressure activated - disk watermark exceeded
    /// Runtime should pause flushes until `BackpressureOff`
    BackpressureOn,
    /// Backpressure released - disk usage below threshold
    /// Runtime can resume normal operations
    BackpressureOff,
}

/// Serializable result type for storage operations.
///
/// Can be converted to/from `MidgeResult` for compatibility.
#[derive(Debug, Clone)]
pub enum StorageOutcome<T: Clone> {
    Ok(T),
    Err(String),
}

impl<T: Clone> StorageOutcome<T> {
    /// Check if this is an Ok outcome
    #[cfg(test)]
    pub fn is_ok(&self) -> bool {
        matches!(self, StorageOutcome::Ok(_))
    }

    /// Check if this is an Err outcome
    #[cfg(test)]
    pub fn is_err(&self) -> bool {
        matches!(self, StorageOutcome::Err(_))
    }
}

/// Callback type: a sync channel to send `StorageEvent` back to runtime
pub type StorageCallback = std::sync::mpsc::Sender<StorageEvent>;

const STORAGE_TIMEOUT_PREFIX: &str = "midge storage timeout: ";

/// Preserve a typed timeout category across the legacy string-valued storage
/// callback boundary. Only trusted adapters add this canonical marker;
/// provider diagnostic text alone must not control public error typing.
#[must_use]
pub(crate) fn storage_timeout_error(message: impl std::fmt::Display) -> String {
    format!("{STORAGE_TIMEOUT_PREFIX}{message}")
}

#[must_use]
pub(crate) fn storage_error_is_timeout(error: &str) -> bool {
    error.trim_start().starts_with(STORAGE_TIMEOUT_PREFIX)
}

/// NEW async-compatible storage backend trait.
///
/// CRITICAL DESIGN:
/// - All operations return immediately (non-blocking)
/// - Real I/O happens asynchronously (in thread pools or tokio tasks)
/// - Results are reported back via `StorageCallback`
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
    fn submit_read(&self, key: &str, callback: StorageCallback);

    /// Submit a read whose callback adapter must not wait longer than
    /// `timeout`. Backends whose submission path is already non-blocking may
    /// retain this default; blocking adapters must override it.
    fn submit_read_with_timeout(
        &self,
        key: &str,
        _timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        self.submit_read(key, callback);
    }

    /// Submit a write operation. Returns immediately.
    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback);

    /// Submit a conditional write operation. Backends that cannot enforce the
    /// supplied preconditions must fail closed rather than writing.
    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        if headers.is_empty() {
            self.submit_write(key, data, callback);
            return;
        }

        let _ = callback.send(StorageEvent::WriteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err(
                "conditional write is not supported by this storage backend".to_string(),
            ),
        });
    }

    /// Submit a conditional write with a bounded callback-adapter wait.
    fn submit_write_with_headers_and_timeout(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        _timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        self.submit_write_with_headers(key, data, headers, callback);
    }

    /// Submit a delete operation. Returns immediately.
    fn submit_delete(&self, key: &str, callback: StorageCallback);

    /// Submit a conditional delete operation. Backends that cannot enforce the
    /// supplied preconditions must fail closed rather than deleting.
    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        if headers.is_empty() {
            self.submit_delete(key, callback);
            return;
        }

        let _ = callback.send(StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err(
                "conditional delete is not supported by this storage backend".to_string(),
            ),
        });
    }

    /// Submit a prefix list operation. Returns immediately.
    #[cfg(test)]
    fn submit_list(&self, prefix: &str, callback: StorageCallback);

    /// Submit an object metadata lookup. Implementations with native HEAD
    /// support should override this. The fallback reads the object and returns
    /// a content fingerprint, which is conservative but may be more expensive.
    fn submit_head(&self, key: &str, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_read(key, tx);
        let result = match rx.recv() {
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Ok(data),
                ..
            }) => StorageOutcome::Ok(StorageObjectMetadata::content_crc(data.len() as u64, &data)),
            Ok(StorageEvent::ReadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => StorageOutcome::Err(error),
            Ok(other) => StorageOutcome::Err(format!(
                "unexpected storage HEAD fallback response for '{key}': {other:?}"
            )),
            Err(error) => StorageOutcome::Err(format!(
                "storage HEAD fallback channel closed for '{key}': {error}"
            )),
        };
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result,
        });
    }

    /// Submit an object metadata lookup with a bounded callback-adapter wait.
    fn submit_head_with_timeout(
        &self,
        key: &str,
        _timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        self.submit_head(key, callback);
    }
}
