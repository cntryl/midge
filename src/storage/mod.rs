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
pub(crate) mod remote_sst;
pub(crate) mod retained_callback;
pub(crate) mod simulated;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
pub(crate) use test_support::forward_storage_backend;

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

impl StorageObjectMetadata {
    /// Metadata for a provider that reports no object generation.
    #[cfg(any(
        test,
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    #[must_use]
    pub fn new(size: u64, etag: String) -> Self {
        Self {
            size,
            etag,
            generation: None,
        }
    }

    /// Metadata for a provider that reports an object generation.
    #[cfg(feature = "cloud-gcp")]
    #[must_use]
    pub fn with_generation(size: u64, etag: String, generation: impl Into<String>) -> Self {
        Self {
            size,
            etag,
            generation: Some(generation.into()),
        }
    }
}

/// Public error category retained across the asynchronous cloud WAL pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudUploadFailureKind {
    Other,
    Timeout,
}

/// Select exactly the identity used by conditional mutations. GCS JSON
/// metadata and media responses may expose different `ETags` for one generation.
pub(crate) fn conditional_object_identity<'a>(
    etag: &'a str,
    generation: Option<&'a str>,
) -> Option<(&'static str, &'a str)> {
    if let Some(generation) = generation.map(str::trim).filter(|value| !value.is_empty()) {
        return Some(("x-goog-if-generation-match", generation));
    }
    let etag = etag.trim();
    (!etag.is_empty()).then_some(("If-Match", etag))
}

impl StorageObjectMetadata {
    pub(crate) fn same_version(&self, other: &Self) -> bool {
        let identity = conditional_object_identity(&self.etag, self.generation.as_deref());
        self.size == other.size
            && identity.is_some()
            && identity == conditional_object_identity(&other.etag, other.generation.as_deref())
    }

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

/// Why a storage operation failed, preserved across the callback boundary.
///
/// Providers classify failures precisely; callers need that classification to
/// decide whether an object is absent, a conditional write lost a race, or a
/// request timed out. Carrying the kind keeps those decisions out of message
/// text, where rewording silently changed behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageErrorKind {
    /// The object does not exist.
    NotFound,
    /// A conditional write or delete lost its race.
    PreconditionFailed,
    /// The operation exceeded its deadline; its effect is unknown.
    Timeout,
    /// Credentials or permissions rejected the request.
    Unauthorized,
    /// The request did not complete end to end.
    Transport,
    /// The response did not follow the expected protocol.
    Protocol,
    /// Local I/O failed.
    Io,
    /// A memory or resource budget refused the operation.
    ResourceLimit,
    /// Bytes read back failed verification.
    Corruption,
}

/// A storage failure with its classification.
#[derive(Debug, Clone)]
pub struct StorageError {
    kind: StorageErrorKind,
    message: String,
}

impl StorageError {
    /// Build an error of `kind`.
    #[must_use]
    pub fn new(kind: StorageErrorKind, message: impl std::fmt::Display) -> Self {
        Self {
            kind,
            message: message.to_string(),
        }
    }

    #[must_use]
    pub fn kind(&self) -> StorageErrorKind {
        self.kind
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Consume the error, keeping only its message.
    #[must_use]
    pub fn into_message(self) -> String {
        self.message
    }

    #[must_use]
    pub fn is_not_found(&self) -> bool {
        self.kind == StorageErrorKind::NotFound
    }

    #[must_use]
    pub fn is_precondition_failed(&self) -> bool {
        self.kind == StorageErrorKind::PreconditionFailed
    }

    #[must_use]
    pub fn is_timeout(&self) -> bool {
        self.kind == StorageErrorKind::Timeout
    }

    /// Local I/O failure.
    #[must_use]
    pub(crate) fn io(message: impl std::fmt::Display) -> Self {
        Self::new(StorageErrorKind::Io, message)
    }

    /// Malformed or unexpected response.
    #[must_use]
    pub(crate) fn protocol(message: impl std::fmt::Display) -> Self {
        Self::new(StorageErrorKind::Protocol, message)
    }

    /// Deadline exceeded; the operation's effect is unknown.
    #[must_use]
    pub(crate) fn timeout(message: impl std::fmt::Display) -> Self {
        Self::new(StorageErrorKind::Timeout, message)
    }

    /// Object absent.
    #[must_use]
    pub(crate) fn not_found(message: impl std::fmt::Display) -> Self {
        Self::new(StorageErrorKind::NotFound, message)
    }

    /// Conditional write or delete lost its race.
    #[must_use]
    pub(crate) fn precondition_failed(message: impl std::fmt::Display) -> Self {
        Self::new(StorageErrorKind::PreconditionFailed, message)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<String> for StorageError {
    /// Unclassified failure text. Prefer a specific constructor: this maps to
    /// `Io`, which callers treat as a generic failure.
    fn from(message: String) -> Self {
        Self::io(message)
    }
}

impl From<&str> for StorageError {
    fn from(message: &str) -> Self {
        Self::io(message)
    }
}

impl From<crate::common::MidgeError> for StorageError {
    /// Keep the classes storage callers branch on; everything else is I/O.
    fn from(error: crate::common::MidgeError) -> Self {
        Self::new(StorageErrorKind::of(&error), error)
    }
}

impl StorageErrorKind {
    /// The storage class of an engine error: the classes storage callers
    /// branch on are kept, everything else is I/O.
    pub(crate) fn of(error: &crate::common::MidgeError) -> Self {
        use crate::common::MidgeError;
        match error {
            MidgeError::Timeout(_) => Self::Timeout,
            MidgeError::ResourceLimit(_) => Self::ResourceLimit,
            MidgeError::Corruption(_) => Self::Corruption,
            _ => Self::Io,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(error: std::io::Error) -> Self {
        match error.kind() {
            std::io::ErrorKind::NotFound => Self::not_found(error),
            std::io::ErrorKind::AlreadyExists => Self::precondition_failed(error),
            std::io::ErrorKind::TimedOut => Self::timeout(error),
            std::io::ErrorKind::PermissionDenied => {
                Self::new(StorageErrorKind::Unauthorized, error)
            }
            _ => Self::io(error),
        }
    }
}

/// Serializable result type for storage operations.
///
/// Can be converted to/from `MidgeResult` for compatibility.
#[derive(Debug, Clone)]
pub enum StorageOutcome<T: Clone> {
    Ok(T),
    Err(StorageError),
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

/// Build a timeout failure for the storage callback boundary.
#[must_use]
pub(crate) fn storage_timeout_error(message: impl std::fmt::Display) -> StorageError {
    StorageError::timeout(message)
}

/// One read response binds the body and provider identity to the same version.
pub type MetadataReadCallback =
    std::sync::mpsc::Sender<Result<(Vec<u8>, StorageObjectMetadata), StorageError>>;

/// Completion of an exact, conditionally versioned object range.
pub type RangeReadCallback = std::sync::mpsc::Sender<Result<Vec<u8>, StorageError>>;

/// Version-aware object I/O required by engine persistence paths.
///
/// CRITICAL DESIGN:
/// - Local implementations may complete inline.
/// - Cloud callback adapters bound every internal wait by the supplied timeout.
/// - Results are reported back via `StorageCallback`
/// - Same trait for both filesystem and cloud backends
///
/// This allows:
/// - Synchronous engine with async I/O workers
/// - Deterministic runtime (events consumed in event loop)
/// - No mutable references (works with Arc)
/// - Ready for batching and pipelining
///
/// Behavior contract, the same for every implementation (#514; pinned by
/// `backend_contract_tests`):
/// - Deleting an absent object succeeds, conditional or not: the targeted
///   version is already gone, so no other version can be deleted instead.
/// - Completion events carry the caller's key, never a provider or
///   namespaced key.
/// - Every identity a backend reports changes whenever the object is
///   replaced, because identities guard compare-and-swap on mutable control
///   objects. `submit_range_head` may use a cheaper identity than
///   `submit_head`: the local filesystem uses file metadata there and a
///   content hash for HEAD, and stamps each new version with a later modified
///   time so a reused inode cannot repeat an old identity (#557).
pub trait StorageBackend: Send + Sync + 'static {
    /// Keep a publication allowance alive until backend completion. Async
    /// adapters must override this if their ordinary callback can time out
    /// before the underlying upload has released its payload.
    fn submit_write_with_reservation(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        timeout: std::time::Duration,
        reservation: std::sync::Arc<crate::common::resource_budget::ResourceReservation>,
        callback: StorageCallback,
    ) {
        match retained_callback::retain(callback.clone(), reservation) {
            Ok(retained) => {
                self.submit_write_with_headers_and_timeout(key, data, headers, timeout, retained);
            }
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key: key.to_string(),
                    result: StorageOutcome::Err(StorageError::new(
                        StorageErrorKind::of(&error),
                        format!("retain upload completion: {error}"),
                    )),
                });
            }
        }
    }
    fn submit_read_range_with_reservation(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        reservation: std::sync::Arc<crate::common::resource_budget::ResourceReservation>,
        callback: RangeReadCallback,
    ) {
        let start = range.start;
        let end = range.end;
        match retained_callback::retain(callback.clone(), reservation) {
            Ok(retained) => self.submit_read_range(key, start, end, expected, timeout, retained),
            Err(error) => {
                // Keep the class: a blocked budget is a retryable resource
                // limit, not an I/O failure.
                let kind = StorageErrorKind::of(&error);
                let _ = callback.send(Err(StorageError::new(
                    kind,
                    format!("retain range completion: {error}"),
                )));
            }
        }
    }

    /// Return a version usable by exact range reads without reading the body.
    /// Unsupported backends must not fall back to whole-object reads.
    #[cfg(not(test))]
    fn submit_range_head(&self, key: &str, timeout: std::time::Duration, callback: StorageCallback);
    #[cfg(test)]
    fn submit_range_head(
        &self,
        _key: &str,
        _timeout: std::time::Duration,
        _callback: StorageCallback,
    ) {
        panic!("test backend received undeclared range HEAD capability");
    }

    /// Read precisely [start, end) from the expected immutable object version.
    /// Implementations must reject unsupported conditions and short responses.
    #[cfg(not(test))]
    fn submit_read_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        callback: RangeReadCallback,
    );
    #[cfg(test)]
    fn submit_read_range(
        &self,
        _key: &str,
        _start: u64,
        _end: u64,
        _expected: StorageObjectMetadata,
        _timeout: std::time::Duration,
        _callback: RangeReadCallback,
    ) {
        panic!("test backend received undeclared range-read capability");
    }

    /// Read bytes and identity from one version. Unsupported backends fail closed;
    /// synthesizing this response from independent GET and HEAD calls is unsafe.
    #[cfg(not(test))]
    fn submit_read_with_metadata(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: MetadataReadCallback,
    );
    #[cfg(test)]
    fn submit_read_with_metadata(
        &self,
        _key: &str,
        _timeout: std::time::Duration,
        _callback: MetadataReadCallback,
    ) {
        panic!("test backend received undeclared metadata-read capability");
    }

    /// Submit a write operation. Returns immediately.
    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback);

    /// Submit a conditional write operation. Backends that cannot enforce the
    /// supplied preconditions must fail closed rather than writing.
    #[cfg(not(test))]
    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    );
    #[cfg(test)]
    fn submit_write_with_headers(
        &self,
        _key: &str,
        _data: Vec<u8>,
        _headers: Vec<(String, String)>,
        _callback: StorageCallback,
    ) {
        panic!("test backend received undeclared conditional-write capability");
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
    #[cfg(not(test))]
    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    );
    #[cfg(test)]
    fn submit_delete_with_headers(
        &self,
        _key: &str,
        _headers: Vec<(String, String)>,
        _callback: StorageCallback,
    ) {
        panic!("test backend received undeclared conditional-delete capability");
    }

    /// Submit an object metadata lookup.
    #[cfg(not(test))]
    fn submit_head(&self, key: &str, callback: StorageCallback);
    #[cfg(test)]
    fn submit_head(&self, _key: &str, _callback: StorageCallback) {
        panic!("test backend received undeclared HEAD capability");
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

#[cfg(test)]
mod backend_contract_tests;

#[cfg(test)]
mod identity_tests {
    use super::StorageObjectMetadata;

    #[test]
    fn should_match_generation_when_media_and_metadata_etags_differ() {
        // Arrange
        let head = StorageObjectMetadata {
            size: 3,
            etag: "json-etag".to_string(),
            generation: Some("42".to_string()),
        };
        let mut get = head.clone();
        get.etag = "media-etag".to_string();

        // Act
        let same = head.same_version(&get);
        get.generation = Some("43".to_string());
        let replaced = head.same_version(&get);
        get.generation = None;
        let missing_generation = head.same_version(&get);

        // Assert
        assert!(same);
        assert!(!replaced);
        assert!(!missing_generation);
    }

    #[test]
    fn should_require_matching_object_version_when_revalidating_proof() {
        // Arrange
        let expected = StorageObjectMetadata {
            size: 3,
            etag: "identity".to_string(),
            generation: None,
        };
        let mut resized = expected.clone();
        resized.size = 4;
        let mut replaced = expected.clone();
        replaced.etag = "replacement".to_string();
        let missing = StorageObjectMetadata {
            size: 3,
            etag: String::new(),
            generation: None,
        };

        // Act
        let results = [
            expected.same_version(&expected),
            expected.same_version(&resized),
            expected.same_version(&replaced),
            missing.same_version(&missing),
        ];

        // Assert
        assert_eq!(results, [true, false, false, false]);
    }
}
