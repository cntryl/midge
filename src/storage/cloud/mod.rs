//! Callback-based cloud storage abstractions.
//!
//! Aligns with the actor runtime model: synchronous submission + async completion.
//! - `CloudBackend` defines submit-only methods (PUT/GET/DELETE/LIST/HEAD).
//! - Backends send results via `CloudCallback` channels (no futures in the engine).
//! - `CloudStorage` is a namespace-aware dispatcher that shields the rest of the engine.
//! - `MockCloudBackend` keeps deterministic testing without async runtimes.
//!
//! ## Architecture
//!
//! ```text
//! CloudStorage (namespace-aware dispatcher)
//!     ↓
//! CloudBackend trait (interface: submit_put, submit_get, etc.)
//!     ↓
//! [Real backends via CloudExecutor]  [MockCloudBackend for testing]
//! ```
//!
//! ## Async Model
//!
//! - `submit_*()` methods return immediately (non-blocking)
//! - Results are sent back via `CloudCallback` channels (`mpsc::Sender<CloudEvent>`)
//! - Events are received asynchronously but callback processing is synchronous
//! - No futures in the engine: all async work happens in `CloudExecutor` embedded tokio runtime

mod config;
#[cfg(test)]
mod test_support;
#[cfg(test)]
pub(crate) use test_support::forward_cloud_backend;
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub mod executor;
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
mod list_budget;

pub use config::CloudWritePolicy;
pub(crate) use config::CloudWritePolicyConfig;

use super::{StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome};
#[cfg(test)]
use crate::common::MidgeError;
use parking_lot::{Mutex, MutexGuard};
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) const REQUEST_TIMEOUT_HEADER: &str = "x-midge-internal-request-timeout-ms";

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub use executor::{CloudExecutor, CloudRequest, CloudResponse, CloudSigner};
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub(crate) use list_budget::CloudListBudget;

/// Structured cloud provider failure, classified at the point the HTTP
/// response (or transport failure) is first observed — inside each provider
/// implementation (`s3.rs`, `azure.rs`, `gcs.rs`), where the real status code
/// or connection error is still a typed value.
///
/// Downstream consumers (lease acquisition, WAL/SST GC, flush publication)
/// match on these variants directly instead of re-deriving meaning from a
/// formatted message string. In particular, [`CloudError::PreconditionFailed`]
/// is reserved for a genuine conditional-write/delete race lost to another
/// writer after the provider-specific error code has been checked — every
/// other failure mode (auth, transport, server
/// error, malformed protocol response) uses a distinct variant so callers
/// can no longer conflate "someone else holds it" with "we don't know".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloudError {
    /// The object does not exist (404 / `NoSuchKey` / `NotFound` /
    /// `BlobNotFound`).
    #[cfg(any(test, feature = "cloud-common"))]
    NotFound(String),
    /// A conditional request (`If-Match` / `If-None-Match`) lost a genuine
    /// race with a concurrent writer.
    #[cfg_attr(not(any(test, feature = "cloud-common")), allow(dead_code))]
    PreconditionFailed(String),
    /// Authentication or authorization failure (401 / 403).
    #[cfg(any(test, feature = "cloud-common"))]
    Unauthorized(String),
    /// The provider rejected the request as malformed (non-retryable 4xx other
    /// than not-found/precondition/auth).
    #[cfg(any(test, feature = "cloud-common"))]
    InvalidRequest(String),
    /// The provider reported a retryable or server-side failure
    /// (408 / 425 / 429 / 5xx).
    #[cfg(any(test, feature = "cloud-common"))]
    ServerError(String),
    /// The request could not reach the provider (network, DNS, TLS, or
    /// connection failure) without a confirmed deadline expiry.
    #[cfg_attr(not(any(test, feature = "cloud-common")), allow(dead_code))]
    Transport(String),
    /// A typed executor or provider deadline expired.
    #[cfg_attr(
        not(any(
            test,
            feature = "cloud-aws",
            feature = "cloud-azure",
            feature = "cloud-gcp",
            feature = "cloud-oci"
        )),
        allow(dead_code)
    )]
    Timeout(String),
    /// The response did not match the expected protocol: malformed body,
    /// unexpected status, or a required header was missing.
    Protocol(String),
}

impl CloudError {
    /// True when the object is confirmed absent.
    #[must_use]
    pub fn is_not_found(&self) -> bool {
        #[cfg(any(test, feature = "cloud-common"))]
        {
            matches!(self, Self::NotFound(_))
        }
        #[cfg(not(any(test, feature = "cloud-common")))]
        {
            let _ = std::mem::discriminant(self);
            false
        }
    }

    /// True when a conditional write/delete genuinely lost a race to a
    /// concurrent writer, as opposed to failing for an unrelated reason.
    #[must_use]
    pub fn is_precondition_failed(&self) -> bool {
        matches!(self, Self::PreconditionFailed(_))
    }

    /// True when the provider or executor specifically reports deadline
    /// exhaustion, rather than another transport failure such as DNS or TLS.
    #[must_use]
    pub(crate) fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout(_) => true,
            #[cfg(any(test, feature = "cloud-common"))]
            Self::ServerError(message) if message.starts_with("status 408:") => true,
            _ => false,
        }
    }

    #[cfg(any(
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    #[must_use]
    pub(crate) fn from_transport_error(error: crate::common::MidgeError) -> Self {
        match error {
            crate::common::MidgeError::Timeout(message) => Self::Timeout(message),
            other => Self::Transport(format!("{other:?}")),
        }
    }

    /// Preserve parser/protocol classification while retaining a typed
    /// executor deadline from a multi-request operation such as LIST.
    #[cfg(any(
        test,
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    #[must_use]
    pub(crate) fn from_protocol_or_timeout_error(error: crate::common::MidgeError) -> Self {
        match error {
            crate::common::MidgeError::Timeout(message) => Self::Timeout(message),
            other => Self::Protocol(format!("{other:?}")),
        }
    }

    /// Classify a raw HTTP status code from a provider response.
    ///
    /// Status alone is intentionally insufficient to classify a lost
    /// precondition: providers also use 409/412 for leases, retention policy,
    /// snapshots, and other failures. Provider adapters must inspect their
    /// structured error code before constructing [`Self::PreconditionFailed`].
    #[cfg(any(test, feature = "cloud-common"))]
    #[must_use]
    pub(crate) fn from_http_status(status: u16, detail: impl std::fmt::Display) -> Self {
        match status {
            404 => Self::NotFound(format!("status {status}: {detail}")),
            401 | 403 => Self::Unauthorized(format!("status {status}: {detail}")),
            408 | 425 | 429 | 500..=599 => Self::ServerError(format!("status {status}: {detail}")),
            400..=499 => Self::InvalidRequest(format!("status {status}: {detail}")),
            _ => Self::Protocol(format!("unexpected status {status}: {detail}")),
        }
    }
}

impl std::fmt::Display for CloudError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            #[cfg(any(test, feature = "cloud-common"))]
            Self::NotFound(msg) => write!(f, "not found: {msg}"),
            Self::PreconditionFailed(msg) => write!(f, "precondition failed: {msg}"),
            #[cfg(any(test, feature = "cloud-common"))]
            Self::Unauthorized(msg) => write!(f, "unauthorized: {msg}"),
            #[cfg(any(test, feature = "cloud-common"))]
            Self::InvalidRequest(msg) => write!(f, "invalid request: {msg}"),
            #[cfg(any(test, feature = "cloud-common"))]
            Self::ServerError(msg) => write!(f, "server error: {msg}"),
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::Timeout(msg) => write!(f, "timeout: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

impl std::error::Error for CloudError {}

pub(crate) fn contextualize_operation_error(
    error: &CloudError,
    context: impl std::fmt::Display,
    deadline: &crate::common::OperationDeadline,
) -> crate::common::MidgeError {
    let message = format!("{context}: {error}");
    if deadline.is_expired() || error.is_timeout() {
        crate::common::MidgeError::Timeout(message)
    } else {
        crate::common::MidgeError::Internal(message)
    }
}

/// Cloud operation outcome sent across the callback boundary.
pub type CloudOutcome<T> = Result<T, CloudError>;

#[cfg(test)]
fn cloud_outcome_from_result<T>(result: Result<T, MidgeError>) -> CloudOutcome<T> {
    result.map_err(|error| match error {
        MidgeError::NotFound => CloudError::NotFound(format!("{error:?}")),
        other => CloudError::Protocol(format!("{other:?}")),
    })
}

/// Cloud operation completion events sent back via callback.
#[derive(Clone, Debug)]
pub enum CloudEvent {
    #[cfg_attr(not(any(test, feature = "cloud-common")), allow(dead_code))]
    Put {
        key: String,
        result: CloudOutcome<()>,
    },
    Get {
        key: String,
        result: CloudOutcome<Vec<u8>>,
    },
    GetWithMetadata {
        key: String,
        result: CloudOutcome<(Vec<u8>, ObjectMetadata)>,
    },
    #[cfg(any(test, feature = "cloud-common"))]
    GetRange {
        key: String,
        start: u64,
        end: Option<u64>,
        result: CloudOutcome<Vec<u8>>,
    },
    Delete {
        key: String,
        result: CloudOutcome<()>,
    },
    List {
        prefix: String,
        result: CloudOutcome<Vec<String>>,
    },
    Head {
        key: String,
        result: CloudOutcome<ObjectMetadata>,
    },
}

/// Callback type used to send `CloudEvent`s back to the runtime.
pub type CloudCallback = std::sync::mpsc::Sender<CloudEvent>;

/// Basic metadata emitted by HEAD operations.
#[derive(Clone, Debug)]
pub struct ObjectMetadata {
    pub size: u64,
    pub etag: String,
    pub generation: Option<String>,
}
impl ObjectMetadata {
    #[cfg(any(
        test,
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    pub fn new(size: u64, etag: String) -> Self {
        Self {
            size,
            etag,
            generation: None,
        }
    }

    #[cfg(feature = "cloud-gcp")]
    pub fn with_generation(size: u64, etag: String, generation: impl Into<String>) -> Self {
        Self {
            size,
            etag,
            generation: Some(generation.into()),
        }
    }
}

pub(crate) fn object_match_precondition_headers(
    etag: &str,
    generation: Option<&str>,
) -> Option<Vec<(String, String)>> {
    crate::storage::conditional_object_identity(etag, generation)
        .map(|(header, value)| vec![(header.to_string(), value.to_string())])
}

/// Non-blocking cloud backend interface used by the engine.
pub trait CloudBackend: Send + Sync + 'static {
    /// Override the default deadline applied to provider HTTP requests.
    #[cfg(feature = "cloud-common")]
    fn set_request_timeout(&self, _timeout: std::time::Duration) {}

    /// Submit a PUT request for `key` with optional HTTP headers. Implementations
    /// MUST honor headers (e.g. `If-None-Match`, `If-Match`) when supported by the
    /// provider to allow conditional writes.
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    );
    fn submit_get(&self, key: &str, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::Get {
            key: key.to_string(),
            result: Err(CloudError::Protocol(
                "cloud backend does not support GET".to_string(),
            )),
        });
    }
    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::GetWithMetadata {
            key: key.to_string(),
            result: Err(CloudError::Protocol(
                "cloud backend does not support metadata-bearing GET".to_string(),
            )),
        });
    }
    /// Submit a ranged GET. `end` is an exclusive byte offset.
    #[cfg(any(test, feature = "cloud-common"))]
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback);
    /// Submit an idempotent delete. Implementations must report success when
    /// the target is already absent; conditional-precondition failures remain
    /// errors.
    fn submit_delete(&self, key: &str, _headers: Vec<(String, String)>, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::Delete {
            key: key.to_string(),
            result: Err(CloudError::Protocol(
                "cloud backend does not support DELETE".to_string(),
            )),
        });
    }
    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::List {
            prefix: prefix.to_string(),
            result: Err(CloudError::Protocol(
                "cloud backend does not support LIST".to_string(),
            )),
        });
    }
    fn submit_head(&self, key: &str, callback: CloudCallback) {
        let _ = callback.send(CloudEvent::Head {
            key: key.to_string(),
            result: Err(CloudError::Protocol(
                "cloud backend does not support HEAD".to_string(),
            )),
        });
    }
}

/// Deterministic mock backend for testing (synchronous).
#[cfg(test)]
pub struct MockCloudBackend {
    storage: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    gens: Arc<Mutex<HashMap<String, u64>>>,
    /// Serializes conditional mutation checks with their corresponding write
    /// or delete. Separate storage and generation maps otherwise permit a
    /// stale request to pass a read-then-write race.
    mutation_lock: Arc<Mutex<()>>,
    uploads: Arc<Mutex<Vec<(String, u64)>>>,
    downloads: Arc<Mutex<Vec<String>>>,
}
#[cfg(test)]
impl MockCloudBackend {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            gens: Arc::new(Mutex::new(HashMap::new())),
            mutation_lock: Arc::new(Mutex::new(())),
            uploads: Arc::new(Mutex::new(Vec::new())),
            downloads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_uploads(&self) -> Vec<(String, u64)> {
        self.uploads.lock().clone()
    }

    pub fn get_downloads(&self) -> Vec<String> {
        self.downloads.lock().clone()
    }

    pub fn clear_history(&self) {
        self.uploads.lock().clear();
        self.downloads.lock().clear();
    }
}

#[cfg(test)]
impl Default for MockCloudBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl CloudBackend for MockCloudBackend {
    fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let key = key.to_string();
        let _mutation = self.mutation_lock.lock();
        // Honor `If-None-Match: *` (conditional create).
        let if_none_match = headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("if-none-match") && v == "*");
        if if_none_match && self.storage.lock().contains_key(&key) {
            // Simulate conditional failure (precondition failed)
            let event = CloudEvent::Put {
                key,
                result: CloudOutcome::Err(CloudError::PreconditionFailed(
                    "precondition failed".to_string(),
                )),
            };
            let _ = callback.send(event);
            return;
        }

        // Honor `If-Match: <etag>` (conditional update).
        // If object missing → precondition failed. If etag mismatches → precondition failed.
        if let Some((_, expected)) = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("if-match"))
        {
            let exists = self.storage.lock().contains_key(&key);
            if !exists {
                let event = CloudEvent::Put {
                    key,
                    result: CloudOutcome::Err(CloudError::PreconditionFailed(
                        "precondition failed".to_string(),
                    )),
                };
                let _ = callback.send(event);
                return;
            }

            // compare expected value to stored generation-based etag
            let gens_lock = self.gens.lock();
            let current_gen = gens_lock.get(&key).copied().unwrap_or(0);
            let current_etag = format!("mock-gen-{current_gen}");
            if expected != &current_etag {
                let event = CloudEvent::Put {
                    key,
                    result: CloudOutcome::Err(CloudError::PreconditionFailed(
                        "precondition failed".to_string(),
                    )),
                };
                let _ = callback.send(event);
                return;
            }
        }

        // Perform put: store data and bump generation (etag)
        {
            let mut store = self.storage.lock();
            store.insert(key.clone(), data.clone());
        }
        let mut gens = self.gens.lock();
        let new_gen = gens.get(&key).copied().unwrap_or(0).saturating_add(1);
        gens.insert(key.clone(), new_gen);

        self.uploads.lock().push((key.clone(), data.len() as u64));
        let event = CloudEvent::Put {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_get(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        let result = self
            .storage
            .lock()
            .get(&key)
            .cloned()
            .ok_or(MidgeError::NotFound);
        self.downloads.lock().push(key.clone());
        let event = CloudEvent::Get {
            key,
            result: cloud_outcome_from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        self.downloads.lock().push(key.to_string());
        let _guard = self.mutation_lock.lock();
        let data = self.storage.lock().get(key).cloned();
        let generation = self.gens.lock().get(key).copied();
        let result = match (data, generation) {
            (Some(data), Some(generation)) => CloudOutcome::Ok((
                data.clone(),
                ObjectMetadata::new(data.len() as u64, format!("mock-gen-{generation}")),
            )),
            _ => CloudOutcome::Err(CloudError::NotFound(key.to_string())),
        };
        let _ = callback.send(CloudEvent::GetWithMetadata {
            key: key.to_string(),
            result,
        });
    }

    #[cfg(any(test, feature = "cloud-common"))]
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback) {
        let key = key.to_string();
        let result = self
            .storage
            .lock()
            .get(&key)
            .map(|data| {
                let end_idx =
                    usize::try_from(end.unwrap_or(usize_to_u64(data.len()))).unwrap_or(usize::MAX);
                let start_idx = usize::try_from(start).unwrap_or(usize::MAX);
                data[start_idx..end_idx].to_vec()
            })
            .ok_or(MidgeError::NotFound);
        let event = CloudEvent::GetRange {
            key,
            start,
            end,
            result: cloud_outcome_from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback) {
        let key = key.to_string();
        let _mutation = self.mutation_lock.lock();
        if let Some((_, expected)) = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("if-match"))
        {
            let exists = self.storage.lock().contains_key(&key);
            if !exists {
                let event = CloudEvent::Delete {
                    key,
                    result: CloudOutcome::Err(CloudError::PreconditionFailed(
                        "precondition failed".to_string(),
                    )),
                };
                let _ = callback.send(event);
                return;
            }

            let current_gen = self.gens.lock().get(&key).copied().unwrap_or(0);
            let current_etag = format!("mock-gen-{current_gen}");
            if expected.trim_matches('"') != current_etag {
                let event = CloudEvent::Delete {
                    key,
                    result: CloudOutcome::Err(CloudError::PreconditionFailed(
                        "precondition failed".to_string(),
                    )),
                };
                let _ = callback.send(event);
                return;
            }
        }

        self.storage.lock().remove(&key);
        self.gens.lock().remove(&key);
        let event = CloudEvent::Delete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        let prefix = prefix.to_string();
        let results: Vec<_> = self
            .storage
            .lock()
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        let event = CloudEvent::List {
            prefix,
            result: CloudOutcome::Ok(results),
        };
        let _ = callback.send(event);
    }

    fn submit_head(&self, key: &str, callback: CloudCallback) {
        let key = key.to_string();
        let result = self
            .storage
            .lock()
            .get(&key)
            .map(|data| {
                // ETag is generation based and independent from content length.
                let gen = self.gens.lock().get(&key).copied().unwrap_or(0);
                ObjectMetadata::new(data.len() as u64, format!("mock-gen-{gen}"))
            })
            .ok_or(MidgeError::NotFound);
        let event = CloudEvent::Head {
            key,
            result: cloud_outcome_from_result(result),
        };
        let _ = callback.send(event);
    }
}

/// Namespace-aware dispatcher that forwards calls to the active backend.
pub struct CloudStorage {
    backend: Arc<dyn CloudBackend>,
    namespace: String,
    callback_timeout: std::time::Duration,
    metadata_publication_lock: Mutex<()>,
}

pub(crate) const CLOUD_METADATA_FILES: &[&str] = &[
    "FORMAT",
    "manifest.snapshot.json",
    "manifest.json",
    "manifest.journal",
    "intent_log.json",
];

pub(crate) fn cloud_metadata_key(file_name: &str) -> String {
    format!(
        "{}{file_name}",
        crate::cloud_layout::CloudObjectLayout::METADATA_PREFIX
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CloudObjectProof {
    pub bytes: Vec<u8>,
    pub metadata: StorageObjectMetadata,
}

pub(crate) fn storage_object_metadata(metadata: ObjectMetadata) -> StorageObjectMetadata {
    StorageObjectMetadata {
        size: metadata.size,
        etag: metadata.etag,
        generation: metadata.generation,
    }
}

/// Validate observations from one metadata-bearing read. Never pair a body
/// with identity from a separate HEAD, even when both lengths agree.
pub(crate) fn validate_object_proof(
    key: &str,
    bytes: &[u8],
    metadata: &StorageObjectMetadata,
) -> crate::common::MidgeResult<()> {
    if metadata.size != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
        return Err(crate::common::MidgeError::Internal(format!(
            "cloud object '{key}' length mismatch: read={}, metadata={}",
            bytes.len(),
            metadata.size
        )));
    }
    if crate::storage::conditional_object_identity(&metadata.etag, metadata.generation.as_deref())
        .is_none()
    {
        return Err(crate::common::MidgeError::Internal(format!(
            "cloud object '{key}' is missing an identity token"
        )));
    }
    Ok(())
}

pub(crate) fn blocking_cloud_object_proof(
    cloud: &CloudStorage,
    key: &str,
) -> Result<Option<CloudObjectProof>, String> {
    blocking_cloud_object_proof_within(cloud, key, &crate::common::OperationDeadline::unbounded())
        .map_err(|error| error.to_string())
}

pub(crate) fn blocking_cloud_object_proof_within(
    cloud: &CloudStorage,
    key: &str,
    deadline: &crate::common::OperationDeadline,
) -> crate::common::MidgeResult<Option<CloudObjectProof>> {
    let get_timeout = deadline
        .clamp_nonzero(cloud.callback_timeout())
        .ok_or_else(|| {
            crate::common::MidgeError::Timeout(format!(
                "operation deadline exhausted before cloud object GET for '{key}'"
            ))
        })?;
    let (get_tx, get_rx) = std::sync::mpsc::channel();
    cloud.submit_get_with_metadata(key, get_tx);
    let (bytes, metadata) = match get_rx.recv_timeout(get_timeout) {
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Ok((bytes, metadata)),
            ..
        }) => (bytes, storage_object_metadata(metadata)),
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Err(error),
            ..
        }) if is_not_found_error(&error) => return Ok(None),
        Ok(CloudEvent::GetWithMetadata {
            result: CloudOutcome::Err(error),
            ..
        }) => {
            return Err(contextualize_operation_error(
                &error,
                format_args!("cloud object '{key}' is unreadable"),
                deadline,
            ))
        }
        Ok(other) => {
            return Err(crate::common::MidgeError::Internal(format!(
                "unexpected cloud object GET response for '{key}': {other:?}"
            )))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            return Err(crate::common::MidgeError::Timeout(format!(
                "cloud object GET exceeded the operation deadline for '{key}'"
            )))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            return Err(crate::common::MidgeError::Internal(format!(
                "cloud object GET callback closed for '{key}'"
            )))
        }
    };

    validate_object_proof(key, &bytes, &metadata)?;

    Ok(Some(CloudObjectProof { bytes, metadata }))
}

impl CloudStorage {
    #[cfg(test)]
    pub fn new(backend: Arc<dyn CloudBackend>, namespace: String) -> Self {
        Self::new_with_timeout(
            backend,
            namespace,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )
    }

    #[cfg(any(test, feature = "cloud-common"))]
    pub(crate) fn new_with_timeout(
        backend: Arc<dyn CloudBackend>,
        namespace: String,
        callback_timeout: std::time::Duration,
    ) -> Self {
        Self {
            backend,
            namespace,
            callback_timeout,
            metadata_publication_lock: Mutex::new(()),
        }
    }

    #[cfg(test)]
    pub fn with_mock() -> Self {
        let backend = Arc::new(MockCloudBackend::new());
        Self::new(backend, "midge".to_string())
    }

    pub(crate) fn callback_timeout(&self) -> std::time::Duration {
        self.callback_timeout
    }

    pub(crate) fn try_lock_metadata_publication(&self) -> Option<MutexGuard<'_, ()>> {
        self.metadata_publication_lock.try_lock()
    }

    pub(crate) fn lock_metadata_publication(&self) -> MutexGuard<'_, ()> {
        self.metadata_publication_lock.lock()
    }

    fn full_path(&self, suffix: &str) -> String {
        let namespace = self.namespace.trim_matches('/');
        let suffix = suffix.trim_start_matches('/');
        if namespace.is_empty() {
            suffix.to_string()
        } else if suffix.is_empty() {
            namespace.to_string()
        } else {
            format!("{namespace}/{suffix}")
        }
    }

    pub(crate) fn strip_namespace<'a>(&self, key: &'a str) -> &'a str {
        let namespace = self.namespace.trim_matches('/');
        if namespace.is_empty() {
            return key;
        }
        key.strip_prefix(namespace)
            .and_then(|rest| rest.strip_prefix('/'))
            .unwrap_or(key)
    }

    pub fn submit_put(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let full_key = self.full_path(key);
        self.backend.submit_put(&full_key, data, headers, callback);
    }

    pub fn submit_get(&self, key: &str, callback: CloudCallback) {
        let full_key = self.full_path(key);
        self.backend.submit_get(&full_key, callback);
    }

    pub fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
        let full_key = self.full_path(key);
        self.backend.submit_get_with_metadata(&full_key, callback);
    }

    #[cfg(test)]
    pub fn submit_get_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
        callback: CloudCallback,
    ) {
        let full_key = self.full_path(key);
        self.backend
            .submit_get_range(&full_key, start, end, callback);
    }

    pub fn submit_delete(&self, key: &str, callback: CloudCallback) {
        self.submit_delete_with_headers(key, vec![], callback);
    }

    pub fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let full_key = self.full_path(key);
        self.backend.submit_delete(&full_key, headers, callback);
    }

    pub fn submit_list(&self, prefix: &str, callback: CloudCallback) {
        let full_prefix = self.full_path(prefix);
        self.backend.submit_list(&full_prefix, callback);
    }

    pub fn submit_head(&self, key: &str, callback: CloudCallback) {
        let full_key = self.full_path(key);
        self.backend.submit_head(&full_key, callback);
    }
}

pub(crate) fn is_not_found_error(error: &CloudError) -> bool {
    error.is_not_found()
}

fn cloud_to_storage_outcome<T: Clone>(result: CloudOutcome<T>) -> StorageOutcome<T> {
    match result {
        CloudOutcome::Ok(value) => StorageOutcome::Ok(value),
        CloudOutcome::Err(error) => {
            let message = if error.is_timeout() {
                crate::storage::storage_timeout_error(error)
            } else {
                error.to_string()
            };
            StorageOutcome::Err(message)
        }
    }
}

#[cfg(test)]
fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl StorageBackend for CloudStorage {
    fn submit_read_with_metadata(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: crate::storage::MetadataReadCallback,
    ) {
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
        let result = blocking_cloud_object_proof_within(self, key, &deadline)
            .map_err(|error| match error {
                crate::common::MidgeError::Timeout(message) => {
                    crate::storage::storage_timeout_error(message)
                }
                other => other.to_string(),
            })
            .and_then(|proof| {
                proof
                    .map(|proof| (proof.bytes, proof.metadata))
                    .ok_or_else(|| format!("not found: cloud object '{key}'"))
            });
        let _ = callback.send(result);
    }

    fn submit_read(&self, key: &str, callback: StorageCallback) {
        self.submit_read_with_timeout(key, self.callback_timeout, callback);
    }

    fn submit_read_with_timeout(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        if timeout.is_zero() {
            let _ = callback.send(StorageEvent::ReadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud GET refused because no callback budget remained",
                )),
            });
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_get(key, tx);
        let event = match rx.recv_timeout(timeout) {
            Ok(CloudEvent::Get { key, result }) => StorageEvent::ReadComplete {
                key,
                result: cloud_to_storage_outcome(result),
            },
            Ok(other) => StorageEvent::ReadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(format!("unexpected cloud GET response: {other:?}")),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::ReadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud GET callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::ReadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud GET callback closed".to_string()),
            },
        };
        let _ = callback.send(event);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        self.submit_write_with_headers_and_timeout(
            key,
            data,
            Vec::new(),
            self.callback_timeout,
            callback,
        );
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.submit_write_with_headers_and_timeout(
            key,
            data,
            headers,
            self.callback_timeout,
            callback,
        );
    }

    fn submit_write_with_headers_and_timeout(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        if timeout.is_zero() {
            let _ = callback.send(StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud PUT refused because no callback budget remained",
                )),
            });
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_put(key, data, headers, tx);
        let event = match rx.recv_timeout(timeout) {
            Ok(CloudEvent::Put { key, result }) => StorageEvent::WriteComplete {
                key,
                result: cloud_to_storage_outcome(result),
            },
            Ok(other) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(format!("unexpected cloud PUT response: {other:?}")),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud PUT callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud PUT callback closed".to_string()),
            },
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        if self.callback_timeout.is_zero() {
            let _ = callback.send(StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud DELETE refused because no callback budget remained",
                )),
            });
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_delete(self, key, tx);
        let event = match rx.recv_timeout(self.callback_timeout) {
            Ok(CloudEvent::Delete { key, result }) => StorageEvent::DeleteComplete {
                key,
                result: cloud_to_storage_outcome(result),
            },
            Ok(other) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(format!("unexpected cloud DELETE response: {other:?}")),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud DELETE callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud DELETE callback closed".to_string()),
            },
        };
        let _ = callback.send(event);
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        if self.callback_timeout.is_zero() {
            let _ = callback.send(StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud DELETE refused because no callback budget remained",
                )),
            });
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_delete_with_headers(self, key, headers, tx);
        let event = match rx.recv_timeout(self.callback_timeout) {
            Ok(CloudEvent::Delete { key, result }) => StorageEvent::DeleteComplete {
                key,
                result: cloud_to_storage_outcome(result),
            },
            Ok(other) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(format!("unexpected cloud DELETE response: {other:?}")),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud DELETE callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud DELETE callback closed".to_string()),
            },
        };
        let _ = callback.send(event);
    }

    #[cfg(test)]
    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        if self.callback_timeout.is_zero() {
            let _ = callback.send(StorageEvent::ListComplete {
                prefix: prefix.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud LIST refused because no callback budget remained",
                )),
            });
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_list(self, prefix, tx);
        let event = match rx.recv_timeout(self.callback_timeout) {
            Ok(CloudEvent::List {
                prefix: key_prefix,
                result,
            }) => StorageEvent::ListComplete {
                prefix: key_prefix,
                result: cloud_to_storage_outcome(result),
            },
            Ok(other) => StorageEvent::ListComplete {
                prefix: prefix.to_string(),
                result: StorageOutcome::Err(format!("unexpected cloud LIST response: {other:?}")),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::ListComplete {
                prefix: prefix.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud LIST callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::ListComplete {
                prefix: prefix.to_string(),
                result: StorageOutcome::Err("cloud LIST callback closed".to_string()),
            },
        };
        let _ = callback.send(event);
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        self.submit_head_with_timeout(key, self.callback_timeout, callback);
    }

    fn submit_head_with_timeout(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        if timeout.is_zero() {
            let _ = callback.send(StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud HEAD refused because no callback budget remained",
                )),
            });
            return;
        }
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_head(self, key, tx);
        let event = match rx.recv_timeout(timeout) {
            Ok(CloudEvent::Head { key, result }) => {
                let outcome = match result {
                    CloudOutcome::Ok(metadata) => StorageOutcome::Ok(StorageObjectMetadata {
                        size: metadata.size,
                        etag: metadata.etag,
                        generation: metadata.generation,
                    }),
                    CloudOutcome::Err(err) => {
                        cloud_to_storage_outcome::<StorageObjectMetadata>(CloudOutcome::Err(err))
                    }
                };
                StorageEvent::HeadComplete {
                    key,
                    result: outcome,
                }
            }
            Ok(other) => StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(format!("unexpected cloud HEAD response: {other:?}")),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud HEAD callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud HEAD callback closed".to_string()),
            },
        };
        let _ = callback.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    #[derive(Default)]
    struct ConditionalPutOnlyBackend {
        puts: AtomicUsize,
        gets: AtomicUsize,
        heads: AtomicUsize,
        deletes: AtomicUsize,
        lists: AtomicUsize,
    }

    struct DelayedMissingGetBackend;

    struct DroppedHeadCallbackBackend;

    impl CloudBackend for ConditionalPutOnlyBackend {
        fn submit_put(
            &self,
            key: &str,
            _data: Vec<u8>,
            headers: Vec<(String, String)>,
            callback: CloudCallback,
        ) {
            self.puts.fetch_add(1, Ordering::SeqCst);
            let has_condition = headers
                .iter()
                .any(|(name, value)| name.eq_ignore_ascii_case("if-none-match") && value == "*");
            let result = if has_condition {
                CloudOutcome::Ok(())
            } else {
                CloudOutcome::Err(CloudError::Protocol(
                    "conditional header was not delegated".to_string(),
                ))
            };
            let _ = callback.send(CloudEvent::Put {
                key: key.to_string(),
                result,
            });
        }

        fn submit_get(&self, key: &str, callback: CloudCallback) {
            self.gets.fetch_add(1, Ordering::SeqCst);
            let _ = callback.send(CloudEvent::Get {
                key: key.to_string(),
                result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
            });
        }

        fn submit_delete(
            &self,
            key: &str,
            _headers: Vec<(String, String)>,
            callback: CloudCallback,
        ) {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            let _ = callback.send(CloudEvent::Delete {
                key: key.to_string(),
                result: CloudOutcome::Ok(()),
            });
        }

        fn submit_list(&self, prefix: &str, callback: CloudCallback) {
            self.lists.fetch_add(1, Ordering::SeqCst);
            let _ = callback.send(CloudEvent::List {
                prefix: prefix.to_string(),
                result: CloudOutcome::Ok(Vec::new()),
            });
        }

        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: CloudCallback,
        ) {
            let _ = callback.send(CloudEvent::GetRange {
                key: key.to_string(),
                start,
                end,
                result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
            });
        }

        fn submit_head(&self, key: &str, callback: CloudCallback) {
            self.heads.fetch_add(1, Ordering::SeqCst);
            let _ = callback.send(CloudEvent::Head {
                key: key.to_string(),
                result: CloudOutcome::Err(CloudError::Unauthorized(
                    "HEAD permission intentionally absent".to_string(),
                )),
            });
        }
    }

    impl CloudBackend for DelayedMissingGetBackend {
        fn submit_put(
            &self,
            key: &str,
            _data: Vec<u8>,
            _headers: Vec<(String, String)>,
            callback: CloudCallback,
        ) {
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let _ = callback.send(CloudEvent::Put {
                    key,
                    result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
                });
            });
        }

        fn submit_get(&self, key: &str, callback: CloudCallback) {
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let _ = callback.send(CloudEvent::Get {
                    key,
                    result: CloudOutcome::Err(CloudError::NotFound("delayed miss".to_string())),
                });
            });
        }

        fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let _ = callback.send(CloudEvent::GetWithMetadata {
                    key,
                    result: Err(CloudError::NotFound("delayed miss".to_string())),
                });
            });
        }

        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: CloudCallback,
        ) {
            let _ = callback.send(CloudEvent::GetRange {
                key: key.to_string(),
                start,
                end,
                result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
            });
        }

        fn submit_head(&self, key: &str, callback: CloudCallback) {
            let key = key.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let _ = callback.send(CloudEvent::Head {
                    key,
                    result: CloudOutcome::Err(CloudError::NotFound("delayed miss".to_string())),
                });
            });
        }
    }

    impl CloudBackend for DroppedHeadCallbackBackend {
        fn submit_put(
            &self,
            key: &str,
            _data: Vec<u8>,
            _headers: Vec<(String, String)>,
            callback: CloudCallback,
        ) {
            let _ = callback.send(CloudEvent::Put {
                key: key.to_string(),
                result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
            });
        }

        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: CloudCallback,
        ) {
            let _ = callback.send(CloudEvent::GetRange {
                key: key.to_string(),
                start,
                end,
                result: CloudOutcome::Err(CloudError::Protocol("unsupported".to_string())),
            });
        }

        fn submit_head(&self, _key: &str, callback: CloudCallback) {
            drop(callback);
        }
    }

    // =========== CloudOutcome Tests ===========

    #[test]
    fn should_classify_provider_http_statuses() {
        // Arrange
        let statuses = [404, 401, 400, 503];

        // Act
        let errors = [
            CloudError::from_http_status(statuses[0], "missing"),
            CloudError::from_http_status(statuses[1], "credentials"),
            CloudError::from_http_status(statuses[2], "request"),
            CloudError::from_http_status(statuses[3], "unavailable"),
        ];

        // Assert
        assert!(matches!(errors[0], CloudError::NotFound(_)));
        assert!(matches!(errors[1], CloudError::Unauthorized(_)));
        assert!(matches!(errors[2], CloudError::InvalidRequest(_)));
        assert!(matches!(errors[3], CloudError::ServerError(_)));
    }

    #[test]
    fn should_classify_exhausted_retryable_http_statuses_as_server_errors() {
        // Arrange
        let statuses = [408, 425, 429];

        // Act
        let errors = statuses.map(|status| CloudError::from_http_status(status, "retry exhausted"));

        // Assert
        assert!(errors
            .iter()
            .all(|error| matches!(error, CloudError::ServerError(_))));
    }

    #[test]
    fn should_classify_http_408_as_timeout_without_relying_on_provider_detail_text() {
        // Arrange
        let error = CloudError::from_http_status(408, "request rejected");

        // Act
        let is_timeout = error.is_timeout();

        // Assert
        assert!(is_timeout, "HTTP 408 is intrinsically a request timeout");
    }

    #[test]
    fn should_not_classify_transport_diagnostic_text_as_timeout() {
        // Arrange: a host or certificate name can legitimately contain this
        // word without the transport failure being deadline exhaustion.
        let error = CloudError::Transport(
            "TLS certificate rejected for https://timeout.example".to_string(),
        );

        // Act
        let is_timeout = error.is_timeout();

        // Assert
        assert!(!is_timeout);
    }

    #[test]
    fn should_preserve_timeout_without_reclassifying_list_parser_errors() {
        // Arrange
        let timeout = MidgeError::Timeout("LIST deadline expired".to_string());
        let malformed = MidgeError::Internal("LIST response contained invalid XML".to_string());

        // Act
        let timeout = CloudError::from_protocol_or_timeout_error(timeout);
        let malformed = CloudError::from_protocol_or_timeout_error(malformed);

        // Assert
        assert!(matches!(timeout, CloudError::Timeout(_)));
        assert!(matches!(malformed, CloudError::Protocol(_)));
    }

    #[test]
    fn should_preserve_typed_timeout_when_crossing_storage_callback_bridge() {
        // Arrange
        let error = CloudError::from_http_status(408, "request rejected");

        // Act
        let outcome = cloud_to_storage_outcome::<()>(CloudOutcome::Err(error));

        // Assert
        assert!(matches!(
            outcome,
            StorageOutcome::Err(message)
                if crate::storage::storage_error_is_timeout(&message)
        ));
    }

    #[test]
    fn should_not_classify_disconnected_cloud_callback_as_timeout() {
        // Arrange
        let storage = CloudStorage::new_with_timeout(
            Arc::new(DroppedHeadCallbackBackend),
            "tenant".to_string(),
            std::time::Duration::from_secs(1),
        );
        let (sender, receiver) = mpsc::channel();

        // Act
        StorageBackend::submit_head_with_timeout(
            &storage,
            "metadata/manifest.json",
            std::time::Duration::from_secs(1),
            sender,
        );
        let event = receiver
            .recv()
            .expect("receive disconnected adapter result");

        // Assert
        assert!(matches!(
            event,
            StorageEvent::HeadComplete {
                result: StorageOutcome::Err(message),
                ..
            } if !crate::storage::storage_error_is_timeout(&message)
                && message.contains("closed")
        ));
    }

    #[test]
    fn should_not_submit_cloud_operations_given_operation_timeout_is_zero() {
        // Arrange
        let backend = Arc::new(ConditionalPutOnlyBackend::default());
        let storage = CloudStorage::new_with_timeout(
            backend.clone(),
            "tenant".to_string(),
            std::time::Duration::from_secs(1),
        );
        let (read_sender, read_receiver) = mpsc::channel();
        let (write_sender, write_receiver) = mpsc::channel();
        let (head_sender, head_receiver) = mpsc::channel();

        // Act
        StorageBackend::submit_read_with_timeout(
            &storage,
            "metadata/manifest.json",
            std::time::Duration::ZERO,
            read_sender,
        );
        StorageBackend::submit_write_with_headers_and_timeout(
            &storage,
            "metadata/manifest.json",
            b"manifest".to_vec(),
            vec![("If-None-Match".to_string(), "*".to_string())],
            std::time::Duration::ZERO,
            write_sender,
        );
        StorageBackend::submit_head_with_timeout(
            &storage,
            "metadata/manifest.json",
            std::time::Duration::ZERO,
            head_sender,
        );
        let read = read_receiver
            .recv()
            .expect("receive zero-budget read result");
        let write = write_receiver
            .recv()
            .expect("receive zero-budget write result");
        let head = head_receiver
            .recv()
            .expect("receive zero-budget head result");

        // Assert
        assert_eq!(backend.gets.load(Ordering::SeqCst), 0);
        assert_eq!(backend.puts.load(Ordering::SeqCst), 0);
        assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
        assert!(matches!(
            read,
            StorageEvent::ReadComplete {
                result: StorageOutcome::Err(message),
                ..
            } if crate::storage::storage_error_is_timeout(&message)
        ));
        assert!(matches!(
            write,
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(message),
                ..
            } if crate::storage::storage_error_is_timeout(&message)
        ));
        assert!(matches!(
            head,
            StorageEvent::HeadComplete {
                result: StorageOutcome::Err(message),
                ..
            } if crate::storage::storage_error_is_timeout(&message)
        ));
    }

    #[test]
    fn should_not_submit_delete_or_list_given_configured_callback_timeout_is_zero() {
        // Arrange
        let backend = Arc::new(ConditionalPutOnlyBackend::default());
        let storage = CloudStorage::new_with_timeout(
            backend.clone(),
            "tenant".to_string(),
            std::time::Duration::ZERO,
        );
        let (delete_sender, delete_receiver) = mpsc::channel();
        let (conditional_delete_sender, conditional_delete_receiver) = mpsc::channel();
        let (list_sender, list_receiver) = mpsc::channel();

        // Act
        StorageBackend::submit_delete(&storage, "metadata/manifest.json", delete_sender);
        StorageBackend::submit_delete_with_headers(
            &storage,
            "metadata/manifest.json",
            vec![("If-Match".to_string(), "etag".to_string())],
            conditional_delete_sender,
        );
        StorageBackend::submit_list(&storage, "metadata/", list_sender);
        let delete = delete_receiver
            .recv()
            .expect("receive zero-budget delete result");
        let conditional_delete = conditional_delete_receiver
            .recv()
            .expect("receive zero-budget conditional delete result");
        let list = list_receiver
            .recv()
            .expect("receive zero-budget list result");

        // Assert
        assert_eq!(backend.deletes.load(Ordering::SeqCst), 0);
        assert_eq!(backend.lists.load(Ordering::SeqCst), 0);
        assert!(matches!(
            delete,
            StorageEvent::DeleteComplete {
                result: StorageOutcome::Err(message),
                ..
            } if crate::storage::storage_error_is_timeout(&message)
        ));
        assert!(matches!(
            conditional_delete,
            StorageEvent::DeleteComplete {
                result: StorageOutcome::Err(message),
                ..
            } if crate::storage::storage_error_is_timeout(&message)
        ));
        assert!(matches!(
            list,
            StorageEvent::ListComplete {
                result: StorageOutcome::Err(message),
                ..
            } if crate::storage::storage_error_is_timeout(&message)
        ));
    }

    #[test]
    fn should_convert_engine_result_to_cloud_outcome() {
        // Arrange
        let ok_result: Result<i32, MidgeError> = Ok(100);
        let err_result: Result<i32, MidgeError> = Err(MidgeError::Corruption("test".into()));

        // Act
        let ok_outcome = cloud_outcome_from_result(ok_result);
        let err_outcome = cloud_outcome_from_result(err_result);

        // Assert: the success payload and the error message must survive the conversion,
        // not just the Ok/Err discriminant.
        match ok_outcome {
            CloudOutcome::Ok(value) => assert_eq!(value, 100),
            CloudOutcome::Err(e) => panic!("expected Ok(100), got Err({e:?})"),
        }
        match err_outcome {
            CloudOutcome::Err(CloudError::Protocol(message)) => {
                assert!(
                    message.contains("test"),
                    "converted error should preserve source message, got: {message}"
                );
            }
            other => {
                panic!("expected CloudError::Protocol wrapping the source error, got {other:?}")
            }
        }
    }

    /// Replaces the object immediately after taking the GET response snapshot.
    struct ReplacingGetBackend {
        inner: MockCloudBackend,
    }

    impl CloudBackend for ReplacingGetBackend {
        crate::storage::cloud::forward_cloud_backend!(inner; submit_put);

        fn submit_get(&self, key: &str, callback: CloudCallback) {
            self.inner.submit_get(key, callback);
            let (tx, _rx) = mpsc::channel();
            self.inner.submit_put(key, b"new".to_vec(), Vec::new(), tx);
        }

        fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback) {
            self.inner.submit_get_with_metadata(key, callback);
            let (tx, _rx) = mpsc::channel();
            self.inner.submit_put(key, b"new".to_vec(), Vec::new(), tx);
        }

        crate::storage::cloud::forward_cloud_backend!(inner; submit_head, submit_get_range, submit_delete);
    }

    fn replacing_get_storage() -> CloudStorage {
        let backend = Arc::new(ReplacingGetBackend {
            inner: MockCloudBackend::new(),
        });
        let storage = CloudStorage::new(backend, "tenant".to_string());
        let (tx, rx) = mpsc::channel();
        storage.submit_put("object", b"old".to_vec(), Vec::new(), tx);
        assert!(matches!(
            rx.recv().unwrap(),
            CloudEvent::Put { result: Ok(()), .. }
        ));
        storage
    }

    #[test]
    fn should_bind_proof_to_get_version_when_same_length_replacement_follows_get() {
        // Arrange
        let storage = replacing_get_storage();
        let (tx, rx) = mpsc::channel();
        storage.submit_head("object", tx);
        let CloudEvent::Head {
            result: Ok(original),
            ..
        } = rx.recv().unwrap()
        else {
            panic!("initial object must exist");
        };

        // Act
        let proof = blocking_cloud_object_proof(&storage, "object")
            .unwrap()
            .unwrap();

        // Assert
        assert_eq!(proof.bytes, b"old");
        assert_eq!(proof.metadata.etag, original.etag);
    }

    #[test]
    fn should_reject_stale_proof_mutations_when_same_length_replacement_follows_get() {
        // Arrange
        let storage = replacing_get_storage();
        let proof = blocking_cloud_object_proof(&storage, "object")
            .unwrap()
            .unwrap();
        let headers = object_match_precondition_headers(
            &proof.metadata.etag,
            proof.metadata.generation.as_deref(),
        )
        .unwrap();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_put("object", b"bad".to_vec(), headers.clone(), tx);
        let write = rx.recv().unwrap();
        let (tx, rx) = mpsc::channel();
        storage.submit_delete_with_headers("object", headers, tx);
        let delete = rx.recv().unwrap();

        // Assert
        assert!(matches!(
            write,
            CloudEvent::Put {
                result: Err(CloudError::PreconditionFailed(_)),
                ..
            }
        ));
        assert!(matches!(
            delete,
            CloudEvent::Delete {
                result: Err(CloudError::PreconditionFailed(_)),
                ..
            }
        ));
    }

    #[test]
    fn should_reject_proof_when_response_has_missing_identity_or_incorrect_length() {
        // Arrange
        let body = b"abc";
        let cases = [
            StorageObjectMetadata {
                size: 3,
                etag: " ".to_string(),
                generation: None,
            },
            StorageObjectMetadata {
                size: 3,
                etag: String::new(),
                generation: Some(" ".to_string()),
            },
            StorageObjectMetadata {
                size: 2,
                etag: "identity".to_string(),
                generation: None,
            },
            StorageObjectMetadata {
                size: 4,
                etag: "identity".to_string(),
                generation: None,
            },
        ];

        // Act
        let results: Vec<_> = cases
            .iter()
            .map(|metadata| validate_object_proof("object", body, metadata))
            .collect();

        // Assert
        assert!(results.iter().all(Result::is_err));
    }

    #[test]
    fn should_fail_closed_when_backend_lacks_metadata_bearing_get() {
        // Arrange
        let backend = Arc::new(ConditionalPutOnlyBackend::default());
        let storage = CloudStorage::new(backend.clone(), "tenant".to_string());

        // Act
        let result = blocking_cloud_object_proof(&storage, "object");

        // Assert
        assert!(result
            .unwrap_err()
            .contains("does not support metadata-bearing GET"));
        assert_eq!(backend.gets.load(Ordering::SeqCst), 0);
        assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
    }

    // =========== ObjectMetadata Tests ===========

    #[test]
    fn should_prefer_generation_when_building_object_match_precondition() {
        // Arrange
        let quoted_etag = "  \"etag-value\"  ";

        // Act
        let headers = object_match_precondition_headers(quoted_etag, Some(" 42 "));

        // Assert
        assert_eq!(
            headers,
            Some(vec![(
                "x-goog-if-generation-match".to_string(),
                "42".to_string()
            )])
        );
    }

    #[test]
    fn should_preserve_quoted_etag_when_building_object_match_precondition() {
        // Arrange
        let quoted_etag = "  \"etag-value\"  ";

        // Act
        let headers = object_match_precondition_headers(quoted_etag, None);

        // Assert
        assert_eq!(
            headers,
            Some(vec![("If-Match".to_string(), "\"etag-value\"".to_string())])
        );
    }

    // =========== CloudStorage Routing Tests ===========

    #[test]
    fn should_apply_configured_callback_timeout_to_blocking_cloud_proof() {
        // Arrange
        let storage = CloudStorage::new_with_timeout(
            Arc::new(DelayedMissingGetBackend),
            "tenant".to_string(),
            std::time::Duration::from_millis(5),
        );

        // Act
        let error = blocking_cloud_object_proof(&storage, "metadata/manifest.json")
            .expect_err("configured callback timeout must bound proof reads");

        // Assert
        assert!(
            error.contains("timed out")
                || (error.contains("Timeout") && error.contains("deadline")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn should_report_storage_callback_timeout_when_cloud_backend_is_slow() {
        // Arrange
        let storage = CloudStorage::new_with_timeout(
            Arc::new(DelayedMissingGetBackend),
            "tenant".to_string(),
            std::time::Duration::from_millis(5),
        );
        let (sender, receiver) = mpsc::channel();

        // Act
        StorageBackend::submit_read(&storage, "metadata/manifest.json", sender);
        let event = receiver.recv().expect("receive bounded adapter result");

        // Assert
        assert!(matches!(
            event,
            StorageEvent::ReadComplete {
                result: StorageOutcome::Err(message),
                ..
            } if message.contains("timed out")
        ));
    }

    #[test]
    fn should_apply_operation_timeout_to_cloud_read_adapter_when_shorter_than_configured_timeout() {
        // Arrange: the one-second provider response stays well beyond both the
        // 5 ms operation budget and the scheduler-tolerant 500 ms assertion.
        let storage = CloudStorage::new_with_timeout(
            Arc::new(DelayedMissingGetBackend),
            "tenant".to_string(),
            std::time::Duration::from_secs(1),
        );
        let (sender, receiver) = mpsc::channel();

        // Act
        let started = std::time::Instant::now();
        StorageBackend::submit_read_with_timeout(
            &storage,
            "metadata/manifest.json",
            std::time::Duration::from_millis(5),
            sender,
        );
        let event = receiver.recv().expect("receive bounded adapter result");

        // Assert
        assert!(started.elapsed() < std::time::Duration::from_millis(500));
        assert!(matches!(
            event,
            StorageEvent::ReadComplete {
                result: StorageOutcome::Err(message),
                ..
            } if message.contains("timed out")
        ));
    }

    #[test]
    fn should_apply_operation_timeout_to_cloud_head_adapter_when_shorter_than_configured_timeout() {
        // Arrange: keep the same wide separation for the HEAD adapter.
        let storage = CloudStorage::new_with_timeout(
            Arc::new(DelayedMissingGetBackend),
            "tenant".to_string(),
            std::time::Duration::from_secs(1),
        );
        let (sender, receiver) = mpsc::channel();

        // Act
        let started = std::time::Instant::now();
        StorageBackend::submit_head_with_timeout(
            &storage,
            "metadata/manifest.json",
            std::time::Duration::from_millis(5),
            sender,
        );
        let event = receiver.recv().expect("receive bounded adapter result");

        // Assert
        assert!(started.elapsed() < std::time::Duration::from_millis(500));
        assert!(matches!(
            event,
            StorageEvent::HeadComplete {
                result: StorageOutcome::Err(message),
                ..
            } if message.contains("timed out")
        ));
    }

    #[test]
    fn should_apply_operation_timeout_to_cloud_cas_adapter_when_shorter_than_configured_timeout() {
        // Arrange: keep the same wide separation for the conditional-write adapter.
        let storage = CloudStorage::new_with_timeout(
            Arc::new(DelayedMissingGetBackend),
            "tenant".to_string(),
            std::time::Duration::from_secs(1),
        );
        let (sender, receiver) = mpsc::channel();

        // Act
        let started = std::time::Instant::now();
        StorageBackend::submit_write_with_headers_and_timeout(
            &storage,
            "metadata/manifest.json",
            b"manifest".to_vec(),
            vec![("If-None-Match".to_string(), "*".to_string())],
            std::time::Duration::from_millis(5),
            sender,
        );
        let event = receiver.recv().expect("receive bounded adapter result");

        // Assert
        assert!(started.elapsed() < std::time::Duration::from_millis(500));
        assert!(matches!(
            event,
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(message),
                ..
            } if message.contains("timed out")
        ));
    }

    #[test]
    fn should_route_namespace_put_operation() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        let data = vec![1, 2, 3];

        // Act
        storage.submit_put("file", data, vec![], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Put { key, result } => {
                assert_eq!(key, "midge/file");
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete"),
        }
    }

    #[test]
    fn should_delegate_conditional_put_without_head_preflight() {
        // Arrange
        let backend = Arc::new(ConditionalPutOnlyBackend::default());
        let storage = CloudStorage::new(backend.clone(), "tenant".to_string());
        let (sender, receiver) = mpsc::channel();

        // Act
        storage.submit_put(
            "lease/primary",
            b"holder".to_vec(),
            vec![("If-None-Match".to_string(), "*".to_string())],
            sender,
        );
        let event = receiver.recv().expect("receive conditional PUT result");

        // Assert
        assert!(matches!(
            event,
            CloudEvent::Put {
                result: CloudOutcome::Ok(()),
                ..
            }
        ));
        assert_eq!(backend.heads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn should_route_namespace_get_operation() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile", vec![1, 2, 3], vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_get("testfile", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Get { key, result } => {
                assert!(key.starts_with("midge/"));
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_route_delete_with_namespace_applied() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_delete("file", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Delete { key, result } => {
                assert_eq!(key, "midge/file");
                assert!(result.is_ok());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_route_head_return_metadata() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile", vec![1, 2, 3], vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_head("testfile", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Head { key, result } => {
                assert!(key.starts_with("midge/"));
                match result {
                    CloudOutcome::Ok(metadata) => {
                        assert_eq!(metadata.size, 3);
                    }
                    CloudOutcome::Err(_) => panic!("Expected Ok metadata"),
                }
            }
            _ => panic!("Expected HeadComplete"),
        }
    }

    #[test]
    fn should_honor_if_match_header_on_put() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file1", vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        // Get current etag via HEAD
        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("file1", head_tx);
        let head_event = head_rx.recv().unwrap();
        let current_etag = match head_event {
            CloudEvent::Head { result, .. } => match result {
                CloudOutcome::Ok(meta) => meta.etag,
                CloudOutcome::Err(_) => panic!("expected head ok"),
            },
            _ => panic!("expected head event"),
        };

        // Act - conditional update with matching If-Match
        let headers = vec![("If-Match".into(), current_etag.clone())];
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file1", vec![9, 9, 9], headers, put_tx);
        let put_event = put_rx.recv().unwrap();

        // Assert - success and new etag changed
        match put_event {
            CloudEvent::Put { result, .. } => assert!(result.is_ok()),
            _ => panic!("expected put complete"),
        }

        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("file1", head_tx);
        let head_event = head_rx.recv().unwrap();
        let new_etag = match head_event {
            CloudEvent::Head { result, .. } => match result {
                CloudOutcome::Ok(meta) => meta.etag,
                CloudOutcome::Err(_) => panic!("expected head ok"),
            },
            _ => panic!("expected head event"),
        };

        assert_ne!(current_etag, new_etag);
    }

    #[test]
    fn should_fail_put_when_if_match_mismatch() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file2", vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        // Act - conditional update with non-matching If-Match
        let headers = vec![("If-Match".into(), "mock-gen-999".into())];
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file2", vec![2], headers, put_tx);
        let put_event = put_rx.recv().unwrap();

        // Assert - precondition failed
        match put_event {
            CloudEvent::Put { result, .. } => assert!(result.is_err()),
            _ => panic!("expected put complete"),
        }
    }

    #[test]
    fn should_fail_if_match_on_missing_object() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // Act - If-Match on non-existent key should fail
        let headers = vec![("If-Match".into(), "mock-gen-1".into())];
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("no-such", vec![1], headers, put_tx);
        let put_event = put_rx.recv().unwrap();

        // Assert - precondition failed
        match put_event {
            CloudEvent::Put { result, .. } => assert!(result.is_err()),
            _ => panic!("expected put complete"),
        }
    }

    #[test]
    fn should_respect_if_none_match_star_on_existing_object() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file3", vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        // Act - conditional create should fail when object exists
        let headers = vec![("If-None-Match".into(), "*".into())];
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file3", vec![2], headers, put_tx);
        let put_event = put_rx.recv().unwrap();

        // Assert - precondition failed
        match put_event {
            CloudEvent::Put { result, .. } => assert!(result.is_err()),
            _ => panic!("expected put complete"),
        }
    }

    #[test]
    fn should_enforce_if_match_if_none_match_given_concurrent_remote_writers_when_publishing() {
        fn concurrent_puts(
            storage: &Arc<CloudStorage>,
            values: [Vec<u8>; 2],
            headers: [Vec<(String, String)>; 2],
        ) -> Vec<CloudOutcome<()>> {
            let barrier = Arc::new(std::sync::Barrier::new(3));
            let mut handles = Vec::new();
            for (value, headers) in values.into_iter().zip(headers) {
                let storage = Arc::clone(storage);
                let barrier = Arc::clone(&barrier);
                handles.push(std::thread::spawn(move || {
                    let (sender, receiver) = mpsc::channel();
                    barrier.wait();
                    storage.submit_put("concurrent", value, headers, sender);
                    match receiver.recv().expect("receive concurrent PUT result") {
                        CloudEvent::Put { result, .. } => result,
                        event => panic!("expected PUT event, got {event:?}"),
                    }
                }));
            }
            barrier.wait();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("concurrent writer panicked"))
                .collect()
        }

        // Arrange
        let storage = Arc::new(CloudStorage::with_mock());

        // Act
        let creates = concurrent_puts(
            &storage,
            [b"create-a".to_vec(), b"create-b".to_vec()],
            [
                vec![("If-None-Match".to_string(), "*".to_string())],
                vec![("If-None-Match".to_string(), "*".to_string())],
            ],
        );
        let (head_sender, head_receiver) = mpsc::channel();
        storage.submit_head("concurrent", head_sender);
        let etag = match head_receiver
            .recv()
            .expect("receive HEAD after create race")
        {
            CloudEvent::Head {
                result: CloudOutcome::Ok(metadata),
                ..
            } => metadata.etag,
            event => panic!("expected successful HEAD event, got {event:?}"),
        };
        let updates = concurrent_puts(
            &storage,
            [b"update-a".to_vec(), b"update-b".to_vec()],
            [
                vec![("If-Match".to_string(), etag.clone())],
                vec![("If-Match".to_string(), etag)],
            ],
        );

        // Assert
        for outcomes in [&creates, &updates] {
            assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
            assert_eq!(
                outcomes
                    .iter()
                    .filter(|result| matches!(result, Err(CloudError::PreconditionFailed(_))))
                    .count(),
                1
            );
        }
    }

    #[test]
    fn should_honor_if_match_header_on_delete() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("delete-file", vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("delete-file", head_tx);
        let etag = match head_rx.recv().unwrap() {
            CloudEvent::Head {
                result: CloudOutcome::Ok(metadata),
                ..
            } => metadata.etag,
            other => panic!("expected HEAD ok, got {other:?}"),
        };

        // Act
        let (delete_tx, delete_rx) = mpsc::channel();
        storage.submit_delete_with_headers(
            "delete-file",
            vec![("If-Match".into(), etag)],
            delete_tx,
        );

        // Assert
        match delete_rx.recv().unwrap() {
            CloudEvent::Delete { result, .. } => assert!(result.is_ok()),
            other => panic!("expected delete complete, got {other:?}"),
        }
        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("delete-file", head_tx);
        match head_rx.recv().unwrap() {
            CloudEvent::Head { result, .. } => assert!(result.is_err()),
            other => panic!("expected HEAD complete, got {other:?}"),
        }
    }

    #[test]
    fn should_reject_delete_when_if_match_mismatches() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("stale-delete-file", vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        let (delete_tx, delete_rx) = mpsc::channel();
        storage.submit_delete_with_headers(
            "stale-delete-file",
            vec![("If-Match".into(), "mock-gen-999".into())],
            delete_tx,
        );

        // Assert
        match delete_rx.recv().unwrap() {
            CloudEvent::Delete { result, .. } => assert!(result.is_err()),
            other => panic!("expected delete complete, got {other:?}"),
        }
        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("stale-delete-file", head_tx);
        match head_rx.recv().unwrap() {
            CloudEvent::Head {
                result: CloudOutcome::Ok(_),
                ..
            } => {}
            other => panic!("expected object to survive stale delete, got {other:?}"),
        }
    }

    #[test]
    fn should_route_get_range_with_bounds() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("rangefile", vec![1, 2, 3, 4, 5], vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_get_range("rangefile", 1, Some(4), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetRange {
                key,
                start,
                end,
                result,
            } => {
                assert!(key.starts_with("midge/"));
                assert_eq!(start, 1);
                assert_eq!(end, Some(4));
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetRangeComplete"),
        }
    }

    #[test]
    fn should_handle_get_range_with_none_end_bound() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_get_range("file", 0, None, tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetRange { end, .. } => {
                assert_eq!(end, None);
            }
            _ => panic!("Expected GetRangeComplete"),
        }
    }

    // =========== CloudEvent Tests ===========

    #[test]
    fn should_send_list_complete_event_via_callback() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("prefix/file1", vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("prefix/file2", vec![2], vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_list("prefix", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::List { prefix, result } => {
                assert_eq!(prefix, "midge/prefix");
                match result {
                    CloudOutcome::Ok(items) => {
                        assert!(items.len() >= 2);
                        assert!(items.iter().any(|k| k.contains("file1")));
                        assert!(items.iter().any(|k| k.contains("file2")));
                    }
                    CloudOutcome::Err(_) => panic!("Expected Ok result"),
                }
            }
            _ => panic!("Expected ListComplete"),
        }
    }

    // =========== Data Handling & Integration Tests ===========

    #[test]
    fn should_handle_large_file_operations() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let large_data = vec![42u8; 1_000_000]; // 1 MB
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_put("largefile", large_data.clone(), vec![], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Put { result, .. } => {
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete"),
        }

        // Verify we can retrieve it
        let (tx, rx) = mpsc::channel();
        storage.submit_get("largefile", tx);
        let event = rx.recv().unwrap();

        match event {
            CloudEvent::Get { result, .. } => match result {
                CloudOutcome::Ok(data) => {
                    assert_eq!(data.len(), 1_000_000);
                }
                CloudOutcome::Err(_) => panic!("Expected Ok"),
            },
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_preserve_binary_data_fidelity() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let binary_data = vec![0u8, 1u8, 255u8, 254u8, 127u8, 128u8];

        // Act: put and get binary data round-trip
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("binaryfile", binary_data.clone(), vec![], put_tx);
        let _ = put_rx.recv();

        let (tx, rx) = mpsc::channel();
        storage.submit_get("binaryfile", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Get { result, .. } => match result {
                CloudOutcome::Ok(data) => {
                    assert_eq!(data, binary_data, "binary data must be preserved exactly");
                }
                CloudOutcome::Err(_) => panic!("Expected Ok result"),
            },
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_dispatch_all_cloud_operations_successfully() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // Act: dispatch every supported operation through the same production queue.

        let (tx, rx) = mpsc::channel();
        storage.submit_put("f1", vec![1, 2], vec![], tx);

        // Assert: each operation must reach the backend with the namespaced key and
        // report its real outcome, not just "didn't panic".
        match rx.recv().unwrap() {
            CloudEvent::Put { key, result } => {
                assert_eq!(key, "midge/f1");
                assert!(result.is_ok());
            }
            other => panic!("expected Put event, got {other:?}"),
        }

        // f2 was never put, so the get is expected to miss.
        let (tx, rx) = mpsc::channel();
        storage.submit_get("f2", tx);
        match rx.recv().unwrap() {
            CloudEvent::Get { key, result } => {
                assert_eq!(key, "midge/f2");
                assert!(result.is_err());
            }
            other => panic!("expected Get event, got {other:?}"),
        }

        let (tx, rx) = mpsc::channel();
        storage.submit_delete("f3", tx);
        match rx.recv().unwrap() {
            CloudEvent::Delete { key, result } => {
                assert_eq!(key, "midge/f3");
                assert!(result.is_ok());
            }
            other => panic!("expected Delete event, got {other:?}"),
        }

        let (tx, rx) = mpsc::channel();
        storage.submit_put("prefix/f", vec![9], vec![], tx);
        let _ = rx.recv();
        let (tx, rx) = mpsc::channel();
        storage.submit_list("prefix", tx);
        match rx.recv().unwrap() {
            CloudEvent::List { prefix, result } => {
                assert_eq!(prefix, "midge/prefix");
                let items = result.expect("list should succeed");
                assert!(items.iter().any(|k| k.contains("prefix/f")));
            }
            other => panic!("expected List event, got {other:?}"),
        }

        let (tx, rx) = mpsc::channel();
        storage.submit_put("f4", vec![1, 2, 3], vec![], tx);
        let _ = rx.recv();
        let (tx, rx) = mpsc::channel();
        storage.submit_head("f4", tx);
        match rx.recv().unwrap() {
            CloudEvent::Head { key, result } => {
                assert_eq!(key, "midge/f4");
                let metadata = result.expect("head should succeed for an existing object");
                assert_eq!(metadata.size, 3);
            }
            other => panic!("expected Head event, got {other:?}"),
        }

        let (tx, rx) = mpsc::channel();
        storage.submit_get_range("f4", 0, Some(2), tx);
        match rx.recv().unwrap() {
            CloudEvent::GetRange {
                key,
                start,
                end,
                result,
            } => {
                assert_eq!(key, "midge/f4");
                assert_eq!(start, 0);
                assert_eq!(end, Some(2));
                assert!(result.is_ok());
            }
            other => panic!("expected GetRange event, got {other:?}"),
        }
    }

    #[test]
    fn should_handle_get_missing_file_gracefully() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_get("nonexistent", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Get { result, .. } => {
                assert!(result.is_err());
            }
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_handle_metadata_for_empty_files() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // Put an empty file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("emptyfile", vec![], vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_head("emptyfile", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Head { result, .. } => match result {
                CloudOutcome::Ok(metadata) => {
                    assert_eq!(metadata.size, 0);
                }
                CloudOutcome::Err(_) => panic!("Expected Ok metadata"),
            },
            _ => panic!("Expected HeadComplete"),
        }
    }
}
