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

use super::{StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome};
#[cfg(test)]
use crate::common::MidgeError;
#[cfg(test)]
use parking_lot::Mutex;
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;

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
/// writer (S3 412/409, Azure 412 `ConditionNotMet`, GCS 412
/// `conditionNotMet`) — every other failure mode (auth, transport, server
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
    PreconditionFailed(String),
    /// Authentication or authorization failure (401 / 403).
    #[cfg(any(test, feature = "cloud-common"))]
    Unauthorized(String),
    /// The provider rejected the request as malformed (4xx other than
    /// not-found/precondition/auth).
    #[cfg(any(test, feature = "cloud-common"))]
    InvalidRequest(String),
    /// The provider reported a server-side failure (5xx).
    #[cfg(any(test, feature = "cloud-common"))]
    ServerError(String),
    /// The request could not reach the provider or timed out (network,
    /// DNS, TLS, connection, or client-side timeout failure).
    Transport(String),
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

    /// Classify a raw HTTP status code from a provider response.
    ///
    /// This is the single place that decides which statuses represent a
    /// genuine conditional-write/delete race (412 Precondition Failed / 409
    /// Conflict — S3, Azure, and GCS all use 412 for `If-Match`/
    /// `If-None-Match` mismatches; some S3-compatible services use 409 for
    /// the same case) versus every other failure mode. Providers call this
    /// from their response mapper, where `status` is still a real `u16`
    /// rather than a formatted string — the point this module exists to
    /// preserve.
    #[cfg(any(test, feature = "cloud-common"))]
    #[must_use]
    pub(crate) fn from_http_status(status: u16, detail: impl std::fmt::Display) -> Self {
        match status {
            404 => Self::NotFound(format!("status {status}: {detail}")),
            401 | 403 => Self::Unauthorized(format!("status {status}: {detail}")),
            409 | 412 => Self::PreconditionFailed(format!("status {status}: {detail}")),
            400..=499 => Self::InvalidRequest(format!("status {status}: {detail}")),
            500..=599 => Self::ServerError(format!("status {status}: {detail}")),
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
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

impl std::error::Error for CloudError {}

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
    Put {
        key: String,
        result: CloudOutcome<()>,
    },
    Get {
        key: String,
        result: CloudOutcome<Vec<u8>>,
    },
    #[cfg(test)]
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
    #[cfg(test)]
    pub last_modified: u64,
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
    pub fn new(size: u64, etag: String, last_modified: u64) -> Self {
        #[cfg(not(test))]
        let _ = last_modified;
        Self {
            size,
            etag,
            #[cfg(test)]
            last_modified,
            generation: None,
        }
    }

    #[cfg(feature = "cloud-gcp")]
    pub fn with_generation(
        size: u64,
        etag: String,
        last_modified: u64,
        generation: impl Into<String>,
    ) -> Self {
        #[cfg(not(test))]
        let _ = last_modified;
        Self {
            size,
            etag,
            #[cfg(test)]
            last_modified,
            generation: Some(generation.into()),
        }
    }
}

/// Non-blocking cloud backend interface used by the engine.
pub trait CloudBackend: Send + Sync + 'static {
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
    #[cfg(test)]
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback);
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
                ObjectMetadata::new(data.len() as u64, format!("mock-gen-{gen}"), 0)
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

pub(crate) fn blocking_cloud_object_proof(
    cloud: &CloudStorage,
    key: &str,
) -> Result<Option<CloudObjectProof>, String> {
    let (get_tx, get_rx) = std::sync::mpsc::channel();
    cloud.submit_get(key, get_tx);
    let bytes = match get_rx.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(CloudEvent::Get {
            result: CloudOutcome::Ok(bytes),
            ..
        }) => bytes,
        Ok(CloudEvent::Get {
            result: CloudOutcome::Err(error),
            ..
        }) if is_not_found_error(&error) => return Ok(None),
        Ok(CloudEvent::Get {
            result: CloudOutcome::Err(error),
            ..
        }) => return Err(format!("cloud object '{key}' is unreadable: {error}")),
        Ok(other) => {
            return Err(format!(
                "unexpected cloud object GET response for '{key}': {other:?}"
            ))
        }
        Err(error) => return Err(format!("cloud object GET timed out for '{key}': {error}")),
    };

    let (head_tx, head_rx) = std::sync::mpsc::channel();
    cloud.submit_head(key, head_tx);
    let metadata = match head_rx.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(CloudEvent::Head {
            result: CloudOutcome::Ok(metadata),
            ..
        }) => storage_object_metadata(metadata),
        Ok(CloudEvent::Head {
            result: CloudOutcome::Err(error),
            ..
        }) => {
            return Err(format!(
                "cloud object '{key}' identity is unreadable after GET: {error}"
            ))
        }
        Ok(other) => {
            return Err(format!(
                "unexpected cloud object HEAD response for '{key}': {other:?}"
            ))
        }
        Err(error) => return Err(format!("cloud object HEAD timed out for '{key}': {error}")),
    };

    let bytes_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if metadata.size != bytes_len {
        return Err(format!(
            "cloud object '{key}' changed during validation: read={bytes_len}, head={}",
            metadata.size
        ));
    }

    Ok(Some(CloudObjectProof { bytes, metadata }))
}

impl CloudStorage {
    #[cfg(any(test, feature = "cloud-common"))]
    pub fn new(backend: Arc<dyn CloudBackend>, namespace: String) -> Self {
        Self { backend, namespace }
    }

    #[cfg(test)]
    pub fn with_mock() -> Self {
        let backend = Arc::new(MockCloudBackend::new());
        Self::new(backend, "midge".to_string())
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
        if let Some(error) = self.precondition_error(&full_key, &headers) {
            let _ = callback.send(CloudEvent::Put {
                key: full_key,
                result: CloudOutcome::Err(error),
            });
            return;
        }
        self.backend.submit_put(&full_key, data, headers, callback);
    }

    pub fn submit_get(&self, key: &str, callback: CloudCallback) {
        let full_key = self.full_path(key);
        self.backend.submit_get(&full_key, callback);
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

    fn precondition_error(
        &self,
        full_key: &str,
        headers: &[(String, String)],
    ) -> Option<CloudError> {
        let if_none_match = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-none-match"))
            .map(|(_, value)| value.trim().to_string());
        let if_match = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-match"))
            .map(|(_, value)| value.trim().trim_matches('"').to_string());

        if if_none_match.as_deref() != Some("*") && if_match.is_none() {
            return None;
        }

        let (tx, rx) = std::sync::mpsc::channel();
        self.backend.submit_head(full_key, tx);
        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(CloudEvent::Head { result, .. }) => match result {
                CloudOutcome::Ok(metadata) => {
                    if if_none_match.as_deref() == Some("*") {
                        Some(CloudError::PreconditionFailed(
                            "object already exists".to_string(),
                        ))
                    } else if let Some(expected) = if_match {
                        let current = metadata.etag.trim_matches('"');
                        if current == expected {
                            None
                        } else {
                            Some(CloudError::PreconditionFailed("etag mismatch".to_string()))
                        }
                    } else {
                        None
                    }
                }
                CloudOutcome::Err(error) => {
                    if error.is_not_found() {
                        if if_match.is_some() {
                            Some(CloudError::PreconditionFailed("object missing".to_string()))
                        } else {
                            None
                        }
                    } else {
                        Some(CloudError::Protocol(format!(
                            "precondition check failed: {error}"
                        )))
                    }
                }
            },
            Ok(other) => Some(CloudError::Protocol(format!(
                "unexpected precondition HEAD response: {other:?}"
            ))),
            Err(error) => Some(CloudError::Transport(format!(
                "precondition HEAD timed out: {error}"
            ))),
        }
    }
}

pub(crate) fn is_not_found_error(error: &CloudError) -> bool {
    error.is_not_found()
}

#[cfg(test)]
fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl StorageBackend for CloudStorage {
    fn submit_read(&self, key: &str, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_get(key, tx);
        if let Ok(CloudEvent::Get { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(data) => StorageOutcome::Ok(data),
                CloudOutcome::Err(err) => StorageOutcome::Err(err.to_string()),
            };
            let event = StorageEvent::ReadComplete {
                key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_put(key, data, vec![], tx);
        if let Ok(CloudEvent::Put { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(()) => StorageOutcome::Ok(()),
                CloudOutcome::Err(err) => StorageOutcome::Err(err.to_string()),
            };
            let event = StorageEvent::WriteComplete {
                key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_put(key, data, headers, tx);
        if let Ok(CloudEvent::Put { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(()) => StorageOutcome::Ok(()),
                CloudOutcome::Err(err) => StorageOutcome::Err(err.to_string()),
            };
            let event = StorageEvent::WriteComplete {
                key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_delete(self, key, tx);
        if let Ok(CloudEvent::Delete { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(()) => StorageOutcome::Ok(()),
                CloudOutcome::Err(err) => StorageOutcome::Err(err.to_string()),
            };
            let event = StorageEvent::DeleteComplete {
                key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_delete_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_delete_with_headers(self, key, headers, tx);
        if let Ok(CloudEvent::Delete { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(()) => StorageOutcome::Ok(()),
                CloudOutcome::Err(err) => StorageOutcome::Err(err.to_string()),
            };
            let event = StorageEvent::DeleteComplete {
                key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    #[cfg(test)]
    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_list(self, prefix, tx);
        if let Ok(CloudEvent::List {
            prefix: key_prefix,
            result,
        }) = rx.recv()
        {
            let outcome = match result {
                CloudOutcome::Ok(items) => StorageOutcome::Ok(items),
                CloudOutcome::Err(err) => StorageOutcome::Err(err.to_string()),
            };
            let event = StorageEvent::ListComplete {
                prefix: key_prefix,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_head(self, key, tx);
        let event = match rx.recv() {
            Ok(CloudEvent::Head { key, result }) => {
                let outcome = match result {
                    CloudOutcome::Ok(metadata) => StorageOutcome::Ok(StorageObjectMetadata {
                        size: metadata.size,
                        etag: metadata.etag,
                        generation: metadata.generation,
                    }),
                    CloudOutcome::Err(err) => StorageOutcome::Err(err.to_string()),
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
            Err(error) => StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(format!("cloud HEAD channel closed: {error}")),
            },
        };
        let _ = callback.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

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
    fn should_discriminate_success_from_failure_outcomes() {
        // Arrange
        let ok_outcome: CloudOutcome<String> = CloudOutcome::Ok("success".into());
        let err_outcome: CloudOutcome<String> =
            CloudOutcome::Err(CloudError::Protocol("failure".to_string()));

        // Act
        let ok_is_ok = ok_outcome.is_ok();
        let ok_is_err = ok_outcome.is_err();
        let err_is_ok = err_outcome.is_ok();
        let err_is_err = err_outcome.is_err();

        // Assert
        assert!(ok_is_ok);
        assert!(!ok_is_err);
        assert!(!err_is_ok);
        assert!(err_is_err);
    }

    #[test]
    fn should_clone_outcomes_with_different_types() {
        // Arrange
        let int_ok = CloudOutcome::Ok(42);
        let int_err: CloudOutcome<i32> =
            CloudOutcome::Err(CloudError::Protocol("error".to_string()));

        // Act
        let int_ok_cloned = int_ok.clone();
        let int_err_cloned = int_err.clone();

        // Assert
        assert!(int_ok_cloned.is_ok());
        assert!(int_err_cloned.is_err());
    }

    #[test]
    fn should_convert_engine_result_to_cloud_outcome() {
        // Arrange
        let ok_result: Result<i32, MidgeError> = Ok(100);
        let err_result: Result<i32, MidgeError> = Err(MidgeError::Corruption("test".into()));

        // Act
        let ok_outcome = cloud_outcome_from_result(ok_result);
        let err_outcome = cloud_outcome_from_result(err_result);

        // Assert
        assert!(ok_outcome.is_ok());
        assert!(err_outcome.is_err());
    }

    // =========== ObjectMetadata Tests ===========

    #[test]
    fn should_create_object_metadata_with_correct_fields() {
        // Arrange
        let metadata = ObjectMetadata::new(1024, "etag123".into(), 1_000_000);

        // Act
        let cloned = metadata.clone();

        // Assert
        assert_eq!(cloned.size, 1024);
        assert_eq!(cloned.etag, "etag123");
        assert_eq!(cloned.last_modified, 1_000_000);
    }

    #[test]
    fn should_handle_metadata_with_boundary_sizes() {
        // Arrange
        let zero_etag = "zero".to_string();
        let max_etag = "max".to_string();

        // Act
        let zero_size = ObjectMetadata::new(0, zero_etag, 100);
        let max_size = ObjectMetadata::new(u64::MAX, max_etag, 200);

        // Assert
        assert_eq!(zero_size.size, 0);
        assert_eq!(max_size.size, u64::MAX);
    }

    #[test]
    fn should_handle_metadata_with_empty_etag() {
        // Arrange
        let empty_etag = String::new();

        // Act
        let metadata = ObjectMetadata::new(100, empty_etag, 1000);

        // Assert
        assert_eq!(metadata.etag, "");
        assert_eq!(metadata.size, 100);
    }

    // =========== CloudStorage Routing Tests ===========

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
    fn should_route_list_with_namespace_applied() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put multiple files
        let (tx, rx) = mpsc::channel();
        storage.submit_put("prefix/file1", vec![1], vec![], tx);
        let _ = rx.recv();

        let (tx, rx) = mpsc::channel();
        storage.submit_put("prefix/file2", vec![2], vec![], tx);
        let _ = rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_list("prefix", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::List { prefix, result } => {
                assert!(prefix.starts_with("midge/"));
                match result {
                    CloudOutcome::Ok(items) => {
                        assert!(!items.is_empty());
                    }
                    CloudOutcome::Err(_) => panic!("Expected Ok result"),
                }
            }
            _ => panic!("Expected ListComplete"),
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
    fn should_send_put_complete_event_via_callback() {
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
    fn should_send_get_complete_event_via_callback() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // First put a file so we can get it
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile", vec![1, 2, 3], vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        storage.submit_get("testfile", tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::Get { key, result } => {
                assert_eq!(key, "midge/testfile");
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetComplete"),
        }
    }

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

        // Act

        // Put operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_put("f1", vec![1, 2], vec![], tx);

        // Get operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_get("f2", tx);

        // Delete operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_delete("f3", tx);

        // List operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_list("prefix", tx);

        // Head operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_head("f4", tx);

        // Get range operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_get_range("f5", 0, Some(100), tx);

        // Assert
        // Smoke test: all calls complete without panic and storage remains valid.
        assert_eq!(storage.namespace, "midge");
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
