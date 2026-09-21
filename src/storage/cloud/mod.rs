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

mod blocking;
mod config;
#[cfg(feature = "cloud-common")]
pub(crate) mod range;
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

pub(crate) use blocking::BlockingCloud;
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

/// Bound the provider request by the same budget the storage adapter waits
/// on, replacing any timeout the caller already supplied. Without it the
/// provider falls back to the executor default and a mutation can commit
/// remotely after the caller has already reported a timeout.
pub(crate) fn set_request_timeout_header(
    headers: &mut Vec<(String, String)>,
    timeout: std::time::Duration,
) {
    headers.retain(|(name, _)| !name.eq_ignore_ascii_case(REQUEST_TIMEOUT_HEADER));
    headers.push((
        REQUEST_TIMEOUT_HEADER.into(),
        timeout.as_millis().max(1).to_string(),
    ));
}

/// Wire headers plus the provider request timeout split out of them.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub(crate) type SplitRequestHeaders = (Vec<(String, String)>, Option<std::time::Duration>);

/// Split the internal timeout pseudo-header from the headers a provider
/// sends on the wire.
#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
pub(crate) fn split_request_timeout_header(
    headers: Vec<(String, String)>,
) -> Result<SplitRequestHeaders, String> {
    let mut timeout = None;
    let mut wire_headers = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        if name.eq_ignore_ascii_case(REQUEST_TIMEOUT_HEADER) {
            let milliseconds = value
                .parse::<u64>()
                .map_err(|error| format!("invalid internal request timeout: {error}"))?;
            timeout = Some(std::time::Duration::from_millis(milliseconds));
        } else {
            wire_headers.push((name, value));
        }
    }
    Ok((wire_headers, timeout))
}

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
    /// Carry admission through a bounded read until the backend finishes.
    /// Implementations whose callback can precede payload release must override
    /// this method and attach the reservation to the underlying operation.
    fn submit_get_range_with_reservation(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
        callback: CloudCallback,
    ) {
        let start = range.start;
        let end = range.end;
        let Some(reservation) = reservation else {
            self.submit_get_range_with_identity(key, start, end, expected, timeout, callback);
            return;
        };
        match crate::storage::retained_callback::retain(callback.clone(), reservation) {
            Ok(retained) => {
                self.submit_get_range_with_identity(key, start, end, expected, timeout, retained);
            }
            Err(error) => {
                let _ = callback.send(CloudEvent::GetRange {
                    key: key.to_string(),
                    start,
                    end: Some(end),
                    result: Err(CloudError::Protocol(format!(
                        "retain range completion: {error}"
                    ))),
                });
            }
        }
    }

    /// Exact bounded read with an identity precondition; no whole-object fallback.
    fn submit_get_range_with_identity(
        &self,
        key: &str,
        start: u64,
        end: u64,
        _expected: StorageObjectMetadata,
        _timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        let _ = callback.send(CloudEvent::GetRange {
            key: key.to_string(),
            start,
            end: Some(end),
            result: Err(CloudError::Protocol(
                "conditional range reads unsupported".into(),
            )),
        });
    }

    /// Override the default deadline applied to provider HTTP requests.
    #[cfg(feature = "cloud-common")]
    fn set_request_timeout(&self, _timeout: std::time::Duration) {}

    /// Carry upload admission until the backend releases its payload. Native
    /// providers can attach it to the transport; compatibility implementations
    /// must signal completion only after their upload buffer is no longer used.
    fn submit_put_with_reservation(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
        callback: CloudCallback,
    ) {
        let Some(reservation) = reservation else {
            self.submit_put(key, data, headers, callback);
            return;
        };
        match crate::storage::retained_callback::retain(callback.clone(), reservation) {
            Ok(retained) => self.submit_put(key, data, headers, retained),
            Err(error) => {
                let _ = callback.send(CloudEvent::Put {
                    key: key.to_string(),
                    result: Err(CloudError::Protocol(format!(
                        "retain upload completion: {error}"
                    ))),
                });
            }
        }
    }

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

    /// HEAD carrying internal headers, which is how the caller's deadline
    /// reaches the provider. Backends that ignore headers keep the default.
    fn submit_head_with_headers(
        &self,
        key: &str,
        _headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.submit_head(key, callback);
    }

    /// LIST carrying internal headers; see [`Self::submit_head_with_headers`].
    fn submit_list_with_headers(
        &self,
        prefix: &str,
        _headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.submit_list(prefix, callback);
    }
}

#[cfg(test)]
mod mock;
#[cfg(test)]
pub use mock::MockCloudBackend;

/// Namespace-aware dispatcher that forwards calls to the active backend.
pub struct CloudStorage {
    backend: Arc<dyn CloudBackend>,
    namespace: String,
    callback_timeout: std::time::Duration,
    metadata_publication_lock: Mutex<()>,
}

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

    pub(crate) fn lock_metadata_publication_for(
        &self,
        timeout: std::time::Duration,
    ) -> Option<MutexGuard<'_, ()>> {
        self.metadata_publication_lock.try_lock_for(timeout)
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

    /// Submit an unconditional DELETE bounded by this adapter's callback
    /// timeout.
    pub fn submit_delete(&self, key: &str, callback: CloudCallback) {
        let mut headers = Vec::new();
        set_request_timeout_header(&mut headers, self.callback_timeout);
        self.submit_delete_with_headers(key, headers, callback);
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
        let mut headers = Vec::new();
        set_request_timeout_header(&mut headers, self.callback_timeout);
        let full_prefix = self.full_path(prefix);
        self.backend
            .submit_list_with_headers(&full_prefix, headers, callback);
    }

    pub fn submit_head(&self, key: &str, callback: CloudCallback) {
        self.submit_head_within(key, self.callback_timeout, callback);
    }

    pub fn submit_head_within(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        let mut headers = Vec::new();
        set_request_timeout_header(&mut headers, timeout);
        let full_key = self.full_path(key);
        self.backend
            .submit_head_with_headers(&full_key, headers, callback);
    }
}

pub(crate) fn is_not_found_error(error: &CloudError) -> bool {
    error.is_not_found()
}

fn cloud_to_storage_outcome<T: Clone>(result: CloudOutcome<T>) -> StorageOutcome<T> {
    match result {
        CloudOutcome::Ok(value) => StorageOutcome::Ok(value),
        CloudOutcome::Err(error) => StorageOutcome::Err(storage_error_from_cloud(error)),
    }
}

/// Carry a provider's classification across the storage boundary, so callers
/// do not re-derive it from message text.
fn storage_error_from_cloud(error: CloudError) -> crate::storage::StorageError {
    use crate::storage::StorageErrorKind;
    let kind = if error.is_timeout() {
        StorageErrorKind::Timeout
    } else {
        match &error {
            #[cfg(any(test, feature = "cloud-common"))]
            CloudError::NotFound(_) => StorageErrorKind::NotFound,
            CloudError::PreconditionFailed(_) => StorageErrorKind::PreconditionFailed,
            #[cfg(any(test, feature = "cloud-common"))]
            CloudError::Unauthorized(_) => StorageErrorKind::Unauthorized,
            CloudError::Transport(_) => StorageErrorKind::Transport,
            CloudError::Protocol(_) => StorageErrorKind::Protocol,
            #[cfg(any(test, feature = "cloud-common"))]
            CloudError::InvalidRequest(_) | CloudError::ServerError(_) => {
                StorageErrorKind::Protocol
            }
            _ => StorageErrorKind::Io,
        }
    };
    crate::storage::StorageError::new(kind, error)
}

#[cfg(test)]
fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl StorageBackend for CloudStorage {
    fn submit_range_head(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_head_with_timeout(key, timeout, tx);
        let result = match rx.recv_timeout(timeout) {
            Ok(StorageEvent::HeadComplete {
                key: actual,
                result,
            }) if actual == self.full_path(key)
                || (actual == key && matches!(result, StorageOutcome::Err(_))) =>
            {
                result
            }
            Ok(event) => StorageOutcome::Err(
                format!("range HEAD returned a different object: {event:?}").into(),
            ),
            Err(error) => StorageOutcome::Err(crate::storage::storage_timeout_error(error)),
        };
        let _ = callback.send(StorageEvent::HeadComplete {
            key: key.to_string(),
            result,
        });
    }

    fn submit_read_range_with_reservation(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        reservation: Arc<crate::common::resource_budget::ResourceReservation>,
        callback: crate::storage::RangeReadCallback,
    ) {
        self.read_range_admitted(key, range, expected, timeout, Some(reservation), &callback);
    }

    fn submit_read_range(
        &self,
        key: &str,
        start: u64,
        end: u64,
        expected: StorageObjectMetadata,
        timeout: std::time::Duration,
        callback: crate::storage::RangeReadCallback,
    ) {
        self.read_range_admitted(key, start..end, expected, timeout, None, &callback);
    }

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
                    crate::storage::StorageError::timeout(message)
                }
                other => crate::storage::StorageError::io(other),
            })
            .and_then(|proof| {
                proof
                    .map(|proof| (proof.bytes, proof.metadata))
                    .ok_or_else(|| {
                        crate::storage::StorageError::not_found(format!("cloud object '{key}'"))
                    })
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
                result: StorageOutcome::Err(
                    format!("unexpected cloud GET response: {other:?}").into(),
                ),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::ReadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud GET callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::ReadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud GET callback closed".to_string().into()),
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

    fn submit_write_with_reservation(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        timeout: std::time::Duration,
        reservation: Arc<crate::common::resource_budget::ResourceReservation>,
        callback: StorageCallback,
    ) {
        self.write_admitted(key, data, headers, timeout, Some(reservation), &callback);
    }

    fn submit_write_with_headers_and_timeout(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        timeout: std::time::Duration,
        callback: StorageCallback,
    ) {
        self.write_admitted(key, data, headers, timeout, None, &callback);
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
                result: StorageOutcome::Err(
                    format!("unexpected cloud DELETE response: {other:?}").into(),
                ),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud DELETE callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud DELETE callback closed".to_string().into()),
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
        let mut headers = headers;
        set_request_timeout_header(&mut headers, self.callback_timeout);
        CloudStorage::submit_delete_with_headers(self, key, headers, tx);
        let event = match rx.recv_timeout(self.callback_timeout) {
            Ok(CloudEvent::Delete { key, result }) => StorageEvent::DeleteComplete {
                key,
                result: cloud_to_storage_outcome(result),
            },
            Ok(other) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(
                    format!("unexpected cloud DELETE response: {other:?}").into(),
                ),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud DELETE callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud DELETE callback closed".to_string().into()),
            },
        };
        let _ = callback.send(event);
    }

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
                result: StorageOutcome::Err(
                    format!("unexpected cloud LIST response: {other:?}").into(),
                ),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::ListComplete {
                prefix: prefix.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud LIST callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::ListComplete {
                prefix: prefix.to_string(),
                result: StorageOutcome::Err("cloud LIST callback closed".to_string().into()),
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
        CloudStorage::submit_head_within(self, key, timeout, tx);
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
                result: StorageOutcome::Err(
                    format!("unexpected cloud HEAD response: {other:?}").into(),
                ),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud HEAD callback timed out",
                )),
            },
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err("cloud HEAD callback closed".to_string().into()),
            },
        };
        let _ = callback.send(event);
    }
}

mod admitted;

#[cfg(test)]
mod retained_tests;

#[cfg(test)]
mod tests;
