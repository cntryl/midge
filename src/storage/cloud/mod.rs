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

mod error;
#[cfg(test)]
use error::cloud_outcome_from_result;
pub(crate) use error::contextualize_operation_error;
pub use error::{CloudError, CloudOutcome};

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

mod adapter;
use adapter::{cloud_to_storage_outcome, storage_error_from_cloud};

mod admitted;

#[cfg(test)]
mod retained_tests;

#[cfg(test)]
mod tests;
