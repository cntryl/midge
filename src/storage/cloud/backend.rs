//! The non-blocking cloud-provider boundary.
//!
//! Each capability owns one kind of object operation. A complete backend
//! composes those capabilities; consumers retain only the operations they use.

use super::{CloudCallback, CloudError, CloudEvent};
use crate::storage::StorageObjectMetadata;
use std::sync::Arc;
use std::time::Duration;

/// Shared request controls passed to a cloud operation.
///
/// The deadline is absolute so time spent waiting in the runtime queue and
/// earlier calls in the same request remain charged to the same budget.
#[derive(Clone, Default)]
pub struct CloudRequestOptions {
    deadline: Option<crate::common::OperationDeadline>,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
}

impl CloudRequestOptions {
    #[must_use]
    pub fn with_deadline(mut self, deadline: crate::common::OperationDeadline) -> Self {
        self.deadline = Some(deadline);
        self
    }

    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    #[must_use]
    pub fn with_headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers.extend(headers);
        self
    }

    #[must_use]
    pub fn with_reservation(
        mut self,
        reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
    ) -> Self {
        self.reservation = reservation;
        self
    }

    pub(crate) fn timeout(&self) -> Option<Duration> {
        self.deadline.map(|deadline| deadline.remaining())
    }

    pub(crate) fn headers_with_deadline(&self) -> Vec<(String, String)> {
        let mut headers = self.headers.clone();
        if let Some(deadline) = self
            .deadline
            .filter(crate::common::OperationDeadline::is_bounded)
        {
            super::set_request_timeout_header(&mut headers, deadline.remaining());
        }
        headers
    }
}

/// Read-specific controls, including an immutable identity and an optional
/// exact byte range.
#[derive(Clone, Default)]
pub struct CloudReadOptions {
    request: CloudRequestOptions,
    range: Option<std::ops::Range<u64>>,
    expected_identity: Option<StorageObjectMetadata>,
    include_metadata: bool,
}

impl CloudReadOptions {
    #[must_use]
    pub fn with_request_options(mut self, request: CloudRequestOptions) -> Self {
        self.request = request;
        self
    }

    #[must_use]
    pub fn with_range(
        mut self,
        range: std::ops::Range<u64>,
        expected_identity: StorageObjectMetadata,
    ) -> Self {
        self.range = Some(range);
        self.expected_identity = Some(expected_identity);
        self
    }

    #[must_use]
    pub fn with_metadata(mut self) -> Self {
        self.include_metadata = true;
        self
    }
}

fn operation_timeout_error(operation: &str) -> CloudError {
    CloudError::Timeout(format!("{operation} deadline exhausted"))
}

fn retain_read_callback(
    key: &str,
    include_metadata: bool,
    reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
    callback: CloudCallback,
) -> Option<CloudCallback> {
    let Some(reservation) = reservation else {
        return Some(callback);
    };
    match crate::storage::retained_callback::retain(callback.clone(), reservation) {
        Ok(retained) => Some(retained),
        Err(error) => {
            let error = CloudError::Protocol(format!("retain cloud read completion: {error}"));
            let event = if include_metadata {
                CloudEvent::GetWithMetadata {
                    key: key.into(),
                    result: Err(error),
                }
            } else {
                CloudEvent::Get {
                    key: key.into(),
                    result: Err(error),
                }
            };
            let _ = callback.send(event);
            None
        }
    }
}

fn normalize_range_read_headers(
    headers: Vec<(String, String)>,
    range: &std::ops::Range<u64>,
    expected: &StorageObjectMetadata,
) -> Result<Vec<(String, String)>, CloudError> {
    let (identity_name, identity_value) =
        crate::storage::conditional_object_identity(&expected.etag, expected.generation.as_deref())
            .ok_or_else(|| CloudError::Protocol("range request lacks object identity".into()))?;
    if range.start >= range.end || range.end > expected.size {
        return Err(CloudError::Protocol("invalid object byte range".into()));
    }
    let expected_range = format!("bytes={}-{}", range.start, range.end - 1);
    let mut normalized: Vec<(String, String)> = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("range") || name.eq_ignore_ascii_case("x-ms-range") {
            if value.trim() != expected_range {
                return Err(CloudError::Protocol(
                    "caller range conflicts with typed range".into(),
                ));
            }
            continue;
        }
        if name.eq_ignore_ascii_case(identity_name) {
            if value.trim() != identity_value.trim() {
                return Err(CloudError::PreconditionFailed(
                    "caller identity conflicts with typed range identity".into(),
                ));
            }
            continue;
        }
        if (name.eq_ignore_ascii_case("if-none-match")
            && (value.trim() == "*" || value.trim() == expected.etag.trim()))
            || (name.eq_ignore_ascii_case("x-goog-if-generation-not-match")
                && expected
                    .generation
                    .as_deref()
                    .is_some_and(|generation| generation.trim() == value.trim()))
        {
            return Err(CloudError::PreconditionFailed(
                "caller exclusion conflicts with typed range identity".into(),
            ));
        }
        let identity_header = name.eq_ignore_ascii_case("if-match")
            || name.eq_ignore_ascii_case("x-goog-if-generation-match");
        if identity_header {
            let selected = if name.eq_ignore_ascii_case("if-match") {
                Some(expected.etag.as_str())
            } else {
                expected.generation.as_deref()
            };
            if selected.is_none_or(|selected| {
                selected.trim().is_empty() || selected.trim() != value.trim()
            }) {
                return Err(CloudError::PreconditionFailed(
                    "caller identity conflicts with typed range identity".into(),
                ));
            }
            if normalized
                .iter()
                .any(|(prior, _)| prior.eq_ignore_ascii_case(&name))
            {
                continue;
            }
        }
        normalized.push((name, value));
    }
    Ok(normalized)
}

/// A complete provider composes the independently usable operation capabilities.
/// Consumers that only read or publish objects depend on the corresponding
/// capability rather than this aggregate interface.
pub trait CloudBackend:
    CloudReadCapability
    + CloudWriteCapability
    + CloudDeleteCapability
    + CloudListCapability
    + CloudHeadCapability
    + Send
    + Sync
    + 'static
{
    /// Override the default deadline applied to provider HTTP requests.
    #[cfg(feature = "cloud-common")]
    fn set_request_timeout(&self, _timeout: std::time::Duration) {}
}

/// Object and conditional range reads.
///
/// A reader can be implemented and consumed without any mutation capability:
///
/// ```no_run
/// use cntryl_midge::__internal::storage::cloud::{CloudReadCapability, CloudReadOptions, CloudCallback};
/// struct ReadOnly;
/// impl CloudReadCapability for ReadOnly {
///     fn submit_get(&self, _key: &str, _callback: CloudCallback) {}
///     fn submit_get_with_metadata(&self, _key: &str, _callback: CloudCallback) {}
/// }
/// fn read_object(reader: &dyn CloudReadCapability, callback: CloudCallback) {
///     reader.read("object", CloudReadOptions::default(), callback);
/// }
/// ```
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::{CloudReadCapability, CloudCallback};
/// struct MissingMetadataRead;
/// impl CloudReadCapability for MissingMetadataRead {
///     fn submit_get(&self, _key: &str, _callback: CloudCallback) {}
/// }
/// ```
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::{CloudReadCapability, CloudCallback, CloudRequestOptions};
/// fn delete_with_reader(reader: &dyn CloudReadCapability, callback: CloudCallback) {
///     reader.delete("object", CloudRequestOptions::default(), callback);
/// }
/// ```
pub trait CloudReadCapability: Send + Sync {
    /// Submit a GET with typed deadline, reservation, identity and range
    /// controls.
    fn read(&self, key: &str, mut options: CloudReadOptions, callback: CloudCallback) {
        if options
            .request
            .timeout()
            .is_some_and(|timeout| timeout.is_zero())
        {
            let event = match (options.range, options.include_metadata) {
                (Some(range), _) => CloudEvent::GetRange {
                    key: key.to_string(),
                    start: range.start,
                    end: Some(range.end),
                    result: Err(operation_timeout_error("range GET")),
                },
                (None, true) => CloudEvent::GetWithMetadata {
                    key: key.to_string(),
                    result: Err(operation_timeout_error("GET")),
                },
                (None, false) => CloudEvent::Get {
                    key: key.to_string(),
                    result: Err(operation_timeout_error("GET")),
                },
            };
            let _ = callback.send(event);
            return;
        }
        let request_timeout = options.request.timeout();
        if let Some(range) = options.range {
            let Some(expected_identity) = options.expected_identity else {
                let _ = callback.send(CloudEvent::GetRange {
                    key: key.to_string(),
                    start: range.start,
                    end: Some(range.end),
                    result: Err(CloudError::Protocol(
                        "bounded cloud read requires an expected object identity".into(),
                    )),
                });
                return;
            };
            options.request.headers = match normalize_range_read_headers(
                options.request.headers,
                &range,
                &expected_identity,
            ) {
                Ok(headers) => headers,
                Err(error) => {
                    let _ = callback.send(CloudEvent::GetRange {
                        key: key.into(),
                        start: range.start,
                        end: Some(range.end),
                        result: Err(error),
                    });
                    return;
                }
            };
            self.submit_get_range_with_request(
                key,
                range,
                expected_identity,
                options.request,
                callback,
            );
            return;
        }

        let Some(callback) = retain_read_callback(
            key,
            options.include_metadata,
            options.request.reservation,
            callback,
        ) else {
            return;
        };

        self.submit_get_with_headers(
            key,
            options.request.headers,
            request_timeout,
            options.include_metadata,
            callback,
        );
    }

    /// Conditional range read carrying all of the caller's transport controls.
    fn submit_get_range_with_request(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
        expected: StorageObjectMetadata,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        if !options.headers.is_empty() {
            let _ = callback.send(CloudEvent::GetRange {
                key: key.into(),
                start: range.start,
                end: Some(range.end),
                result: Err(CloudError::Protocol(
                    "conditional range GET headers unsupported".into(),
                )),
            });
            return;
        }
        self.submit_get_range_with_reservation(
            key,
            range,
            expected,
            options.timeout().unwrap_or(Duration::MAX),
            options.reservation,
            callback,
        );
    }

    /// Whole-object read with caller conditions. Backends without support for
    /// condition headers reject them instead of silently reading another version.
    fn submit_get_with_headers(
        &self,
        key: &str,
        headers: Vec<(String, String)>,
        timeout: Option<Duration>,
        include_metadata: bool,
        callback: CloudCallback,
    ) {
        if !headers.is_empty() {
            let error = CloudError::Protocol("conditional GET headers unsupported".into());
            let event = if include_metadata {
                CloudEvent::GetWithMetadata {
                    key: key.into(),
                    result: Err(error),
                }
            } else {
                CloudEvent::Get {
                    key: key.into(),
                    result: Err(error),
                }
            };
            let _ = callback.send(event);
            return;
        }
        match (include_metadata, timeout) {
            (true, Some(timeout)) => {
                self.submit_get_with_metadata_with_timeout(key, timeout, callback);
            }
            (true, None) => self.submit_get_with_metadata(key, callback),
            (false, Some(timeout)) => self.submit_get_with_timeout(key, timeout, callback),
            (false, None) => self.submit_get(key, callback),
        }
    }

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

    /// Submit an object GET.
    fn submit_get(&self, key: &str, callback: CloudCallback);

    /// Submit a GET with a per-request provider budget. Network backends must
    /// override this compatibility fallback; synchronous backends can finish
    /// through their existing GET implementation.
    fn submit_get_with_timeout(
        &self,
        key: &str,
        _timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        self.submit_get(key, callback);
    }

    /// Submit an object GET that also returns the observed object identity.
    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback);

    /// Submit a metadata-bearing GET under the caller's provider budget; see
    /// [`Self::submit_get_with_timeout`] for the compatibility fallback.
    fn submit_get_with_metadata_with_timeout(
        &self,
        key: &str,
        _timeout: std::time::Duration,
        callback: CloudCallback,
    ) {
        self.submit_get_with_metadata(key, callback);
    }

    /// Submit a ranged GET for provider tests. `end` is an exclusive byte offset.
    #[cfg(test)]
    fn submit_get_range(&self, key: &str, start: u64, end: Option<u64>, callback: CloudCallback);
}

/// Object creation and replacement.
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::CloudWriteCapability;
/// struct MissingWrite;
/// impl CloudWriteCapability for MissingWrite {}
/// ```
pub trait CloudWriteCapability: Send + Sync {
    fn supports_file_upload(&self) -> bool {
        false
    }

    fn write_file_with_receipt(
        &self,
        key: &str,
        _source: crate::storage::StorageFileUpload,
        _options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        let _ = callback.send(CloudEvent::PutReceipt {
            key: key.into(),
            result: Err(CloudError::Protocol(
                "backend has no conditional streaming upload capability".into(),
            )),
        });
    }

    /// Configure retries; this does not authorize replay of shared-key mutations.
    fn set_retry_policy(&self, _policy: super::CloudRetryPolicy) {}

    /// [`crate::storage::StorageBackend::write_payload_copies`]. A provider
    /// lowers the conservative default only when it bounds its own copies.
    fn write_payload_copies(&self) -> usize {
        crate::storage::CONSERVATIVE_WRITE_PAYLOAD_COPIES
    }
    /// Request a checksum-validated write receipt when supported. The default
    /// emits the ordinary PUT completion, requiring the caller's readback.
    fn write_with_receipt(
        &self,
        key: &str,
        data: Vec<u8>,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        if reject_exhausted_receipt(key, &options, &callback) {
            return;
        }
        self.write(key, data, options, callback);
    }

    /// Submit a PUT with typed deadline, conditional headers and payload
    /// reservation controls.
    fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        if options.timeout().is_some_and(|timeout| timeout.is_zero()) {
            let _ = callback.send(CloudEvent::Put {
                key: key.to_string(),
                result: Err(operation_timeout_error("PUT")),
            });
            return;
        }
        let headers = options.headers_with_deadline();
        self.submit_put_with_reservation(key, data, headers, options.reservation, callback);
    }

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
}

/// Idempotent object removal.
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::CloudDeleteCapability;
/// struct MissingDelete;
/// impl CloudDeleteCapability for MissingDelete {}
/// ```
pub trait CloudDeleteCapability: Send + Sync {
    /// Submit a DELETE with typed deadline and conditional headers.
    fn delete(&self, key: &str, options: CloudRequestOptions, callback: CloudCallback) {
        if options.timeout().is_some_and(|timeout| timeout.is_zero()) {
            let _ = callback.send(CloudEvent::Delete {
                key: key.to_string(),
                result: Err(operation_timeout_error("DELETE")),
            });
            return;
        }
        self.submit_delete(key, options.headers_with_deadline(), callback);
    }

    /// Submit an idempotent delete. Implementations must report success when
    /// the target is already absent; conditional-precondition failures remain
    /// errors.
    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback);
}

/// Prefix enumeration.
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::CloudListCapability;
/// struct MissingList;
/// impl CloudListCapability for MissingList {}
/// ```
pub trait CloudListCapability: Send + Sync {
    /// Submit a LIST with typed deadline controls.
    fn list(&self, prefix: &str, options: CloudRequestOptions, callback: CloudCallback) {
        if options.timeout().is_some_and(|timeout| timeout.is_zero()) {
            let _ = callback.send(CloudEvent::List {
                prefix: prefix.to_string(),
                result: Err(operation_timeout_error("LIST")),
            });
            return;
        }
        self.submit_list_with_headers(prefix, options.headers_with_deadline(), callback);
    }

    /// Submit a prefix listing.
    fn submit_list(&self, prefix: &str, callback: CloudCallback);

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

/// Object metadata lookup.
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::CloudHeadCapability;
/// struct MissingHead;
/// impl CloudHeadCapability for MissingHead {}
/// ```
pub trait CloudHeadCapability: Send + Sync {
    /// Submit a HEAD with typed deadline controls.
    fn head(&self, key: &str, options: CloudRequestOptions, callback: CloudCallback) {
        if options.timeout().is_some_and(|timeout| timeout.is_zero()) {
            let _ = callback.send(CloudEvent::Head {
                key: key.to_string(),
                result: Err(operation_timeout_error("HEAD")),
            });
            return;
        }
        self.submit_head_with_headers(key, options.headers_with_deadline(), callback);
    }

    /// Submit a HEAD request.
    fn submit_head(&self, key: &str, callback: CloudCallback);

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
}

/// Answer a receipt write whose deadline is already spent without sending
/// it. Returns whether the callback was answered.
pub(crate) fn reject_exhausted_receipt(
    key: &str,
    options: &CloudRequestOptions,
    callback: &CloudCallback,
) -> bool {
    if !options.timeout().is_some_and(|timeout| timeout.is_zero()) {
        return false;
    }
    let _ = callback.send(CloudEvent::PutReceipt {
        key: key.into(),
        result: Err(operation_timeout_error("PUT receipt")),
    });
    true
}
