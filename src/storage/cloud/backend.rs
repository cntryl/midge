//! The non-blocking cloud-provider boundary.
//!
//! A backend must implement every core object operation. Reservation and
//! identity helpers retain their compatibility defaults because they decorate
//! an already-required operation rather than describe a provider capability.

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
    headers: Vec<(String, String)>,
    reservation: Option<Arc<crate::common::resource_budget::ResourceReservation>>,
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

    fn timeout(&self) -> Option<Duration> {
        self.deadline.map(|deadline| deadline.remaining())
    }

    fn headers_with_deadline(&self) -> Vec<(String, String)> {
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

/// Non-blocking cloud backend interface used by the engine.
///
/// The core object operations are deliberately required rather than runtime
/// fallbacks. The compile-fail fixtures below keep that contract mechanical:
/// removing one method from an implementation must make the implementation
/// itself fail to type-check.
///
/// ```no_run
/// use cntryl_midge::__internal::storage::cloud::{CloudBackend, CloudCallback};
///
/// struct CompleteBackend;
///
/// impl CloudBackend for CompleteBackend {
///     fn submit_put(
///         &self,
///         _key: &str,
///         _data: Vec<u8>,
///         _headers: Vec<(String, String)>,
///         _callback: CloudCallback,
///     ) {
///     }
///
///     fn submit_get(&self, _key: &str, _callback: CloudCallback) {}
///
///     fn submit_get_with_metadata(&self, _key: &str, _callback: CloudCallback) {}
///
///     fn submit_delete(
///         &self,
///         _key: &str,
///         _headers: Vec<(String, String)>,
///         _callback: CloudCallback,
///     ) {
///     }
///
///     fn submit_list(&self, _prefix: &str, _callback: CloudCallback) {}
///
///     fn submit_head(&self, _key: &str, _callback: CloudCallback) {}
/// }
/// ```
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::{CloudBackend, CloudCallback};
///
/// struct MissingMetadataGet;
///
/// impl CloudBackend for MissingMetadataGet {
///     fn submit_put(&self, _key: &str, _data: Vec<u8>, _headers: Vec<(String, String)>, _callback: CloudCallback) {}
///     fn submit_get(&self, _key: &str, _callback: CloudCallback) {}
///     fn submit_delete(&self, _key: &str, _headers: Vec<(String, String)>, _callback: CloudCallback) {}
///     fn submit_list(&self, _prefix: &str, _callback: CloudCallback) {}
///     fn submit_head(&self, _key: &str, _callback: CloudCallback) {}
/// }
/// ```
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::{CloudBackend, CloudCallback};
///
/// struct MissingDelete;
///
/// impl CloudBackend for MissingDelete {
///     fn submit_put(&self, _key: &str, _data: Vec<u8>, _headers: Vec<(String, String)>, _callback: CloudCallback) {}
///     fn submit_get(&self, _key: &str, _callback: CloudCallback) {}
///     fn submit_get_with_metadata(&self, _key: &str, _callback: CloudCallback) {}
///     fn submit_list(&self, _prefix: &str, _callback: CloudCallback) {}
///     fn submit_head(&self, _key: &str, _callback: CloudCallback) {}
/// }
/// ```
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::{CloudBackend, CloudCallback};
///
/// struct MissingList;
///
/// impl CloudBackend for MissingList {
///     fn submit_put(&self, _key: &str, _data: Vec<u8>, _headers: Vec<(String, String)>, _callback: CloudCallback) {}
///     fn submit_get(&self, _key: &str, _callback: CloudCallback) {}
///     fn submit_get_with_metadata(&self, _key: &str, _callback: CloudCallback) {}
///     fn submit_delete(&self, _key: &str, _headers: Vec<(String, String)>, _callback: CloudCallback) {}
///     fn submit_head(&self, _key: &str, _callback: CloudCallback) {}
/// }
/// ```
///
/// ```compile_fail
/// use cntryl_midge::__internal::storage::cloud::{CloudBackend, CloudCallback};
///
/// struct MissingHead;
///
/// impl CloudBackend for MissingHead {
///     fn submit_put(&self, _key: &str, _data: Vec<u8>, _headers: Vec<(String, String)>, _callback: CloudCallback) {}
///     fn submit_get(&self, _key: &str, _callback: CloudCallback) {}
///     fn submit_get_with_metadata(&self, _key: &str, _callback: CloudCallback) {}
///     fn submit_delete(&self, _key: &str, _headers: Vec<(String, String)>, _callback: CloudCallback) {}
///     fn submit_list(&self, _prefix: &str, _callback: CloudCallback) {}
/// }
/// ```
pub trait CloudBackend: Send + Sync + 'static {
    /// Submit a PUT with typed deadline, conditional headers and payload
    /// reservation controls.
    fn submit_put_with_options(
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

    /// Submit a GET with typed deadline, reservation, identity and range
    /// controls.
    fn submit_get_with_options(
        &self,
        key: &str,
        options: CloudReadOptions,
        callback: CloudCallback,
    ) {
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
        let timeout = options.request.timeout().unwrap_or(Duration::MAX);
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
            self.submit_get_range_with_reservation(
                key,
                range,
                expected_identity,
                timeout,
                options.request.reservation,
                callback,
            );
            return;
        }

        let callback = if let Some(reservation) = options.request.reservation {
            match crate::storage::retained_callback::retain(callback.clone(), reservation) {
                Ok(retained) => retained,
                Err(error) => {
                    let error =
                        CloudError::Protocol(format!("retain cloud read completion: {error}"));
                    let event = if options.include_metadata {
                        CloudEvent::GetWithMetadata {
                            key: key.to_string(),
                            result: Err(error),
                        }
                    } else {
                        CloudEvent::Get {
                            key: key.to_string(),
                            result: Err(error),
                        }
                    };
                    let _ = callback.send(event);
                    return;
                }
            }
        } else {
            callback
        };

        match (options.include_metadata, request_timeout) {
            (true, Some(timeout)) => {
                self.submit_get_with_metadata_with_timeout(key, timeout, callback);
            }
            (true, None) => self.submit_get_with_metadata(key, callback),
            (false, Some(timeout)) => self.submit_get_with_timeout(key, timeout, callback),
            (false, None) => self.submit_get(key, callback),
        }
    }

    /// Submit a DELETE with typed deadline and conditional headers.
    fn submit_delete_with_options(
        &self,
        key: &str,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        if options.timeout().is_some_and(|timeout| timeout.is_zero()) {
            let _ = callback.send(CloudEvent::Delete {
                key: key.to_string(),
                result: Err(operation_timeout_error("DELETE")),
            });
            return;
        }
        self.submit_delete(key, options.headers_with_deadline(), callback);
    }

    /// Submit a LIST with typed deadline controls.
    fn submit_list_with_options(
        &self,
        prefix: &str,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        if options.timeout().is_some_and(|timeout| timeout.is_zero()) {
            let _ = callback.send(CloudEvent::List {
                prefix: prefix.to_string(),
                result: Err(operation_timeout_error("LIST")),
            });
            return;
        }
        self.submit_list_with_headers(prefix, options.headers_with_deadline(), callback);
    }

    /// Submit a HEAD with typed deadline controls.
    fn submit_head_with_options(
        &self,
        key: &str,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        if options.timeout().is_some_and(|timeout| timeout.is_zero()) {
            let _ = callback.send(CloudEvent::Head {
                key: key.to_string(),
                result: Err(operation_timeout_error("HEAD")),
            });
            return;
        }
        self.submit_head_with_headers(key, options.headers_with_deadline(), callback);
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

    /// Submit an idempotent delete. Implementations must report success when
    /// the target is already absent; conditional-precondition failures remain
    /// errors.
    fn submit_delete(&self, key: &str, headers: Vec<(String, String)>, callback: CloudCallback);

    /// Submit a prefix listing.
    fn submit_list(&self, prefix: &str, callback: CloudCallback);

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

/// Read-only cloud operations used by layers that do not publish objects.
pub trait CloudReadCapability: Send + Sync {
    fn read(&self, key: &str, options: CloudReadOptions, callback: CloudCallback);
}

impl<T: CloudBackend + ?Sized> CloudReadCapability for T {
    fn read(&self, key: &str, options: CloudReadOptions, callback: CloudCallback) {
        self.submit_get_with_options(key, options, callback);
    }
}

/// Object creation and replacement operations.
pub trait CloudWriteCapability: Send + Sync {
    fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        options: CloudRequestOptions,
        callback: CloudCallback,
    );
}

impl<T: CloudBackend + ?Sized> CloudWriteCapability for T {
    fn write(
        &self,
        key: &str,
        data: Vec<u8>,
        options: CloudRequestOptions,
        callback: CloudCallback,
    ) {
        self.submit_put_with_options(key, data, options, callback);
    }
}

/// Idempotent object removal operations.
pub trait CloudDeleteCapability: Send + Sync {
    fn delete(&self, key: &str, options: CloudRequestOptions, callback: CloudCallback);
}

impl<T: CloudBackend + ?Sized> CloudDeleteCapability for T {
    fn delete(&self, key: &str, options: CloudRequestOptions, callback: CloudCallback) {
        self.submit_delete_with_options(key, options, callback);
    }
}

/// Prefix enumeration operations.
pub trait CloudListCapability: Send + Sync {
    fn list(&self, prefix: &str, options: CloudRequestOptions, callback: CloudCallback);
}

impl<T: CloudBackend + ?Sized> CloudListCapability for T {
    fn list(&self, prefix: &str, options: CloudRequestOptions, callback: CloudCallback) {
        self.submit_list_with_options(prefix, options, callback);
    }
}

/// Object metadata lookup operations.
pub trait CloudHeadCapability: Send + Sync {
    fn head(&self, key: &str, options: CloudRequestOptions, callback: CloudCallback);
}

impl<T: CloudBackend + ?Sized> CloudHeadCapability for T {
    fn head(&self, key: &str, options: CloudRequestOptions, callback: CloudCallback) {
        self.submit_head_with_options(key, options, callback);
    }
}
