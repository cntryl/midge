//! The non-blocking cloud-provider boundary.
//!
//! A backend must implement every core object operation. Reservation and
//! identity helpers retain their compatibility defaults because they decorate
//! an already-required operation rather than describe a provider capability.

use super::{CloudCallback, CloudError, CloudEvent};
use crate::storage::StorageObjectMetadata;
use std::sync::Arc;

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

    /// Submit an object GET that also returns the observed object identity.
    fn submit_get_with_metadata(&self, key: &str, callback: CloudCallback);

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
