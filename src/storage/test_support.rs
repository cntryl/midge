/// Shared forwarding for test backends that override selected storage
/// operations. Fault-injecting methods stay explicit in each wrapper.
///
/// Mirrors `crate::storage::cloud::forward_cloud_backend` for the
/// `StorageBackend` trait. `$inner` names a field holding the delegate; both
/// `T` and `Arc<T>` work, because forwarding uses method-call syntax.
#[cfg(test)]
pub(crate) fn forward_typed_write_to_legacy<B: super::StorageBackend + ?Sized>(
    backend: &B,
    request: super::StorageRequest,
    data: Vec<u8>,
    callback: super::StorageCallback,
) {
    let timeout = request.remaining_timeout();
    if timeout.is_zero() {
        let _ = callback.send(super::StorageEvent::WriteComplete {
            key: request.key,
            result: super::StorageOutcome::Err(super::storage_timeout_error("write timed out")),
        });
        return;
    }
    let headers = match request.precondition.headers() {
        Ok(headers) => headers,
        Err(error) => {
            let _ = callback.send(super::StorageEvent::WriteComplete {
                key: request.key,
                result: super::StorageOutcome::Err(error),
            });
            return;
        }
    };
    if let Some(reservation) = request.reservation {
        match super::retained_callback::retain(callback.clone(), reservation) {
            Ok(retained) => backend.submit_write_with_headers_and_timeout(
                &request.key,
                data,
                headers,
                timeout,
                retained,
            ),
            Err(error) => {
                let _ = callback.send(super::StorageEvent::WriteComplete {
                    key: request.key,
                    result: super::StorageOutcome::Err(super::StorageError::new(
                        super::StorageErrorKind::of(&error),
                        format!("retain upload completion: {error}"),
                    )),
                });
            }
        }
    } else {
        backend.submit_write_with_headers_and_timeout(
            &request.key,
            data,
            headers,
            timeout,
            callback,
        );
    }
}

#[cfg(test)]
pub(crate) fn forward_typed_delete_to_legacy<B: super::StorageBackend + ?Sized>(
    backend: &B,
    request: super::StorageRequest,
    callback: super::StorageCallback,
) {
    if request.remaining_timeout().is_zero() {
        let _ = callback.send(super::StorageEvent::DeleteComplete {
            key: request.key,
            result: super::StorageOutcome::Err(super::storage_timeout_error("delete timed out")),
        });
        return;
    }
    let headers = match request.precondition.delete_headers() {
        Ok(headers) => headers,
        Err(error) => {
            let _ = callback.send(super::StorageEvent::DeleteComplete {
                key: request.key,
                result: super::StorageOutcome::Err(error),
            });
            return;
        }
    };
    let submit = |callback| {
        if headers.is_empty() {
            backend.submit_delete(&request.key, callback);
        } else {
            backend.submit_delete_with_headers(&request.key, headers, callback);
        }
    };
    if let Some(reservation) = request.reservation {
        match super::retained_callback::retain(callback.clone(), reservation) {
            Ok(retained) => submit(retained),
            Err(error) => {
                let _ = callback.send(super::StorageEvent::DeleteComplete {
                    key: request.key,
                    result: super::StorageOutcome::Err(super::StorageError::new(
                        super::StorageErrorKind::of(&error),
                        format!("retain delete completion: {error}"),
                    )),
                });
            }
        }
    } else {
        submit(callback);
    }
}

#[cfg(test)]
macro_rules! forward_storage_backend {
    ($inner:ident; $($method:ident),+ $(,)?) => {
        $($crate::storage::forward_storage_backend!(@method $inner, $method);)+
    };
    (@method $inner:ident, submit_write_request) => {
        fn submit_write_request(&self, request: $crate::storage::StorageRequest,
            data: Vec<u8>, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_write_request(request, data, callback);
        }
    };
    (@method $inner:ident, submit_delete_request) => {
        fn submit_delete_request(&self, request: $crate::storage::StorageRequest,
            callback: $crate::storage::StorageCallback) {
            self.$inner.submit_delete_request(request, callback);
        }
    };
    (@method $inner:ident, submit_read_with_metadata) => {
        fn submit_read_with_metadata(&self, key: &str, timeout: std::time::Duration, callback: $crate::storage::MetadataReadCallback) {
            self.$inner.submit_read_with_metadata(key, timeout, callback);
        }
    };
    (@method $inner:ident, submit_read_range) => {
        fn submit_read_range(&self, key: &str, start: u64, end: u64,
            expected: $crate::storage::StorageObjectMetadata, timeout: std::time::Duration,
            callback: $crate::storage::RangeReadCallback) {
            self.$inner.submit_read_range(key, start, end, expected, timeout, callback);
        }
    };
    (@method $inner:ident, submit_read_range_with_reservation) => {
        fn submit_read_range_with_reservation(&self, key: &str, range: std::ops::Range<u64>,
            expected: $crate::storage::StorageObjectMetadata, timeout: std::time::Duration,
            reservation: std::sync::Arc<$crate::common::resource_budget::ResourceReservation>,
            callback: $crate::storage::RangeReadCallback) {
            self.$inner.submit_read_range_with_reservation(key, range, expected, timeout, reservation, callback);
        }
    };
    (@method $inner:ident, submit_range_head) => {
        fn submit_range_head(&self, key: &str, timeout: std::time::Duration, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_range_head(key, timeout, callback);
        }
    };
    (@method $inner:ident, submit_write) => {
        fn submit_write(&self, key: &str, data: Vec<u8>, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_write(key, data, callback);
        }
    };
    (@method $inner:ident, submit_write_with_headers) => {
        fn submit_write_with_headers(&self, key: &str, data: Vec<u8>,
            headers: Vec<(String, String)>, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_write_with_headers(key, data, headers, callback);
        }
    };
    (@method $inner:ident, submit_write_with_headers_and_timeout) => {
        fn submit_write_with_headers_and_timeout(&self, key: &str, data: Vec<u8>,
            headers: Vec<(String, String)>, timeout: std::time::Duration,
            callback: $crate::storage::StorageCallback) {
            self.$inner.submit_write_with_headers_and_timeout(key, data, headers, timeout, callback);
        }
    };
    (@method $inner:ident, submit_delete) => {
        fn submit_delete(&self, key: &str, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_delete(key, callback);
        }
    };
    (@method $inner:ident, submit_delete_with_headers) => {
        fn submit_delete_with_headers(&self, key: &str, headers: Vec<(String, String)>,
            callback: $crate::storage::StorageCallback) {
            self.$inner.submit_delete_with_headers(key, headers, callback);
        }
    };
    (@method $inner:ident, submit_head) => {
        fn submit_head(&self, key: &str, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_head(key, callback);
        }
    };
    (@method $inner:ident, submit_head_with_timeout) => {
        fn submit_head_with_timeout(&self, key: &str, timeout: std::time::Duration, callback: $crate::storage::StorageCallback) {
            self.$inner.submit_head_with_timeout(key, timeout, callback);
        }
    };
}

#[cfg(test)]
pub(crate) use forward_storage_backend;
