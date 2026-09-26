/// Shared forwarding for test backends that override selected storage
/// operations. Fault-injecting methods stay explicit in each wrapper.
///
/// Mirrors `crate::storage::cloud::forward_cloud_backend` for the
/// `StorageBackend` trait. `$inner` names a field holding the delegate; both
/// `T` and `Arc<T>` work, because forwarding uses method-call syntax.
#[cfg(test)]
pub(crate) trait StorageBackendTestExt: super::StorageBackend {
    fn write_for_test(&self, key: &str, data: Vec<u8>, callback: super::StorageCallback) {
        self.submit_write_request(
            super::StorageRequest::new(
                key,
                crate::common::OperationDeadline::from_budget(std::time::Duration::from_secs(5)),
                std::time::Duration::from_secs(5),
            ),
            data,
            callback,
        );
    }
}

#[cfg(test)]
impl<T: super::StorageBackend + ?Sized> StorageBackendTestExt for T {}

#[cfg(test)]
pub(crate) fn dispatch_test_write_request(
    request: super::StorageRequest,
    data: Vec<u8>,
    callback: super::StorageCallback,
    submit: impl FnOnce(&str, super::StoragePrecondition, Vec<u8>, super::StorageCallback),
) {
    if request.remaining_timeout().is_zero() {
        let _ = callback.send(super::StorageEvent::WriteComplete {
            key: request.key,
            result: super::StorageOutcome::Err(super::storage_timeout_error("write timed out")),
        });
        return;
    }
    let callback = if let Some(reservation) = request.reservation {
        match super::retained_callback::retain(callback.clone(), reservation) {
            Ok(retained) => retained,
            Err(error) => {
                let _ = callback.send(super::StorageEvent::WriteComplete {
                    key: request.key,
                    result: super::StorageOutcome::Err(super::StorageError::from(error)),
                });
                return;
            }
        }
    } else {
        callback
    };
    submit(&request.key, request.precondition, data, callback);
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
    (@method $inner:ident, submit_head_request) => {
        fn submit_head_request(&self, request: $crate::storage::StorageRequest,
            callback: $crate::storage::StorageCallback) {
            self.$inner.submit_head_request(request, callback);
        }
    };
    (@method $inner:ident, submit_range_head_request) => {
        fn submit_range_head_request(&self, request: $crate::storage::StorageRequest,
            callback: $crate::storage::StorageCallback) {
            self.$inner.submit_range_head_request(request, callback);
        }
    };
    (@method $inner:ident, submit_range_read_request) => {
        fn submit_range_read_request(&self, request: $crate::storage::StorageRequest,
            range: std::ops::Range<u64>, callback: $crate::storage::RangeReadCallback) {
            self.$inner.submit_range_read_request(request, range, callback);
        }
    };
    (@method $inner:ident, submit_metadata_read_request) => {
        fn submit_metadata_read_request(&self, request: $crate::storage::StorageRequest,
            callback: $crate::storage::MetadataReadCallback) {
            self.$inner.submit_metadata_read_request(request, callback);
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
}

#[cfg(test)]
pub(crate) use forward_storage_backend;
