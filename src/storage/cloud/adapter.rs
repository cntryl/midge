//! The `StorageBackend` adapter over `CloudStorage`.
//!
//! Every operation submits to the provider, blocks on the callback for the
//! caller's budget, and maps the outcome into the storage layer's types.
//! Completion events carry the caller's key, never the namespaced provider
//! key (#514).

#[allow(clippy::wildcard_imports)]
use super::*;
/// Wait for a provider callback and report the two ways it can fail to arrive.
///
/// The adapter blocks on a channel for every operation. A missing answer is
/// either the callback budget running out or the provider dropping the
/// callback, and callers report those identically apart from the operation.
pub(super) fn await_cloud_event(
    rx: &std::sync::mpsc::Receiver<CloudEvent>,
    timeout: std::time::Duration,
    operation: &str,
) -> Result<CloudEvent, crate::storage::StorageError> {
    rx.recv_timeout(timeout).map_err(|error| match error {
        std::sync::mpsc::RecvTimeoutError::Timeout => {
            crate::storage::storage_timeout_error(format!("cloud {operation} callback timed out"))
        }
        std::sync::mpsc::RecvTimeoutError::Disconnected => {
            format!("cloud {operation} callback closed").into()
        }
    })
}

/// Wait for a delete callback and forward its outcome to the storage caller.
pub(super) fn deliver_delete_outcome(
    key: &str,
    rx: &std::sync::mpsc::Receiver<CloudEvent>,
    timeout: std::time::Duration,
    callback: &StorageCallback,
) {
    let event = match await_cloud_event(rx, timeout, "DELETE") {
        Ok(CloudEvent::Delete { result, .. }) => StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: cloud_to_storage_outcome(result),
        },
        Ok(other) => StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err(
                format!("unexpected cloud DELETE response: {other:?}").into(),
            ),
        },
        Err(error) => StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Err(error),
        },
    };
    let _ = callback.send(event);
}

pub(super) fn cloud_to_storage_outcome<T: Clone>(result: CloudOutcome<T>) -> StorageOutcome<T> {
    match result {
        CloudOutcome::Ok(value) => StorageOutcome::Ok(value),
        CloudOutcome::Err(error) => StorageOutcome::Err(storage_error_from_cloud(error)),
    }
}

/// Carry a provider's classification across the storage boundary, so callers
/// do not re-derive it from message text.
pub(super) fn storage_error_from_cloud(error: CloudError) -> crate::storage::StorageError {
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

impl CloudStorage {
    fn range_head_within(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: &StorageCallback,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.head_with_timeout(key, timeout, &tx);
        let result = match rx.recv_timeout(timeout) {
            Ok(StorageEvent::HeadComplete {
                key: actual,
                result,
            }) if actual == key => result,
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

    fn metadata_read_within(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: &crate::storage::MetadataReadCallback,
    ) {
        let deadline = crate::common::OperationDeadline::from_budget(timeout);
        let result = blocking_cloud_object_proof_within(self, key, &deadline)
            .map_err(crate::storage::StorageError::from)
            .and_then(|proof| {
                proof
                    .map(|proof| (proof.bytes, proof.metadata))
                    .ok_or_else(|| {
                        crate::storage::StorageError::not_found(format!("cloud object '{key}'"))
                    })
            });
        let _ = callback.send(result);
    }
}

impl StorageBackend for CloudStorage {
    fn submit_range_read_request(
        &self,
        request: crate::storage::StorageRequest,
        range: std::ops::Range<u64>,
        callback: crate::storage::RangeReadCallback,
    ) {
        let timeout = request.remaining_timeout();
        if timeout.is_zero() {
            let _ = callback.send(Err(crate::storage::storage_timeout_error(
                "range read timed out",
            )));
            return;
        }
        let crate::storage::StoragePrecondition::IfMatch(expected) = request.precondition else {
            let _ = callback.send(Err(crate::storage::StorageError::protocol(
                "range read requires an object identity",
            )));
            return;
        };
        self.read_range_admitted(
            &request.key,
            range,
            expected,
            timeout,
            request.reservation,
            &callback,
        );
    }

    fn submit_metadata_read_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: crate::storage::MetadataReadCallback,
    ) {
        crate::storage::dispatch_metadata_read_request(
            request,
            callback,
            |key, timeout, callback| {
                self.metadata_read_within(key, timeout, &callback);
            },
        );
    }

    fn submit_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: StorageCallback,
    ) {
        crate::storage::dispatch_head_request(request, callback, |key, timeout, callback| {
            self.head_with_timeout(key, timeout, &callback);
        });
    }

    fn submit_range_head_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: StorageCallback,
    ) {
        crate::storage::dispatch_head_request(request, callback, |key, timeout, callback| {
            self.range_head_within(key, timeout, &callback);
        });
    }

    fn submit_delete_request(
        &self,
        request: crate::storage::StorageRequest,
        callback: StorageCallback,
    ) {
        let timeout = request.remaining_timeout();
        let key = request.key;
        if timeout.is_zero() {
            let _ = callback.send(StorageEvent::DeleteComplete {
                key,
                result: StorageOutcome::Err(crate::storage::storage_timeout_error(
                    "cloud DELETE refused because no callback budget remained",
                )),
            });
            return;
        }
        let mut headers = match request.precondition.delete_headers() {
            Ok(headers) => headers,
            Err(error) => {
                let _ = callback.send(StorageEvent::DeleteComplete {
                    key,
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        let _reservation = request.reservation;
        set_request_timeout_header(&mut headers, timeout);
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_delete_with_headers(self, &key, headers, tx);
        deliver_delete_outcome(&key, &rx, timeout, &callback);
    }

    fn submit_write_request(
        &self,
        request: crate::storage::StorageRequest,
        data: Vec<u8>,
        callback: StorageCallback,
    ) {
        let timeout = request.remaining_timeout();
        let key = request.key;
        let headers = match request.precondition.headers() {
            Ok(headers) => headers,
            Err(error) => {
                let _ = callback.send(StorageEvent::WriteComplete {
                    key,
                    result: StorageOutcome::Err(error),
                });
                return;
            }
        };
        self.write_admitted(&key, data, headers, timeout, request.reservation, &callback);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        self.write_admitted(
            key,
            data,
            Vec::new(),
            self.callback_timeout,
            None,
            &callback,
        );
    }

    fn submit_write_with_headers(
        &self,
        key: &str,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: StorageCallback,
    ) {
        self.write_admitted(key, data, headers, self.callback_timeout, None, &callback);
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
        deliver_delete_outcome(key, &rx, self.callback_timeout, &callback);
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
        deliver_delete_outcome(key, &rx, self.callback_timeout, &callback);
    }

    fn submit_head(&self, key: &str, callback: StorageCallback) {
        self.head_with_timeout(key, self.callback_timeout, &callback);
    }
}

impl CloudStorage {
    fn head_with_timeout(
        &self,
        key: &str,
        timeout: std::time::Duration,
        callback: &StorageCallback,
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
        let event = match await_cloud_event(&rx, timeout, "HEAD") {
            Ok(CloudEvent::Head { result, .. }) => {
                let outcome = match result {
                    CloudOutcome::Ok(metadata) => StorageOutcome::Ok(metadata),
                    CloudOutcome::Err(err) => {
                        cloud_to_storage_outcome::<StorageObjectMetadata>(CloudOutcome::Err(err))
                    }
                };
                StorageEvent::HeadComplete {
                    key: key.to_string(),
                    result: outcome,
                }
            }
            Ok(other) => StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(
                    format!("unexpected cloud HEAD response: {other:?}").into(),
                ),
            },
            Err(error) => StorageEvent::HeadComplete {
                key: key.to_string(),
                result: StorageOutcome::Err(error),
            },
        };
        let _ = callback.send(event);
    }
}
