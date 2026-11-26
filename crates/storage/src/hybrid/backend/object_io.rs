//! Raw remote object operations and callback trait bridging.

use super::{
    Arc, Duration, HybridStorage, StorageBackend, StorageEvent, StorageObjectMetadata,
    StorageOutcome,
};

impl HybridStorage {
    /// Exact, caller-bounded bytes from one pinned object identity. Format
    /// validation and whether the key is reclaimable remain runtime policy.
    pub fn read_remote_range_within(
        &self,
        key: &str,
        metadata: &StorageObjectMetadata,
        range: std::ops::Range<u64>,
        deadline: &cntryl_midge_core::OperationDeadline,
    ) -> cntryl_midge_core::MidgeResult<Vec<u8>> {
        let timeout =
            Self::deadline_timeout(key, "pinned object range", self.callback_timeout, deadline)?;
        if range.start > range.end || range.end > metadata.size {
            return Err(cntryl_midge_core::MidgeError::Corruption(
                "pinned object range exceeds its metadata".into(),
            ));
        }
        let expected = range.end - range.start;
        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud_backend_for_key(key).submit_range_read_request(
            crate::StorageRequest::new(key, *deadline, timeout)
                .with_precondition(crate::StoragePrecondition::IfMatch(metadata.clone())),
            range,
            tx,
        );
        let bytes = rx
            .recv_timeout(timeout)
            .map_err(|error| {
                cntryl_midge_core::MidgeError::Timeout(format!("pinned object range: {error}"))
            })?
            .map_err(|error| {
                cntryl_midge_core::MidgeError::Internal(format!("pinned object range: {error}"))
            })?;
        if bytes.len() as u64 != expected {
            return Err(cntryl_midge_core::MidgeError::Corruption(
                "pinned object range has a different length".into(),
            ));
        }
        Ok(bytes)
    }

    pub(super) fn cloud_backend_for_key(&self, key: &str) -> &Arc<dyn StorageBackend> {
        if key.starts_with(cntryl_midge_core::cloud_layout::CloudObjectLayout::WAL_PREFIX) {
            &self.stores.wal
        } else if key.starts_with(cntryl_midge_core::cloud_layout::CloudObjectLayout::SST_PREFIX) {
            &self.stores.sst
        } else {
            &self.stores.control
        }
    }

    #[cfg(any(test, feature = "internal-testing"))]
    pub(super) fn read_object_from_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<Vec<u8>, crate::StorageError> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_metadata_read_request(
            crate::StorageRequest::new(
                key,
                cntryl_midge_core::OperationDeadline::from_budget(callback_timeout),
                callback_timeout,
            ),
            tx,
        );
        match rx.recv_timeout(callback_timeout) {
            Ok(Ok((data, _metadata))) => Ok(data),
            Ok(Err(error)) => Err(crate::StorageError::new(
                error.kind(),
                format!("cloud object '{key}' unreadable: {error}"),
            )),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(crate::storage_timeout_error(
                format!("cloud read timed out for '{key}'"),
            )),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(crate::StorageError::io(
                format!("cloud read callback closed for '{key}'"),
            )),
        }
    }

    #[cfg(any(test, feature = "internal-testing"))]
    pub(super) fn read_object_from_backend_within(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
        deadline: &cntryl_midge_core::OperationDeadline,
    ) -> cntryl_midge_core::MidgeResult<Vec<u8>> {
        let timeout = Self::deadline_timeout(key, "read object", callback_timeout, deadline)?;
        Self::read_object_from_backend_blocking(backend, key, timeout).map_err(|error| {
            if deadline.is_expired() || error.is_timeout() {
                cntryl_midge_core::MidgeError::Timeout(format!(
                    "object read timed out for '{key}': {error}"
                ))
            } else {
                cntryl_midge_core::MidgeError::Internal(error.to_string())
            }
        })
    }

    pub(super) fn head_object_from_backend_blocking(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
    ) -> Result<StorageObjectMetadata, crate::StorageError> {
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_head_request(
            crate::StorageRequest::new(
                key,
                cntryl_midge_core::OperationDeadline::from_budget(callback_timeout),
                callback_timeout,
            ),
            tx,
        );
        match rx.recv_timeout(callback_timeout) {
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Ok(metadata),
            }) => {
                let _ = returned_key;
                Ok(metadata)
            }
            Ok(StorageEvent::HeadComplete {
                key: returned_key,
                result: StorageOutcome::Err(error),
            }) => {
                let _ = returned_key;
                Err(crate::StorageError::new(
                    error.kind(),
                    format!(
                        "cloud object '{key}' unreadable during cached proof revalidation: {error}"
                    ),
                ))
            }
            Ok(other) => Err(crate::StorageError::protocol(format!(
                "unexpected cloud HEAD response for '{key}': {other:?}"
            ))),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(crate::storage_timeout_error(
                format!("cloud HEAD timed out for '{key}'"),
            )),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(crate::StorageError::io(
                format!("cloud HEAD callback closed for '{key}'"),
            )),
        }
    }

    pub(super) fn storage_error_indicates_missing(error: &crate::StorageError) -> bool {
        error.is_not_found()
    }

    pub(super) fn storage_error_indicates_precondition_failure(
        error: &crate::StorageError,
    ) -> bool {
        error.is_precondition_failed()
    }

    pub(super) fn storage_error_indicates_timeout(error: &crate::StorageError) -> bool {
        error.is_timeout()
    }

    #[cfg(any(test, feature = "internal-testing"))]
    pub(crate) fn object_exists_in_backend_within(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        callback_timeout: Duration,
        deadline: &cntryl_midge_core::OperationDeadline,
    ) -> cntryl_midge_core::MidgeResult<bool> {
        let timeout =
            Self::deadline_timeout(key, "HEAD object existence", callback_timeout, deadline)?;
        let (tx, rx) = std::sync::mpsc::channel();
        backend.submit_head_request(crate::StorageRequest::new(key, *deadline, timeout), tx);
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Ok(_),
                ..
            }) => Ok(true),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if Self::storage_error_indicates_missing(&error) => Ok(false),
            Ok(StorageEvent::HeadComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => {
                if deadline.is_expired() || Self::storage_error_indicates_timeout(&error) {
                    Err(cntryl_midge_core::MidgeError::Timeout(format!(
                        "object HEAD timed out for '{key}': {error}"
                    )))
                } else {
                    Err(cntryl_midge_core::MidgeError::Internal(format!(
                        "object '{key}' HEAD failed: {error}"
                    )))
                }
            }
            Ok(other) => Err(cntryl_midge_core::MidgeError::Internal(format!(
                "unexpected storage HEAD response for '{key}': {other:?}"
            ))),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                Err(cntryl_midge_core::MidgeError::Timeout(format!(
                    "object HEAD timed out for '{key}'"
                )))
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                Err(cntryl_midge_core::MidgeError::Internal(format!(
                    "object HEAD callback closed for '{key}'"
                )))
            }
        }
    }
}
