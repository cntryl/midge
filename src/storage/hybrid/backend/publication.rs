//! Immutable SST publication and retry compatibility.

use super::{mpsc, Arc, HybridStorage, StorageBackend, StorageEvent, StorageOutcome};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};

impl HybridStorage {
    /// Publish immutable bytes to the remote backend and local cache. Retries
    /// succeed only when an existing object contains exactly the same bytes.
    pub(crate) fn publish_immutable_object_within(
        &self,
        key: &str,
        data: Vec<u8>,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        let local_exists = self.ensure_local_immutable_retry_compatible(key, &data, deadline)?;
        self.ensure_remote_immutable_published(key, &data, deadline)?;
        if !local_exists {
            self.write_local_immutable_cache(key, data, deadline)?;
        }
        Ok(())
    }

    fn ensure_local_immutable_retry_compatible(
        &self,
        key: &str,
        data: &[u8],
        deadline: &OperationDeadline,
    ) -> MidgeResult<bool> {
        let exists = Self::object_exists_in_backend_within(
            &self.local,
            key,
            self.callback_timeout,
            deadline,
        )
        .map_err(|error| Self::publication_error("local immutable cache preflight", error))?;
        if !exists {
            return Ok(false);
        }

        let existing = Self::read_object_from_backend_within(
            &self.local,
            key,
            self.callback_timeout,
            deadline,
        )?;
        if existing != data {
            return Err(MidgeError::Internal(format!(
                "local cache already exists with different bytes for immutable object '{key}'"
            )));
        }
        Ok(true)
    }

    fn ensure_remote_immutable_published(
        &self,
        key: &str,
        data: &[u8],
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        let exists = Self::object_exists_in_backend_within(
            &self.cloud,
            key,
            self.callback_timeout,
            deadline,
        )?;
        if exists {
            return Self::ensure_backend_object_matches(
                &self.cloud,
                key,
                data,
                None,
                self.callback_timeout,
                deadline,
            );
        }

        Self::deadline_timeout(
            key,
            "conditional immutable upload",
            self.callback_timeout,
            deadline,
        )?;
        let upload = data.to_vec();
        let headers = vec![("If-None-Match".into(), "*".into())];
        let (tx, rx) = std::sync::mpsc::channel();
        let timeout = Self::deadline_timeout(
            key,
            "conditional immutable upload",
            self.callback_timeout,
            deadline,
        )?;
        self.cloud
            .submit_write_with_headers_and_timeout(key, upload, headers, timeout, tx);
        let event = rx.recv_timeout(timeout).map_err(|error| match error {
            mpsc::RecvTimeoutError::Timeout => {
                MidgeError::Timeout("cloud immutable upload callback timed out".to_string())
            }
            mpsc::RecvTimeoutError::Disconnected => {
                MidgeError::Internal("cloud immutable upload callback channel closed".to_string())
            }
        })?;

        match event {
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            } => Ok(()),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            } => {
                if deadline.is_expired() || Self::storage_error_indicates_timeout(&error) {
                    return Err(MidgeError::Timeout(format!(
                        "cloud immutable upload timed out for '{key}': {error}"
                    )));
                }
                Self::ensure_backend_object_matches(
                    &self.cloud,
                    key,
                    data,
                    Some(&error),
                    self.callback_timeout,
                    deadline,
                )
            }
            other => Err(MidgeError::Internal(format!(
                "unexpected cloud immutable upload response: {other:?}"
            ))),
        }
    }

    fn ensure_backend_object_matches(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        expected: &[u8],
        upload_error: Option<&str>,
        callback_timeout: std::time::Duration,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        let existing =
            Self::read_object_from_backend_within(backend, key, callback_timeout, deadline)
                .map_err(|error| {
                    Self::publication_error(
                        &format!(
                            "cloud immutable upload failed{}; readback failed",
                            upload_error.map_or_else(String::new, |error| format!(": {error}"))
                        ),
                        error,
                    )
                })?;
        if existing == expected {
            return Ok(());
        }
        Err(MidgeError::Internal(format!(
            "cloud immutable upload failed{}: object '{key}' contains different bytes",
            upload_error.map_or_else(String::new, |error| format!(": {error}"))
        )))
    }

    fn write_local_immutable_cache(
        &self,
        key: &str,
        data: Vec<u8>,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        let headers = vec![("If-None-Match".into(), "*".into())];
        let (tx, rx) = std::sync::mpsc::channel();
        let timeout = Self::deadline_timeout(
            key,
            "local immutable cache write",
            self.callback_timeout,
            deadline,
        )?;
        self.local
            .submit_write_with_headers_and_timeout(key, data, headers, timeout, tx);
        let event = rx.recv_timeout(timeout).map_err(|error| match error {
            mpsc::RecvTimeoutError::Timeout => {
                MidgeError::Timeout("local immutable cache write callback timed out".to_string())
            }
            mpsc::RecvTimeoutError::Disconnected => MidgeError::Internal(
                "local immutable cache write callback channel closed".to_string(),
            ),
        })?;
        match event {
            StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            } => Ok(()),
            StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            } => {
                if deadline.is_expired() || Self::storage_error_indicates_timeout(&error) {
                    Err(MidgeError::Timeout(format!(
                        "local immutable cache write timed out: {error}"
                    )))
                } else {
                    Err(MidgeError::Internal(format!(
                        "local immutable cache write failed: {error}"
                    )))
                }
            }
            other => Err(MidgeError::Internal(format!(
                "unexpected local immutable cache write response: {other:?}"
            ))),
        }
    }

    fn publication_error(context: &str, error: MidgeError) -> MidgeError {
        match error {
            MidgeError::Timeout(message) => MidgeError::Timeout(format!("{context}: {message}")),
            other => MidgeError::Internal(format!("{context}: {other}")),
        }
    }

    /// Delete one immutable key from the remote backend and best-effort local
    /// cache. The caller owns any manifest or lifecycle decision.
    pub(crate) fn delete_immutable_object_blocking(
        &self,
        key: &str,
    ) -> crate::common::MidgeResult<()> {
        match Self::delete_object_from_backend_blocking(&self.cloud, key, self.callback_timeout) {
            Ok(true) => {
                tracing::info!(key, "deleted obsolete remote immutable object");
            }
            Ok(false) => {
                tracing::debug!(key, "remote immutable object already missing");
            }
            Err(error) => {
                return Err(crate::common::MidgeError::Internal(format!(
                    "remote immutable object delete failed: {error}"
                )));
            }
        }

        // This runs inside the tracked GC worker that owns this deletion.
        // Avoid a detached local-cache delete that could outlive the lease.
        match Self::delete_object_from_backend_blocking(&self.local, key, self.callback_timeout) {
            Ok(true) => {
                tracing::debug!(key, "deleted obsolete local immutable cache object");
            }
            Ok(false) => {
                tracing::debug!(key, "local immutable cache object already missing");
            }
            Err(error) => {
                tracing::warn!(
                    key,
                    error,
                    "failed to delete obsolete local immutable cache object"
                );
            }
        }

        Ok(())
    }
}
