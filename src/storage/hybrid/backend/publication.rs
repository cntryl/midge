//! Immutable SST publication and retry compatibility.

use super::{mpsc, Arc, HybridStorage, StorageBackend, StorageEvent, StorageOutcome};

impl HybridStorage {
    /// Publish immutable bytes to the remote backend and local cache. Retries
    /// succeed only when an existing object contains exactly the same bytes.
    pub(crate) fn publish_immutable_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> crate::common::MidgeResult<()> {
        let local_exists = self.ensure_local_immutable_retry_compatible(key, &data)?;
        self.ensure_remote_immutable_published(key, &data)?;
        if !local_exists {
            self.write_local_immutable_cache(key, data)?;
        }
        Ok(())
    }

    fn ensure_local_immutable_retry_compatible(
        &self,
        key: &str,
        data: &[u8],
    ) -> crate::common::MidgeResult<bool> {
        let exists =
            Self::object_exists_in_backend_blocking(&self.local, key, self.callback_timeout)
                .map_err(|error| {
                    crate::common::MidgeError::Internal(format!(
                        "local immutable cache preflight failed: {error}"
                    ))
                })?;
        if !exists {
            return Ok(false);
        }

        let existing =
            Self::read_cloud_object_from_backend_blocking(&self.local, key, self.callback_timeout)
                .map_err(crate::common::MidgeError::Internal)?;
        if existing != data {
            return Err(crate::common::MidgeError::Internal(format!(
                "local cache already exists with different bytes for immutable object '{key}'"
            )));
        }
        Ok(true)
    }

    fn ensure_remote_immutable_published(
        &self,
        key: &str,
        data: &[u8],
    ) -> crate::common::MidgeResult<()> {
        let exists =
            Self::object_exists_in_backend_blocking(&self.cloud, key, self.callback_timeout)
                .map_err(crate::common::MidgeError::Internal)?;
        if exists {
            return Self::ensure_backend_object_matches(
                &self.cloud,
                key,
                data,
                None,
                self.callback_timeout,
            );
        }

        let (tx, rx) = std::sync::mpsc::channel();
        self.cloud.submit_write_with_headers(
            key,
            data.to_vec(),
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );
        let event = rx
            .recv_timeout(self.callback_timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => crate::common::MidgeError::Timeout(
                    "cloud immutable upload callback timed out".to_string(),
                ),
                mpsc::RecvTimeoutError::Disconnected => crate::common::MidgeError::Internal(
                    "cloud immutable upload callback channel closed".to_string(),
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
            } => Self::ensure_backend_object_matches(
                &self.cloud,
                key,
                data,
                Some(&error),
                self.callback_timeout,
            ),
            other => Err(crate::common::MidgeError::Internal(format!(
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
    ) -> crate::common::MidgeResult<()> {
        let existing =
            Self::read_cloud_object_from_backend_blocking(backend, key, callback_timeout).map_err(
                |read_error| {
                    crate::common::MidgeError::Internal(format!(
                        "cloud immutable upload failed{}; readback failed: {read_error}",
                        upload_error.map_or_else(String::new, |error| format!(": {error}"))
                    ))
                },
            )?;
        if existing == expected {
            return Ok(());
        }
        Err(crate::common::MidgeError::Internal(format!(
            "cloud immutable upload failed{}: object '{key}' contains different bytes",
            upload_error.map_or_else(String::new, |error| format!(": {error}"))
        )))
    }

    fn write_local_immutable_cache(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> crate::common::MidgeResult<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.local.submit_write_with_headers(
            key,
            data,
            vec![("If-None-Match".into(), "*".into())],
            tx,
        );
        let event = rx
            .recv_timeout(self.callback_timeout)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => crate::common::MidgeError::Timeout(
                    "local immutable cache write callback timed out".to_string(),
                ),
                mpsc::RecvTimeoutError::Disconnected => crate::common::MidgeError::Internal(
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
            } => Err(crate::common::MidgeError::Internal(format!(
                "local immutable cache write failed: {error}"
            ))),
            other => Err(crate::common::MidgeError::Internal(format!(
                "unexpected local immutable cache write response: {other:?}"
            ))),
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
