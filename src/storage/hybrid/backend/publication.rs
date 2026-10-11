//! Immutable SST publication and retry compatibility.

use super::HybridStorage;
#[cfg(test)]
use super::{mpsc, Arc, StorageBackend, StorageEvent, StorageOutcome};
#[cfg(test)]
use crate::common::{MidgeError, MidgeResult, OperationDeadline};

impl HybridStorage {
    /// Byte-oriented fixture publication; runtime SST publishers use
    /// `publish_immutable_file` with shared admission and bounded readback.
    /// Retries succeed only when an existing object holds exactly these bytes.
    #[cfg(test)]
    pub(crate) fn publish_immutable_object_within(
        &self,
        key: &str,
        data: &[u8],
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        self.ensure_remote_immutable_published(key, data, deadline)
    }

    #[cfg(test)]
    fn ensure_remote_immutable_published(
        &self,
        key: &str,
        data: &[u8],
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        let exists = Self::object_exists_in_backend_within(
            &self.stores.sst,
            key,
            self.callback_timeout,
            deadline,
        )?;
        if exists {
            return Self::ensure_backend_object_matches(
                &self.stores.sst,
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
        let (tx, rx) = std::sync::mpsc::channel();
        let timeout = Self::deadline_timeout(
            key,
            "conditional immutable upload",
            self.callback_timeout,
            deadline,
        )?;
        self.stores.sst.submit_write_request(
            crate::storage::StorageRequest::new(key, *deadline, timeout)
                .with_precondition(crate::storage::StoragePrecondition::IfAbsent),
            upload,
            tx,
        );
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
                    &self.stores.sst,
                    key,
                    data,
                    Some(&error.to_string()),
                    self.callback_timeout,
                    deadline,
                )
            }
            other => Err(MidgeError::Internal(format!(
                "unexpected cloud immutable upload response: {other:?}"
            ))),
        }
    }

    #[cfg(test)]
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

    #[cfg(test)]
    fn publication_error(context: &str, error: MidgeError) -> MidgeError {
        match error {
            MidgeError::Timeout(message) => MidgeError::Timeout(format!("{context}: {message}")),
            other => MidgeError::Internal(format!("{context}: {other}")),
        }
    }

    /// Delete one immutable key from the remote backend. The caller owns any
    /// manifest or lifecycle decision.
    pub(crate) fn delete_immutable_object_blocking(
        &self,
        key: &str,
    ) -> crate::common::MidgeResult<()> {
        match Self::delete_object_from_backend_blocking(
            &self.stores.sst,
            key,
            self.callback_timeout,
        ) {
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

        Ok(())
    }
}
