//! Immutable SST publication and retry compatibility.

use super::HybridStorage;
#[cfg(test)]
use super::{mpsc, StorageEvent, StorageOutcome};
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
            return self.ensure_remote_object_matches(key, data, None, deadline);
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
                self.ensure_remote_object_matches(key, data, Some(&error.to_string()), deadline)
            }
            other => Err(MidgeError::Internal(format!(
                "unexpected cloud immutable upload response: {other:?}"
            ))),
        }
    }

    #[cfg(test)]
    fn ensure_remote_object_matches(
        &self,
        key: &str,
        expected: &[u8],
        upload_error: Option<&str>,
        deadline: &OperationDeadline,
    ) -> MidgeResult<()> {
        let existing = Self::read_object_from_backend_within(
            &self.stores.sst,
            key,
            self.callback_timeout,
            deadline,
        )
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
}
