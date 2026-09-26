//! Admitted control-object reads and conditional publication.

use super::{
    Arc, HybridStorage, StorageBackend, StorageEvent, StorageObjectMetadata, StorageOutcome,
};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use std::sync::mpsc;
use std::time::Duration;

const RANGE_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub(crate) struct ControlObject {
    bytes: Vec<u8>,
    metadata: StorageObjectMetadata,
    // The object cannot be cloned independently of its charge.
    _memory: Arc<ResourceReservation>,
}

impl ControlObject {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn metadata(&self) -> &StorageObjectMetadata {
        &self.metadata
    }
}

impl HybridStorage {
    pub(crate) fn read_control_object(
        &self,
        key: &str,
        budget: &ResourceBudget,
        deadline: &OperationDeadline,
    ) -> MidgeResult<Option<ControlObject>> {
        Self::read_control_from_backend(
            self.cloud_backend_for_key(key),
            key,
            budget,
            self.callback_timeout,
            deadline,
        )
    }

    pub(crate) fn read_control_from_backend(
        backend: &Arc<dyn StorageBackend>,
        key: &str,
        budget: &ResourceBudget,
        callback_timeout: Duration,
        deadline: &OperationDeadline,
    ) -> MidgeResult<Option<ControlObject>> {
        let Some(metadata) = control_head(backend, key, callback_timeout, deadline)? else {
            return Ok(None);
        };
        if !metadata.same_version(&metadata) {
            return Err(MidgeError::Corruption(
                "control object has no pinned identity".into(),
            ));
        }
        let length = usize::try_from(metadata.size).map_err(|_| {
            MidgeError::ResourceLimit("control object exceeds address space".into())
        })?;
        let memory = Arc::new(budget.reserve(
            length.saturating_add(length.min(RANGE_BYTES).saturating_mul(2)),
            "control object and range workspace",
        )?);
        let mut bytes = Vec::with_capacity(length);
        for start in (0..length).step_by(RANGE_BYTES) {
            let end = start.saturating_add(RANGE_BYTES).min(length);
            let timeout = Self::deadline_timeout(key, "control range", callback_timeout, deadline)?;
            let (tx, rx) = mpsc::channel();
            backend.submit_read_range_with_reservation(
                key,
                start as u64..end as u64,
                metadata.clone(),
                timeout,
                Arc::clone(&memory),
                tx,
            );
            let range = rx
                .recv_timeout(timeout)
                .map_err(|error| MidgeError::Timeout(format!("control range: {error}")))?
                .map_err(|error| control_error_with_reservation(&error, &memory))?;
            if range.len() != end - start {
                return Err(MidgeError::Corruption(
                    "control range has incorrect length".into(),
                ));
            }
            bytes.extend_from_slice(&range);
        }
        let after = control_head(backend, key, callback_timeout, deadline)?.ok_or_else(|| {
            MidgeError::Corruption("control object disappeared during read".into())
        })?;
        if !metadata.same_version(&after) {
            return Err(MidgeError::Corruption(
                "control object changed during read".into(),
            ));
        }
        Ok(Some(ControlObject {
            bytes,
            metadata,
            _memory: memory,
        }))
    }

    pub(crate) fn write_control_object(
        &self,
        key: &str,
        expected: Option<&StorageObjectMetadata>,
        bytes: &[u8],
        budget: &ResourceBudget,
        deadline: &OperationDeadline,
    ) -> MidgeResult<ControlObject> {
        let precondition = match expected {
            Some(metadata) => {
                crate::storage::conditional_object_identity(
                    &metadata.etag,
                    metadata.generation.as_deref(),
                )
                .ok_or_else(|| {
                    MidgeError::Corruption("control CAS has no pinned identity".into())
                })?;
                crate::storage::StoragePrecondition::IfMatch(metadata.clone())
            }
            None => crate::storage::StoragePrecondition::IfAbsent,
        };
        let memory = Arc::new(budget.reserve(
            bytes.len().saturating_mul(4),
            "control upload provider workspace",
        )?);
        let timeout = Self::deadline_timeout(key, "control CAS", self.callback_timeout, deadline)?;
        let (tx, rx) = mpsc::channel();
        self.cloud_backend_for_key(key).submit_write_request(
            crate::storage::StorageRequest::new(key, *deadline, self.callback_timeout)
                .with_precondition(precondition)
                .with_reservation(Arc::clone(&memory)),
            bytes.to_vec(),
            tx,
        );
        match rx.recv_timeout(timeout) {
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Ok(()),
                ..
            }) => {}
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if Self::storage_error_indicates_precondition_failure(&error) => {
                return Err(MidgeError::Busy(error.to_string()))
            }
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) if Self::storage_error_indicates_timeout(&error) => {
                return Err(MidgeError::Timeout(error.to_string()))
            }
            Ok(StorageEvent::WriteComplete {
                result: StorageOutcome::Err(error),
                ..
            }) => return Err(control_error_with_reservation(&error, &memory)),
            Ok(event) => {
                return Err(MidgeError::Internal(format!(
                    "control CAS failed: {event:?}"
                )))
            }
            Err(error) => return Err(MidgeError::Timeout(format!("control CAS: {error}"))),
        }
        drop(memory);
        let proof = self
            .read_control_object(key, budget, deadline)?
            .ok_or_else(|| MidgeError::Corruption("control CAS readback is missing".into()))?;
        if proof.bytes() != bytes {
            return Err(MidgeError::Corruption(
                "control CAS readback differs".into(),
            ));
        }
        Ok(proof)
    }
}

fn control_error_with_reservation(
    error: &crate::storage::StorageError,
    memory: &ResourceReservation,
) -> MidgeError {
    let classified = control_error(error);
    memory.restore_related_contention();
    classified
}

fn control_error(error: &crate::storage::StorageError) -> MidgeError {
    use crate::storage::StorageErrorKind;
    let message = error.message().to_string();
    match error.kind() {
        StorageErrorKind::ResourceLimit => MidgeError::ResourceLimit(message),
        StorageErrorKind::Corruption => MidgeError::Corruption(message),
        StorageErrorKind::Timeout => MidgeError::Timeout(message),
        _ => MidgeError::Internal(message),
    }
}

fn control_head(
    backend: &Arc<dyn StorageBackend>,
    key: &str,
    callback_timeout: Duration,
    deadline: &OperationDeadline,
) -> MidgeResult<Option<StorageObjectMetadata>> {
    let timeout = HybridStorage::deadline_timeout(key, "control HEAD", callback_timeout, deadline)?;
    let (tx, rx) = mpsc::channel();
    backend.submit_range_head(key, timeout, tx);
    match rx.recv_timeout(timeout) {
        Ok(StorageEvent::HeadComplete {
            key: actual,
            result: StorageOutcome::Ok(metadata),
        }) if actual == key => Ok(Some(metadata)),
        Ok(StorageEvent::HeadComplete {
            key: actual,
            result: StorageOutcome::Err(error),
        }) if actual == key && HybridStorage::storage_error_indicates_missing(&error) => Ok(None),
        Ok(StorageEvent::HeadComplete {
            key: actual,
            result: StorageOutcome::Err(error),
        }) if actual == key => Err(control_error(&error)),
        Ok(event) => Err(MidgeError::Internal(format!(
            "control HEAD failed: {event:?}"
        ))),
        Err(error) => Err(MidgeError::Timeout(format!("control HEAD: {error}"))),
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn should_keep_error_class_when_storage_error_crosses_control_io() {
        // Arrange
        let errors = [
            crate::storage::StorageError::from(MidgeError::ResourceLimit("x".into())),
            crate::storage::StorageError::from(MidgeError::Corruption("y".into())),
        ];

        // Act
        let classified: Vec<_> = errors.iter().map(super::control_error).collect();

        // Assert
        assert!(
            matches!(classified[0], MidgeError::ResourceLimit(_)),
            "{classified:?}"
        );
        assert!(
            matches!(classified[1], MidgeError::Corruption(_)),
            "{classified:?}"
        );
    }
    use super::*;
    use crate::common::MidgeError;
    use crate::storage::cloud::CloudStorage;
    use std::sync::Arc;

    #[test]
    fn should_reject_control_object_before_reading_when_shared_budget_is_exhausted() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let storage = HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap()),
            Arc::new(CloudStorage::with_mock()),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        storage
            .compare_exchange_remote_object("metadata/catalog", None, vec![7; 1024])
            .unwrap();
        let budget = ResourceBudget::new(512);

        // Act
        let result = storage.read_control_object(
            "metadata/catalog",
            &budget,
            &OperationDeadline::unbounded(),
        );

        // Assert
        assert!(
            matches!(result, Err(MidgeError::ResourceLimit(_))),
            "{result:?}"
        );
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use crate::storage::cloud::{CloudBackend, CloudCallback, CloudStorage, MockCloudBackend};

    #[derive(Default)]
    struct PendingUpload {
        inner: MockCloudBackend,
        pending: parking_lot::Mutex<Option<(Vec<u8>, CloudCallback)>>,
    }
    impl CloudBackend for PendingUpload {
        crate::storage::cloud::unsupported_cloud_backend!(
            submit_get,
            submit_get_with_metadata,
            submit_delete,
            submit_list,
        );

        fn submit_put(
            &self,
            _key: &str,
            bytes: Vec<u8>,
            _headers: Vec<(String, String)>,
            callback: CloudCallback,
        ) {
            *self.pending.lock() = Some((bytes, callback));
        }
        fn submit_get_range(
            &self,
            key: &str,
            start: u64,
            end: Option<u64>,
            callback: CloudCallback,
        ) {
            self.inner.submit_get_range(key, start, end, callback);
        }
        fn submit_head(&self, key: &str, callback: CloudCallback) {
            self.inner.submit_head(key, callback);
        }
    }

    #[test]
    fn should_retain_control_upload_charge_after_timeout_until_provider_releases_body() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let provider = Arc::new(PendingUpload::default());
        let storage = HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap()),
            Arc::new(CloudStorage::new(provider.clone(), String::new())),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let budget = ResourceBudget::new(1024 * 1024);
        let bytes = vec![7; 4096];
        let deadline = OperationDeadline::from_budget(Duration::from_millis(200));

        // Act
        let result =
            storage.write_control_object("metadata/catalog", None, &bytes, &budget, &deadline);
        let retained = budget.used();
        drop(
            provider
                .pending
                .lock()
                .take()
                .expect("provider retains the body"),
        );
        let until = std::time::Instant::now() + Duration::from_secs(2);
        while budget.used() != 0 && std::time::Instant::now() < until {
            std::thread::yield_now();
        }

        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
        assert!(retained >= bytes.len());
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn should_not_start_control_upload_when_admission_fails() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let provider = Arc::new(PendingUpload::default());
        let storage = HybridStorage::with_policy(
            Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap()),
            Arc::new(CloudStorage::new(provider.clone(), String::new())),
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let budget = ResourceBudget::new(128);

        // Act
        let result = storage.write_control_object(
            "metadata/catalog",
            None,
            &[7; 128],
            &budget,
            &OperationDeadline::unbounded(),
        );

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert!(provider.pending.lock().is_none());
        assert_eq!(budget.used(), 0);
    }
}

#[cfg(test)]
#[path = "control_io_tests.rs"]
mod read_tests;
