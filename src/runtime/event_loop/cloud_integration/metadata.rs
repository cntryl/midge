//! Remote metadata snapshots used by callerless WAL cleanup.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::CloudMetadataPruneSnapshot;

impl EventLoop {
    pub(super) fn cloud_metadata_prune_snapshot_for_wal_cleanup(
        &self,
    ) -> crate::common::MidgeResult<Option<CloudMetadataPruneSnapshot>> {
        let Some(cloud) = self.cloud_metadata_storage.as_ref() else {
            return Ok(None);
        };
        let budget = self
            .hybrid_storage
            .as_ref()
            .and_then(|storage| storage.maintenance_memory())
            .ok_or_else(|| {
                crate::common::MidgeError::Internal(
                    "cloud metadata cleanup requires configured maintenance memory".into(),
                )
            })?;

        Ok(Some(
            CloudMetadataPruneSnapshot::new(
                cloud.clone(),
                self.state.db_path.clone(),
                self.state.fs.clone(),
                self.state.recovery_policy(),
                budget,
                self.metadata_publication_lock.clone(),
            )
            .with_progress(self.cloud_wal_prune_progress.clone()),
        ))
    }

    #[cfg(test)]
    pub(super) fn verify_cloud_metadata_for_wal_cleanup(&self) -> Result<(), String> {
        self.cloud_metadata_prune_snapshot_for_wal_cleanup()
            .map_err(|error| error.to_string())?
            .map_or(Ok(()), |snapshot| {
                snapshot.verify_exact_then(
                    &crate::common::OperationDeadline::unbounded(),
                    |_, _| Ok(()),
                )
            })
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::common::{MidgeError, OperationDeadline};
    use crate::runtime::event_loop::tests::{create_test_cloud_event_loop, create_test_event_loop};
    use std::sync::Arc;
    use std::time::Duration;

    fn event_loop_with_control_dispatcher(
        db_path: &std::path::Path,
        control_cloud: Arc<crate::storage::cloud::CloudStorage>,
        metadata_publication_lock: crate::runtime::MetadataPublicationLock,
    ) -> crate::common::MidgeResult<super::EventLoop> {
        let state = crate::runtime::state::RuntimeState::try_new(
            db_path.to_path_buf(),
            false,
            crate::config::RecoveryPolicy::Strict,
        )?;
        crate::metadata::ManifestPersistence::save(db_path, &state.manifest)
            .map_err(MidgeError::Internal)?;
        let local = Arc::new(crate::storage::filesystem::FileSystem::new(
            db_path.join("hybrid-local"),
        )?);
        let data_cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            "data-dispatcher".to_string(),
        ));
        let hybrid_storage = Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            data_cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let config = crate::runtime::RuntimeConfig {
            hybrid_storage: Some(hybrid_storage),
            cloud_metadata_storage: Some(control_cloud),
            metadata_publication_lock,
            ..crate::runtime::RuntimeConfig::default()
        };

        super::EventLoop::new(
            state,
            false,
            Arc::new(crate::runtime::ResponseRouter::new()),
            config,
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )
    }

    fn mirror_local_metadata(
        cloud: &crate::storage::cloud::CloudStorage,
        db_path: &std::path::Path,
    ) -> crate::common::MidgeResult<()> {
        let deadline = OperationDeadline::unbounded();
        for file_name in crate::metadata::files::CLOUD_MIRRORED {
            let path = db_path.join(file_name);
            if !path.exists() {
                continue;
            }
            crate::runtime::hybrid_persistence::conditional_metadata_mirror_put(
                cloud,
                file_name,
                std::fs::read(path)?,
                0,
                &deadline,
            )?;
        }
        Ok(())
    }

    #[test]
    fn should_refuse_remote_snapshot_when_shared_budget_is_missing() {
        // Arrange
        let mut el = create_test_event_loop().unwrap();
        el.cloud_metadata_storage =
            Some(Arc::new(crate::storage::cloud::CloudStorage::with_mock()));

        // Act
        let result = el.cloud_metadata_prune_snapshot_for_wal_cleanup();

        // Assert
        assert!(matches!(result, Err(MidgeError::Internal(_))));
    }

    #[test]
    fn should_refuse_remote_snapshot_when_storage_budget_is_unconfigured() {
        // Arrange
        let mut el = create_test_event_loop().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let local =
            Arc::new(crate::storage::filesystem::FileSystem::new(directory.path()).unwrap());
        el.hybrid_storage = Some(Arc::new(crate::storage::HybridStorage::with_policy(
            local.clone(),
            local,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )));
        el.cloud_metadata_storage =
            Some(Arc::new(crate::storage::cloud::CloudStorage::with_mock()));

        // Act
        let result = el.cloud_metadata_prune_snapshot_for_wal_cleanup();

        // Assert
        assert!(matches!(result, Err(MidgeError::Internal(_))));
        assert!(el
            .hybrid_storage
            .as_ref()
            .unwrap()
            .maintenance_memory()
            .is_none());
    }

    #[test]
    fn should_use_shared_budget_when_remote_snapshot_verifies_metadata() {
        // Arrange
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )
        .unwrap();
        el.cloud_metadata_storage =
            Some(Arc::new(crate::storage::cloud::CloudStorage::with_mock()));
        let budget = el
            .hybrid_storage
            .as_ref()
            .unwrap()
            .maintenance_memory()
            .unwrap();
        let remaining = budget.limit().saturating_sub(budget.used());
        assert!(remaining > 0, "test requires available maintenance memory");
        let _held = budget.reserve(remaining, "active maintenance").unwrap();
        let snapshot = el
            .cloud_metadata_prune_snapshot_for_wal_cleanup()
            .unwrap()
            .unwrap();

        // Act
        let result: crate::common::MidgeResult<()> =
            snapshot.verify_exact_then(&OperationDeadline::unbounded(), |_, _| {
                panic!("unadmitted metadata must not authorize cleanup");
            });

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
    }

    #[test]
    fn should_serialize_runtime_wired_cleanup_across_separately_constructed_dispatchers(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: each event loop receives an independently constructed
        // control dispatcher/backend for the same logical control location.
        // Their common lock must arrive through RuntimeConfig rather than from
        // a manually assembled snapshot.
        let first_directory = tempfile::tempdir()?;
        let second_directory = tempfile::tempdir()?;
        let metadata_publication_lock = crate::runtime::MetadataPublicationLock::default();
        let first_control = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            "control-location".to_string(),
        ));
        let second_control = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            "control-location".to_string(),
        ));
        let first = event_loop_with_control_dispatcher(
            first_directory.path(),
            first_control.clone(),
            metadata_publication_lock.clone(),
        )?;
        let second = event_loop_with_control_dispatcher(
            second_directory.path(),
            second_control.clone(),
            metadata_publication_lock,
        )?;
        mirror_local_metadata(&first_control, &first.state.db_path)?;
        mirror_local_metadata(&second_control, &second.state.db_path)?;
        let first_snapshot = first
            .cloud_metadata_prune_snapshot_for_wal_cleanup()?
            .expect("configured control dispatcher creates cleanup snapshot");
        let second_snapshot = second
            .cloud_metadata_prune_snapshot_for_wal_cleanup()?
            .expect("configured control dispatcher creates cleanup snapshot");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first_worker = std::thread::spawn(move || {
            first_snapshot.verify_exact_then(&OperationDeadline::unbounded(), |_, _| {
                entered_tx.send(()).expect("signal held publication lock");
                release_rx.recv().expect("release publication lock");
                Ok::<(), MidgeError>(())
            })
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first runtime cleanup holds the configured publication lock");
        let deadline = OperationDeadline::from_budget(Duration::from_millis(50));

        // Act: this is the real EventLoop cleanup snapshot path, not a
        // snapshot constructed with a lock supplied directly by the test.
        let result: crate::common::MidgeResult<()> = second_snapshot
            .verify_exact_then(&deadline, |_, _| {
                panic!("second runtime cleanup must not overlap control publication")
            });
        release_tx.send(()).expect("release first runtime cleanup");

        // Assert
        assert!(matches!(result, Err(MidgeError::Timeout(_))));
        first_worker
            .join()
            .expect("join first runtime cleanup")
            .expect("first cleanup completes after release");
        Ok(())
    }
}
