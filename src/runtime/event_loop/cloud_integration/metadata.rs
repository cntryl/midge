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
}
