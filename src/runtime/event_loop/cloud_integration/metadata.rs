//! Committed shared metadata authority for WAL retirement.

use super::super::{CloudCoordinator, EventLoop};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::hybrid_persistence::SharedMetadataSnapshot;

impl CloudCoordinator {
    pub(in crate::runtime::event_loop) fn metadata_snapshot_for_wal_cleanup(
        &self,
        leader_store: Option<std::sync::Arc<dyn crate::lease::LeaderStore>>,
        holder_id: &str,
        writer_epoch: u64,
    ) -> MidgeResult<SharedMetadataSnapshot> {
        let cloud = self.cloud_metadata_storage.clone().ok_or_else(|| {
            MidgeError::Fenced("cloud WAL retirement has no shared metadata store".into())
        })?;
        let budget = self
            .hybrid_storage
            .as_ref()
            .and_then(|storage| storage.maintenance_memory())
            .ok_or_else(|| {
                MidgeError::Internal(
                    "cloud metadata retirement requires configured maintenance memory".into(),
                )
            })?;
        let store = leader_store.ok_or_else(|| {
            MidgeError::Fenced("cloud WAL retirement has no metadata authority".into())
        })?;
        Ok(SharedMetadataSnapshot::new(
            cloud,
            budget,
            store,
            holder_id.to_string(),
            writer_epoch,
        ))
    }
}

impl EventLoop {
    pub(in crate::runtime::event_loop) fn cloud_metadata_snapshot_for_wal_cleanup(
        &self,
    ) -> MidgeResult<SharedMetadataSnapshot> {
        self.cloud_coordinator.metadata_snapshot_for_wal_cleanup(
            self.fencing.leader_store.clone(),
            self.fencing.leader_holder_id.as_deref().unwrap_or_default(),
            self.fencing.writer_epoch,
        )
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn should_refuse_retirement_when_shared_metadata_is_unconfigured() {
        // Arrange
        let el = crate::runtime::event_loop::tests::create_test_event_loop().unwrap();
        // Act
        let result = el.cloud_metadata_snapshot_for_wal_cleanup();
        // Assert
        assert!(matches!(result, Err(crate::common::MidgeError::Fenced(_))));
    }
}
