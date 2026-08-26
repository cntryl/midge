//! Remote metadata snapshots used by callerless WAL cleanup.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::CloudMetadataPruneSnapshot;

impl EventLoop {
    pub(super) fn cloud_metadata_prune_snapshot_for_wal_cleanup(
        &self,
    ) -> Option<CloudMetadataPruneSnapshot> {
        let cloud = self.cloud_metadata_storage.as_ref()?;

        Some(CloudMetadataPruneSnapshot::new(
            cloud.clone(),
            self.state.db_path.clone(),
            self.state.fs.clone(),
            self.state.recovery_policy(),
        ))
    }

    #[cfg(test)]
    pub(super) fn verify_cloud_metadata_for_wal_cleanup(&self) -> Result<(), String> {
        self.cloud_metadata_prune_snapshot_for_wal_cleanup()
            .map_or(Ok(()), |snapshot| snapshot.verify_exact_then(|_, _| Ok(())))
            .map_err(|error| error.to_string())
    }
}
