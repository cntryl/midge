//! The filesystem directory that stands in for cloud storage in
//! `Storage::CloudSimulated`, a supported open mode (#517).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common::MidgeResult;

use super::filesystem::FileSystem;
use super::{HybridStorage, StorageEvent};

/// The stores a simulated-cloud engine runs on.
pub(crate) struct SimulatedCloudStores {
    pub hybrid_storage: Arc<HybridStorage>,
    pub events: crossbeam::channel::Receiver<StorageEvent>,
    pub cloud_root: PathBuf,
    pub recovery_cloud_wal_dir: PathBuf,
}

/// Root of the filesystem store that simulates cloud storage under `db_path`.
pub(crate) fn simulated_cloud_root(db_path: &Path) -> PathBuf {
    db_path.join("cloud_store")
}

/// Build the simulated cloud: a `FileSystem` store at `cloud_store/` behind
/// `HybridStorage`, with the event stream and the WAL recovery directory.
///
/// # Errors
///
/// Fails when a store directory cannot be created.
pub(crate) fn build_simulated_cloud_stores(
    db_path: &Path,
    local_storage_budget_bytes: Option<u64>,
) -> MidgeResult<SimulatedCloudStores> {
    let cloud_root = simulated_cloud_root(db_path);
    let recovery_cloud_wal_dir = cloud_root.join("wal");
    std::fs::create_dir_all(&recovery_cloud_wal_dir)?;

    let local_backend = Arc::new(FileSystem::new(db_path.join("hybrid_local"))?);
    let cloud_backend = Arc::new(FileSystem::new(cloud_root.clone())?);

    let (tx, rx) = crossbeam::channel::bounded::<StorageEvent>(
        crate::storage::hybrid::backend::HYBRID_STORAGE_EVENT_CHANNEL_CAPACITY,
    );
    let hybrid_storage = if let Some(budget_bytes) = local_storage_budget_bytes {
        Arc::new(HybridStorage::with_policy_and_event_sender(
            local_backend,
            cloud_backend,
            crate::storage::hybrid::policy::StorageBudgetPolicy::new(budget_bytes),
            Some(tx),
        ))
    } else {
        Arc::new(HybridStorage::new_with_event_sender(
            local_backend,
            cloud_backend,
            tx,
        ))
    };

    Ok(SimulatedCloudStores {
        hybrid_storage,
        events: rx,
        cloud_root,
        recovery_cloud_wal_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_fail_simulated_cloud_open_when_wal_dir_cannot_be_created() {
        // Arrange: a regular file where the WAL directory must go.
        let temp = tempfile::tempdir().expect("temp dir");
        let cloud_root = simulated_cloud_root(temp.path());
        std::fs::create_dir_all(&cloud_root).expect("create cloud root");
        std::fs::write(cloud_root.join("wal"), b"not a directory").expect("block wal dir");

        // Act
        let result = build_simulated_cloud_stores(temp.path(), None);

        // Assert
        assert!(result.is_err());
    }
}
