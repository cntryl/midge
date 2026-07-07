use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::common::MidgeResult;

use super::filesystem::FileSystem;
use super::{HybridStorage, StorageEvent};

pub(crate) struct CloudBackedTestSetup {
    pub hybrid_storage: Arc<HybridStorage>,
    pub events: crossbeam::channel::Receiver<StorageEvent>,
    pub cloud_root: PathBuf,
    pub recovery_cloud_wal_dir: PathBuf,
}

/// Builds a deterministic, filesystem-backed “cloud” for tests.
///
/// The engine/testkit should not know about folders/blobs; it only needs the
/// resulting `HybridStorage`, event stream, and a recovery directory.
pub(crate) fn build_cloud_backed_filesystem_simulation(
    db_path: &Path,
    local_storage_budget_bytes: Option<u64>,
) -> MidgeResult<CloudBackedTestSetup> {
    // Simulate cloud with a separate filesystem-backed store under db_path.
    let cloud_root = db_path.join("cloud_store");
    let recovery_cloud_wal_dir = cloud_root.join("wal");
    let _ = std::fs::create_dir_all(&recovery_cloud_wal_dir);

    let local_backend = Arc::new(FileSystem::new(db_path.join("hybrid_local"))?);
    let cloud_backend = Arc::new(FileSystem::new(cloud_root.clone())?);

    let (tx, rx) = crossbeam::channel::unbounded::<StorageEvent>();
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

    Ok(CloudBackedTestSetup {
        hybrid_storage,
        events: rx,
        cloud_root,
        recovery_cloud_wal_dir,
    })
}
