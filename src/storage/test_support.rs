use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};

use crate::common::MidgeResult;

use super::filesystem::FileSystem;
use super::{HybridStorage, StorageEvent};

static SIMULATED_CLOUD_BUDGETS: OnceLock<Mutex<HashMap<PathBuf, u64>>> = OnceLock::new();

fn simulated_cloud_budgets() -> &'static Mutex<HashMap<PathBuf, u64>> {
    SIMULATED_CLOUD_BUDGETS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn register_simulated_cloud_budget(db_path: &Path, budget_bytes: u64) {
    let mut budgets = simulated_cloud_budgets()
        .lock()
        .expect("lock simulated cloud budget registry");
    budgets.insert(db_path.to_path_buf(), budget_bytes);
}

fn take_simulated_cloud_budget(db_path: &Path) -> Option<u64> {
    let mut budgets = simulated_cloud_budgets()
        .lock()
        .expect("lock simulated cloud budget registry");
    budgets.remove(db_path)
}

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
) -> MidgeResult<CloudBackedTestSetup> {
    // Simulate cloud with a separate filesystem-backed store under db_path.
    let cloud_root = db_path.join("cloud_store");
    let recovery_cloud_wal_dir = cloud_root.join("wal");
    let _ = std::fs::create_dir_all(&recovery_cloud_wal_dir);

    let local_backend = Arc::new(FileSystem::new(db_path.join("hybrid_local"))?);
    let cloud_backend = Arc::new(FileSystem::new(cloud_root.clone())?);

    let (tx, rx) = crossbeam::channel::unbounded::<StorageEvent>();
    let hybrid_storage = if let Some(budget_bytes) = take_simulated_cloud_budget(db_path) {
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

#[cfg(test)]
mod test_support_impl {
    use super::*;
    use crate::storage::abstraction::{Storage, StorageError, StorageErrorKind, StoragePath};
    use crate::storage::LocalFsStorage;

    /// Opaque temp local storage for unit tests.
    ///
    /// This lives under `storage/` so other layers (e.g. WAL) can test against
    /// a real backend without directly touching filesystem paths.
    #[allow(dead_code)]
    pub(crate) struct TempLocalStorage {
        _tempdir: tempfile::TempDir,
        pub storage: Arc<dyn Storage>,
        pub root: StoragePath,
    }

    #[allow(dead_code)]
    pub(crate) fn build_temp_local_storage(
    ) -> crate::storage::abstraction::StorageResult<TempLocalStorage> {
        let tempdir = tempfile::TempDir::new()
            .map_err(|e| StorageError::with_source(StorageErrorKind::Io, "TempDir::new", e))?;

        let storage: Arc<dyn Storage> = Arc::new(LocalFsStorage::new(tempdir.path())?);
        let root = StoragePath::new("");

        Ok(TempLocalStorage {
            _tempdir: tempdir,
            storage,
            root,
        })
    }
}
