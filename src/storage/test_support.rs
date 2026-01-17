#![allow(clippy::items_after_test_module)]

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::common::MidgeResult;

use super::filesystem::FileSystem;
use super::{HybridStorage, StorageEvent};

pub(crate) struct CloudBackedTestSetup {
    pub hybrid_storage: Arc<HybridStorage>,
    pub events: crossbeam::channel::Receiver<StorageEvent>,
    pub recovery_cloud_wal_dir: PathBuf,
}

/// Builds a deterministic, filesystem-backed “cloud” for tests.
///
/// The engine/testkit should not know about folders/blobs; it only needs the
/// resulting HybridStorage, event stream, and a recovery directory.
pub(crate) fn build_cloud_backed_filesystem_simulation(
    db_path: &Path,
) -> MidgeResult<CloudBackedTestSetup> {
    // Simulate cloud with a separate filesystem-backed store under db_path.
    let cloud_root = db_path.join("cloud_store");
    let recovery_cloud_wal_dir = cloud_root.join("wal");
    let _ = std::fs::create_dir_all(&recovery_cloud_wal_dir);

    let local_backend = Arc::new(FileSystem::new(db_path.join("hybrid_local"))?);
    let cloud_backend = Arc::new(FileSystem::new(cloud_root)?);

    let (tx, rx) = crossbeam::channel::unbounded::<StorageEvent>();
    let hybrid_storage = Arc::new(HybridStorage::new_with_event_sender(
        local_backend,
        cloud_backend,
        tx,
    ));

    Ok(CloudBackedTestSetup {
        hybrid_storage,
        events: rx,
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
