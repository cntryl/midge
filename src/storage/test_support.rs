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
pub(crate) use test_support_impl::MockStorage;

#[cfg(test)]
mod test_support_impl {
    use super::*;
    use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};

    pub(crate) struct MockStorage {
        data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    }

    impl MockStorage {
        pub(crate) fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl Default for MockStorage {
        fn default() -> Self {
            Self::new()
        }
    }

    impl StorageBackend for MockStorage {
        fn submit_read(&self, key: &str, callback: StorageCallback) {
            let data = self.data.lock().expect("lock mock storage");
            let result = data
                .get(key)
                .cloned()
                .ok_or(crate::common::MidgeError::NotFound);

            let event = StorageEvent::ReadComplete {
                key: key.to_string(),
                result: match result {
                    Ok(value) => StorageOutcome::Ok(value),
                    Err(error) => StorageOutcome::Err(format!("{error:?}")),
                },
            };
            let _ = callback.send(event);
        }

        fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
            let mut storage = self.data.lock().expect("lock mock storage");
            storage.insert(key.to_string(), data);

            let event = StorageEvent::WriteComplete {
                key: key.to_string(),
                result: StorageOutcome::Ok(()),
            };
            let _ = callback.send(event);
        }

        fn submit_delete(&self, key: &str, callback: StorageCallback) {
            let mut storage = self.data.lock().expect("lock mock storage");
            storage.remove(key);

            let event = StorageEvent::DeleteComplete {
                key: key.to_string(),
                result: StorageOutcome::Ok(()),
            };
            let _ = callback.send(event);
        }

        #[cfg(test)]
        fn submit_list(&self, prefix: &str, callback: StorageCallback) {
            let data = self.data.lock().expect("lock mock storage");
            let results: Vec<_> = data
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect();

            let event = StorageEvent::ListComplete {
                prefix: prefix.to_string(),
                result: StorageOutcome::Ok(results),
            };
            let _ = callback.send(event);
        }
    }
}
