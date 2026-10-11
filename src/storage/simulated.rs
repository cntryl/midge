//! The object store a simulated-cloud engine runs on (`Storage::CloudSimulated`,
//! a supported open mode, #517): a host directory under the database root
//! that stands in for cloud storage, or a store a test injects.

use std::path::PathBuf;
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;

use super::cloud::{CloudBackend, CloudStorage};
use super::filesystem::FileSystem;
use super::inventory::ObjectStore;
use super::{HybridStorage, StorageEvent};

/// Directory under the root's host directory that holds a simulated cloud.
const SIMULATED_CLOUD_DIR: &str = "cloud_store";

/// The stores a simulated-cloud engine runs on.
pub(crate) struct SimulatedCloudStores {
    pub hybrid_storage: Arc<HybridStorage>,
    pub events: crossbeam::channel::Receiver<StorageEvent>,
}

/// Where a simulated cloud keeps its objects.
pub(crate) enum SimulatedCloud {
    /// The `cloud_store/` directory under the root's host directory.
    Host(PathBuf),
    /// A store a test injected in place of the host directory.
    Injected(Arc<CloudStorage>),
}

impl SimulatedCloud {
    /// Locate the simulated cloud for `root`, preferring an injected store.
    ///
    /// # Errors
    ///
    /// Fails when nothing is injected and the root has no host directory to
    /// hold the simulated cloud.
    pub(crate) fn locate(
        root: &dyn Fs,
        injected: Option<Arc<dyn CloudBackend>>,
        timeout: std::time::Duration,
    ) -> MidgeResult<Self> {
        if let Some(backend) = injected {
            return Ok(Self::Injected(Arc::new(CloudStorage::new_with_timeout(
                backend,
                String::new(),
                timeout,
            ))));
        }
        let addressing = root.host_addressing().ok_or_else(|| {
            MidgeError::InvalidArgument(
                "a simulated cloud lives under the root's host directory; a root without one \
                 needs an injected object store"
                    .to_string(),
            )
        })?;
        Ok(Self::Host(addressing.root.join(SIMULATED_CLOUD_DIR)))
    }

    /// The object store, created if it does not exist yet. A host store
    /// also gets its WAL directory: the host store reports a missing object
    /// as definitively absent only inside an existing directory, so the
    /// startup catalog probe needs one.
    ///
    /// # Errors
    ///
    /// Fails when the host directories cannot be created.
    pub(crate) fn open(&self) -> MidgeResult<Arc<dyn ObjectStore>> {
        Ok(match self {
            Self::Host(path) => {
                std::fs::create_dir_all(path.join(crate::wal::WAL_DIR))?;
                Arc::new(FileSystem::new(path)?)
            }
            Self::Injected(store) => Arc::clone(store) as Arc<dyn ObjectStore>,
        })
    }

    /// The object store if it already exists; discovery before the lease is
    /// held must not create it.
    ///
    /// # Errors
    ///
    /// Fails when the host directory exists but cannot be inspected.
    pub(crate) fn open_existing(&self) -> MidgeResult<Option<Arc<dyn ObjectStore>>> {
        Ok(match self {
            Self::Host(path) => FileSystem::open_existing(path)?
                .map(|store| Arc::new(store) as Arc<dyn ObjectStore>),
            Self::Injected(store) => Some(Arc::clone(store) as Arc<dyn ObjectStore>),
        })
    }
}

/// Build the `HybridStorage` a simulated-cloud engine publishes through,
/// with its event stream.
pub(crate) fn build_simulated_cloud_stores(
    store: Arc<dyn ObjectStore>,
    local_storage_budget_bytes: Option<u64>,
) -> SimulatedCloudStores {
    let backend: Arc<dyn super::StorageBackend> = store;
    let (tx, rx) = crossbeam::channel::bounded::<StorageEvent>(
        crate::storage::hybrid::backend::HYBRID_STORAGE_EVENT_CHANNEL_CAPACITY,
    );
    let hybrid_storage = if let Some(budget_bytes) = local_storage_budget_bytes {
        Arc::new(HybridStorage::with_policy_and_event_sender(
            backend,
            crate::storage::hybrid::policy::StorageBudgetPolicy::new(budget_bytes),
            Some(tx),
        ))
    } else {
        Arc::new(HybridStorage::new_with_event_sender(backend, tx))
    };
    SimulatedCloudStores {
        hybrid_storage,
        events: rx,
    }
}

/// Root of the host directory that simulates cloud storage under `db_path`.
#[cfg(test)]
pub(crate) fn simulated_cloud_root(db_path: &std::path::Path) -> PathBuf {
    db_path.join(SIMULATED_CLOUD_DIR)
}

/// The simulated-cloud stores over the host directory under `db_path`.
#[cfg(test)]
pub(crate) fn host_simulated_cloud(
    db_path: &std::path::Path,
    local_storage_budget_bytes: Option<u64>,
) -> MidgeResult<SimulatedCloudStores> {
    let store = Arc::new(FileSystem::new(simulated_cloud_root(db_path))?);
    Ok(build_simulated_cloud_stores(
        store,
        local_storage_budget_bytes,
    ))
}
