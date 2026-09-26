//! Cloud state owned by the event loop's cloud coordination path.

use super::cloud_maintenance::CloudMaintenance;
use super::coordination::CloudWalUploadTracker;
use crate::runtime::hybrid_persistence::CloudWalPruneProgress;
use std::sync::Arc;

pub(in crate::runtime) struct CloudCoordinator {
    pub(super) hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub(super) hybrid_storage_events:
        Option<crossbeam::channel::Receiver<crate::storage::StorageEvent>>,
    pub(super) cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    pub(super) cloud_wal: CloudWalUploadTracker,
    pub(super) cloud_wal_prune_worker: Option<std::thread::JoinHandle<()>>,
    pub(super) cloud_wal_prune_progress: CloudWalPruneProgress,
    pub(super) cloud_maintenance: CloudMaintenance,
}

impl CloudCoordinator {
    pub(super) fn new(config: &crate::runtime::RuntimeConfig) -> Self {
        Self {
            hybrid_storage: None,
            hybrid_storage_events: config.hybrid_storage_events.clone(),
            cloud_metadata_storage: config.cloud_metadata_storage.clone(),
            cloud_wal: CloudWalUploadTracker::new(config.recovered_cloud_wal_segments.clone()),
            cloud_wal_prune_worker: None,
            cloud_wal_prune_progress: CloudWalPruneProgress::default(),
            cloud_maintenance: CloudMaintenance::default(),
        }
    }
}
