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

    pub(super) fn maintenance_enabled(&self, is_cloud_async: bool, is_memory_mode: bool) -> bool {
        is_cloud_async
            && !is_memory_mode
            && self
                .hybrid_storage
                .as_ref()
                .is_some_and(|storage| storage.ephemeral_sst_cache_enabled())
    }

    pub(super) fn mirror_metadata_within(
        &self,
        fs: &dyn crate::io::Fs,
        publication_lock: &crate::runtime::MetadataPublicationLock,
        last_persisted_sequence: u64,
        deadline: &crate::common::OperationDeadline,
        validate_lease: impl FnMut(&crate::common::OperationDeadline) -> crate::common::MidgeResult<()>,
    ) -> crate::common::MidgeResult<()> {
        let Some(cloud) = self.cloud_metadata_storage.as_ref() else {
            return Ok(());
        };
        crate::runtime::hybrid_persistence::mirror_control_metadata_within(
            cloud,
            fs,
            publication_lock,
            std::time::Duration::ZERO,
            last_persisted_sequence,
            deadline,
            validate_lease,
        )
    }
}
