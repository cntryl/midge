//! Cloud state owned by the event loop's cloud coordination path.

use super::cloud_maintenance::CloudMaintenance;
use super::coordination::CloudWalUploadTracker;
use std::sync::Arc;

pub(in crate::runtime) struct CloudCoordinator {
    pub(super) hybrid_storage: Option<Arc<crate::storage::HybridStorage>>,
    pub(super) hybrid_storage_events:
        Option<crossbeam::channel::Receiver<crate::storage::StorageEvent>>,
    pub(super) cloud_metadata_storage: Option<Arc<crate::storage::cloud::CloudStorage>>,
    pub(super) provider_ddl_fencing: bool,
    pub(super) cloud_wal: CloudWalUploadTracker,
    pub(super) cloud_publication:
        super::cloud_integration::publication_worker::CloudPublicationPipeline,
    pub(super) cloud_wal_prune_worker: Option<std::thread::JoinHandle<()>>,
    pub(super) cloud_maintenance: CloudMaintenance,
}

impl CloudCoordinator {
    pub(super) fn held_uploads(&self) -> crate::storage::hybrid::backend::HeldUploads {
        let mut held = self.cloud_wal.held_uploads();
        let verification = self.cloud_publication.held_uploads();
        held.entries = held.entries.saturating_add(verification.entries);
        held.bytes = held.bytes.saturating_add(verification.bytes);
        held
    }

    pub(super) fn new(config: &crate::runtime::RuntimeConfig) -> Self {
        Self {
            hybrid_storage: None,
            hybrid_storage_events: config.hybrid_storage_events.clone(),
            cloud_metadata_storage: config.cloud_metadata_storage.clone(),
            provider_ddl_fencing: config.provider_ddl_fencing,
            cloud_wal: CloudWalUploadTracker::with_retry_policy(
                config.recovered_cloud_wal_entries.clone(),
                config.retry_policy,
            ),
            cloud_wal_prune_worker: None,
            cloud_publication: Default::default(),
            cloud_maintenance: CloudMaintenance::default(),
        }
    }

    #[cfg(test)]
    pub(super) fn cloud_wal_retry_delay(&self) -> std::time::Duration {
        self.cloud_wal.backlog().retry_delay()
    }

    pub(super) fn maintenance_enabled(&self, is_cloud_async: bool, is_memory_mode: bool) -> bool {
        is_cloud_async
            && !is_memory_mode
            && self
                .hybrid_storage
                .as_ref()
                .is_some_and(|storage| storage.ephemeral_sst_cache_enabled())
    }

    pub(super) fn attach_hybrid_storage(&mut self, storage: Arc<crate::storage::HybridStorage>) {
        self.hybrid_storage = Some(storage);
    }

    pub(super) fn disconnect_storage_events(&mut self) {
        self.hybrid_storage_events = None;
    }

    pub(super) fn set_prune_worker(&mut self, worker: std::thread::JoinHandle<()>) {
        self.cloud_wal_prune_worker = Some(worker);
    }

    pub(super) fn take_prune_worker(&mut self) -> Option<std::thread::JoinHandle<()>> {
        self.cloud_wal_prune_worker.take()
    }

    pub(super) fn mirror_metadata_within(
        &self,
        fs: &dyn crate::io::Fs,
        publication_lock: &crate::runtime::MetadataPublicationLock,
        deadline: &crate::common::OperationDeadline,
        authority: Option<crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority<'_>>,
        validate_lease: impl FnMut(&crate::common::OperationDeadline) -> crate::common::MidgeResult<()>,
    ) -> crate::common::MidgeResult<()> {
        let Some(cloud) = self.cloud_metadata_storage.as_ref() else {
            return Ok(());
        };
        let authority = authority.ok_or_else(|| {
            crate::common::MidgeError::Fenced("cloud metadata mirror has no leader store".into())
        })?;
        crate::runtime::hybrid_persistence::mirror_control_metadata_within(
            crate::runtime::hybrid_persistence::CloudMetadataMirrorContext {
                cloud,
                fs,
                publication_lock,
                lock_wait_budget: std::time::Duration::ZERO,
                deadline,
                authority,
            },
            validate_lease,
        )
    }
}
