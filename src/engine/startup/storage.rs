use super::super::OpenOptions;
use super::super::IN_MEMORY_OPEN_COUNTER;
use super::{
    CloudStartupRecovery, RuntimeRecoveryMaterialization, RuntimeState,
    RuntimeStorageMaterialization, StartupLease, StartupStoragePath,
};
use crate::common::{MidgeError, MidgeResult};
use crate::config::{RecoveryPolicy, Storage};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl StartupStoragePath {
    pub(super) fn resolve(storage: &Storage) -> Self {
        match storage {
            Storage::InMemory => Self {
                db_path: {
                    let counter = IN_MEMORY_OPEN_COUNTER.fetch_add(1, Ordering::SeqCst);
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |duration| duration.as_nanos());
                    PathBuf::from(format!(
                        "target/tmp/midge_test_memory_{}_{}_{}",
                        std::process::id(),
                        counter,
                        timestamp
                    ))
                },
                memory_mode: true,
            },
            Storage::Local { path } => Self {
                db_path: path.clone(),
                memory_mode: false,
            },
            Storage::Cloud {
                local_cache_path, ..
            }
            | Storage::CloudSimulated {
                local_cache_path, ..
            } => Self {
                db_path: local_cache_path.clone(),
                memory_mode: false,
            },
        }
    }

    pub(super) fn prepare(&self) {
        if !self.memory_mode {
            let _ = std::fs::create_dir_all(&self.db_path);
        }
    }
}

impl StartupLease {
    pub(super) fn acquire(opts: &OpenOptions) -> MidgeResult<Self> {
        let storage = opts.storage();
        let created = crate::lease::create_lease_with_validity(storage).map_err(|error| {
            match MidgeError::from(error) {
                MidgeError::LeaseHeld(message) => MidgeError::LeaseHeld(message),
                MidgeError::LeaseUnavailable(message) => MidgeError::LeaseUnavailable(format!(
                    "failed to create lease for storage backend: {message}"
                )),
                other => other,
            }
        })?;

        Self::acquire_created(created.lease, created.validity, Some(storage))
    }

    #[cfg(test)]
    pub(super) fn acquire_for_test(
        lease: Arc<dyn crate::lease::PrimaryLease>,
        validity: Option<Arc<crate::lease::LeaseValidity>>,
    ) -> MidgeResult<Self> {
        Self::acquire_created(lease, validity, None)
    }

    fn acquire_created(
        lease: Arc<dyn crate::lease::PrimaryLease>,
        lease_validity: Option<Arc<crate::lease::LeaseValidity>>,
        storage: Option<&Storage>,
    ) -> MidgeResult<Self> {
        let lease_guard = lease.clone().try_acquire().map_err(|error| match error {
            crate::lease::LeaseError::AcquisitionFailed(message) => MidgeError::LeaseHeld(format!(
                "another Midge instance is already running against this storage: {message}"
            )),
            crate::lease::LeaseError::IoError(message) => MidgeError::LeaseUnavailable(message),
            crate::lease::LeaseError::RenewalFailed(message) => MidgeError::Fenced(message),
            crate::lease::LeaseError::AlreadyReleased => {
                MidgeError::Fenced("lease was released during acquisition".to_string())
            }
        })?;

        tracing::warn!(
            holder_id = %lease.holder_id(),
            storage = ?storage,
            epoch = lease.epoch(),
            "primary lease acquired - this instance is now the exclusive writer"
        );

        let writer_epoch = lease.epoch();
        let leader_store = lease.get_leader_store();
        let lease_healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let mut startup_lease = Self {
            lease,
            lease_guard: Some(lease_guard),
            writer_epoch,
            leader_store,
            lease_healthy,
            lease_validity,
            lease_heartbeat: None,
        };
        startup_lease.start_heartbeat()?;
        startup_lease.ensure_healthy("immediately after lease acquisition")?;
        Ok(startup_lease)
    }

    fn runtime_lease_health(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.lease_healthy)
    }

    fn start_heartbeat(&mut self) -> MidgeResult<()> {
        let mut lease_heartbeat = crate::lease::LeaseHeartbeat::new_with_healthy_and_validity(
            Arc::clone(&self.lease),
            Arc::clone(&self.lease_healthy),
            self.lease_validity.as_ref().map(Arc::clone),
        );
        lease_heartbeat.start();
        if !lease_heartbeat.is_healthy() {
            return Err(MidgeError::Fenced(
                "lease heartbeat failed immediately after start".to_string(),
            ));
        }

        self.lease_heartbeat = Some(lease_heartbeat);
        Ok(())
    }

    pub(super) fn ensure_healthy(&self, phase: &str) -> MidgeResult<()> {
        if self
            .lease_healthy
            .load(std::sync::atomic::Ordering::Acquire)
        {
            Ok(())
        } else {
            Err(MidgeError::Fenced(format!(
                "primary lease became invalid {phase}"
            )))
        }
    }

    pub(super) fn take_heartbeat(&mut self) -> MidgeResult<crate::lease::LeaseHeartbeat> {
        self.lease_heartbeat.take().ok_or_else(|| {
            MidgeError::Internal("startup lease heartbeat was already transferred".to_string())
        })
    }
}

impl Drop for StartupLease {
    fn drop(&mut self) {
        if let Some(mut heartbeat) = self.lease_heartbeat.take() {
            heartbeat.stop();
        }
        if self.lease_guard.is_some() {
            let _ = self.lease.release();
        }
    }
}

impl RuntimeStorageMaterialization {
    pub(super) fn materialize(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
    ) -> MidgeResult<Self> {
        let cloud_runtime_policy = opts.cloud_runtime_policy();

        match opts.storage() {
            Storage::CloudSimulated { .. } => Self::materialize_simulated_cloud(
                opts,
                storage_path,
                startup_lease,
                cloud_runtime_policy,
            ),
            Storage::Cloud { buckets, .. } => Self::materialize_cloud(
                opts,
                storage_path,
                startup_lease,
                cloud_runtime_policy,
                buckets,
            ),
            _ => Self::materialize_local(opts, storage_path, startup_lease, cloud_runtime_policy),
        }
    }

    fn materialize_simulated_cloud(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
    ) -> MidgeResult<Self> {
        let cloud = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
            &storage_path.db_path,
            opts.simulated_cloud_local_storage_budget_bytes(),
        )?;

        let state = RuntimeState::try_new_with_recovery_dir(
            storage_path.db_path.clone(),
            storage_path.memory_mode,
            Some(&cloud.recovery_cloud_wal_dir),
            opts.recovery_policy(),
        )?;
        let recovered_cloud_wal_segments =
            CloudStartupRecovery::recovered_cloud_wal_cleanup_candidates(
                &cloud.recovery_cloud_wal_dir,
            );

        let runtime_config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
            storage_io_timeout: opts.storage_io_timeout(),
            cloud_runtime_policy,
            hybrid_storage: Some(cloud.hybrid_storage),
            hybrid_storage_events: Some(cloud.events),
            recovered_cloud_wal_segments,
            compression_policy: opts.compression_policy().clone(),
            block_cache_size: opts.block_cache_size(),
            block_cache_policy: opts.block_cache_policy_type(),
            l0_compaction_trigger: opts.l0_compaction_trigger(),
            background_compaction: opts.background_compaction_enabled(),
            writer_epoch: startup_lease.writer_epoch,
            lease_healthy: Some(startup_lease.runtime_lease_health()),
            leader_store: startup_lease.leader_store.clone(),
            ..Default::default()
        };

        Ok(Self {
            state,
            runtime_config,
            cloud_root: Some(cloud.cloud_root.clone()),
            cloud_storage_for_restore: None,
            cloud_metadata_storage_for_mirror: None,
        })
    }

    fn materialize_cloud(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
        buckets: &crate::config::CloudStorageBuckets,
    ) -> MidgeResult<Self> {
        let wal_storage = crate::storage::providers::build_cloud_storage(
            buckets.wal().provider(),
            buckets.wal().prefix(),
        )?;
        let sst_storage = crate::storage::providers::build_cloud_storage(
            buckets.sst().provider(),
            buckets.sst().prefix(),
        )?;
        let metadata_storage = crate::storage::providers::build_cloud_storage(
            buckets.control().provider(),
            buckets.control().prefix(),
        )?;
        CloudStartupRecovery::hydrate_cloud_metadata(
            &metadata_storage,
            &storage_path.db_path,
            opts.recovery_policy(),
        )?;
        let recovery_wal_dir = CloudStartupRecovery::materialize_cloud_wal_recovery_dir(
            &wal_storage,
            &storage_path.db_path,
            opts.recovery_policy(),
        )?;
        let recovered_cloud_wal_segments =
            CloudStartupRecovery::recovered_cloud_wal_cleanup_candidates(&recovery_wal_dir);

        let local_backend = Arc::new(crate::storage::filesystem::FileSystem::new(
            storage_path.db_path.join("hybrid_local"),
        )?);
        let wal_backend: Arc<dyn crate::storage::StorageBackend> = wal_storage;
        let sst_backend: Arc<dyn crate::storage::StorageBackend> = sst_storage.clone();
        let control_backend: Arc<dyn crate::storage::StorageBackend> = metadata_storage.clone();

        let (tx, rx) = crossbeam::channel::bounded::<crate::storage::StorageEvent>(
            crate::storage::hybrid::backend::HYBRID_STORAGE_EVENT_CHANNEL_CAPACITY,
        );
        let hybrid_storage = Arc::new(
            crate::storage::HybridStorage::new_with_class_stores_and_event_sender(
                local_backend,
                wal_backend,
                sst_backend,
                control_backend,
                tx,
            ),
        );

        let state = RuntimeState::try_new_with_recovery_dir(
            storage_path.db_path.clone(),
            storage_path.memory_mode,
            Some(&recovery_wal_dir),
            opts.recovery_policy(),
        )?;

        let runtime_config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
            storage_io_timeout: opts.storage_io_timeout(),
            cloud_runtime_policy,
            hybrid_storage: Some(hybrid_storage),
            hybrid_storage_events: Some(rx),
            cloud_metadata_storage: Some(metadata_storage.clone()),
            recovered_cloud_wal_segments,
            compression_policy: opts.compression_policy().clone(),
            block_cache_size: opts.block_cache_size(),
            block_cache_policy: opts.block_cache_policy_type(),
            l0_compaction_trigger: opts.l0_compaction_trigger(),
            background_compaction: opts.background_compaction_enabled(),
            writer_epoch: startup_lease.writer_epoch,
            lease_healthy: Some(startup_lease.runtime_lease_health()),
            leader_store: startup_lease.leader_store.clone(),
            ..Default::default()
        };

        Ok(Self {
            state,
            runtime_config,
            cloud_root: None,
            cloud_storage_for_restore: Some(sst_storage),
            cloud_metadata_storage_for_mirror: Some(metadata_storage),
        })
    }

    fn materialize_local(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
    ) -> MidgeResult<Self> {
        let batch_config = opts.wal_batch_config().unwrap_or_default();

        let runtime_config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
            wal_batch_config: batch_config,
            storage_io_timeout: opts.storage_io_timeout(),
            cloud_runtime_policy,
            compression_policy: opts.compression_policy().clone(),
            block_cache_size: opts.block_cache_size(),
            block_cache_policy: opts.block_cache_policy_type(),
            l0_compaction_trigger: opts.l0_compaction_trigger(),
            background_compaction: opts.background_compaction_enabled(),
            writer_epoch: startup_lease.writer_epoch,
            lease_healthy: Some(startup_lease.runtime_lease_health()),
            leader_store: startup_lease.leader_store.clone(),
            ..Default::default()
        };

        Ok(Self {
            state: RuntimeState::try_new(
                storage_path.db_path.clone(),
                storage_path.memory_mode,
                opts.recovery_policy(),
            )?,
            runtime_config,
            cloud_root: None,
            cloud_storage_for_restore: None,
            cloud_metadata_storage_for_mirror: None,
        })
    }
}

impl RuntimeRecoveryMaterialization {
    pub(super) fn replay_and_repair(
        mut materialized: RuntimeStorageMaterialization,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<Self> {
        if let Some(cloud_storage) = materialized.cloud_storage_for_restore.as_deref() {
            let sst_proofs = CloudStartupRecovery::cloud_recovery_sst_proofs_for_intent_replay(
                &materialized.state,
            );
            CloudStartupRecovery::ensure_named_sst_cache_from_cloud_storage(
                &mut materialized.state,
                cloud_storage,
                sst_proofs,
            )?;
        }

        crate::runtime::ddl::reconcile_startup(
            &mut materialized.state,
            materialized.runtime_config.hybrid_storage.as_ref(),
        )?;

        materialized.state.replay_intent_log()?;
        if let Some(root) = materialized.cloud_root.as_deref() {
            CloudStartupRecovery::ensure_local_sst_cache_from_cloud(&mut materialized.state, root)?;
        }
        if let Some(cloud_storage) = materialized.cloud_storage_for_restore.as_deref() {
            CloudStartupRecovery::ensure_local_sst_cache_from_cloud_storage(
                &mut materialized.state,
                cloud_storage,
            )?;
        }
        if let Some(metadata_storage) = materialized.cloud_metadata_storage_for_mirror.as_deref() {
            CloudStartupRecovery::mirror_cloud_metadata(
                metadata_storage,
                db_path,
                recovery_policy,
            )?;
        }

        materialized.state.cleanup_storage_residue();
        let recovered_sequence = materialized.state.sequence;
        let recovered_cf_metas = materialized.state.manifest.column_families.clone();

        Ok(Self {
            state: materialized.state,
            runtime_config: materialized.runtime_config,
            recovered_sequence,
            recovered_cf_metas,
        })
    }
}
