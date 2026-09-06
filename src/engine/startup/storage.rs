use super::super::OpenOptions;
use super::super::IN_MEMORY_OPEN_COUNTER;
use super::{
    CloudStartupRecovery, RuntimeRecoveryMaterialization, RuntimeState,
    RuntimeStorageMaterialization, StartupLease, StartupStoragePath,
};
use crate::common::{MidgeError, MidgeResult};
use crate::config::{RecoveryPolicy, Storage};
use crate::runtime::hybrid_persistence::HybridPersistence;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

struct CloudClassStores {
    wal: Arc<crate::storage::cloud::CloudStorage>,
    sst: Arc<crate::storage::cloud::CloudStorage>,
    metadata: Arc<crate::storage::cloud::CloudStorage>,
}

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
        let created = crate::lease::create_lease_with_validity_and_timeout_and_ttl(
            storage,
            opts.lease_clock_skew_tolerance(),
            opts.storage_io_timeout(),
            opts.lease_ttl(),
        )
        .map_err(|error| match MidgeError::from(error) {
            MidgeError::LeaseHeld(message) => MidgeError::LeaseHeld(message),
            MidgeError::LeaseUnavailable(message) => MidgeError::LeaseUnavailable(format!(
                "failed to create lease for storage backend: {message}"
            )),
            other => other,
        })?;

        Self::acquire_created(
            created.lease,
            created.validity,
            Some(storage),
            opts.lease_loss_hook(),
        )
    }

    #[cfg(test)]
    pub(super) fn acquire_for_test(
        lease: Arc<dyn crate::lease::PrimaryLease>,
        validity: Option<Arc<crate::lease::LeaseValidity>>,
    ) -> MidgeResult<Self> {
        Self::acquire_created(lease, validity, None, None)
    }

    fn acquire_created(
        lease: Arc<dyn crate::lease::PrimaryLease>,
        lease_validity: Option<Arc<crate::lease::LeaseValidity>>,
        storage: Option<&Storage>,
        lease_loss_hook: Option<Arc<dyn Fn() + Send + Sync>>,
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
            crate::lease::LeaseError::Indeterminate(message) => {
                MidgeError::LeaseIndeterminate(message)
            }
            crate::lease::LeaseError::EpochExhausted => MidgeError::LeaseEpochExhausted,
            crate::lease::LeaseError::AlreadyAcquired(message) => MidgeError::Busy(message),
            crate::lease::LeaseError::Internal(message) => MidgeError::Internal(message),
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
        startup_lease.start_heartbeat(lease_loss_hook)?;
        startup_lease.ensure_healthy("immediately after lease acquisition")?;
        Ok(startup_lease)
    }

    fn runtime_lease_health(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.lease_healthy)
    }

    fn start_heartbeat(
        &mut self,
        lease_loss_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> MidgeResult<()> {
        let mut lease_heartbeat = crate::lease::LeaseHeartbeat::new_with_healthy_and_validity(
            Arc::clone(&self.lease),
            Arc::clone(&self.lease_healthy),
            self.lease_validity.as_ref().map(Arc::clone),
        );
        if let Some(hook) = lease_loss_hook {
            lease_heartbeat.set_loss_hook(hook);
        }
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
    fn build_cloud_class_stores(
        opts: &OpenOptions,
        topology: &crate::config::CloudStorageTopology,
    ) -> MidgeResult<CloudClassStores> {
        let wal = crate::storage::providers::build_cloud_storage_with_timeout(
            topology.wal().provider(),
            topology.wal().prefix(),
            opts.storage_io_timeout(),
        )?;
        let sst = if topology.sst() == topology.wal() {
            wal.clone()
        } else {
            crate::storage::providers::build_cloud_storage_with_timeout(
                topology.sst().provider(),
                topology.sst().prefix(),
                opts.storage_io_timeout(),
            )?
        };
        let metadata = if topology.control() == topology.wal() {
            wal.clone()
        } else if topology.control() == topology.sst() {
            sst.clone()
        } else {
            crate::storage::providers::build_cloud_storage_with_timeout(
                topology.control().provider(),
                topology.control().prefix(),
                opts.storage_io_timeout(),
            )?
        };
        Ok(CloudClassStores { wal, sst, metadata })
    }

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
            Storage::Cloud { topology, .. } => Self::materialize_cloud(
                opts,
                storage_path,
                startup_lease,
                cloud_runtime_policy,
                topology,
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
        cloud.hybrid_storage.enable_ephemeral_sst_cache(
            opts.simulated_cloud_local_storage_budget_bytes()
                .unwrap_or_else(|| opts.local_storage_budget_bytes()),
        );
        let sst_backend = Arc::new(crate::storage::filesystem::FileSystem::new(
            cloud.cloud_root.clone(),
        )?);
        let sst_read_fs = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            Arc::new(crate::io::RealFs::new(&storage_path.db_path)?),
            sst_backend,
            opts.storage_io_timeout(),
        ));
        CloudStartupRecovery::reject_simulated_cloud_wal_without_catalog(
            &cloud.recovery_cloud_wal_dir,
        )?;
        let wal_catalog = cloud
            .hybrid_storage
            .fence_cloud_wal_catalog(startup_lease.writer_epoch)?;
        startup_lease.ensure_healthy("after cloud WAL catalog fencing")?;

        let limits = super::streaming_recovery::CloudReplay::limits(opts);
        let wal_backend: Arc<dyn crate::storage::StorageBackend> = Arc::new(
            crate::storage::filesystem::FileSystem::new(cloud.cloud_root.clone())?,
        );
        let streaming = super::timing::measure("wal_plan", || {
            super::streaming_wal_plan::StreamingCloudWalRecovery::build(
                &storage_path.db_path,
                &wal_backend,
                &wal_catalog,
                opts.recovery_policy(),
                opts.storage_io_timeout(),
                super::streaming_recovery::CloudReplay::read_window(opts, limits),
                limits,
            )
        })?;
        let recovery_plan = streaming.plan;
        let mut state = RuntimeState::try_new_before_cloud_replay(
            storage_path.db_path.clone(),
            opts.recovery_policy(),
        )?;
        state.wal.current_segment_id = streaming.next_segment_id;
        if recovery_plan.opened_in_salvage_mode {
            state.mark_opened_in_salvage_mode();
            state.mark_persistence_anomaly();
        }

        let runtime_config = crate::runtime::RuntimeConfig {
            ttl_clock: opts.ttl_clock(),
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
            storage_io_timeout: opts.storage_io_timeout(),
            runtime_response_timeout: opts.runtime_response_timeout(),
            shutdown_cloud_drain_timeout: opts.shutdown_cloud_drain_timeout(),
            cloud_runtime_policy,
            hybrid_storage: Some(cloud.hybrid_storage),
            sst_read_fs: Some(sst_read_fs),
            hybrid_storage_events: Some(cloud.events),
            recovered_cloud_wal_segments: recovery_plan.remote_max_sequences(),
            recovered_cloud_wal_segment_epochs: recovery_plan.remote_writer_epochs(),
            recovered_local_wal_segments: recovery_plan.local_max_sequences(),
            recovered_local_wal_segment_epochs: recovery_plan.local_writer_epochs(),
            recovered_cloud_active_wal: recovery_plan.active_wal,
            compression_policy: opts.compression_policy().clone(),
            block_cache_size: opts.block_cache_size(),
            block_cache_policy: opts.block_cache_policy_type(),
            target_sst_size: opts.target_sst_size(),
            compaction_memory_limit: opts.compaction_memory_pool_size(),
            flush_memory_limit: opts.flush_memory_limit(),
            l0_compaction_trigger: opts.l0_compaction_trigger(),
            background_compaction: opts.background_compaction_enabled(),
            writer_epoch: startup_lease.writer_epoch,
            lease_healthy: Some(startup_lease.runtime_lease_health()),
            leader_store: startup_lease.leader_store.clone(),
            leader_holder_id: Some(startup_lease.lease.holder_id()),
            ..Default::default()
        };

        Ok(Self {
            state,
            runtime_config,
            cloud_root: Some(cloud.cloud_root.clone()),
            cloud_storage_for_restore: None,
            cloud_metadata_storage_for_mirror: None,
            streaming_wal: Some(super::streaming_recovery::CloudReplay {
                fs: streaming.fs,
                limits,
            }),
        })
    }

    fn materialize_cloud(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
        topology: &crate::config::CloudStorageTopology,
    ) -> MidgeResult<Self> {
        let stores = Self::build_cloud_class_stores(opts, topology)?;
        let wal_storage = stores.wal;
        let sst_storage = stores.sst;
        let metadata_storage = stores.metadata;

        CloudStartupRecovery::reject_cloud_wal_without_catalog(&wal_storage)?;
        let local_backend = Arc::new(crate::storage::filesystem::FileSystem::new(
            storage_path.db_path.join("hybrid_local"),
        )?);
        let wal_backend: Arc<dyn crate::storage::StorageBackend> = wal_storage.clone();
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
                opts.storage_io_timeout(),
            ),
        );
        hybrid_storage.enable_ephemeral_sst_cache(opts.local_storage_budget_bytes());
        let sst_read_fs = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            Arc::new(crate::io::RealFs::new(&storage_path.db_path)?),
            sst_storage.clone(),
            opts.storage_io_timeout(),
        ));
        let wal_catalog = hybrid_storage.fence_cloud_wal_catalog(startup_lease.writer_epoch)?;
        startup_lease.ensure_healthy("after cloud WAL catalog fencing")?;

        CloudStartupRecovery::hydrate_cloud_metadata(
            &metadata_storage,
            &storage_path.db_path,
            opts.recovery_policy(),
        )?;
        let limits = super::streaming_recovery::CloudReplay::limits(opts);
        let streaming = super::timing::measure("wal_plan", || {
            super::streaming_wal_plan::StreamingCloudWalRecovery::build(
                &storage_path.db_path,
                &(wal_storage.clone() as Arc<dyn crate::storage::StorageBackend>),
                &wal_catalog,
                opts.recovery_policy(),
                opts.storage_io_timeout(),
                super::streaming_recovery::CloudReplay::read_window(opts, limits),
                limits,
            )
        })?;
        let recovery_plan = streaming.plan;
        let mut state = RuntimeState::try_new_before_cloud_replay(
            storage_path.db_path.clone(),
            opts.recovery_policy(),
        )?;
        state.wal.current_segment_id = streaming.next_segment_id;
        if recovery_plan.opened_in_salvage_mode {
            state.mark_opened_in_salvage_mode();
            state.mark_persistence_anomaly();
        }

        let runtime_config = crate::runtime::RuntimeConfig {
            ttl_clock: opts.ttl_clock(),
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
            storage_io_timeout: opts.storage_io_timeout(),
            runtime_response_timeout: opts.runtime_response_timeout(),
            shutdown_cloud_drain_timeout: opts.shutdown_cloud_drain_timeout(),
            cloud_runtime_policy,
            hybrid_storage: Some(hybrid_storage),
            sst_read_fs: Some(sst_read_fs),
            hybrid_storage_events: Some(rx),
            cloud_metadata_storage: Some(metadata_storage.clone()),
            recovered_cloud_wal_segments: recovery_plan.remote_max_sequences(),
            recovered_cloud_wal_segment_epochs: recovery_plan.remote_writer_epochs(),
            recovered_local_wal_segments: recovery_plan.local_max_sequences(),
            recovered_local_wal_segment_epochs: recovery_plan.local_writer_epochs(),
            recovered_cloud_active_wal: recovery_plan.active_wal,
            compression_policy: opts.compression_policy().clone(),
            block_cache_size: opts.block_cache_size(),
            block_cache_policy: opts.block_cache_policy_type(),
            target_sst_size: opts.target_sst_size(),
            compaction_memory_limit: opts.compaction_memory_pool_size(),
            flush_memory_limit: opts.flush_memory_limit(),
            l0_compaction_trigger: opts.l0_compaction_trigger(),
            background_compaction: opts.background_compaction_enabled(),
            writer_epoch: startup_lease.writer_epoch,
            lease_healthy: Some(startup_lease.runtime_lease_health()),
            leader_store: startup_lease.leader_store.clone(),
            leader_holder_id: Some(startup_lease.lease.holder_id()),
            ..Default::default()
        };

        Ok(Self {
            state,
            runtime_config,
            cloud_root: None,
            cloud_storage_for_restore: Some(sst_storage),
            cloud_metadata_storage_for_mirror: Some(metadata_storage),
            streaming_wal: Some(super::streaming_recovery::CloudReplay {
                fs: streaming.fs,
                limits,
            }),
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
            ttl_clock: opts.ttl_clock(),
            wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
            wal_batch_config: batch_config,
            storage_io_timeout: opts.storage_io_timeout(),
            runtime_response_timeout: opts.runtime_response_timeout(),
            shutdown_cloud_drain_timeout: opts.shutdown_cloud_drain_timeout(),
            cloud_runtime_policy,
            compression_policy: opts.compression_policy().clone(),
            block_cache_size: opts.block_cache_size(),
            block_cache_policy: opts.block_cache_policy_type(),
            target_sst_size: opts.target_sst_size(),
            compaction_memory_limit: opts.compaction_memory_pool_size(),
            flush_memory_limit: opts.flush_memory_limit(),
            l0_compaction_trigger: opts.l0_compaction_trigger(),
            background_compaction: opts.background_compaction_enabled(),
            writer_epoch: startup_lease.writer_epoch,
            lease_healthy: Some(startup_lease.runtime_lease_health()),
            leader_store: startup_lease.leader_store.clone(),
            leader_holder_id: Some(startup_lease.lease.holder_id()),
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
            streaming_wal: None,
        })
    }
}

impl RuntimeRecoveryMaterialization {
    fn evict_resident_manifest_ssts(
        materialized: &mut RuntimeStorageMaterialization,
    ) -> MidgeResult<()> {
        let Some(fs) = &materialized.runtime_config.sst_read_fs else {
            return Ok(());
        };
        let Some(storage) = &materialized.runtime_config.hybrid_storage else {
            return Ok(());
        };
        let mut salvaged = Vec::new();
        for meta in &materialized.state.manifest.files {
            if materialized.state.salvaged_local_ssts.contains(&meta.name) {
                continue;
            }
            let path = materialized.state.sst_dir.join(&meta.name);
            let secondary = materialized
                .state
                .db_path
                .join("hybrid_local/sst")
                .join(&meta.name);
            if !path.exists() && !secondary.exists() {
                continue;
            }
            // This migration path reads only objects which already have a
            // resident copy. An empty cache never triggers full SST validation.
            // Retain the local bytes unless the remote publication proof holds.
            let validation = RuntimeState::validate_sst_fs_proof(
                Arc::clone(fs),
                &crate::runtime::FileMeta {
                    name: meta.name.clone(),
                    level: meta.level,
                    size_bytes: meta.size_bytes,
                    content_crc32c: meta.content_crc32c,
                    cf_id: meta.cf_id,
                    smallest_key: meta.smallest_key.clone(),
                    largest_key: meta.largest_key.clone(),
                    smallest_seq: meta.smallest_seq,
                    largest_seq: meta.largest_seq,
                    key_bounds_complete: meta.key_bounds_complete,
                },
            );
            if let Err(error) = validation {
                if materialized.state.recovery_policy() == RecoveryPolicy::Salvage
                    && CloudStartupRecovery::retain_verified_local_sst(&materialized.state, meta)?
                {
                    tracing::warn!(
                        %error,
                        sst_name = %meta.name,
                        "retaining verified local SST during salvage after remote migration proof failed"
                    );
                    salvaged.push(meta.name.clone());
                    continue;
                }
                return Err(error);
            }
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            storage.evict_local_object_cache(&crate::sst::object_key(&meta.name))?;
        }
        if !salvaged.is_empty() {
            materialized.state.salvaged_local_ssts.extend(salvaged);
            materialized.state.mark_opened_in_salvage_mode();
            materialized.state.mark_persistence_anomaly();
        }
        Ok(())
    }

    pub(super) fn local_directory_bytes(path: &Path) -> MidgeResult<u64> {
        let entries = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error.into()),
        };
        let mut total = 0_u64;
        for entry in entries {
            let entry = entry?;
            let kind = entry.file_type()?;
            let bytes = if kind.is_dir() {
                Self::local_directory_bytes(&entry.path())?
            } else if kind.is_file() {
                entry.metadata()?.len()
            } else {
                0
            };
            total = total.checked_add(bytes).ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit("local storage accounting overflow".into())
            })?;
        }
        Ok(total)
    }

    pub(super) fn replay_and_repair(
        mut materialized: RuntimeStorageMaterialization,
        db_path: &Path,
        recovery_policy: RecoveryPolicy,
    ) -> MidgeResult<Self> {
        materialized
            .state
            .recovery_sst_fs
            .clone_from(&materialized.runtime_config.sst_read_fs);
        let remote_cleanup_candidates = materialized
            .state
            .non_authoritative_compaction_outputs_for_remote_cleanup()?;
        let remote_cleanup_names =
            if let Some(storage) = materialized.runtime_config.hybrid_storage.as_deref() {
                CloudStartupRecovery::cleanup_non_authoritative_compaction_outputs(
                    &mut materialized.state,
                    storage,
                    &remote_cleanup_candidates,
                )?
            } else {
                std::collections::BTreeSet::new()
            };

        if let Some(cloud_storage) = materialized.cloud_storage_for_restore.as_deref() {
            let sst_proofs = CloudStartupRecovery::cloud_recovery_sst_proofs_for_intent_replay(
                &materialized.state,
            )
            .into_iter()
            .filter(|proof| !remote_cleanup_names.contains(&proof.name));
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

        if materialized.runtime_config.wal_durability_policy
            == crate::wal::DurabilityPolicy::CloudAsync
        {
            materialized
                .state
                .reset_cloud_durable_sequence_for_recovery();
        }

        materialized.state.cleanup_storage_residue();
        Self::evict_resident_manifest_ssts(&mut materialized)?;
        if !materialized.state.salvaged_local_ssts.is_empty() {
            if let Some(storage) = &materialized.runtime_config.hybrid_storage {
                let fs: Arc<dyn crate::io::Fs> = Arc::new(
                    crate::storage::remote_sst::RemoteSstFs::new(
                        Arc::clone(&materialized.state.fs),
                        storage.remote_sst_backend(),
                        storage.storage_io_timeout(),
                    )
                    .with_verified_local_overrides(materialized.state.salvaged_local_ssts.clone()),
                );
                materialized.runtime_config.sst_read_fs = Some(Arc::clone(&fs));
                materialized.state.recovery_sst_fs = Some(fs);
            }
        }
        if let Some(storage) = &materialized.runtime_config.hybrid_storage {
            storage.reconcile_local_disk_usage(
                Self::local_directory_bytes(&materialized.state.sst_dir)?.saturating_add(
                    Self::local_directory_bytes(&db_path.join("hybrid_local/sst"))?,
                ),
                Self::local_directory_bytes(&materialized.state.wal_dir)?.saturating_add(
                    Self::local_directory_bytes(&db_path.join("hybrid_local/wal"))?,
                ),
            );
            storage.reconcile_startup_scratch_residue(
                materialized.state.retained_startup_scratch_bytes()?,
            )?;
        }
        if let Some(replay) = materialized.streaming_wal.take() {
            super::timing::measure("wal_replay", || replay.replay(&mut materialized))?;
        }
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
