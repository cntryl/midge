use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::{ingest, ColumnFamilyHandle, Engine, OpenOptions, IN_MEMORY_OPEN_COUNTER};
use crate::common::{MidgeError, MidgeResult};
use crate::config::Storage;
use crate::runtime::{Runtime, RuntimeState};

pub(super) struct EngineStartup;

impl EngineStartup {
    pub(super) fn open(opts: OpenOptions) -> MidgeResult<Engine> {
        let start = std::time::Instant::now();
        let (db_path, memory_mode) = Self::resolve_storage_path(&opts.storage);

        if !memory_mode {
            let _ = std::fs::create_dir_all(&db_path);
        }

        let lease = crate::lease::create_lease(&opts.storage).map_err(|error| {
            MidgeError::Internal(format!(
                "failed to create lease for storage backend: {}",
                error
            ))
        })?;

        let lease_guard = lease.clone().try_acquire().map_err(|error| match error {
            crate::lease::LeaseError::AcquisitionFailed(message) => MidgeError::Internal(format!(
                "FATAL: another Midge instance is already running against this storage. \
                 Only one writable instance is allowed at a time. Error: {}",
                message
            )),
            crate::lease::LeaseError::IoError(message) => {
                MidgeError::Internal(format!("lease acquisition I/O error: {}", message))
            }
            _ => MidgeError::Internal(format!("lease acquisition failed: {}", error)),
        })?;

        tracing::warn!(
            holder_id = %lease.holder_id(),
            storage = ?opts.storage,
            epoch = lease.epoch(),
            "primary lease acquired - this instance is now the exclusive writer"
        );

        let writer_epoch = lease.epoch();
        let leader_store = lease.get_leader_store();
        let lease_healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let mut cloud_root = None;
        let mut cloud_storage_for_restore: Option<Arc<crate::storage::cloud::CloudStorage>> = None;
        let cloud_runtime_policy = opts.cloud_runtime_policy.clone().unwrap_or_default();
        let (mut state, runtime_config) = match &opts.storage {
            Storage::CloudSimulated { .. } => {
                let cloud = crate::storage::test_support::build_cloud_backed_filesystem_simulation(
                    &db_path,
                )?;
                cloud_root = Some(cloud.cloud_root.clone());

                let state = RuntimeState::try_new_with_recovery_dir(
                    db_path.clone(),
                    memory_mode,
                    Some(cloud.recovery_cloud_wal_dir.clone()),
                    opts.recovery_policy,
                )?;

                let config = crate::runtime::RuntimeConfig {
                    wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
                    cloud_runtime_policy: cloud_runtime_policy.clone(),
                    hybrid_storage: Some(cloud.hybrid_storage),
                    hybrid_storage_events: Some(cloud.events),
                    compression_policy: opts.compression_policy.clone(),
                    writer_epoch,
                    lease_healthy: Some(Arc::clone(&lease_healthy)),
                    leader_store: leader_store.clone(),
                    ..Default::default()
                };

                (state, config)
            }
            Storage::Cloud {
                provider, prefix, ..
            } => {
                let cloud_storage =
                    crate::storage::providers::build_cloud_storage(provider, prefix)?;
                Engine::hydrate_cloud_metadata(&cloud_storage, &db_path, opts.recovery_policy)?;
                let recovery_wal_dir = Engine::materialize_cloud_wal_recovery_dir(
                    &cloud_storage,
                    &db_path,
                    opts.recovery_policy,
                )?;

                let local_backend = Arc::new(crate::storage::filesystem::FileSystem::new(
                    db_path.join("hybrid_local"),
                )?);
                let cloud_backend: Arc<dyn crate::storage::StorageBackend> = cloud_storage.clone();

                let (tx, rx) = crossbeam::channel::unbounded::<crate::storage::StorageEvent>();
                let hybrid_storage =
                    Arc::new(crate::storage::HybridStorage::new_with_event_sender(
                        local_backend,
                        cloud_backend,
                        tx,
                    ));

                let state = RuntimeState::try_new_with_recovery_dir(
                    db_path.clone(),
                    memory_mode,
                    Some(recovery_wal_dir),
                    opts.recovery_policy,
                )?;

                let config = crate::runtime::RuntimeConfig {
                    wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
                    cloud_runtime_policy: cloud_runtime_policy.clone(),
                    hybrid_storage: Some(hybrid_storage),
                    hybrid_storage_events: Some(rx),
                    cloud_metadata_storage: Some(cloud_storage.clone()),
                    compression_policy: opts.compression_policy.clone(),
                    writer_epoch,
                    lease_healthy: Some(Arc::clone(&lease_healthy)),
                    leader_store: leader_store.clone(),
                    ..Default::default()
                };

                cloud_storage_for_restore = Some(cloud_storage);
                (state, config)
            }
            _ => {
                let batch_config = opts.wal_batch_config.unwrap_or_default();

                let config = crate::runtime::RuntimeConfig {
                    wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
                    wal_batch_config: batch_config,
                    cloud_runtime_policy: cloud_runtime_policy.clone(),
                    compression_policy: opts.compression_policy.clone(),
                    writer_epoch,
                    lease_healthy: Some(Arc::clone(&lease_healthy)),
                    leader_store: leader_store.clone(),
                    ..Default::default()
                };

                (
                    RuntimeState::try_new(db_path.clone(), memory_mode, opts.recovery_policy)?,
                    config,
                )
            }
        };

        if let Some(cloud_storage) = cloud_storage_for_restore.as_deref() {
            let sst_proofs = Engine::cloud_recovery_sst_proofs_for_intent_replay(&state);
            Engine::ensure_named_sst_cache_from_cloud_storage(
                &mut state,
                cloud_storage,
                sst_proofs,
            )?;
        }

        state.replay_intent_log()?;
        if let Some(root) = cloud_root.as_deref() {
            Engine::ensure_local_sst_cache_from_cloud(&mut state, root)?;
        }
        if let Some(cloud_storage) = cloud_storage_for_restore.as_deref() {
            Engine::ensure_local_sst_cache_from_cloud_storage(&mut state, cloud_storage)?;
            Engine::mirror_cloud_metadata(cloud_storage, &db_path, opts.recovery_policy)?;
        }
        state.cleanup_storage_residue();
        let recovered_sequence = state.sequence;
        let recovered_cf_metas = state.manifest.column_families.clone();

        let (runtime_inst, _) = Runtime::new()?;
        let (runtime, runtime_handle) = runtime_inst.start_with_config(state, runtime_config)?;

        Self::apply_post_start_config(&opts, &runtime_handle)?;

        let column_families = dashmap::DashMap::new();
        let default_handle = ColumnFamilyHandle::new(0, "default".to_string());
        column_families.insert(default_handle.id(), default_handle);

        let ingest_coordinators = dashmap::DashMap::new();
        let default_coordinator =
            Arc::new(ingest::IngestCoordinator::new(0, runtime_handle.clone())?);
        ingest_coordinators.insert(0, default_coordinator);

        let mut lease_heartbeat =
            crate::lease::LeaseHeartbeat::new_with_healthy(Arc::clone(&lease), lease_healthy);
        lease_heartbeat.start();
        if !lease_heartbeat.is_healthy() {
            return Err(MidgeError::Internal(
                "lease heartbeat failed immediately after start".to_string(),
            ));
        }

        tracing::info!(db_path = %db_path.display(), open_ms = start.elapsed().as_secs_f64() * 1000.0, "engine open completed");

        for cf_meta in &recovered_cf_metas {
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                let handle = ColumnFamilyHandle::new(cf_meta.id, cf_meta.name.clone());
                column_families.insert(cf_meta.id, handle);

                let coordinator = Arc::new(ingest::IngestCoordinator::new(
                    cf_meta.id,
                    runtime_handle.clone(),
                )?);
                ingest_coordinators.insert(cf_meta.id, coordinator);
            }
        }

        Ok(Engine {
            _runtime: Some(runtime),
            runtime_handle,
            db_path,
            memory_mode,
            cloud_mode: matches!(
                &opts.storage,
                Storage::Cloud { .. } | Storage::CloudSimulated { .. }
            ),
            recovery_policy: opts.recovery_policy,
            sequence: Arc::new(std::sync::atomic::AtomicU64::new(recovered_sequence)),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
            column_families,
            _lease: Some(lease),
            _lease_guard: Some(lease_guard),
            _lease_heartbeat: Some(std::sync::Mutex::new(lease_heartbeat)),
            ingest_coordinators,
        })
    }

    fn resolve_storage_path(storage: &Storage) -> (PathBuf, bool) {
        match storage {
            Storage::InMemory => (
                {
                    let counter = IN_MEMORY_OPEN_COUNTER.fetch_add(1, Ordering::SeqCst);
                    let timestamp = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|duration| duration.as_nanos())
                        .unwrap_or(0);
                    PathBuf::from(format!(
                        "target/tmp/midge_test_memory_{}_{}_{}",
                        std::process::id(),
                        counter,
                        timestamp
                    ))
                },
                true,
            ),
            Storage::Local { path } => (path.clone(), false),
            Storage::Cloud {
                local_cache_path, ..
            }
            | Storage::CloudSimulated {
                local_cache_path, ..
            } => (local_cache_path.clone(), false),
        }
    }

    fn apply_post_start_config(
        opts: &OpenOptions,
        runtime_handle: &crate::runtime::RuntimeHandle,
    ) -> MidgeResult<()> {
        let request_id = crate::runtime::next_request_id()?;
        let response =
            runtime_handle.send_and_wait(crate::runtime::RuntimeMsg::SetRuntimeConfig {
                request_id,
                memtable_size_limit: Some(opts.runtime_memtable_size_limit()),
                memtable_flush_threshold: Some(opts.runtime_memtable_flush_threshold()),
                enable_compaction: None,
                l0_compaction_trigger: Some(opts.l0_compaction_trigger()),
                wal_durability_policy: None,
                wal_batch_config: None,
            })?;

        match response {
            crate::runtime::RuntimeResponse::Ok { .. } => Ok(()),
            crate::runtime::RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "unexpected response to SetRuntimeConfig".to_string(),
            )),
        }
    }
}
