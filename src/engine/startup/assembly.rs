use super::super::{ColumnFamilyHandle, Engine, OpenOptions};
use super::{
    provider_endpoint, provider_kind, redact_endpoint_metadata, EngineStartup, FacadeAssembly,
    RuntimeRecoveryMaterialization, RuntimeStorageMaterialization, StartedRuntime, StartupLease,
    StartupStoragePath,
};
use crate::common::{MidgeError, MidgeResult};
use crate::config::Storage;
use crate::engine::ingest;
use crate::runtime::Runtime;
use std::sync::Arc;

impl StartedRuntime {
    fn start(opts: &OpenOptions, recovered: RuntimeRecoveryMaterialization) -> MidgeResult<Self> {
        let recovered_sequence = recovered.recovered_sequence;
        let recovered_cf_metas = recovered.recovered_cf_metas;
        let (runtime_inst, _) = Runtime::new();
        let (runtime, runtime_handle) =
            runtime_inst.start_with_config(recovered.state, recovered.runtime_config)?;

        EngineStartup::apply_post_start_config(opts, &runtime_handle)?;

        Ok(Self {
            runtime,
            runtime_handle,
            recovered_sequence,
            recovered_cf_metas,
        })
    }
}

impl FacadeAssembly {
    fn assemble(
        opts: &OpenOptions,
        storage_path: StartupStoragePath,
        mut startup_lease: StartupLease,
        started: StartedRuntime,
        start: std::time::Instant,
    ) -> MidgeResult<Engine> {
        let column_families = dashmap::DashMap::new();
        let default_handle = ColumnFamilyHandle::new(0, "default".to_string());
        column_families.insert(default_handle.id(), default_handle);

        let ingest_coordinators = dashmap::DashMap::new();
        let default_coordinator = Arc::new(ingest::IngestCoordinator::new(0));
        ingest_coordinators.insert(0, default_coordinator);

        startup_lease.ensure_healthy("before engine assembly")?;
        let lease_heartbeat = startup_lease.take_heartbeat()?;

        tracing::info!(
            db_path = %storage_path.db_path.display(),
            open_ms = start.elapsed().as_secs_f64() * 1000.0,
            "engine open completed"
        );

        for cf_meta in &started.recovered_cf_metas {
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                let handle = ColumnFamilyHandle::new(cf_meta.id, cf_meta.name.clone());
                column_families.insert(cf_meta.id, handle);

                let coordinator = Arc::new(ingest::IngestCoordinator::new(cf_meta.id));
                ingest_coordinators.insert(cf_meta.id, coordinator);
            }
        }

        let lease = Arc::clone(&startup_lease.lease);
        let lease_guard = startup_lease.lease_guard.take().ok_or_else(|| {
            MidgeError::Internal("startup lease guard was already transferred".to_string())
        })?;

        startup_lease.ensure_healthy("before returning the engine")?;

        Ok(Engine {
            runtime: Some(started.runtime),
            runtime_handle: started.runtime_handle,
            db_path: storage_path.db_path,
            memory_mode: storage_path.memory_mode,
            cloud_mode: matches!(
                opts.storage(),
                Storage::Cloud { .. } | Storage::CloudSimulated { .. }
            ),
            sequence: Arc::new(std::sync::atomic::AtomicU64::new(
                started.recovered_sequence,
            )),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
            column_families,
            lease: Some(lease),
            lease_guard: Some(lease_guard),
            lease_heartbeat: Some(std::sync::Mutex::new(lease_heartbeat)),
            pending_fencing_cleanup: None,
            ingest_coordinators,
            transaction_memory_pool: Arc::new(
                crate::runtime::transaction_spill::TransactionMemoryPool::new(
                    opts.transaction_memory_pool_size(),
                ),
            ),
            ttl_clock: opts.ttl_clock(),
        })
    }
}

impl EngineStartup {
    pub(crate) fn open_owned(opts: OpenOptions) -> MidgeResult<Engine> {
        let opts = std::sync::Arc::new(opts);
        Self::open(opts.as_ref())
    }

    pub(crate) fn open(opts: &OpenOptions) -> MidgeResult<Engine> {
        let start = std::time::Instant::now();
        Self::trace_open(opts);
        let storage_path = StartupStoragePath::resolve(opts.storage());
        storage_path.prepare();

        let startup_lease = StartupLease::acquire(opts)?;
        if !storage_path.memory_mode {
            crate::runtime::transaction_spill::cleanup_orphaned_runs(&storage_path.db_path)?;
        }
        let materialized =
            RuntimeStorageMaterialization::materialize(opts, &storage_path, &startup_lease)?;
        let recovered = RuntimeRecoveryMaterialization::replay_and_repair(
            materialized,
            &storage_path.db_path,
            opts.recovery_policy(),
        )?;
        startup_lease.ensure_healthy("before runtime creation")?;
        let started = StartedRuntime::start(opts, recovered)?;

        FacadeAssembly::assemble(opts, storage_path, startup_lease, started, start)
    }

    pub(super) fn trace_open(opts: &OpenOptions) {
        if let Storage::Cloud { topology, .. } = opts.storage() {
            if topology.wal() == topology.sst() && topology.wal() == topology.control() {
                let endpoint = provider_endpoint(topology.wal().provider()).map_or_else(
                    || "<provider-default>".to_string(),
                    redact_endpoint_metadata,
                );
                tracing::debug!(
                    storage = "cloud",
                    provider = provider_kind(topology.wal().provider()),
                    endpoint = %endpoint,
                    credentials = "[REDACTED]",
                    "opening midge engine"
                );
                return;
            }
            let wal_endpoint = provider_endpoint(topology.wal().provider()).map_or_else(
                || "<provider-default>".to_string(),
                redact_endpoint_metadata,
            );
            let sst_endpoint = provider_endpoint(topology.sst().provider()).map_or_else(
                || "<provider-default>".to_string(),
                redact_endpoint_metadata,
            );
            let control_endpoint = provider_endpoint(topology.control().provider()).map_or_else(
                || "<provider-default>".to_string(),
                redact_endpoint_metadata,
            );
            tracing::debug!(
                storage = "cloud",
                wal_provider = provider_kind(topology.wal().provider()),
                wal_endpoint = %wal_endpoint,
                sst_provider = provider_kind(topology.sst().provider()),
                sst_endpoint = %sst_endpoint,
                control_provider = provider_kind(topology.control().provider()),
                control_endpoint = %control_endpoint,
                credentials = "[REDACTED]",
                "opening midge engine"
            );
        } else {
            tracing::debug!(storage = ?opts.storage(), "opening midge engine");
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
                l0_compaction_trigger: None,
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
