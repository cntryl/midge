use super::super::{lease_state::LeaseState, ColumnFamilyHandle, Engine, OpenOptions};
use super::{
    provider_kind, redact_endpoint_metadata, EngineStartup, FacadeAssembly,
    RuntimeRecoveryMaterialization, RuntimeStorageMaterialization, StartedRuntime, StartupLease,
    StartupStoragePath,
};
use crate::common::{DeadlineScope, MidgeError, MidgeResult};
use crate::config::Storage;
use crate::engine::ingest;
use crate::runtime::Runtime;
use std::sync::Arc;

impl StartedRuntime {
    fn start(
        opts: &OpenOptions,
        mut recovered: RuntimeRecoveryMaterialization,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Self> {
        let recovered_sequence = recovered.recovered_sequence;
        let recovered_cf_metas = recovered.recovered_cf_metas;
        let (runtime_inst, _) = Runtime::new();
        recovered.state.limits.memtable_size_limit = opts.runtime_memtable_size_limit();
        recovered.state.limits.memtable_flush_threshold = opts.runtime_memtable_flush_threshold();
        let (runtime, runtime_handle, admission) = if let Some(scope) = scope {
            let (runtime, handle, admission) = runtime_inst.prepare_with_config(
                recovered.state,
                recovered.runtime_config,
                scope.clone(),
            )?;
            (runtime, handle, Some(admission))
        } else {
            let (runtime, handle) =
                runtime_inst.start_with_config(recovered.state, recovered.runtime_config)?;
            (runtime, handle, None)
        };

        Ok(Self {
            runtime,
            runtime_handle,
            recovered_sequence,
            recovered_cf_metas,
            admission,
        })
    }
}

impl FacadeAssembly {
    fn assemble(
        opts: &OpenOptions,
        storage_path: StartupStoragePath,
        mut startup_lease: StartupLease,
        started: StartedRuntime,
    ) -> MidgeResult<Engine> {
        let column_families = dashmap::DashMap::new();
        let default_handle = ColumnFamilyHandle::new(0, "default".to_string());
        column_families.insert(default_handle.id(), default_handle);

        let ingest_coordinators = dashmap::DashMap::new();
        let default_coordinator = Arc::new(ingest::IngestCoordinator::new(0));
        ingest_coordinators.insert(0, default_coordinator);

        startup_lease.ensure_healthy("before engine assembly")?;

        for cf_meta in &started.recovered_cf_metas {
            if cf_meta.id != 0 && cf_meta.deleted_at.is_none() {
                let handle = ColumnFamilyHandle::new(cf_meta.id, cf_meta.name.clone());
                column_families.insert(cf_meta.id, handle);

                let coordinator = Arc::new(ingest::IngestCoordinator::new(cf_meta.id));
                ingest_coordinators.insert(cf_meta.id, coordinator);
            }
        }

        startup_lease.ensure_healthy("before transferring engine ownership")?;
        if startup_lease.lease_guard.is_none() {
            return Err(MidgeError::Internal(
                "startup lease guard was already transferred".into(),
            ));
        }
        let lease_heartbeat = startup_lease.take_heartbeat()?;
        let lease = Arc::clone(&startup_lease.lease);
        let lease_guard = startup_lease.lease_guard.take().ok_or_else(|| {
            MidgeError::Internal("startup lease guard was already transferred".to_string())
        })?;

        let hybrid_storage = started.runtime_handle.storage_budget.clone();

        Ok(Engine {
            runtime: Some(started.runtime),
            runtime_handle: started.runtime_handle,
            db_path: storage_path.db_path,
            memory_mode: storage_path.memory_mode,
            cloud_mode: matches!(
                opts.storage(),
                Storage::Cloud { .. } | Storage::CloudSimulated { .. }
            ),
            simulated_cloud_mode: matches!(opts.storage(), Storage::CloudSimulated { .. }),
            sequence: Arc::new(std::sync::atomic::AtomicU64::new(
                started.recovered_sequence,
            )),
            next_snapshot_id: std::sync::atomic::AtomicU64::new(1),
            column_families,
            lease_state: LeaseState::new(lease, lease_guard, lease_heartbeat, hybrid_storage),
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
        if let Some(budget) = opts.open_timeout() {
            super::deadline::open(opts, budget)
        } else {
            Self::open(&opts)
        }
    }

    pub(crate) fn open(opts: &OpenOptions) -> MidgeResult<Engine> {
        if let Some(budget) = opts.open_timeout() {
            return super::deadline::open(opts.clone(), budget);
        }
        super::timing::measure("open", || {
            Self::open_profiled(opts, None, std::time::Instant::now()).map(|(engine, _)| engine)
        })
    }

    pub(super) fn prepare_within(
        opts: &OpenOptions,
        scope: &DeadlineScope,
        start: std::time::Instant,
    ) -> MidgeResult<super::deadline::PreparedEngine> {
        let (engine, admission) = super::timing::measure("open_preparation", || {
            Self::open_profiled(opts, Some(scope), start)
        })?;
        Ok(super::deadline::PreparedEngine {
            engine,
            admission: admission.expect("timed startup always prepares an admission gate"),
        })
    }

    fn open_profiled(
        opts: &OpenOptions,
        scope: Option<&DeadlineScope>,
        start: std::time::Instant,
    ) -> MidgeResult<(Engine, Option<crate::runtime::StartupAdmission>)> {
        check_scope(scope, "startup entry")?;
        Self::trace_open(opts);
        let storage_path = StartupStoragePath::resolve(opts.storage());
        storage_path.prepare()?;
        check_scope(scope, "storage path preparation")?;

        let minimum_epoch = super::timing::measure("lease_epoch_floor", || {
            if let Some(scope) = scope {
                super::epoch_floor::StartupEpochFloor::discover_within(opts, &storage_path, scope)
            } else {
                super::epoch_floor::StartupEpochFloor::discover(opts, &storage_path)
            }
        })?;
        check_scope(scope, "lease epoch discovery")?;
        let startup_lease = super::timing::measure("lease_acquisition", || {
            if let Some(scope) = scope {
                StartupLease::acquire_within(opts, minimum_epoch, scope)
            } else {
                StartupLease::acquire(opts, minimum_epoch)
            }
        })?;
        if let Some(observer) = opts.startup_observer() {
            observer.observe(crate::runtime::StartupEvent::LeaseAcquired {
                epoch: startup_lease.writer_epoch,
            });
        }
        check_scope(scope, "lease acquisition")?;
        if !storage_path.memory_mode {
            crate::runtime::transaction_spill::cleanup_orphaned_runs(&storage_path.db_path)?;
        }
        check_scope(scope, "orphaned spill cleanup")?;
        let materialized = super::timing::measure("storage_materialization", || {
            if let Some(scope) = scope {
                RuntimeStorageMaterialization::materialize_within(
                    opts,
                    &storage_path,
                    &startup_lease,
                    scope,
                )
            } else {
                RuntimeStorageMaterialization::materialize(opts, &storage_path, &startup_lease)
            }
        })?;
        check_scope(scope, "storage materialization")?;
        let recovered = super::timing::measure("replay_and_repair", || {
            if let Some(scope) = scope {
                RuntimeRecoveryMaterialization::replay_and_repair_within(
                    materialized,
                    &storage_path.db_path,
                    opts.recovery_policy(),
                    scope,
                )
            } else {
                RuntimeRecoveryMaterialization::replay_and_repair(
                    materialized,
                    &storage_path.db_path,
                    opts.recovery_policy(),
                )
            }
        })?;
        check_scope(scope, "replay and repair")?;
        startup_lease.ensure_healthy("before runtime creation")?;
        let started = super::timing::measure("runtime_start", || {
            StartedRuntime::start(opts, recovered, scope)
        })?;
        check_scope(scope, "runtime preparation")?;
        let admission = started.admission.clone();
        let engine = FacadeAssembly::assemble(opts, storage_path, startup_lease, started)?;
        if admission.is_none() {
            tracing::info!(
                db_path = %engine.db_path.display(),
                open_ms = start.elapsed().as_secs_f64() * 1000.0,
                "engine open completed"
            );
        }
        Ok((engine, admission))
    }

    pub(super) fn trace_open(opts: &OpenOptions) {
        if let Storage::Cloud { topology, .. } = opts.storage() {
            if topology.wal() == topology.sst() && topology.wal() == topology.control() {
                let endpoint = topology.wal().provider().endpoint().map_or_else(
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
            let wal_endpoint = topology.wal().provider().endpoint().map_or_else(
                || "<provider-default>".to_string(),
                redact_endpoint_metadata,
            );
            let sst_endpoint = topology.sst().provider().endpoint().map_or_else(
                || "<provider-default>".to_string(),
                redact_endpoint_metadata,
            );
            let control_endpoint = topology.control().provider().endpoint().map_or_else(
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
}

fn check_scope(scope: Option<&DeadlineScope>, context: &str) -> MidgeResult<()> {
    scope.map_or(Ok(()), |scope| scope.check(context))
}
