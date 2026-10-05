use super::super::OpenOptions;
use super::super::IN_MEMORY_OPEN_COUNTER;
use super::{
    CloudStartupRecovery, RuntimeState, RuntimeStorageMaterialization, StartupLease,
    StartupStoragePath,
};
use crate::common::{DeadlineScope, MidgeError, MidgeResult};
use crate::config::{RecoveryPolicy, Storage};
use crate::io::FsError;
use crate::runtime::ddl::DdlLeaseAuthority;
use crate::runtime::hybrid_persistence::{CloudMetadataMirrorAuthority, CloudPersistence};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

struct CloudClassStores {
    wal: Arc<crate::storage::cloud::CloudStorage>,
    sst: Arc<crate::storage::cloud::CloudStorage>,
    metadata: Arc<crate::storage::cloud::CloudStorage>,
}

fn scope_cloud_store(
    store: Arc<crate::storage::cloud::CloudStorage>,
    scope: Option<&DeadlineScope>,
) -> Arc<crate::storage::cloud::CloudStorage> {
    if let Some(scope) = scope {
        Arc::new(store.with_startup_scope(scope.clone()))
    } else {
        store
    }
}

fn check_startup_scope(scope: Option<&DeadlineScope>, context: &str) -> MidgeResult<()> {
    scope.map_or(Ok(()), |scope| scope.check(context))
}

fn hydrate_metadata(
    storage: &crate::storage::cloud::CloudStorage,
    authority: &dyn crate::lease::LeaderStore,
    db_path: &Path,
    policy: RecoveryPolicy,
    scope: Option<&DeadlineScope>,
) -> MidgeResult<()> {
    if let Some(scope) = scope {
        CloudStartupRecovery::hydrate_cloud_metadata_within(
            storage, authority, db_path, policy, scope,
        )
    } else {
        CloudStartupRecovery::hydrate_cloud_metadata(storage, authority, db_path, policy)
    }
}

fn initialize_startup_state(
    opts: &OpenOptions,
    path: &StartupStoragePath,
    replay_wal: bool,
    scope: Option<&DeadlineScope>,
) -> MidgeResult<RuntimeState> {
    if let Some(scope) = scope {
        scope.check("local recovery initialization")?;
        if replay_wal {
            RuntimeState::try_new_with_recovery_dir_within(
                path.db_path.clone(),
                path.memory_mode,
                None,
                opts.recovery_policy(),
                scope,
            )
        } else {
            RuntimeState::try_new_before_cloud_replay_within(
                path.db_path.clone(),
                opts.recovery_policy(),
                scope,
            )
        }
    } else if replay_wal {
        RuntimeState::try_new(
            path.db_path.clone(),
            path.memory_mode,
            opts.recovery_policy(),
        )
    } else {
        RuntimeState::try_new_before_cloud_replay(path.db_path.clone(), opts.recovery_policy())
    }
}

#[allow(clippy::too_many_arguments)]
fn build_wal_plan(
    db_path: &Path,
    remote: &Arc<dyn crate::storage::StorageBackend>,
    catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    policy: RecoveryPolicy,
    timeout: std::time::Duration,
    read_window: usize,
    limits: crate::wal::recovery::streaming::StreamingReplayLimits,
    scope: Option<&DeadlineScope>,
) -> MidgeResult<crate::runtime::cloud_startup::streaming_wal_plan::StreamingCloudWalRecovery> {
    use crate::runtime::cloud_startup::streaming_wal_plan::StreamingCloudWalRecovery;
    if let Some(scope) = scope {
        StreamingCloudWalRecovery::build_within(
            db_path,
            remote,
            catalog,
            policy,
            timeout,
            read_window,
            limits,
            scope,
        )
    } else {
        StreamingCloudWalRecovery::build(
            db_path,
            remote,
            catalog,
            policy,
            timeout,
            read_window,
            limits,
        )
    }
}

fn commit_salvage_plan(
    plan: &crate::runtime::cloud_startup::CloudWalRecoveryPlan,
    persistence: &CloudPersistence,
    epoch: u64,
    catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    db_path: &Path,
    validate: &dyn Fn() -> MidgeResult<()>,
    scope: Option<&DeadlineScope>,
) -> MidgeResult<()> {
    if let Some(scope) = scope {
        plan.commit_set_aside_with_authority_within(
            persistence,
            epoch,
            catalog,
            db_path,
            validate,
            scope,
        )
    } else {
        plan.commit_set_aside_with_authority(persistence, epoch, catalog, db_path, validate)
    }
}

fn provider_ddl_authority(lease: &StartupLease) -> MidgeResult<DdlLeaseAuthority> {
    Ok(DdlLeaseAuthority {
        store: lease.leader_store.clone().ok_or_else(|| {
            MidgeError::Internal("cloud metadata startup requires a leader store".into())
        })?,
        holder_id: lease.lease.holder_id(),
        writer_epoch: lease.writer_epoch,
    })
}

fn fence_provider_ddl(
    storage: &crate::storage::HybridStorage,
    lease: &StartupLease,
    timeout: std::time::Duration,
) -> MidgeResult<()> {
    fence_provider_ddl_within(
        storage,
        lease,
        &crate::common::OperationDeadline::from_budget(timeout),
    )
}

fn fence_provider_ddl_within(
    storage: &crate::storage::HybridStorage,
    lease: &StartupLease,
    deadline: &crate::common::OperationDeadline,
) -> MidgeResult<()> {
    lease.install_storage_write_authority(storage)?;
    let authority = provider_ddl_authority(lease)?;
    crate::runtime::ddl::fence_remote_registry_on_startup(storage, &authority, deadline)?;
    lease.ensure_healthy("after cloud DDL registry fencing")
}

fn require_empty_local_cache_for_cloud_bootstrap(
    db_path: &Path,
    scope: Option<&DeadlineScope>,
) -> MidgeResult<()> {
    let mut directories = vec![db_path.to_path_buf()];
    while let Some(directory) = directories.pop() {
        check_startup_scope(scope, "bootstrap cache discovery")?;
        for entry in std::fs::read_dir(directory)? {
            check_startup_scope(scope, "bootstrap cache entry")?;
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                directories.push(entry.path());
            } else {
                return Err(MidgeError::RecoveryFailed(format!(
                    "local cache '{}' contains state without a committed cloud metadata generation",
                    entry.path().display()
                )));
            }
        }
    }
    Ok(())
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

    pub(super) fn prepare(&self) -> MidgeResult<()> {
        if !self.memory_mode {
            crate::io::durable_dir::create_path_durably(&self.db_path)?;
        }
        Ok(())
    }
}

impl StartupLease {
    /// Acquire the primary lease with an epoch strictly above
    /// `minimum_epoch`, the highest writer epoch already durable in this
    /// engine's storage.
    pub(super) fn acquire(opts: &OpenOptions, minimum_epoch: u64) -> MidgeResult<Self> {
        Self::acquire_configured(opts, minimum_epoch, None)
    }

    pub(super) fn acquire_within(
        opts: &OpenOptions,
        minimum_epoch: u64,
        scope: &DeadlineScope,
    ) -> MidgeResult<Self> {
        scope.check("lease creation")?;
        Self::acquire_configured(opts, minimum_epoch, Some(scope))
    }

    fn acquire_configured(
        opts: &OpenOptions,
        minimum_epoch: u64,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Self> {
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

        let mut acquired = Self::acquire_created(
            created.lease,
            created.validity,
            Some(storage),
            opts.lease_loss_hook(),
            minimum_epoch,
            scope,
            opts.storage_io_timeout(),
        )?;
        acquired.cleanup_observer = opts.startup_observer();
        Ok(acquired)
    }

    #[cfg(test)]
    pub(super) fn acquire_for_test(
        lease: Arc<dyn crate::lease::PrimaryLease>,
        validity: Option<Arc<crate::lease::LeaseValidity>>,
    ) -> MidgeResult<Self> {
        Self::acquire_created(
            lease,
            validity,
            None,
            None,
            0,
            None,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )
    }

    fn acquire_created(
        lease: Arc<dyn crate::lease::PrimaryLease>,
        lease_validity: Option<Arc<crate::lease::LeaseValidity>>,
        storage: Option<&Storage>,
        lease_loss_hook: Option<Arc<dyn Fn() + Send + Sync>>,
        minimum_epoch: u64,
        scope: Option<&DeadlineScope>,
        per_io_timeout: std::time::Duration,
    ) -> MidgeResult<Self> {
        let attempt = if let Some(scope) = scope {
            lease
                .clone()
                .try_acquire_with_minimum_epoch_within(minimum_epoch, scope)
        } else {
            lease.clone().try_acquire_with_minimum_epoch(minimum_epoch)
        };
        let lease_guard = match attempt {
            Ok(guard) => guard,
            Err(error) => {
                // The fresh timed attempt retains its original owner even if
                // CAS succeeded before sentinel/readback could return a guard.
                if scope.is_some() {
                    let _ = lease.release();
                }
                return Err(match error {
                    crate::lease::LeaseError::AcquisitionFailed(message) => {
                        MidgeError::LeaseHeld(format!(
                        "another Midge instance is already running against this storage: {message}"
                    ))
                    }
                    // A store that did not answer in time is unavailable for
                    // acquisition, as before the timeout had its own variant.
                    crate::lease::LeaseError::Timeout(message) if scope.is_some() => {
                        MidgeError::Timeout(message)
                    }
                    crate::lease::LeaseError::IoError(message)
                    | crate::lease::LeaseError::Timeout(message) => {
                        MidgeError::LeaseUnavailable(message)
                    }
                    crate::lease::LeaseError::RenewalFailed(message) => MidgeError::Fenced(message),
                    crate::lease::LeaseError::Indeterminate(message) => {
                        MidgeError::LeaseIndeterminate(message)
                    }
                    crate::lease::LeaseError::EpochExhausted => MidgeError::LeaseEpochExhausted,
                    crate::lease::LeaseError::AlreadyAcquired(message) => MidgeError::Busy(message),
                    crate::lease::LeaseError::Internal(message) => MidgeError::Internal(message),
                });
            }
        };

        tracing::warn!(
            holder_id = %lease.holder_id(),
            storage = ?storage,
            epoch = lease.epoch(),
            minimum_epoch,
            "primary lease acquired - this instance is now the exclusive writer"
        );

        let writer_epoch = lease.epoch();
        let leader_store = lease.get_leader_store().map(|store| {
            scope.map_or_else(
                || store.clone(),
                |scope| {
                    crate::lease::scoped_leader_store(store.clone(), scope.clone(), per_io_timeout)
                },
            )
        });
        let lease_healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let mut startup_lease = Self {
            lease,
            lease_guard: Some(lease_guard),
            writer_epoch,
            leader_store,
            lease_healthy,
            lease_validity,
            lease_heartbeat: None,
            cleanup_observer: None,
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

    fn prepare_wal_catalog(
        &self,
        storage: &Arc<crate::storage::HybridStorage>,
    ) -> MidgeResult<crate::runtime::hybrid_persistence::AdmittedCatalog> {
        let catalog = CloudPersistence::new(Arc::clone(storage))
            .fence_cloud_wal_catalog(self.writer_epoch)?;
        self.ensure_healthy("after cloud WAL catalog fencing")?;
        Ok(catalog)
    }

    fn prepare_wal_catalog_within(
        &self,
        storage: &Arc<crate::storage::HybridStorage>,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<crate::runtime::hybrid_persistence::AdmittedCatalog> {
        if let Some(scope) = scope {
            scope.check("cloud WAL catalog fencing")?;
            let catalog = CloudPersistence::new(Arc::clone(storage))
                .fence_cloud_wal_catalog_within(self.writer_epoch, &scope.deadline())?;
            self.ensure_healthy("after cloud WAL catalog fencing")?;
            scope.check("cloud WAL catalog fencing")?;
            Ok(catalog)
        } else {
            self.prepare_wal_catalog(storage)
        }
    }

    fn install_storage_write_authority(
        &self,
        storage: &crate::storage::HybridStorage,
    ) -> MidgeResult<()> {
        let validity = self.lease_validity.clone();
        let healthy = Arc::clone(&self.lease_healthy);
        let epoch = self.writer_epoch;
        storage.configure_write_authority(Arc::new(move || {
            if let Some(validity) = &validity {
                validity.remaining(epoch).map_err(|error| {
                    error.into_validation_error("storage mutation monotonic lease validity")
                })?;
            }
            if healthy.load(std::sync::atomic::Ordering::Acquire) {
                Ok(())
            } else {
                Err(MidgeError::Fenced(
                    "storage mutation lost writer authority".into(),
                ))
            }
        }))
    }

    pub(super) fn ensure_healthy(&self, phase: &str) -> MidgeResult<()> {
        if let Some(validity) = &self.lease_validity {
            validity.remaining(self.writer_epoch).map_err(|error| {
                error.into_validation_error(&format!("startup monotonic lease validity {phase}"))
            })?;
        }
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
            let result = self.lease.release();
            if let Some(observer) = &self.cleanup_observer {
                observer.observe(crate::runtime::StartupEvent::CleanupFinished {
                    successful: result.is_ok(),
                });
            }
        }
    }
}

impl RuntimeStorageMaterialization {
    /// Publish the empty provider generation from isolated scratch before
    /// creating any local cache metadata. A failed upload or lease CAS then
    /// leaves the cache reusable, while a committed CAS hydrates on retry.
    #[cfg(test)]
    pub(super) fn bootstrap_provider_metadata_if_uncommitted(
        metadata_storage: &crate::storage::cloud::CloudStorage,
        sst_storage: &crate::storage::cloud::CloudStorage,
        hybrid_storage: &crate::storage::HybridStorage,
        authority: &DdlLeaseAuthority,
        wal_catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        db_path: &Path,
        timeout: std::time::Duration,
    ) -> MidgeResult<()> {
        Self::bootstrap_provider_metadata_scoped(
            metadata_storage,
            sst_storage,
            hybrid_storage,
            authority,
            wal_catalog,
            db_path,
            timeout,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bootstrap_provider_metadata_scoped(
        metadata_storage: &crate::storage::cloud::CloudStorage,
        sst_storage: &crate::storage::cloud::CloudStorage,
        hybrid_storage: &crate::storage::HybridStorage,
        authority: &DdlLeaseAuthority,
        wal_catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
        db_path: &Path,
        timeout: std::time::Duration,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<()> {
        check_startup_scope(scope, "cloud metadata bootstrap")?;
        let leader_store = authority.store.as_ref();
        let head = leader_store
            .read_committed_metadata(scope.map_or(timeout, |scope| scope.clamp(timeout)))
            .map_err(|error| {
                if scope.is_some() && matches!(error, crate::lease::LeaseError::Timeout(_)) {
                    return MidgeError::Timeout(format!(
                        "cloud metadata authority bootstrap: {error}"
                    ));
                }
                if scope.is_some() && matches!(error, crate::lease::LeaseError::Indeterminate(_)) {
                    return MidgeError::LeaseIndeterminate(format!(
                        "cloud metadata authority bootstrap: {error}"
                    ));
                }
                MidgeError::RecoveryFailed(format!(
                    "failed to read cloud metadata authority before bootstrap: {error}"
                ))
            })?;
        match head {
            crate::lease::CloudMetadataHead::MissingLease => {
                return Err(MidgeError::RecoveryFailed(
                    "cloud metadata lease disappeared before bootstrap".into(),
                ));
            }
            crate::lease::CloudMetadataHead::Committed(_) => {
                return hydrate_metadata(
                    metadata_storage,
                    leader_store,
                    db_path,
                    RecoveryPolicy::Strict,
                    scope,
                );
            }
            crate::lease::CloudMetadataHead::Uncommitted => {}
        }

        // The uncommitted lease is not proof that a copied local cache or a
        // remote WAL/DDL history belongs to an empty database. Preserve them
        // and fail closed rather than publishing a default manifest over data.
        hydrate_metadata(
            metadata_storage,
            leader_store,
            db_path,
            RecoveryPolicy::Strict,
            scope,
        )?;
        require_empty_local_cache_for_cloud_bootstrap(db_path, scope)?;
        if !wal_catalog.segments.is_empty() || wal_catalog.sequence_floor != 0 {
            return Err(MidgeError::RecoveryFailed(
                "cannot bootstrap empty cloud metadata over existing WAL state".into(),
            ));
        }
        let deadline = scope.map_or_else(
            || crate::common::OperationDeadline::from_budget(timeout.saturating_mul(16)),
            DeadlineScope::deadline,
        );
        crate::runtime::ddl::require_empty_registry_for_metadata_bootstrap(
            hybrid_storage,
            authority,
            &deadline,
        )?;
        if !crate::storage::cloud::BlockingCloud::new(sst_storage, &deadline)
            .list(crate::cloud_layout::CloudObjectLayout::SST_PREFIX)?
            .is_empty()
        {
            return Err(MidgeError::RecoveryFailed(
                "cannot bootstrap empty cloud metadata over existing SST objects".into(),
            ));
        }

        // Scratch files are never local cache authority. In-memory staging
        // also keeps a failed or interrupted first open retryable regardless
        // of TMPDIR or parent-directory permissions.
        let staging_fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());
        crate::io::staging::stage_bytes(
            &staging_fs,
            &crate::io::FsPath::new("FORMAT.tmp"),
            &crate::io::FsPath::new(crate::metadata::files::FORMAT),
            &crate::metadata::format::current_format_marker_bytes(),
            MidgeError::RecoveryFailed,
        )?;
        crate::metadata::store::ManifestStore::new(Arc::clone(&staging_fs))
            .save_snapshot(&crate::metadata::Manifest::default())?;
        let publication_lock = crate::runtime::MetadataPublicationLock::default();
        crate::runtime::hybrid_persistence::mirror_control_metadata_within(
            crate::runtime::hybrid_persistence::CloudMetadataMirrorContext {
                cloud: metadata_storage,
                fs: staging_fs.as_ref(),
                publication_lock: &publication_lock,
                lock_wait_budget: timeout,
                local_manifest_sequence: 0,
                deadline: &deadline,
                authority: CloudMetadataMirrorAuthority {
                    store: leader_store,
                    holder_id: &authority.holder_id,
                    writer_epoch: authority.writer_epoch,
                },
            },
            |deadline| authority.validate(deadline),
        )?;

        hydrate_metadata(
            metadata_storage,
            leader_store,
            db_path,
            RecoveryPolicy::Strict,
            scope,
        )
    }

    fn build_cloud_class_stores(
        opts: &OpenOptions,
        topology: &crate::config::CloudStorageTopology,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<CloudClassStores> {
        let wal = crate::storage::providers::build_cloud_storage_with_timeout(
            topology.wal().provider(),
            topology.wal().prefix(),
            opts.storage_io_timeout(),
        )?;
        let wal = scope_cloud_store(wal, scope);
        let sst = if topology.sst() == topology.wal() {
            wal.clone()
        } else {
            scope_cloud_store(
                crate::storage::providers::build_cloud_storage_with_timeout(
                    topology.sst().provider(),
                    topology.sst().prefix(),
                    opts.storage_io_timeout(),
                )?,
                scope,
            )
        };
        let metadata = if topology.control() == topology.wal() {
            wal.clone()
        } else if topology.control() == topology.sst() {
            sst.clone()
        } else {
            scope_cloud_store(
                crate::storage::providers::build_cloud_storage_with_timeout(
                    topology.control().provider(),
                    topology.control().prefix(),
                    opts.storage_io_timeout(),
                )?,
                scope,
            )
        };
        if let Some(scope) = scope {
            CloudStartupRecovery::reject_cloud_wal_without_catalog_within(&wal, scope)?;
        } else {
            CloudStartupRecovery::reject_cloud_wal_without_catalog(&wal)?;
        }
        Ok(CloudClassStores { wal, sst, metadata })
    }

    pub(super) fn materialize(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
    ) -> MidgeResult<Self> {
        Self::materialize_scoped(opts, storage_path, startup_lease, None)
    }

    pub(super) fn materialize_within(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        scope: &DeadlineScope,
    ) -> MidgeResult<Self> {
        scope.check("storage materialization")?;
        Self::materialize_scoped(opts, storage_path, startup_lease, Some(scope))
    }

    fn materialize_scoped(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Self> {
        let cloud_runtime_policy = opts.cloud_runtime_policy();

        match opts.storage() {
            Storage::CloudSimulated { .. } => Self::materialize_simulated_cloud(
                opts,
                storage_path,
                startup_lease,
                cloud_runtime_policy,
                scope,
            ),
            Storage::Cloud { topology, .. } => Self::materialize_cloud(
                opts,
                storage_path,
                startup_lease,
                cloud_runtime_policy,
                topology,
                scope,
            ),
            _ => Self::materialize_local(
                opts,
                storage_path,
                startup_lease,
                cloud_runtime_policy,
                scope,
            ),
        }
    }

    fn cloud_runtime_config(
        opts: &OpenOptions,
        startup_lease: &StartupLease,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
        scope: Option<&DeadlineScope>,
    ) -> crate::runtime::RuntimeConfig {
        crate::runtime::RuntimeConfig {
            ttl_clock: opts.ttl_clock(),
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudAsync,
            storage_io_timeout: opts.storage_io_timeout(),
            runtime_response_timeout: opts.runtime_response_timeout(),
            startup_observer: opts.startup_observer(),
            startup_scope: scope.cloned(),
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
            lease_validity: startup_lease.lease_validity.clone(),
            leader_store: startup_lease.leader_store.clone(),
            leader_holder_id: Some(startup_lease.lease.holder_id()),
            ..Default::default()
        }
    }

    fn materialize_simulated_cloud(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Self> {
        let cloud = crate::storage::simulated::build_simulated_cloud_stores(
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
        let sst_read_fs: Arc<dyn crate::io::Fs> =
            Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
                Arc::new(
                    crate::io::RealFs::new(&storage_path.db_path).map_err(FsError::into_midge)?,
                ),
                sst_backend,
                opts.storage_io_timeout(),
            ));
        let sst_read_fs = scope.map_or_else(
            || sst_read_fs.clone(),
            |scope| crate::io::scope_fs(sst_read_fs.clone(), scope.clone()),
        );
        CloudStartupRecovery::reject_simulated_cloud_wal_without_catalog(
            &cloud.recovery_cloud_wal_dir,
        )?;
        cloud
            .hybrid_storage
            .configure_maintenance_memory(opts.compaction_memory_pool_size());
        startup_lease.install_storage_write_authority(&cloud.hybrid_storage)?;
        let wal_catalog = startup_lease.prepare_wal_catalog_within(&cloud.hybrid_storage, scope)?;

        let limits = super::streaming_recovery::CloudReplay::limits(opts);
        let wal_backend: Arc<dyn crate::storage::StorageBackend> = Arc::new(
            crate::storage::filesystem::FileSystem::new(cloud.cloud_root.clone())?,
        );
        let streaming = super::timing::measure("wal_plan", || {
            build_wal_plan(
                &storage_path.db_path,
                &wal_backend,
                &wal_catalog,
                opts.recovery_policy(),
                opts.storage_io_timeout(),
                super::streaming_recovery::CloudReplay::read_window(opts, limits),
                limits,
                scope,
            )
        })?;
        let recovery_plan = streaming.plan;
        commit_salvage_plan(
            &recovery_plan,
            &CloudPersistence::new(Arc::clone(&cloud.hybrid_storage)),
            startup_lease.writer_epoch,
            &wal_catalog,
            &storage_path.db_path,
            &|| startup_lease.ensure_healthy("during WAL salvage set aside"),
            scope,
        )?;
        let mut state = initialize_startup_state(opts, storage_path, false, scope)?;
        state.wal.current_segment_id = streaming.next_segment_id;
        if recovery_plan.opened_in_salvage_mode {
            state.mark_opened_in_salvage_mode();
            state.mark_persistence_anomaly();
        }

        let runtime_config = crate::runtime::RuntimeConfig {
            hybrid_storage: Some(cloud.hybrid_storage),
            sst_read_fs: Some(sst_read_fs),
            hybrid_storage_events: Some(cloud.events),
            recovered_cloud_wal_segments: recovery_plan.remote_max_sequences(),
            recovered_cloud_wal_segment_epochs: recovery_plan.remote_writer_epochs(),
            recovered_local_wal_segments: recovery_plan.local_max_sequences(),
            recovered_local_wal_segment_epochs: recovery_plan.local_writer_epochs(),
            recovered_cloud_active_wal: recovery_plan.active_wal,
            max_replayable_txn_bytes: Some(limits.max_replayable_txn_bytes()),
            ..Self::cloud_runtime_config(opts, startup_lease, cloud_runtime_policy, scope)
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
                sequence_floor: recovery_plan.max_unreplayed_sequence,
                scope: scope.cloned(),
            }),
        })
    }

    fn build_hybrid_storage(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        wal_storage: &Arc<crate::storage::cloud::CloudStorage>,
        sst_storage: &Arc<crate::storage::cloud::CloudStorage>,
        metadata_storage: &Arc<crate::storage::cloud::CloudStorage>,
    ) -> MidgeResult<(
        Arc<crate::storage::HybridStorage>,
        crossbeam::channel::Receiver<crate::storage::StorageEvent>,
    )> {
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
        hybrid_storage.configure_maintenance_memory(opts.compaction_memory_pool_size());
        Ok((hybrid_storage, rx))
    }

    fn build_provider_sst_read_fs(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        sst_storage: &Arc<crate::storage::cloud::CloudStorage>,
    ) -> MidgeResult<Arc<crate::storage::remote_sst::RemoteSstFs>> {
        let local_fs =
            Arc::new(crate::io::RealFs::new(&storage_path.db_path).map_err(FsError::into_midge)?);
        Ok(Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
            local_fs,
            sst_storage.clone(),
            opts.storage_io_timeout(),
        )))
    }

    fn materialize_cloud(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
        topology: &crate::config::CloudStorageTopology,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Self> {
        let stores = Self::build_cloud_class_stores(opts, topology, scope)?;
        let wal_storage = stores.wal;
        let sst_storage = stores.sst;
        let metadata_storage = stores.metadata;

        let (hybrid_storage, rx) = Self::build_hybrid_storage(
            opts,
            storage_path,
            &wal_storage,
            &sst_storage,
            &metadata_storage,
        )?;
        if let Some(scope) = scope {
            fence_provider_ddl_within(&hybrid_storage, startup_lease, &scope.deadline())?;
        } else {
            fence_provider_ddl(&hybrid_storage, startup_lease, opts.storage_io_timeout())?;
        }
        let sst_read_fs: Arc<dyn crate::io::Fs> =
            Self::build_provider_sst_read_fs(opts, storage_path, &sst_storage)?;
        let sst_read_fs = scope.map_or_else(
            || sst_read_fs.clone(),
            |scope| crate::io::scope_fs(sst_read_fs.clone(), scope.clone()),
        );
        let wal_catalog = startup_lease.prepare_wal_catalog_within(&hybrid_storage, scope)?;

        let authority = provider_ddl_authority(startup_lease)?;
        Self::bootstrap_provider_metadata_scoped(
            &metadata_storage,
            &sst_storage,
            &hybrid_storage,
            &authority,
            &wal_catalog,
            &storage_path.db_path,
            opts.storage_io_timeout(),
            scope,
        )?;
        let limits = super::streaming_recovery::CloudReplay::limits(opts);
        let streaming = super::timing::measure("wal_plan", || {
            build_wal_plan(
                &storage_path.db_path,
                &(wal_storage.clone() as Arc<dyn crate::storage::StorageBackend>),
                &wal_catalog,
                opts.recovery_policy(),
                opts.storage_io_timeout(),
                super::streaming_recovery::CloudReplay::read_window(opts, limits),
                limits,
                scope,
            )
        })?;
        let recovery_plan = streaming.plan;
        commit_salvage_plan(
            &recovery_plan,
            &CloudPersistence::new(Arc::clone(&hybrid_storage)),
            startup_lease.writer_epoch,
            &wal_catalog,
            &storage_path.db_path,
            &|| startup_lease.ensure_healthy("during WAL salvage set aside"),
            scope,
        )?;
        let mut state = initialize_startup_state(opts, storage_path, false, scope)?;
        state.wal.current_segment_id = streaming.next_segment_id;
        if recovery_plan.opened_in_salvage_mode {
            state.mark_opened_in_salvage_mode();
            state.mark_persistence_anomaly();
        }

        let runtime_config = crate::runtime::RuntimeConfig {
            hybrid_storage: Some(hybrid_storage),
            sst_read_fs: Some(sst_read_fs),
            hybrid_storage_events: Some(rx),
            cloud_metadata_storage: Some(metadata_storage.clone()),
            provider_ddl_fencing: true,
            recovered_cloud_wal_segments: recovery_plan.remote_max_sequences(),
            recovered_cloud_wal_segment_epochs: recovery_plan.remote_writer_epochs(),
            recovered_local_wal_segments: recovery_plan.local_max_sequences(),
            recovered_local_wal_segment_epochs: recovery_plan.local_writer_epochs(),
            recovered_cloud_active_wal: recovery_plan.active_wal,
            max_replayable_txn_bytes: Some(limits.max_replayable_txn_bytes()),
            ..Self::cloud_runtime_config(opts, startup_lease, cloud_runtime_policy, scope)
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
                sequence_floor: recovery_plan.max_unreplayed_sequence,
                scope: scope.cloned(),
            }),
        })
    }

    fn materialize_local(
        opts: &OpenOptions,
        storage_path: &StartupStoragePath,
        startup_lease: &StartupLease,
        cloud_runtime_policy: crate::runtime::CloudRuntimePolicy,
        scope: Option<&DeadlineScope>,
    ) -> MidgeResult<Self> {
        let batch_config = opts.wal_batch_config().unwrap_or_default();

        let runtime_config = crate::runtime::RuntimeConfig {
            ttl_clock: opts.ttl_clock(),
            wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
            wal_batch_config: batch_config,
            startup_observer: opts.startup_observer(),
            startup_scope: scope.cloned(),
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
            lease_validity: startup_lease.lease_validity.clone(),
            leader_store: startup_lease.leader_store.clone(),
            leader_holder_id: Some(startup_lease.lease.holder_id()),
            ..Default::default()
        };

        Ok(Self {
            state: initialize_startup_state(opts, storage_path, true, scope)?,
            runtime_config,
            cloud_root: None,
            cloud_storage_for_restore: None,
            cloud_metadata_storage_for_mirror: None,
            streaming_wal: None,
        })
    }
}

#[cfg(all(test, feature = "failpoints"))]
mod authority_tests {
    use super::*;

    #[test]
    fn should_preserve_registry_when_startup_validity_expires_before_ddl_cas() {
        // Arrange
        let _guard = crate::failpoints::test_failpoint_guard();
        let _scenario = fail::FailScenario::setup();
        let directory = tempfile::tempdir().unwrap();
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            "startup-ddl-expiry".into(),
        ));
        let lease = Arc::new(crate::lease::CloudStorageLease::new_provider_backed(
            crate::lease::CloudLeaseConfig {
                bucket: "startup-ddl-expiry".into(),
                prefix: "test/".into(),
            },
            directory.path().join("lease"),
            Arc::clone(&cloud),
        ));
        let validity = lease.lease_validity();
        let lease_object: Arc<dyn crate::lease::PrimaryLease> = lease;
        let startup =
            StartupLease::acquire_for_test(lease_object, Some(Arc::clone(&validity))).unwrap();
        let storage = crate::storage::HybridStorage::with_policy(
            Arc::new(
                crate::storage::filesystem::FileSystem::new(directory.path().join("local"))
                    .unwrap(),
            ),
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        );
        let registry_key = crate::runtime::ddl::REMOTE_DDL_REGISTRY_KEY;
        assert!(storage
            .remote_object_proof_optional(registry_key)
            .unwrap()
            .is_none());
        fail::cfg_callback("midge::ddl::before_remote_cas", move || {
            validity.expire_for_test();
        })
        .unwrap();

        // Act
        let result = fence_provider_ddl(&storage, &startup, std::time::Duration::from_secs(5));

        // Assert
        assert!(matches!(result, Err(MidgeError::Fenced(_))), "{result:?}");
        assert!(storage
            .remote_object_proof_optional(registry_key)
            .unwrap()
            .is_none());
    }
}
