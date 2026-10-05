//! Incremental cloud WAL replay through the engine's durable flush protocol.

use super::RuntimeStorageMaterialization;
use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::memtable::SkipListMemtable;
use crate::metadata::accounting::Origin;
use crate::runtime::actors::flush::{
    FlushActor, FlushBuildOutput, FlushIdentity, FlushMirrorTask, FlushPublicationDelta,
    FlushPublishTask, FlushWorkerResult,
};
use crate::runtime::state::FlushManifestPublication;
use crate::wal::recovery::streaming::{
    replay_wal_with_options, ReplayOptions, StreamingReplayLimits,
};
use std::collections::HashMap;
use std::sync::Arc;

use crate::runtime::cloud_startup::replay_coverage as coverage;
mod names;
#[cfg(test)]
mod tests;

pub(super) struct CloudReplay {
    pub fs: Arc<dyn Fs>,
    pub limits: StreamingReplayLimits,
    /// Sequences at or below this belong to WAL salvage set aside unreplayed.
    pub sequence_floor: u64,
    pub scope: Option<crate::common::DeadlineScope>,
}

impl CloudReplay {
    pub(super) fn read_window(
        opts: &super::super::OpenOptions,
        limits: StreamingReplayLimits,
    ) -> usize {
        (opts.memory_budget_bytes() / 64)
            .min(limits.max_frame_bytes)
            .max(1)
    }

    pub(super) fn limits(opts: &super::super::OpenOptions) -> StreamingReplayLimits {
        let disk_window =
            usize::try_from(opts.local_storage_budget_bytes() / 2).unwrap_or(usize::MAX);
        let hard_limit = (opts.memory_budget_bytes() / 8).min(disk_window);
        let target = opts
            .memtable_size_limit()
            .saturating_add(crate::memtable::size_bound::FIXED_SST_BYTES)
            .min(hard_limit);
        StreamingReplayLimits {
            max_frame_bytes: (opts.memory_budget_bytes() / 16).min(hard_limit),
            max_pending_txn_bytes: (opts.memory_budget_bytes() / 16).min(hard_limit),
            max_memtable_encoded_bytes: hard_limit,
            target_memtable_encoded_bytes: target,
        }
    }

    pub(super) fn replay(
        mut self,
        materialized: &mut RuntimeStorageMaterialization,
    ) -> MidgeResult<()> {
        if let Some(storage) = &materialized.runtime_config.hybrid_storage {
            self.limits.max_memtable_encoded_bytes = self.limits.max_memtable_encoded_bytes.min(
                usize::try_from(storage.budget_snapshot().free_bytes / 2).unwrap_or(usize::MAX),
            );
            self.limits.target_memtable_encoded_bytes = self
                .limits
                .target_memtable_encoded_bytes
                .min(self.limits.max_memtable_encoded_bytes);
            if self.limits.max_memtable_encoded_bytes == 0 {
                return Err(MidgeError::NoSpace(
                    "local residue leaves no WAL recovery checkpoint capacity".into(),
                ));
            }
        }
        let policy = match materialized.state.recovery_policy() {
            crate::config::RecoveryPolicy::Strict => crate::wal::recovery::ReplayPolicy::Strict,
            crate::config::RecoveryPolicy::Salvage => {
                crate::wal::recovery::ReplayPolicy::SalvageValidPrefix
            }
        };
        let known_cfs: std::collections::HashSet<_> =
            materialized.state.column_families.keys().copied().collect();
        let coverage = coverage::ReplayCoverage::new(
            materialized.state.manifest.clone(),
            materialized
                .runtime_config
                .sst_read_fs
                .clone()
                .unwrap_or_else(|| Arc::clone(&materialized.state.fs)),
            self.limits.max_frame_bytes,
        );
        let scope = self
            .scope
            .clone()
            .or_else(|| materialized.state.startup_scope.clone());
        let should_apply = |record: &crate::wal::WalRecord| {
            let covered = match scope.as_ref() {
                Some(scope) => coverage.contains_within(record, scope)?,
                None => coverage.contains(record),
            };
            Ok(known_cfs.contains(&record.cf_id) && !covered)
        };
        let (tx, rx) = crossbeam::channel::bounded(1);
        let mut actor = FlushActor::new_with_memory_limit(
            &materialized.state.sst_dir,
            false,
            materialized.runtime_config.compression_policy.clone(),
            tx,
            materialized.runtime_config.flush_memory_limit,
        )?;
        let mut memtables = HashMap::new();
        let mut names = names::Names::new(self.limits);
        let replay_result = replay_wal_with_options(
            self.fs.as_ref(),
            &FsPath::new("wal"),
            &mut memtables,
            policy,
            None,
            self.limits,
            ReplayOptions {
                scope: scope.as_ref(),
                fallible_should_apply: Some(&should_apply),
                ..ReplayOptions::default()
            },
            &mut |tables, _stats| {
                coverage.release_reader();
                checkpoint(materialized, &mut actor, &rx, tables, &mut names)
            },
        );
        // Accepted worker work stays owned while the outer startup worker holds its lease.
        let shutdown = actor.shutdown_and_join();
        let stats = replay_result?;
        shutdown?;
        self.install_recovered_tables(materialized, &stats, memtables)
    }

    fn install_recovered_tables(
        &self,
        materialized: &mut RuntimeStorageMaterialization,
        stats: &crate::wal::recovery::RecoveryStats,
        memtables: HashMap<u32, Arc<SkipListMemtable>>,
    ) -> MidgeResult<()> {
        materialized
            .state
            .check_startup_scope("cloud WAL replay installation")?;
        let runtime = &mut materialized.state;
        runtime.sequence = runtime
            .sequence
            .max(stats.max_sequence.unwrap_or(0))
            .max(self.sequence_floor);
        runtime.wal.frontiers.advance_local_to(runtime.sequence);
        runtime.compaction_output_generation = runtime
            .compaction_output_generation
            .max(runtime.sequence)
            .max(
                runtime
                    .manifest
                    .next_sst_seqs
                    .values()
                    .copied()
                    .max()
                    .unwrap_or(0),
            );
        let recovery_stats = &mut runtime.recovery_stats;
        recovery_stats.wal_recovery_records_replayed = stats.record_count;
        recovery_stats.wal_recovery_bytes_replayed = stats.bytes;
        if stats.had_corruption {
            runtime.mark_opened_in_salvage_mode();
            runtime.mark_persistence_anomaly();
        }
        for (cf_id, memtable) in memtables {
            if let Some(cf) = runtime.column_families.get_mut(&cf_id) {
                cf.memtable = memtable;
            }
        }
        runtime.total_memtable_bytes = runtime
            .column_families
            .values()
            .map(|cf| cf.memtable.size_bytes())
            .sum();
        runtime.reinitialize_active_memtable_segment_tracking();
        Ok(())
    }
}

fn checkpoint(
    materialized: &mut RuntimeStorageMaterialization,
    actor: &mut FlushActor,
    rx: &crossbeam::channel::Receiver<FlushWorkerResult>,
    tables: &mut HashMap<u32, Arc<SkipListMemtable>>,
    names: &mut names::Names,
) -> MidgeResult<()> {
    let mut families: Vec<_> = tables.keys().copied().collect();
    families.sort_unstable();
    for cf_id in families {
        materialized
            .state
            .check_startup_scope("recovery checkpoint family")?;
        let table = Arc::clone(&tables[&cf_id]);
        if table.size_bytes() == 0 {
            tables.remove(&cf_id);
            continue;
        }
        super::timing::measure("recovery_checkpoint", || {
            checkpoint_family(materialized, actor, rx, cf_id, table, names)
        })?;
        // Release replay memory only after the existing publication protocol
        // has made the new SST authoritative. Remote WAL remains retained.
        tables.remove(&cf_id);
    }
    Ok(())
}

fn checkpoint_family(
    materialized: &mut RuntimeStorageMaterialization,
    actor: &mut FlushActor,
    rx: &crossbeam::channel::Receiver<FlushWorkerResult>,
    cf_id: u32,
    table: Arc<SkipListMemtable>,
    names: &mut names::Names,
) -> MidgeResult<()> {
    materialized
        .state
        .check_startup_scope("recovery checkpoint acceptance")?;
    let sst_seq = super::timing::measure("recovery_checkpoint_reservation", || {
        names.take(cf_id, |count| {
            reserve_sst_sequence(materialized, cf_id, count)
        })
    })?;
    let state = &mut materialized.state;
    let config = &materialized.runtime_config;
    let identity = FlushIdentity {
        flush_id: state.next_flush_id,
        writer_epoch: config.writer_epoch,
        cf_id,
        sequence: 0,
    };
    state.next_flush_id = state.next_flush_id.checked_add(1).ok_or_else(|| {
        MidgeError::ResourceLimit("flush identity space exhausted during recovery".into())
    })?;
    let staging_path = state.sst_dir.join(".flush-staging").join(format!(
        "recovery-{}-{}.tmp",
        config.writer_epoch, identity.flush_id
    ));
    let completion = super::timing::measure("recovery_checkpoint_construction", || {
        actor.submit_build(identity, table, staging_path, config.hybrid_storage.clone())?;
        let FlushWorkerResult::Build(completion) =
            receive_completion(rx, state.startup_scope.as_ref(), "recovery flush build")?
        else {
            return Err(MidgeError::Internal(
                "unexpected recovery publication completion".into(),
            ));
        };
        Ok(completion)
    })?;
    let file_meta = completion.result?;
    let identity = FlushIdentity {
        sequence: file_meta.largest_seq.unwrap_or(0),
        ..identity
    };
    let accounting = state.metadata_accounting().clone();
    let medium = state.metadata_medium();
    let started = std::time::Instant::now();
    let result = publish_checkpoint_output(
        materialized,
        actor,
        rx,
        FlushBuildOutput {
            identity,
            staging_path: completion.staging_path,
            file_meta,
            reservation: completion.reservation,
        },
        sst_seq,
    );
    accounting.publication_attempt(Origin::Recovery, medium, started.elapsed(), result.is_err());
    if let Ok(bytes) = &result {
        accounting.flush_committed(Origin::Recovery, medium, *bytes, started.elapsed());
    } else if actor.is_inflight() {
        // A timed receive can return while accepted work remains owned.
        // Do not claim this prefix is a complete publication observation.
        accounting.invalidate_missing_publication_start();
    }
    result?;
    crate::failpoints::fail_point!("midge::recovery::after_checkpoint");
    Ok(())
}

fn publish_checkpoint_output(
    materialized: &mut RuntimeStorageMaterialization,
    actor: &mut FlushActor,
    rx: &crossbeam::channel::Receiver<FlushWorkerResult>,
    build: FlushBuildOutput,
    sst_seq: u64,
) -> MidgeResult<u64> {
    let state = &mut materialized.state;
    let config = &materialized.runtime_config;
    let identity = build.identity;
    let name = crate::cloud_layout::file_name(identity.cf_id, 0, sst_seq);
    let completion = super::timing::measure("recovery_checkpoint_publication", || {
        actor.submit_publish(FlushPublishTask {
            build,
            sst_name: name.clone(),
            sst_seq,
            sst_dir: state.sst_dir.clone(),
            fs: Arc::clone(&state.fs),
            storage: Arc::new(crate::runtime::actors::flush::HybridFlushStorage::new(
                config.hybrid_storage.clone(),
                config.cloud_metadata_storage.clone(),
            )),
            lease_validity: config.lease_validity.clone(),
            lease_healthy: config.lease_healthy.clone(),
            leader_store: config.leader_store.clone(),
            leader_holder_id: config.leader_holder_id.clone(),
        })?;
        let FlushWorkerResult::Publish(completion) =
            receive_completion(rx, state.startup_scope.as_ref(), "recovery flush publish")?
        else {
            return Err(MidgeError::Internal(
                "unexpected recovery build completion".into(),
            ));
        };
        Ok(completion)
    })?;
    let delta = completion.result?;
    actor.finish_pipeline();
    let cloud_metadata_published =
        commit_and_mirror_checkpoint(state, config, actor, rx, &delta, completion.reservation)?;
    super::timing::measure("recovery_checkpoint_installation", || {
        install_checkpoint_output(
            materialized,
            &delta,
            completion.reservation,
            cloud_metadata_published,
        )
    })?;
    Ok(delta.file_meta.size_bytes)
}

fn commit_and_mirror_checkpoint(
    state: &mut crate::runtime::state::RuntimeState,
    config: &crate::runtime::RuntimeConfig,
    actor: &mut FlushActor,
    rx: &crossbeam::channel::Receiver<FlushWorkerResult>,
    delta: &FlushPublicationDelta,
    reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
) -> MidgeResult<bool> {
    state.check_startup_scope("recovery checkpoint commit")?;
    validate_lease(config)?;
    state.record_flush_publication_intent(
        delta.identity.cf_id,
        delta.identity.sequence,
        &delta.file_meta,
    )?;
    state.check_startup_scope("recovery checkpoint manifest publication")?;
    validate_monotonic_lease(config)?;
    state.commit_flush_publication_for(FlushManifestPublication {
        origin: Origin::Recovery,
        cf_id: delta.identity.cf_id,
        sequence: delta.identity.sequence,
        file_meta: &delta.file_meta,
        next_sst_seq: delta.next_sst_seq,
        require_snapshot: config.hybrid_storage.is_some(),
    })?;
    actor.submit_mirror(FlushMirrorTask {
        delta: delta.clone(),
        reservation,
        fs: Arc::clone(&state.fs),
        storage: Arc::new(crate::runtime::actors::flush::HybridFlushStorage::new(
            config.hybrid_storage.clone(),
            config.cloud_metadata_storage.clone(),
        )),
        metadata_publication_lock: config.metadata_publication_lock.clone(),
        lease_validity: config.lease_validity.clone(),
        lease_healthy: config.lease_healthy.clone(),
        leader_store: config.leader_store.clone(),
        leader_holder_id: config.leader_holder_id.clone(),
        manifest_sequence: state.manifest.last_persisted_sequence,
        runtime_response_timeout: state
            .startup_scope
            .as_ref()
            .map_or(config.runtime_response_timeout, |scope| {
                scope.clamp(config.runtime_response_timeout)
            }),
    })?;
    let FlushWorkerResult::Mirror(mirror) =
        receive_completion(rx, state.startup_scope.as_ref(), "recovery flush mirror")?
    else {
        return Err(MidgeError::Internal(
            "unexpected recovery mirror completion".into(),
        ));
    };
    let cloud_metadata_published = mirror.result?;
    actor.finish_pipeline();
    Ok(cloud_metadata_published)
}

fn reserve_sst_sequence(
    materialized: &mut RuntimeStorageMaterialization,
    cf_id: u32,
    count: u64,
) -> MidgeResult<std::ops::Range<u64>> {
    materialized
        .state
        .check_startup_scope("recovery SST name reservation")?;
    validate_lease(&materialized.runtime_config)?;
    let state = &mut materialized.state;
    let config = &materialized.runtime_config;
    let sst_seq = state
        .manifest
        .next_sst_seqs
        .get(&cf_id)
        .copied()
        .unwrap_or(1);
    let next_seq = sst_seq.checked_add(count).ok_or_else(|| {
        MidgeError::ResourceLimit("SST sequence space exhausted during recovery".into())
    })?;
    // Reserve the immutable object name durably before any upload. A restart
    // must never reuse an orphan's name for a different replay partition.
    state.manifest.set_next_sst_seq(cf_id, next_seq);
    crate::failpoints::fail_point!("midge::recovery::before_name_reservation");
    let checkpoint = state
        .manifest_store
        .save_snapshot_for(Origin::Recovery, &state.manifest)?;
    state.manifest.adopt_checkpoint(checkpoint);
    if let Some(cloud) = &materialized.cloud_metadata_storage_for_mirror {
        validate_lease(config)?;
        super::CloudStartupRecovery::mirror_cloud_metadata_within(
            cloud,
            &state.db_path,
            crate::config::RecoveryPolicy::Strict,
            crate::runtime::hybrid_persistence::CloudMetadataMirrorAuthority {
                store: config.leader_store.as_deref().ok_or_else(|| {
                    MidgeError::Internal(
                        "cloud metadata publication requires a leader store".into(),
                    )
                })?,
                holder_id: config.leader_holder_id.as_deref().ok_or_else(|| {
                    MidgeError::Internal(
                        "cloud metadata publication requires a lease holder".into(),
                    )
                })?,
                writer_epoch: config.writer_epoch,
            },
            &config.metadata_publication_lock,
            |_| validate_lease(config),
            &state.startup_scope.clone().unwrap_or_else(|| {
                crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded())
            }),
        )?;
        validate_lease(config)?;
    }
    crate::failpoints::fail_point!("midge::recovery::after_name_reservation");
    tracing::info!(target: "midge::recovery", phase = "name_reservation", reserved_names = count,
        "recovery SST names reserved durably");
    Ok(sst_seq..next_seq)
}

fn install_checkpoint_output(
    materialized: &mut RuntimeStorageMaterialization,
    delta: &crate::runtime::actors::flush::FlushPublicationDelta,
    reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    cloud_metadata_published: bool,
) -> MidgeResult<()> {
    materialized
        .state
        .check_startup_scope("recovery checkpoint installation")?;
    let state = &mut materialized.state;
    let config = &materialized.runtime_config;
    let name = &delta.file_meta.name;
    if let Some(storage) = &config.hybrid_storage {
        if let Some(token) = reservation {
            storage.flush_completed_with_token(token, delta.file_meta.size_bytes);
        }
        if cloud_metadata_published {
            std::fs::remove_file(state.sst_dir.join(name))?;
            storage.evict_local_object_cache(&crate::cloud_layout::object_key(name))?;
            storage.reconcile_local_disk_usage(
                super::RuntimeRecoveryMaterialization::local_directory_bytes_within(
                    &state.sst_dir,
                    state.startup_scope.as_ref(),
                )?
                .saturating_add(
                    super::RuntimeRecoveryMaterialization::local_directory_bytes_within(
                        &state.db_path.join("hybrid_local/sst"),
                        state.startup_scope.as_ref(),
                    )?,
                ),
                super::RuntimeRecoveryMaterialization::local_directory_bytes_within(
                    &state.wal_dir,
                    state.startup_scope.as_ref(),
                )?
                .saturating_add(
                    super::RuntimeRecoveryMaterialization::local_directory_bytes_within(
                        &state.db_path.join("hybrid_local/wal"),
                        state.startup_scope.as_ref(),
                    )?,
                ),
            );
        }
    }
    Ok(())
}

pub(super) fn validate_lease(config: &crate::runtime::RuntimeConfig) -> MidgeResult<()> {
    if let Some(scope) = &config.startup_scope {
        scope.check("recovery lease validation")?;
    }
    validate_monotonic_lease(config)?;
    if config
        .lease_healthy
        .as_ref()
        .is_some_and(|health| !health.load(std::sync::atomic::Ordering::Acquire))
    {
        return Err(MidgeError::Fenced(
            "lease lost during cloud WAL recovery".into(),
        ));
    }
    if let Some(store) = &config.leader_store {
        store
            .validate_epoch(
                config.leader_holder_id.as_deref().unwrap_or_default(),
                config.writer_epoch,
            )
            .map_err(|error| error.into_validation_error("cloud WAL recovery"))?;
    }
    if let Some(scope) = &config.startup_scope {
        scope.check("recovery lease validation completion")?;
    }
    validate_monotonic_lease(config)
}

fn validate_monotonic_lease(config: &crate::runtime::RuntimeConfig) -> MidgeResult<()> {
    if let Some(validity) = &config.lease_validity {
        validity.remaining(config.writer_epoch).map_err(|error| {
            error.into_validation_error("cloud WAL recovery monotonic lease validity")
        })?;
    }
    Ok(())
}

fn receive_completion(
    rx: &crossbeam::channel::Receiver<FlushWorkerResult>,
    scope: Option<&crate::common::DeadlineScope>,
    context: &str,
) -> MidgeResult<FlushWorkerResult> {
    if let Some(scope) = scope {
        scope.check(context)?;
    }
    let result = match scope.filter(|scope| scope.deadline().is_bounded()) {
        Some(scope) => rx
            .recv_timeout(scope.deadline().remaining())
            .map_err(|error| match error {
                crossbeam::channel::RecvTimeoutError::Timeout => {
                    MidgeError::Timeout(format!("{context} timed out"))
                }
                crossbeam::channel::RecvTimeoutError::Disconnected => {
                    MidgeError::Internal(format!("{context} disconnected"))
                }
            }),
        None => rx
            .recv()
            .map_err(|error| MidgeError::Internal(format!("{context}: {error}"))),
    };
    if let Some(scope) = scope {
        scope.check(context)?;
    }
    result
}
