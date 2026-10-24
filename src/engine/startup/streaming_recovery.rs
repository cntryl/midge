//! Incremental cloud WAL replay through the engine's durable flush protocol.

use super::RuntimeStorageMaterialization;
use crate::common::{MidgeError, MidgeResult};
use crate::io::ReadFs;
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

mod names;
#[cfg(test)]
mod tests;

pub(super) struct CloudReplay {
    pub fs: Arc<dyn ReadFs>,
    /// The planned WAL files in `fs`, in replay order.
    pub segments: crate::wal::recovery::ReplaySegments,
    pub limits: StreamingReplayLimits,
    /// Sequences at or below this belong to WAL salvage set aside unreplayed.
    pub sequence_floor: u64,
    pub scope: Option<crate::common::DeadlineScope>,
    pub substituted_wal: Option<SubstitutedWal>,
}

/// A damaged catalog suffix replayed through verified local members. Its
/// original immutable keys must never be uploaded again.
pub(super) struct SubstitutedWal {
    entries: Vec<crate::wal::cloud_catalog::CatalogEntry>,
    anchor: crate::wal::format::ChainLink,
}

impl SubstitutedWal {
    pub(super) fn from_plan(
        plan: &crate::wal::planner::RecoveryPlan,
        catalog: &crate::wal::cloud_catalog::WalPublicationCatalog,
    ) -> Option<Self> {
        let first = plan
            .local_segments
            .keys()
            .filter(|id| plan.retire_catalog_from.is_none_or(|from| **id < from))
            .find_map(|id| catalog.entries().iter().position(|entry| entry.id() == *id))?;
        let end = catalog
            .entries()
            .iter()
            .position(|entry| plan.retire_catalog_from == Some(entry.id()))
            .unwrap_or(catalog.entries().len());
        if first >= end {
            return None;
        }
        Some(Self {
            entries: catalog.entries()[first..end].to_vec(),
            anchor: catalog.entries()[first].prev(),
        })
    }
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
        let frontiers = materialized.state.manifest.flush_frontiers().clone();
        let scope = self
            .scope
            .clone()
            .or_else(|| materialized.state.startup_scope.clone());
        let (tx, rx) = crossbeam::channel::bounded(1);
        let mut actor = FlushActor::new_with_memory_limit(
            Arc::clone(&materialized.state.fs),
            false,
            materialized.runtime_config.compression_policy.clone(),
            tx,
            materialized.runtime_config.flush_memory_limit,
        )?;
        let mut memtables = HashMap::new();
        let mut names = names::Names::new(self.limits);
        let replay_result = replay_wal_with_options(
            self.fs.as_ref(),
            &self.segments,
            &mut memtables,
            policy,
            self.limits,
            ReplayOptions {
                scope: scope.as_ref(),
                ..ReplayOptions::new(crate::wal::replay::ReplaySelection::Frontiers(&frontiers))
            },
            &mut |tables, boundary| {
                checkpoint(
                    materialized,
                    &mut actor,
                    &rx,
                    tables,
                    &mut names,
                    boundary.through_sequence,
                )
            },
        );
        // Accepted worker work stays owned while the outer startup worker holds its lease.
        let recovery = (|| {
            let stats = replay_result?;
            // Planning sets aside what it finds unreadable and covers it with
            // the sequence floor. A break only replay sees has neither, so
            // do not seal and upload what follows it (#821).
            if let Some(stop) = &stats.salvage_stop {
                return Err(crate::common::MidgeError::RecoveryFailed(format!(
                    "cloud WAL replay stopped at segment {} after planning; salvage cannot set it aside",
                    stop.segment
                )));
            }
            if let Some(substitute) = &self.substituted_wal {
                let boundary = stats.max_sequence.unwrap_or(0).max(self.sequence_floor);
                checkpoint(
                    materialized,
                    &mut actor,
                    &rx,
                    &mut memtables,
                    &mut names,
                    boundary,
                )?;
                crate::failpoints::fail_point!(
                    "midge::startup::substituted_wal_after_checkpoint_before_catalog_retire"
                );
                substitute.retire_after_checkpoint(materialized, boundary)?;
            }
            Ok(stats)
        })();
        #[cfg(test)]
        super::test_control::actor_join_started();
        let shutdown = actor.shutdown_and_join();
        let stats = recovery?;
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
        runtime.raise_sequence_to(stats.max_sequence.unwrap_or(0).max(self.sequence_floor));
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

impl SubstitutedWal {
    /// Publish recovered rows and frontiers first; only then withdraw the
    /// damaged suffix in a fenced CAS and replace local history. A crash at
    /// any boundary retains either the original local WAL or authoritative SSTs.
    fn retire_after_checkpoint(
        &self,
        materialized: &mut RuntimeStorageMaterialization,
        floor: u64,
    ) -> MidgeResult<()> {
        let config = &materialized.runtime_config;
        let storage = config
            .hybrid_storage
            .as_ref()
            .ok_or_else(|| MidgeError::Internal("substituted WAL without cloud storage".into()))?;
        let scope = materialized.state.startup_scope.clone().unwrap_or_else(|| {
            crate::common::DeadlineScope::new(crate::common::OperationDeadline::unbounded())
        });
        let first = self
            .entries
            .first()
            .expect("substituted suffix is nonempty")
            .id();
        crate::runtime::hybrid_persistence::CloudPersistence::new(Arc::clone(storage))
            .set_aside_catalog_suffix_with_authority_within(
                config.writer_epoch,
                Some(first),
                &self.entries,
                floor,
                &|| validate_lease(config),
                &scope.deadline(),
            )?;
        crate::failpoints::fail_point!(
            "midge::startup::substituted_wal_after_catalog_retire_before_local_rebase"
        );
        scope.check("substituted WAL local history")?;
        let history = &mut materialized.state.wal.history;
        let floor = floor.max(history.ledger().seq_reserved_through());
        if floor > history.ledger().seq_reserved_through() {
            history.reserve(floor)?;
        }
        history.rebase(self.anchor, history.ledger().next_segment_id())?;
        let config = &mut materialized.runtime_config;
        config
            .recovered_cloud_wal_entries
            .retain(|id, _| *id < first);
        config
            .recovered_cloud_wal_segments
            .retain(|id, _| *id < first);
        config
            .recovered_cloud_wal_segment_epochs
            .retain(|id, _| *id < first);
        config.recovered_local_wal_segments.clear();
        config.recovered_local_wal_segment_epochs.clear();
        Ok(())
    }
}

fn checkpoint(
    materialized: &mut RuntimeStorageMaterialization,
    actor: &mut FlushActor,
    rx: &crossbeam::channel::Receiver<FlushWorkerResult>,
    tables: &mut HashMap<u32, Arc<SkipListMemtable>>,
    names: &mut names::Names,
    through_sequence: u64,
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
            checkpoint_family(
                materialized,
                actor,
                rx,
                cf_id,
                table,
                names,
                through_sequence,
            )
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
    through_sequence: u64,
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
        sequence: through_sequence,
    };
    state.next_flush_id = state.next_flush_id.checked_add(1).ok_or_else(|| {
        MidgeError::ResourceLimit("flush identity space exhausted during recovery".into())
    })?;
    let staging_path = std::path::Path::new(crate::sst::fs::FLUSH_STAGING_DIR).join(format!(
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
        #[cfg(test)]
        super::test_control::before_publish_receive();
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
    actor.finish_pipeline(completion.identity.flush_id);
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
    actor.finish_pipeline(mirror.delta.identity.flush_id);
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
            &state.fs,
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
            state
                .fs
                .remove_file(&crate::io::FsPath::new(crate::cloud_layout::object_key(
                    name,
                )))
                .map_err(crate::io::FsError::into_midge)?;
            let (sst_bytes, wal_bytes) = local_storage_bytes(state)?;
            storage.reconcile_local_disk_usage(sst_bytes, wal_bytes);
        }
    }
    Ok(())
}

/// Local SST and WAL bytes below the database root.
pub(super) fn local_storage_bytes(state: &crate::runtime::RuntimeState) -> MidgeResult<(u64, u64)> {
    let bytes = |directory: &str| {
        super::RuntimeRecoveryMaterialization::local_directory_bytes_within(
            state.fs.as_ref(),
            directory,
            state.startup_scope.as_ref(),
        )
    };
    Ok((bytes(crate::sst::fs::SST_DIR)?, bytes(crate::wal::WAL_DIR)?))
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
