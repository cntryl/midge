//! Incremental cloud WAL replay through the engine's durable flush protocol.

use super::RuntimeStorageMaterialization;
use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::runtime::actors::flush::{
    FlushActor, FlushBuildOutput, FlushIdentity, FlushPublishTask, FlushWorkerResult,
};
use crate::sst::{Memtable, SkipListMemtable};
use crate::wal::recovery::streaming::{replay_wal_with_checkpoint, StreamingReplayLimits};
use std::collections::HashMap;
use std::sync::Arc;

mod coverage;
mod names;
#[cfg(test)]
mod tests;

pub(super) struct CloudReplay {
    pub fs: Arc<dyn Fs>,
    pub limits: StreamingReplayLimits,
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
            .saturating_add(crate::sst::size_bound::FIXED_SST_BYTES)
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
        let should_apply = |record: &crate::wal::WalRecord| {
            known_cfs.contains(&record.cf_id) && !coverage.contains(record)
        };
        let (tx, rx) = crossbeam::channel::bounded(1);
        let mut actor = FlushActor::new(
            &materialized.state.sst_dir,
            false,
            materialized.runtime_config.compression_policy.clone(),
            tx,
        )?;
        let mut memtables = HashMap::new();
        let mut names = names::Names::new(self.limits);
        let replay_result = replay_wal_with_checkpoint(
            self.fs.as_ref(),
            &FsPath::new("wal"),
            &mut memtables,
            policy,
            Some(&should_apply),
            self.limits,
            &mut |tables, _stats| {
                coverage.release_reader();
                checkpoint(materialized, &mut actor, &rx, tables, &mut names)
            },
        );
        let shutdown = actor.shutdown_and_join();
        let stats = replay_result?;
        shutdown?;
        materialized.state.sequence = materialized
            .state
            .sequence
            .max(stats.max_sequence.unwrap_or(0));
        materialized.state.wal.local_durable_seq = materialized.state.sequence;
        materialized.state.compaction_output_generation = materialized
            .state
            .compaction_output_generation
            .max(materialized.state.sequence)
            .max(
                materialized
                    .state
                    .manifest
                    .next_sst_seqs
                    .values()
                    .copied()
                    .max()
                    .unwrap_or(0),
            );
        materialized.state.wal_recovery_records_replayed = stats.record_count;
        materialized.state.wal_recovery_bytes_replayed = stats.bytes;
        if stats.had_corruption {
            materialized.state.mark_opened_in_salvage_mode();
            materialized.state.mark_persistence_anomaly();
        }
        for (cf_id, memtable) in memtables {
            if let Some(cf) = materialized.state.column_families.get_mut(&cf_id) {
                cf.memtable = memtable;
            }
        }
        materialized.state.total_memtable_bytes = materialized
            .state
            .column_families
            .values()
            .map(|cf| cf.memtable.size_bytes())
            .sum();
        materialized
            .state
            .reinitialize_active_memtable_segment_tracking();
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
        let FlushWorkerResult::Build(completion) = rx
            .recv()
            .map_err(|error| MidgeError::Internal(format!("recovery flush build: {error}")))?
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
    let name = crate::sst::file_name(cf_id, 0, sst_seq);
    let completion = super::timing::measure("recovery_checkpoint_publication", || {
        actor.submit_publish(FlushPublishTask {
            build: FlushBuildOutput {
                identity,
                staging_path: completion.staging_path,
                file_meta,
                reservation: completion.reservation,
            },
            sst_name: name.clone(),
            sst_seq,
            db_path: state.db_path.clone(),
            sst_dir: state.sst_dir.clone(),
            fs: Arc::clone(&state.fs),
            recovery_policy: state.recovery_policy(),
            hybrid_storage: config.hybrid_storage.clone(),
            cloud_metadata_storage: config.cloud_metadata_storage.clone(),
            lease_healthy: config.lease_healthy.clone(),
            leader_store: config.leader_store.clone(),
            leader_holder_id: config.leader_holder_id.clone(),
        })?;
        let FlushWorkerResult::Publish(completion) = rx
            .recv()
            .map_err(|error| MidgeError::Internal(format!("recovery flush publish: {error}")))?
        else {
            return Err(MidgeError::Internal(
                "unexpected recovery build completion".into(),
            ));
        };
        Ok(completion)
    })?;
    let delta = completion.result?;
    actor.finish_pipeline();
    super::timing::measure("recovery_checkpoint_installation", || {
        install_checkpoint_output(materialized, &delta, completion.reservation)
    })?;
    crate::failpoints::fail_point!("midge::recovery::after_checkpoint");
    Ok(())
}

fn reserve_sst_sequence(
    materialized: &mut RuntimeStorageMaterialization,
    cf_id: u32,
    count: u64,
) -> MidgeResult<std::ops::Range<u64>> {
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
    state.manifest.next_sst_seqs.insert(cf_id, next_seq);
    crate::failpoints::fail_point!("midge::recovery::before_name_reservation");
    crate::metadata::ManifestPersistence::save_snapshot_and_truncate_journal(
        &state.db_path,
        &state.manifest,
    )
    .map_err(MidgeError::Internal)?;
    if let Some(cloud) = &materialized.cloud_metadata_storage_for_mirror {
        validate_lease(config)?;
        super::CloudStartupRecovery::mirror_cloud_metadata(
            cloud,
            &state.db_path,
            crate::config::RecoveryPolicy::Strict,
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
) -> MidgeResult<()> {
    let state = &mut materialized.state;
    let config = &materialized.runtime_config;
    let name = &delta.file_meta.name;
    state.manifest = crate::metadata::ManifestPersistence::load_with_fs_and_policy(
        &state.fs,
        state.recovery_policy(),
    )
    .map_err(MidgeError::Internal)?;
    if delta.persistence_anomaly {
        state.mark_persistence_anomaly();
    }
    if let Some(storage) = &config.hybrid_storage {
        if let Some(token) = reservation {
            storage.flush_completed_with_token(token, delta.file_meta.size_bytes);
        }
        if delta.cloud_metadata_published {
            std::fs::remove_file(state.sst_dir.join(name))?;
            storage.evict_local_object_cache(&crate::sst::object_key(name))?;
            storage.reconcile_local_disk_usage(
                super::RuntimeRecoveryMaterialization::local_directory_bytes(&state.sst_dir)?
                    .saturating_add(
                        super::RuntimeRecoveryMaterialization::local_directory_bytes(
                            &state.db_path.join("hybrid_local/sst"),
                        )?,
                    ),
                super::RuntimeRecoveryMaterialization::local_directory_bytes(&state.wal_dir)?
                    .saturating_add(
                        super::RuntimeRecoveryMaterialization::local_directory_bytes(
                            &state.db_path.join("hybrid_local/wal"),
                        )?,
                    ),
            );
        }
    }
    Ok(())
}

fn validate_lease(config: &crate::runtime::RuntimeConfig) -> MidgeResult<()> {
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
            .map_err(|error| MidgeError::Fenced(error.to_string()))?;
    }
    Ok(())
}
