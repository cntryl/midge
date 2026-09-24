use super::EventLoop;
use crate::runtime::actors::flush::{
    FlushBuildCompletion, FlushBuildOutput, FlushIdentity, FlushPublicationDelta,
    FlushPublishCompletion, FlushPublishTask, FlushWorkerResult,
};
use crate::runtime::state::{ImmutableFlush, ImmutableFlushPhase};
use crate::runtime::RuntimeResponse;
use std::sync::Arc;
use std::time::Duration;

/// What one local WAL prune pass did.
#[derive(Debug, Default)]
struct LocalWalPruneOutcome {
    removed: Vec<u64>,
    anomaly: bool,
    lease_lost: bool,
}

#[cfg(test)]
thread_local! {
    /// Whole-file reads of local WAL segments, for tests that bound a prune pass.
    static RUNTIME_FILE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl EventLoop {
    pub(super) fn column_family_flush_pipeline_active(&self, cf_id: u32) -> bool {
        self.state
            .get_cf(cf_id)
            .is_some_and(|cf| !cf.immutable_flushes.is_empty())
    }

    pub(super) fn column_family_publication_pipeline_active(&self, cf_id: u32) -> bool {
        if self.column_family_flush_pipeline_active(cf_id) {
            return true;
        }

        // Compactions use one global worker slot. Until its completion message
        // has performed the manifest authority switch, a CF drop cannot know
        // whether the worker's replacement belongs in its reclamation set.
        // Deferring all drops during that bounded window is conservative and
        // avoids relying on mutable input metadata for CF attribution.
        self.state
            .active_compactions
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
            || !self.state.compaction.compacting_ssts.is_empty()
    }

    pub(super) fn freeze_active_memtable(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> crate::common::MidgeResult<Option<u64>> {
        let active_size = self
            .state
            .get_cf(cf_id)
            .ok_or_else(|| {
                crate::common::MidgeError::InvalidArgument(format!(
                    "column family {cf_id} does not exist"
                ))
            })?
            .memtable
            .size_bytes();
        if active_size == 0 {
            return Ok(None);
        }
        if self.state.is_immutable_memtable_queue_full(cf_id) {
            self.state.diagnostics.record(|m| {
                m.record_write_stall_memory();
            });
            self.refresh_write_stall_timing();
            return Err(crate::common::MidgeError::WriteStall(format!(
                "immutable memtable queue is full for column family {cf_id}"
            )));
        }

        let current_segment_id = self.state.wal.current_segment_id;

        let appended_bytes = self.state.wal.appended_bytes;
        let sequence = self.state.sequence;
        let frozen = {
            let cf = self.state.get_cf(cf_id).ok_or_else(|| {
                crate::common::MidgeError::Internal(format!(
                    "column family {cf_id} disappeared during freeze"
                ))
            })?;
            Arc::clone(&cf.memtable)
        };
        let flush = self
            .state
            .track_new_immutable_flush(cf_id, Arc::clone(&frozen), sequence)
            .ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit(
                    "flush identity space exhausted".to_string(),
                )
            })?;
        // Publish immutable ownership before replacing the active generation.
        // Exhausted flush identities must leave accepted writes readable.
        let cf = self
            .state
            .get_cf_mut(cf_id)
            .expect("tracked flush family exists");
        cf.memtable = Arc::new(crate::memtable::SkipListMemtable::new());
        cf.active_memtable_started_in_segment = current_segment_id;
        cf.active_memtable_started_at_wal_bytes = appended_bytes;
        crate::failpoints::fail_point!("midge::flush_worker::after_freeze");
        self.publish_snapshot();
        Ok(Some(flush.flush_id))
    }

    pub(super) fn schedule_next_flush_worker(&mut self) {
        self.schedule_next_flush_worker_with_shutdown(false);
    }

    pub(super) fn schedule_next_flush_worker_during_shutdown(&mut self) {
        self.schedule_next_flush_worker_with_shutdown(true);
    }

    /// Whether a queued flush cannot start right now. The run loop uses the
    /// same condition, so a queued flush it cannot start does not count as
    /// actionable work and spin the loop.
    pub(super) fn flush_start_blocked(&self, allow_during_shutdown: bool) -> bool {
        (self.shutting_down && !allow_during_shutdown)
            || self.state.is_memory_mode()
            || self.flush_actor.is_inflight()
            || self.publication_gate.active
            || (!allow_during_shutdown && self.pending_msg.is_some())
    }

    fn schedule_next_flush_worker_with_shutdown(&mut self, allow_during_shutdown: bool) {
        if !allow_during_shutdown
            && self.cloud_maintenance_enabled()
            && !self.cloud_maintenance.dispatching
        {
            self.schedule_cloud_maintenance();
            return;
        }
        if self.flush_start_blocked(allow_during_shutdown) {
            return;
        }
        let Some(flush) = self.state.begin_next_immutable_flush() else {
            return;
        };
        if let Some(mut build) = flush.built.clone() {
            if build.reservation.is_none() {
                let Some((cf_id, _)) = self.state.immutable_flush_by_id(flush.flush_id) else {
                    return;
                };
                let reservation = match crate::runtime::actors::flush::FlushActor::reserve_flush(
                    self.hybrid_storage.as_ref(),
                    cf_id,
                    build.file_meta.size_bytes.saturating_mul(2),
                ) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        self.fail_flush_pipeline(flush.flush_id, None, &error, true);
                        return;
                    }
                };
                build.reservation = reservation;
                if let Some((_, current)) = self.state.immutable_flush_by_id_mut(flush.flush_id) {
                    current.built = Some(build.clone());
                }
            }
            self.schedule_flush_publication(&flush, build);
            return;
        }

        let Some((cf_id, _)) = self.state.immutable_flush_by_id(flush.flush_id) else {
            return;
        };
        let identity = FlushIdentity {
            flush_id: flush.flush_id,
            writer_epoch: flush.writer_epoch,
            cf_id,
            sequence: flush.sequence,
        };
        let staging_path = self.state.sst_dir.join(".flush-staging").join(format!(
            "{}-{}.sst",
            identity.writer_epoch, identity.flush_id
        ));
        if let Err(error) = self.flush_actor.submit_build(
            identity,
            Arc::clone(&flush.memtable),
            staging_path,
            self.hybrid_storage.clone(),
        ) {
            self.fail_flush_pipeline(flush.flush_id, None, &error, false);
        }
    }

    fn schedule_flush_publication(&mut self, flush: &ImmutableFlush, build: FlushBuildOutput) {
        let (Some(sst_name), Some(sst_seq)) = (flush.sst_name.clone(), flush.sst_seq) else {
            self.fail_flush_pipeline(
                flush.flush_id,
                build.reservation,
                &crate::common::MidgeError::Internal(
                    "built flush has no canonical SST identity".to_string(),
                ),
                false,
            );
            return;
        };
        // The worker's result is installed onto the in-memory manifest, so
        // memory must be current before another publication starts (#500).
        if let Err(error) = self.state.retry_metadata_reload() {
            self.fail_flush_pipeline(flush.flush_id, build.reservation, &error, false);
            return;
        }
        self.publication_gate.active = true;
        let task = FlushPublishTask {
            build,
            sst_name,
            sst_seq,
            sst_dir: self.state.sst_dir.clone(),
            fs: Arc::clone(&self.state.fs),
            manifest_store: Arc::clone(&self.state.manifest_store),
            hybrid_storage: self.hybrid_storage.clone(),
            cloud_metadata_storage: self.cloud_metadata_storage.clone(),
            metadata_publication_lock: self.metadata_publication_lock.clone(),
            lease_healthy: self.fencing.lease_healthy.clone(),
            leader_store: self.fencing.leader_store.clone(),
            leader_holder_id: self.fencing.leader_holder_id.clone(),
        };
        if let Err(error) = self.flush_actor.submit_publish(task) {
            self.publication_gate.active = false;
            self.fail_flush_pipeline(flush.flush_id, None, &error, true);
        }
    }

    pub(super) fn handle_flush_worker_result(&mut self, result: FlushWorkerResult) {
        let should_continue = match result {
            FlushWorkerResult::Build(completion) => self.handle_flush_build_completion(completion),
            FlushWorkerResult::Publish(completion) => {
                self.handle_flush_publish_completion(completion)
            }
        };
        if !should_continue {
            return;
        }
        self.restore_publication_deferred_message();
        self.schedule_next_flush_worker();
    }

    fn handle_flush_build_completion(&mut self, mut completion: FlushBuildCompletion) -> bool {
        self.state.flush_metrics.build_count =
            self.state.flush_metrics.build_count.saturating_add(1);
        self.state.flush_metrics.build_ns_total = self
            .state
            .flush_metrics
            .build_ns_total
            .saturating_add(completion.build_ns);
        self.state.flush_metrics.build_ns_max = self
            .state
            .flush_metrics
            .build_ns_max
            .max(completion.build_ns);

        let same_immutable = self
            .state
            .immutable_flush_by_id(completion.identity.flush_id)
            .is_some_and(|(_, flush)| Arc::ptr_eq(&flush.memtable, &completion.memtable));
        if !same_immutable {
            self.cleanup_failed_flush_build(&mut completion);
            let error = crate::common::MidgeError::Fenced(format!(
                "flush {} build completion no longer owns its immutable",
                completion.identity.flush_id
            ));
            self.fail_flush_pipeline(
                completion.identity.flush_id,
                completion.reservation,
                &error,
                false,
            );
            return false;
        }
        if let Err(error) = self.validate_flush_completion(completion.identity) {
            self.cleanup_failed_flush_build(&mut completion);
            self.fail_flush_pipeline(
                completion.identity.flush_id,
                completion.reservation,
                &error,
                false,
            );
            return false;
        }
        if completion.result.is_err() {
            self.cleanup_failed_flush_build(&mut completion);
        }
        match completion.result {
            Ok(file_meta) => self.prepare_flush_publication(
                completion.identity,
                completion.staging_path,
                completion.reservation,
                file_meta,
            ),
            Err(error) => self.fail_flush_pipeline(
                completion.identity.flush_id,
                completion.reservation,
                &error,
                false,
            ),
        }
        true
    }

    fn cleanup_failed_flush_build(&self, completion: &mut FlushBuildCompletion) {
        let mut temp = completion.staging_path.as_os_str().to_os_string();
        temp.push(".tmp");
        for path in [&completion.staging_path, &std::path::PathBuf::from(temp)] {
            let Ok(path) = self.runtime_fs_path(path) else {
                tracing::warn!(
                    ?path,
                    "retaining failed flush outside the runtime filesystem"
                );
                completion.reservation = None;
                continue;
            };
            match self.state.fs.remove_file(&path) {
                Ok(()) | Err(crate::io::FsError::NotFound(_)) => {}
                Err(error) => {
                    // Keep the admission charged until startup can reconcile
                    // the residue. Failed deletion never returns capacity.
                    tracing::warn!(?path, %error, "retaining failed flush disk reservation");
                    completion.reservation = None;
                }
            }
        }
    }

    fn runtime_fs_path(
        &self,
        path: &std::path::Path,
    ) -> crate::common::MidgeResult<crate::io::FsPath> {
        let relative = path
            .strip_prefix(&self.state.db_path)
            .map_err(|_| crate::common::MidgeError::InvalidPath)?;
        let relative = relative
            .to_str()
            .ok_or(crate::common::MidgeError::InvalidPath)?;
        Ok(crate::io::FsPath::new(relative))
    }

    /// Picks the SST sequence for a flush output and, when the output can
    /// reach remote storage before it is published, makes the name durable.
    fn reserve_flush_sst_seq(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> crate::common::MidgeResult<u64> {
        let durable_next = self
            .state
            .manifest
            .next_sst_seqs
            .get(&cf_id)
            .copied()
            .unwrap_or(1);
        if self.hybrid_storage.is_some() {
            // The publish worker uploads before it journals AddSst, so the
            // name must already be durable (as compaction does) or a crash in
            // that window leaves an orphan whose name the next flush reuses.
            // Names come from a cursor inside a durably reserved block, so
            // most flushes reserve nothing (#491). A restart resumes at the
            // durable reservation, past every name this session handed out.
            let cursor = self
                .state
                .sst_names
                .cursor
                .entry(cf_id)
                .or_insert(durable_next);
            let sst_seq = *cursor;
            *cursor = sst_seq.checked_add(1).ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit("SST filename allocation exhausted".into())
            })?;
            self.reserve_sst_name_durably(cf_id, sst_seq)?;
            return Ok(sst_seq);
        }
        let sst_seq = durable_next;
        let next_sst_seq = sst_seq.saturating_add(1);
        self.state
            .manifest
            .next_sst_seqs
            .entry(cf_id)
            .and_modify(|next| *next = (*next).max(next_sst_seq))
            .or_insert(next_sst_seq);
        Ok(sst_seq)
    }

    fn prepare_flush_publication(
        &mut self,
        identity: FlushIdentity,
        staging_path: std::path::PathBuf,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
        file_meta: crate::runtime::FileMeta,
    ) {
        let sst_seq = match self.reserve_flush_sst_seq(identity.cf_id) {
            Ok(sst_seq) => sst_seq,
            Err(error) => {
                self.fail_flush_pipeline(identity.flush_id, reservation, &error, true);
                return;
            }
        };
        let sst_name = crate::cloud_layout::file_name(identity.cf_id, 0, sst_seq);
        let build = FlushBuildOutput {
            identity,
            staging_path,
            file_meta,
            reservation,
        };
        let flush = {
            let Some((_, flush)) = self.state.immutable_flush_by_id_mut(identity.flush_id) else {
                let error = crate::common::MidgeError::Fenced(format!(
                    "flush {} lost immutable ownership before publication",
                    identity.flush_id
                ));
                self.fail_flush_pipeline(identity.flush_id, reservation, &error, false);
                return;
            };
            flush.sst_name = Some(sst_name);
            flush.sst_seq = Some(sst_seq);
            flush.built = Some(build.clone());
            flush.phase = ImmutableFlushPhase::Built;
            flush.clone()
        };
        if let Some((_, current)) = self.state.immutable_flush_by_id_mut(identity.flush_id) {
            current.phase = ImmutableFlushPhase::Publishing;
        }
        self.schedule_flush_publication(&flush, build);
    }

    fn handle_flush_publish_completion(&mut self, completion: FlushPublishCompletion) -> bool {
        self.state.flush_metrics.publish_count =
            self.state.flush_metrics.publish_count.saturating_add(1);
        self.state.flush_metrics.publish_ns_total = self
            .state
            .flush_metrics
            .publish_ns_total
            .saturating_add(completion.publish_ns);
        self.state.flush_metrics.publish_ns_max = self
            .state
            .flush_metrics
            .publish_ns_max
            .max(completion.publish_ns);
        self.publication_gate.active = false;

        if let Err(error) = self.validate_flush_completion(completion.identity) {
            // Busy and Timeout both mean the store did not answer: authority
            // is unknown, not lost.
            if matches!(
                error,
                crate::common::MidgeError::Busy(_) | crate::common::MidgeError::Timeout(_)
            ) && self
                .state
                .immutable_flush_by_id(completion.identity.flush_id)
                .is_some()
            {
                // Validation could not complete, but this runtime still owns
                // the flush. The worker may already have published it, so
                // retry; the worker reconciles an already-published output.
                self.fail_flush_pipeline(
                    completion.identity.flush_id,
                    completion.reservation,
                    &error,
                    true,
                );
                return true;
            }
            self.settle_stale_publish_reservation(&completion);
            self.flush_actor.finish_pipeline();
            self.fail_flush_waiters(
                completion.identity.cf_id,
                completion.identity.sequence,
                &error,
            );
            return false;
        }
        match completion.result {
            Ok(delta) => self.install_flush_publication(&delta, completion.reservation),
            Err(error) => self.fail_flush_pipeline(
                completion.identity.flush_id,
                completion.reservation,
                &error,
                true,
            ),
        }
        true
    }

    fn validate_flush_completion(&self, identity: FlushIdentity) -> crate::common::MidgeResult<()> {
        self.check_lease_health()?;
        if identity.writer_epoch != self.fencing.writer_epoch {
            return Err(crate::common::MidgeError::Fenced(format!(
                "flush {} epoch {} does not match runtime epoch {}",
                identity.flush_id, identity.writer_epoch, self.fencing.writer_epoch
            )));
        }
        if let Some(store) = &self.fencing.leader_store {
            store
                .validate_epoch(
                    self.fencing.leader_holder_id.as_deref().unwrap_or_default(),
                    identity.writer_epoch,
                )
                .map_err(|error| {
                    error.into_validation_error(&format!(
                        "flush {} writer validation",
                        identity.flush_id
                    ))
                })?;
        }
        let Some((cf_id, flush)) = self.state.immutable_flush_by_id(identity.flush_id) else {
            return Err(crate::common::MidgeError::Fenced(format!(
                "flush {} is no longer owned by this runtime",
                identity.flush_id
            )));
        };
        if cf_id != identity.cf_id
            || flush.writer_epoch != identity.writer_epoch
            || flush.sequence != identity.sequence
        {
            return Err(crate::common::MidgeError::Fenced(format!(
                "flush {} completion identity is stale",
                identity.flush_id
            )));
        }
        Ok(())
    }

    fn install_flush_publication(
        &mut self,
        delta: &FlushPublicationDelta,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) {
        let Some((cf_id, flush)) = self.state.immutable_flush_by_id(delta.identity.flush_id) else {
            if let (Some(hybrid), Some(token)) = (&self.hybrid_storage, reservation) {
                hybrid.flush_completed_with_token(token, delta.file_meta.size_bytes);
            }
            self.flush_actor.finish_pipeline();
            return;
        };
        let frozen = Arc::clone(&flush.memtable);
        self.state
            .manifest
            .next_sst_seqs
            .entry(cf_id)
            .and_modify(|next| *next = (*next).max(delta.next_sst_seq))
            .or_insert(delta.next_sst_seq);
        self.state.manifest.add_file(crate::metadata::FileMeta {
            name: delta.file_meta.name.clone(),
            level: delta.file_meta.level,
            size_bytes: delta.file_meta.size_bytes,
            content_crc32c: delta.file_meta.content_crc32c,
            cf_id: delta.file_meta.cf_id,
            smallest_key: delta.file_meta.smallest_key.clone(),
            largest_key: delta.file_meta.largest_key.clone(),
            smallest_seq: delta.file_meta.smallest_seq,
            largest_seq: delta.file_meta.largest_seq,
            key_bounds_complete: delta.file_meta.key_bounds_complete,
            ..Default::default()
        });
        self.invalidate_sst_read_views();
        self.state.manifest.last_persisted_sequence = self
            .state
            .manifest
            .last_persisted_sequence
            .max(delta.identity.sequence);
        if let Some(checkpoint) = delta.journal_checkpoint {
            checkpoint.advance(&mut self.state.manifest);
        }
        if delta.persistence_anomaly {
            self.state.mark_persistence_anomaly();
        }
        if let Some(removed_size) = self.state.complete_immutable_flush(cf_id, &frozen) {
            self.state.total_memtable_bytes =
                self.state.total_memtable_bytes.saturating_sub(removed_size);
        }
        if let Err(error) = self.refresh_cloud_flush_headroom() {
            tracing::warn!(%error, "flush staging headroom awaits output retirement");
        }
        if let (Some(hybrid), Some(token)) = (&self.hybrid_storage, reservation) {
            hybrid.flush_completed_with_token(token, delta.file_meta.size_bytes);
        }
        if delta.cloud_metadata_published {
            self.evict_published_sst_cache(std::slice::from_ref(&delta.file_meta.name));
        }
        self.flush_actor.finish_pipeline();
        self.publish_snapshot();
        self.refresh_write_stall_timing();
        self.wake_write_stall_waiters();
        self.complete_flush_waiters(cf_id);
        self.schedule_compaction_after_flush_publication(&delta.file_meta.name);
        crate::failpoints::fail_point!("midge::flush_worker::before_wal_prune");
        if delta.cloud_metadata_published {
            self.prune_cloud_wal_segments_covered_by_manifest();
        } else {
            self.prune_local_wal_segments_covered_by_manifest();
        }
    }

    fn settle_stale_publish_reservation(&mut self, completion: &FlushPublishCompletion) {
        let (Some(hybrid), Some(token)) = (self.hybrid_storage.clone(), completion.reservation)
        else {
            return;
        };
        // A stale failed publisher may have durable local output. Retain
        // its charge until recovery inspects the authoritative metadata.
        if let Ok(delta) = &completion.result {
            hybrid.flush_completed_with_token(token, delta.file_meta.size_bytes);
            return;
        }
        let Some((_, flush)) = self
            .state
            .immutable_flush_by_id(completion.identity.flush_id)
        else {
            return;
        };
        let (Some(build), Some(name)) = (&flush.built, &flush.sst_name) else {
            return;
        };
        if build.identity != completion.identity || build.reservation != Some(token) {
            return;
        }
        let mut temporary = build.staging_path.as_os_str().to_os_string();
        temporary.push(".tmp");
        let paths = [
            build.staging_path.clone(),
            std::path::PathBuf::from(temporary),
            self.state.sst_dir.join(name),
        ];
        let primary_absent = paths.iter().all(|path| {
            self.runtime_fs_path(path)
                .and_then(|path| self.state.fs.exists(&path).map_err(Into::into))
                .is_ok_and(|exists| !exists)
        });
        if !primary_absent
            || !matches!(
                hybrid.local_object_cache_is_absent(&crate::cloud_layout::object_key(name)),
                Ok(true)
            )
        {
            return;
        }
        // This completed worker owns no bytes. Clear only its exact token;
        // another generation's retained or reusable reservation is untouched.
        if let Err(error) = self.refresh_cloud_flush_headroom() {
            tracing::warn!(%error, "flush staging headroom awaits failed publisher cleanup");
        }
        hybrid.flush_failed_with_token(token);
        if let Some((_, flush)) = self
            .state
            .immutable_flush_by_id_mut(completion.identity.flush_id)
        {
            if let Some(build) = &mut flush.built {
                build.reservation = None;
            }
        }
    }

    fn prune_local_wal_segments_covered_by_manifest(&mut self) {
        if self.state.is_memory_mode() || self.wal_actor.is_cloud_async() {
            return;
        }
        if let Err(error) = self.validate_runtime_lease_for_wal_prune() {
            tracing::warn!(%error, "retaining local WAL because the writer lease is no longer valid");
            return;
        }
        if !self.seal_active_wal_for_prune() {
            return;
        }

        let wal_dir = crate::io::FsPath::new("wal");
        let mut sealed_segments = match self.state.fs.list_dir(&wal_dir) {
            Ok(entries) => entries
                .into_iter()
                .filter(|entry| !entry.is_dir)
                .filter_map(|entry| {
                    crate::wal::parse_segment_id(&entry.name).map(|segment_id| {
                        (
                            segment_id,
                            crate::io::FsPath::new(format!("wal/{}", entry.name)),
                        )
                    })
                })
                .filter(|(segment_id, _)| *segment_id < self.state.wal.current_segment_id)
                .collect::<Vec<_>>(),
            Err(error) => {
                self.state.mark_persistence_anomaly();
                tracing::warn!(%error, "retaining local WAL because sealed segments could not be listed");
                return;
            }
        };
        sealed_segments.sort_by_key(|(segment_id, _)| *segment_id);
        // Memtables flush in FIFO order per column family, so a segment below
        // the oldest unflushed memtable's first segment holds only data that
        // is already in published SSTs, including deletes, which the
        // per-record proof below cannot certify.
        let flushed_floor = self.state.wal_recovery_floor_segment();
        let mut proofs = std::mem::take(&mut self.state.wal.local_segment_proofs);
        let outcome = self.retire_sealed_local_wal(&sealed_segments, flushed_floor, &mut proofs);
        // Forget proofs for segments that are gone.
        let listed: std::collections::HashSet<u64> = sealed_segments
            .iter()
            .map(|(segment_id, _)| *segment_id)
            .collect();
        let removed: std::collections::HashSet<u64> = outcome.removed.iter().copied().collect();
        proofs.retain(|segment_id, _| listed.contains(segment_id) && !removed.contains(segment_id));
        self.state.wal.local_segment_proofs = proofs;
        if outcome.anomaly {
            self.state.mark_persistence_anomaly();
        }
        if outcome.lease_lost {
            return;
        }

        if let Err(error) = self.state.fs.sync_dir(
            &crate::io::FsPath::new("wal"),
            crate::io::Durability::Durable,
        ) {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "failed to sync local WAL directory after pruning");
        }
    }

    /// Retires sealed local WAL segments, oldest first. A segment below the
    /// recovery floor goes whole; one at or above it goes only when every
    /// record is exactly in the manifest's SSTs.
    ///
    /// Every segment is considered, not just a prefix: an idle column family
    /// can keep the first one uncovered forever while later ones are covered.
    /// A failed proof is remembered in `proofs` and not repeated until the
    /// blocking family's SSTs change, so a pass reads only segments whose
    /// answer could differ (#490).
    fn retire_sealed_local_wal(
        &self,
        sealed_segments: &[(u64, crate::io::FsPath)],
        flushed_floor: Option<u64>,
        proofs: &mut std::collections::HashMap<
            u64,
            crate::runtime::hybrid_persistence::FailedWalProof,
        >,
    ) -> LocalWalPruneOutcome {
        use crate::runtime::hybrid_persistence::{
            coverage_fingerprint, FailedWalProof, VerifiedManifestWalCoverage,
        };
        let mut outcome = LocalWalPruneOutcome::default();
        // One prover per pass, built only if a proof is needed, so each
        // covering SST is opened and verified once per pass.
        let coverage = std::cell::OnceCell::new();
        // Each family's fingerprint once per pass, however many segments ask.
        let mut fingerprints = std::collections::HashMap::new();
        let mut fingerprint_of = |cf_id: u32| {
            *fingerprints
                .entry(cf_id)
                .or_insert_with(|| coverage_fingerprint(&self.state.manifest, cf_id))
        };
        for (segment_id, path) in sealed_segments {
            let segment_id = *segment_id;
            if outcome.lease_lost {
                return outcome;
            }
            if flushed_floor.is_some_and(|floor| segment_id < floor) {
                self.remove_local_wal_segment(segment_id, path, "flushed", &mut outcome);
                continue;
            }
            if proofs
                .get(&segment_id)
                .is_some_and(|proof| proof.still_fails(&mut fingerprint_of))
            {
                continue;
            }
            let bytes = match self.read_runtime_file(path) {
                Ok(bytes) => bytes,
                Err(error) => {
                    outcome.anomaly = true;
                    tracing::warn!(segment_id, %error, "retaining unreadable local WAL segment");
                    continue;
                }
            };
            if bytes.is_empty() {
                self.remove_local_wal_segment(segment_id, path, "empty", &mut outcome);
                continue;
            }
            let readback = match crate::wal::cloud_segment::inspect_local_bytes(&path.0, &bytes) {
                Ok(readback) => readback,
                Err(error) => {
                    outcome.anomaly = true;
                    tracing::warn!(segment_id, %error, "retaining invalid local WAL segment");
                    continue;
                }
            };
            let prover = coverage.get_or_init(|| {
                VerifiedManifestWalCoverage::open(&self.state.sst_dir, &self.state.manifest)
            });
            match prover.first_uncovered(&readback.data_records) {
                None => {
                    self.remove_local_wal_segment(
                        segment_id,
                        path,
                        "exactly covered",
                        &mut outcome,
                    );
                }
                Some(reason) => match FailedWalProof::remember(reason, &mut fingerprint_of) {
                    Some(proof) => {
                        proofs.insert(segment_id, proof);
                    }
                    // Unverifiable: prove it again next pass.
                    None => {
                        proofs.remove(&segment_id);
                    }
                },
            }
        }
        outcome
    }

    /// Removes one sealed segment. Every removal is preceded by a fresh
    /// writer-lease check, so a fenced writer stops deleting at once. Reads
    /// and skipped segments mutate nothing and need no check.
    fn remove_local_wal_segment(
        &self,
        segment_id: u64,
        path: &crate::io::FsPath,
        why: &'static str,
        outcome: &mut LocalWalPruneOutcome,
    ) {
        if let Err(error) = self.validate_runtime_lease_for_wal_prune() {
            tracing::warn!(segment_id, %error, "stopped local WAL pruning after lease validation failed");
            outcome.lease_lost = true;
            return;
        }
        match self.state.fs.remove_file(path) {
            Ok(()) => {
                tracing::debug!(segment_id, why, "removed local WAL segment");
                outcome.removed.push(segment_id);
            }
            Err(crate::io::FsError::NotFound(_)) => outcome.removed.push(segment_id),
            Err(error) => {
                outcome.anomaly = true;
                tracing::warn!(segment_id, why, %error, "failed to remove local WAL segment");
            }
        }
    }

    /// Seals the active WAL segment so it can be retired later. In local
    /// mode this is the only place segments are sealed, so it must happen
    /// even while some memtable is unflushed, or `wal.log` would grow without
    /// bound. An empty active segment has nothing to seal (#490): rotating it
    /// would only create an empty sealed file for the loop below to delete.
    ///
    /// The on-disk length is a sound emptiness test because an append returns
    /// only after the writer thread has written its bytes, and appends run on
    /// this thread, so none is in flight here. A failed stat seals as before.
    /// Returns false when pruning must stop.
    fn seal_active_wal_for_prune(&mut self) -> bool {
        let active = crate::io::FsPath::new(format!("wal/{}", crate::wal::ACTIVE_FILE_NAME));
        if self
            .state
            .fs
            .metadata(&active)
            .is_ok_and(|metadata| metadata.len == 0)
        {
            return true;
        }
        if let Err(error) = self.sync_local_wal_before_prune_rotation() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "retaining local WAL because buffered records could not be synced");
            return false;
        }
        if let Err(error) = self.rotate_local_wal_transition() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "retaining local WAL because the active segment could not be sealed");
            return false;
        }
        true
    }

    fn read_runtime_file(&self, path: &crate::io::FsPath) -> crate::io::FsResult<bytes::Bytes> {
        #[cfg(test)]
        RUNTIME_FILE_READS.with(|reads| reads.set(reads.get() + 1));
        let length = self.state.fs.metadata(path)?.len;
        let file = self.state.fs.open(
            path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;
        file.read_at(0, length)
    }

    fn validate_runtime_lease_for_wal_prune(&self) -> crate::common::MidgeResult<()> {
        self.check_lease_health()?;
        if let Some(store) = &self.fencing.leader_store {
            store
                .validate_epoch(
                    self.fencing.leader_holder_id.as_deref().unwrap_or_default(),
                    self.fencing.writer_epoch,
                )
                .map_err(|error| error.into_validation_error("local WAL prune"))?;
        }
        Ok(())
    }

    fn fail_flush_pipeline(
        &mut self,
        flush_id: u64,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
        error: &crate::common::MidgeError,
        publication_phase: bool,
    ) {
        let identity = self
            .state
            .immutable_flush_by_id(flush_id)
            .map(|(cf_id, flush)| (cf_id, flush.sequence));
        let mut retained = false;
        if let Some((_, flush)) = self.state.immutable_flush_by_id_mut(flush_id) {
            if let Some(build) = &mut flush.built {
                // The local output survives publication failure and retries
                // reuse both its bytes and its original capacity admission.
                build.reservation = build.reservation.or(reservation);
                retained = true;
            }
        }
        if !retained {
            crate::runtime::actors::flush::FlushActor::release_reservation(
                self.hybrid_storage.as_ref(),
                reservation,
            );
        }
        self.flush_actor.finish_pipeline();
        if publication_phase {
            // The worker may have already appended the manifest journal batch
            // or the durable intent before failing, leaving disk ahead of the
            // in-memory copies this loop publishes from. Reconcile before the
            // gate opens, or the next publication overwrites those edits. If
            // the reload fails, state fences every publication from memory
            // until a later reload succeeds.
            if let Err(error) = self.state.reload_persisted_metadata() {
                tracing::error!(
                    flush_id,
                    %error,
                    "failed to reload persisted metadata after a failed publication; \
                     publication is fenced until a reload succeeds"
                );
            }
            self.publication_gate.active = false;
        }
        let retry_after = self.state.mark_immutable_flush_failed(flush_id);
        tracing::warn!(flush_id, ?retry_after, %error, "flush pipeline failed; immutable retained");
        if let Some((cf_id, sequence)) = identity {
            self.fail_flush_waiters(cf_id, sequence, error);
            self.fail_deferred_column_family_drops(cf_id, error);
        }
        self.refresh_write_stall_timing();
    }

    fn fail_deferred_column_family_drops(&mut self, cf_id: u32, error: &crate::common::MidgeError) {
        let mut retained = std::collections::VecDeque::new();
        while let Some(message) = self.publication_gate.deferred_messages.pop_front() {
            match message {
                crate::runtime::RuntimeMsg::ManifestDropColumnFamily {
                    request_id,
                    cf_id: deferred_cf,
                    ..
                } if deferred_cf == cf_id => {
                    let error = Self::deferred_drop_failure(error);
                    self.respond(request_id, RuntimeResponse::Error { request_id, error });
                }
                other => retained.push_back(other),
            }
        }
        self.publication_gate.deferred_messages = retained;
    }

    /// Translate a flush-pipeline failure into the error a deferred
    /// `drop_column_family` caller receives.
    ///
    /// Every variant replays faithfully except `Busy`, which is reported as
    /// `Aborted`.
    ///
    /// `Busy` no longer carries the discard licence: `drop_column_family`
    /// grants that only through `MidgeError::UnflushedDataPresent`, which
    /// nothing in this pipeline can construct, so a verbatim replay would no
    /// longer be misread as permission to throw committed data away. The
    /// remap stays because it is still the more accurate report. The flush
    /// pipeline raises `Busy` for transient, internally retried conditions
    /// (an `IoError`/`Indeterminate` lease validation, for example), but from
    /// the caller's side the drop was cancelled before it could publish a
    /// result, which is what `Aborted` says. The original message is kept
    /// intact, and `Aborted` is `Severity::Transient`, so the caller retries
    /// the safe drop.
    fn deferred_drop_failure(error: &crate::common::MidgeError) -> crate::common::MidgeError {
        match error {
            crate::common::MidgeError::Busy(message) => crate::common::MidgeError::Aborted(
                format!("column family drop abandoned by a failed flush: {message}"),
            ),
            other => other.replay(),
        }
    }

    pub(super) fn flush_frontier_satisfied(&self, cf_id: u32, frontier: u64) -> bool {
        self.state.get_cf(cf_id).is_none_or(|cf| {
            cf.immutable_flushes
                .iter()
                .all(|flush| flush.sequence > frontier)
        })
    }

    fn complete_flush_waiters(&mut self, cf_id: u32) {
        let Some(waiters) = self.flush_barrier_waiters.remove(&cf_id) else {
            return;
        };
        let mut pending = Vec::new();
        for waiter in waiters {
            if self.flush_frontier_satisfied(cf_id, waiter.frontier) {
                self.respond(
                    waiter.request_id,
                    RuntimeResponse::Ok {
                        request_id: waiter.request_id,
                    },
                );
            } else {
                pending.push(waiter);
            }
        }
        if !pending.is_empty() {
            self.flush_barrier_waiters.insert(cf_id, pending);
        }
    }

    fn fail_flush_waiters(
        &mut self,
        cf_id: u32,
        failed_sequence: u64,
        error: &crate::common::MidgeError,
    ) {
        let Some(waiters) = self.flush_barrier_waiters.remove(&cf_id) else {
            return;
        };
        let mut pending = Vec::new();
        for waiter in waiters {
            if waiter.frontier >= failed_sequence {
                self.respond(
                    waiter.request_id,
                    RuntimeResponse::Error {
                        request_id: waiter.request_id,
                        error: error.replay(),
                    },
                );
            } else {
                pending.push(waiter);
            }
        }
        if !pending.is_empty() {
            self.flush_barrier_waiters.insert(cf_id, pending);
        }
    }

    pub(super) fn drain_flush_worker_results(&mut self) {
        while let Ok(result) = self.flush_worker_result_rx.try_recv() {
            self.handle_flush_worker_result(result);
        }
    }

    pub(super) fn drain_inline_flush_worker(&mut self) {
        if !self.inline_flush_worker {
            return;
        }
        while self.flush_actor.is_inflight() {
            match self
                .flush_worker_result_rx
                .recv_timeout(Duration::from_secs(5))
            {
                Ok(result) => self.handle_flush_worker_result(result),
                Err(_) => break,
            }
        }
    }

    pub(super) fn drain_auto_flush_memtables(&mut self) -> usize {
        if self.state.is_memory_mode() || self.shutting_down {
            return 0;
        }
        let mut frozen_count = match self.freeze_cloud_memtables_near_staging_limit() {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!(%error, "cloud staging pressure freeze deferred");
                0
            }
        };
        let mut attempted_cfs = std::collections::HashSet::new();
        // The eventual flush stops an idle family pinning the WAL recovery
        // floor. Cloud measures the gap in segments, bounding the WAL
        // catalog. Local measures it in WAL bytes appended since the memtable
        // started (#552): flushes append none, so gap flushes cannot feed
        // each other. Local retirement stays prefix-only, because retiring a
        // tombstone's segment ahead of an older retained put would let
        // recovery resurrect that put once compaction drops both (#550).
        let rule = if self.wal_actor.is_cloud_async() {
            crate::runtime::state::EventualFlush::SegmentGap
        } else {
            crate::runtime::state::EventualFlush::WalBytes
        };
        while let Some(candidate) = self
            .state
            .next_flush_candidate_skipping(rule, &attempted_cfs)
        {
            attempted_cfs.insert(candidate.cf_id);
            if candidate.reason != crate::runtime::state::FlushReason::PendingImmutable {
                match self.freeze_active_memtable(candidate.cf_id) {
                    Ok(Some(_)) => frozen_count = frozen_count.saturating_add(1),
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(cf_id = candidate.cf_id, %error, "automatic freeze deferred");
                    }
                }
            }
        }
        self.schedule_next_flush_worker();
        self.drain_inline_flush_worker();
        self.refresh_write_stall_timing();
        frozen_count
    }

    pub(super) fn refresh_write_stall_timing(&mut self) {
        let stalled = self.state.has_any_hard_write_stall();
        match (stalled, self.state.flush_metrics.write_stall_started_at) {
            (true, None) => {
                self.state.flush_metrics.write_stall_started_at = Some(std::time::Instant::now());
            }
            (false, Some(started)) => {
                let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                self.state.flush_metrics.write_stall_ns_total = self
                    .state
                    .flush_metrics
                    .write_stall_ns_total
                    .saturating_add(elapsed);
                self.state.flush_metrics.write_stall_ns_max =
                    self.state.flush_metrics.write_stall_ns_max.max(elapsed);
                self.state.flush_metrics.write_stall_started_at = None;
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    fn event_loop_with_hybrid_storage(
        directory: &tempfile::TempDir,
    ) -> crate::common::MidgeResult<(EventLoop, Arc<crate::storage::HybridStorage>)> {
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let local = Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("hybrid-local"),
        )?);
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            String::new(),
        ));
        let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let config = crate::runtime::RuntimeConfig {
            hybrid_storage: Some(Arc::clone(&hybrid)),
            ..crate::runtime::RuntimeConfig::default()
        };
        let event_loop = EventLoop::new(
            state,
            false,
            Arc::new(crate::runtime::ResponseRouter::new()),
            config,
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        Ok((event_loop, hybrid))
    }

    #[test]
    fn should_preserve_resource_limit_kind_when_flush_waiter_is_failed(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let request_id = 416;
        let response = event_loop.router.register(request_id, "FlushMemtable");
        event_loop.flush_barrier_waiters.insert(
            0,
            vec![super::super::flush::FlushBarrierWaiter {
                request_id,
                frontier: 7,
            }],
        );

        // Act
        event_loop.fail_flush_waiters(
            0,
            7,
            &crate::common::MidgeError::ResourceLimit("flush budget exhausted".to_string()),
        );

        // Assert: backpressure must reach the flush caller as backpressure, not
        // as an Internal defect.
        match response.recv_timeout(std::time::Duration::from_secs(1)) {
            Ok(crate::runtime::RuntimeResponse::Error { error, .. }) => assert!(
                matches!(
                    &error,
                    crate::common::MidgeError::ResourceLimit(message)
                        if message == "flush budget exhausted"
                ),
                "flush waiter lost the error kind: {error:?}"
            ),
            other => panic!("unexpected flush waiter response: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn should_not_report_busy_when_deferred_drop_fails_from_flush_pipeline(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: a safe drop_column_family deferred behind an active
        // publication, then failed by a transient flush-pipeline Busy.
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let request_id = 4161;
        let response = event_loop
            .router
            .register(request_id, "ManifestDropColumnFamily");
        event_loop.publication_gate.deferred_messages.push_back(
            crate::runtime::RuntimeMsg::ManifestDropColumnFamily {
                request_id,
                cf_id: 0,
                discard_unflushed: false,
            },
        );

        // Act
        event_loop.fail_deferred_column_family_drops(
            0,
            &crate::common::MidgeError::Busy(
                "flush 7 writer validation could not complete: lease io error".to_string(),
            ),
        );

        // Assert: Busy on drop_column_family licenses the caller to call
        // drop_column_family_discarding_unflushed, so a transient pipeline
        // failure must never wear it — and must not lose its message either.
        match response.recv_timeout(std::time::Duration::from_secs(1)) {
            Ok(crate::runtime::RuntimeResponse::Error { error, .. }) => {
                assert!(
                    !matches!(error, crate::common::MidgeError::Busy(_)),
                    "flush pipeline failure reached drop_column_family as Busy: {error:?}"
                );
                assert!(
                    matches!(
                        &error,
                        crate::common::MidgeError::Aborted(message)
                            if message.contains(
                                "flush 7 writer validation could not complete: lease io error"
                            )
                    ),
                    "deferred drop failure lost the original message: {error:?}"
                );
            }
            other => panic!("unexpected deferred drop response: {other:?}"),
        }
        assert!(event_loop.publication_gate.deferred_messages.is_empty());
        Ok(())
    }

    #[test]
    fn should_yield_flush_publication_turn_to_restored_deferred_request(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        event_loop.state.sequence = 1;
        event_loop
            .state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        event_loop
            .freeze_active_memtable(0)?
            .expect("freeze non-empty memtable");
        event_loop.pending_msg = Some(crate::runtime::RuntimeMsg::CompactAll { request_id: 71 });

        let request_id = 71;
        let response = event_loop.router.register(request_id, "CompactAll");
        let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let restored = event_loop.pending_msg.take().expect("restored request");

        // Act
        event_loop.process_restored_one(restored, &msg_rx);

        // Assert
        assert!(
            !event_loop.flush_actor.is_inflight(),
            "a restored control request must run before the next flush takes the publication gate"
        );
        assert!(matches!(
            response.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(crate::runtime::RuntimeResponse::Ok { request_id: 71 })
        ));
        Ok(())
    }

    #[test]
    fn should_separate_writes_at_freeze_linearization_boundary() -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        event_loop
            .state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .put_bytes_with_seq(
                bytes::Bytes::from_static(b"before"),
                bytes::Bytes::from_static(b"old"),
                1,
                None,
            )?;

        // Act
        event_loop
            .freeze_active_memtable(0)?
            .expect("freeze non-empty memtable");
        event_loop
            .state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .put_bytes_with_seq(
                bytes::Bytes::from_static(b"after"),
                bytes::Bytes::from_static(b"new"),
                2,
                None,
            )?;

        // Assert
        let cf = event_loop.state.get_cf(0).expect("default column family");
        let frozen = cf.immutable_memtables.last().expect("frozen memtable");
        assert_eq!(
            frozen.get_bytes(b"before")?,
            Some(bytes::Bytes::from_static(b"old"))
        );
        assert_eq!(frozen.get_bytes(b"after")?, None);
        assert_eq!(cf.memtable.get_bytes(b"before")?, None);
        assert_eq!(
            cf.memtable.get_bytes(b"after")?,
            Some(bytes::Bytes::from_static(b"new"))
        );
        Ok(())
    }

    #[test]
    fn should_release_storage_reservation_when_orphan_build_completion_is_discarded(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let reservation = hybrid
            .reserve_for_flush_with_token(256)
            .expect("reserve flush storage");
        let identity = FlushIdentity {
            flush_id: 99,
            writer_epoch: 0,
            cf_id: 0,
            sequence: 1,
        };
        let completion = crate::runtime::actors::flush::FlushBuildCompletion {
            identity,
            memtable: Arc::new(crate::memtable::SkipListMemtable::new()),
            staging_path: directory.path().join("orphan.sst"),
            reservation: Some(reservation),
            build_ns: 1,
            result: Err(crate::common::MidgeError::Fenced("orphan".to_string())),
        };

        // Act
        let should_continue = event_loop.handle_flush_build_completion(completion);

        // Assert
        assert!(!should_continue);
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 0);
        Ok(())
    }

    #[test]
    fn should_remove_failed_flush_output_from_injected_filesystem() -> crate::common::MidgeResult<()>
    {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let injected_fs = Arc::new(crate::io::MockFs::new());
        let mut staged = crate::io::Fs::open(
            injected_fs.as_ref(),
            &crate::io::FsPath::new("orphan.sst"),
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: true,
            },
        )?;
        staged.write_at(0, bytes::Bytes::from_static(b"orphan"))?;
        event_loop.state.fs = injected_fs.clone();
        let reservation = hybrid
            .reserve_for_flush_with_token(256)
            .expect("reserve flush storage");
        let completion = FlushBuildCompletion {
            identity: FlushIdentity {
                flush_id: 99,
                writer_epoch: 0,
                cf_id: 0,
                sequence: 1,
            },
            memtable: Arc::new(crate::memtable::SkipListMemtable::new()),
            staging_path: directory.path().join("orphan.sst"),
            reservation: Some(reservation),
            build_ns: 1,
            result: Err(crate::common::MidgeError::Fenced("orphan".to_string())),
        };

        // Act
        event_loop.handle_flush_build_completion(completion);

        // Assert
        assert_eq!(injected_fs.get_file("orphan.sst"), None);
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 0);
        Ok(())
    }

    struct FlakyLeaderStore {
        epoch: u64,
        fail_next_read: std::sync::atomic::AtomicBool,
        error: fn() -> crate::lease::LeaseError,
    }

    impl crate::lease::LeaderStore for FlakyLeaderStore {
        fn acquire_leadership(
            &self,
            _holder_id: &str,
        ) -> Result<crate::lease::LeaderRecord, crate::lease::LeaseError> {
            Err(crate::lease::LeaseError::Internal("not used".into()))
        }

        fn read_current(
            &self,
        ) -> Result<Option<crate::lease::LeaderRecord>, crate::lease::LeaseError> {
            if self
                .fail_next_read
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err((self.error)());
            }
            Ok(Some(crate::lease::LeaderRecord {
                epoch: self.epoch,
                holder_id: "writer".to_string(),
                acquired_at: "test".to_string(),
            }))
        }
    }

    /// Answers the first `valid_reads` leader reads, then reports the lease
    /// taken by another holder.
    struct ExpiringLeaderStore {
        epoch: u64,
        valid_reads: std::sync::atomic::AtomicUsize,
    }

    impl crate::lease::LeaderStore for ExpiringLeaderStore {
        fn acquire_leadership(
            &self,
            _holder_id: &str,
        ) -> Result<crate::lease::LeaderRecord, crate::lease::LeaseError> {
            Err(crate::lease::LeaseError::Internal("not used".into()))
        }

        fn read_current(
            &self,
        ) -> Result<Option<crate::lease::LeaderRecord>, crate::lease::LeaseError> {
            let remaining = self.valid_reads.load(std::sync::atomic::Ordering::SeqCst);
            let holder = if remaining == 0 {
                "successor"
            } else {
                self.valid_reads
                    .store(remaining - 1, std::sync::atomic::Ordering::SeqCst);
                "writer"
            };
            Ok(Some(crate::lease::LeaderRecord {
                epoch: self.epoch,
                holder_id: holder.to_string(),
                acquired_at: "test".to_string(),
            }))
        }
    }

    #[test]
    fn should_stop_removing_local_wal_when_lease_is_lost_during_prune(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: three flushed sealed segments, and a lease that stays
        // valid for the pass preamble and one removal only.
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        let wal_dir = event_loop.state.wal_dir.clone();
        for segment_id in 1..=3 {
            std::fs::write(
                wal_dir.join(crate::wal::segment_file_name(segment_id)),
                b"x",
            )?;
        }
        event_loop.state.wal.current_segment_id = 4;
        event_loop.fencing.leader_store = Some(Arc::new(ExpiringLeaderStore {
            epoch: event_loop.fencing.writer_epoch,
            valid_reads: std::sync::atomic::AtomicUsize::new(2),
        }));
        event_loop.fencing.leader_holder_id = Some("writer".to_string());

        // Act
        event_loop.prune_local_wal_segments_covered_by_manifest();

        // Assert: the first removal ran under a valid lease; after the loss
        // nothing else is deleted.
        let exists = |segment_id| {
            wal_dir
                .join(crate::wal::segment_file_name(segment_id))
                .exists()
        };
        assert_eq!((exists(1), exists(2), exists(3)), (false, true, true));
        Ok(())
    }

    #[test]
    fn should_retry_flush_publication_when_completion_validation_fails_transiently(
    ) -> crate::common::MidgeResult<()> {
        let unanswered: [fn() -> crate::lease::LeaseError; 2] = [
            || crate::lease::LeaseError::IoError("leader read failed".into()),
            || crate::lease::LeaseError::Timeout("leader read".into()),
        ];
        for error in unanswered {
            // Arrange
            let directory = tempfile::tempdir()?;
            let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
            event_loop.state.sequence = 1;
            event_loop
                .state
                .get_cf(0)
                .expect("family")
                .memtable
                .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
            let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
            event_loop.fencing.leader_store = Some(Arc::new(FlakyLeaderStore {
                epoch: event_loop.fencing.writer_epoch,
                fail_next_read: std::sync::atomic::AtomicBool::new(true),
                error,
            }));
            event_loop.fencing.leader_holder_id = Some("writer".to_string());
            let identity = FlushIdentity {
                flush_id,
                writer_epoch: event_loop.fencing.writer_epoch,
                cf_id: 0,
                sequence: 1,
            };
            let (_, flush) = event_loop
                .state
                .immutable_flush_by_id_mut(flush_id)
                .expect("immutable");
            flush.phase = ImmutableFlushPhase::Publishing;
            event_loop.publication_gate.active = true;

            // Act: the leader-store read behind validation fails once. The flush
            // may already be durably published, so it must be retried, not
            // stranded in Publishing forever.
            event_loop.handle_flush_publish_completion(FlushPublishCompletion {
                identity,
                reservation: None,
                publish_ns: 1,
                result: Err(crate::common::MidgeError::Internal("unused".into())),
            });

            // Assert
            let (_, flush) = event_loop
                .state
                .immutable_flush_by_id(flush_id)
                .expect("immutable still owned");
            assert!(
                matches!(flush.phase, ImmutableFlushPhase::RetryPending),
                "phase after transient validation failure: {:?}",
                flush.phase
            );
            assert!(!event_loop.publication_gate.active);
        }
        Ok(())
    }

    #[test]
    fn should_fence_publication_when_reload_fails_after_failed_flush_publication(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        event_loop.state.sequence = 1;
        event_loop
            .state
            .get_cf(0)
            .expect("family")
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
        std::fs::write(
            event_loop
                .state
                .db_path
                .join(crate::metadata::files::MANIFEST_SNAPSHOT),
            b"not a manifest",
        )?;
        event_loop.publication_gate.active = true;

        // Act
        event_loop.fail_flush_pipeline(
            flush_id,
            None,
            &crate::common::MidgeError::Internal("worker failed".into()),
            true,
        );

        // Assert
        assert!(!event_loop.publication_gate.active);
        assert!(event_loop.state.persistence_anomaly_detected());
        assert!(matches!(
            event_loop.state.ensure_metadata_current(),
            Err(crate::common::MidgeError::Fenced(_))
        ));
        Ok(())
    }

    #[test]
    fn should_release_fenced_failed_publish_admission_only_when_all_owned_outputs_are_absent(
    ) -> crate::common::MidgeResult<()> {
        for residue in [
            "absent",
            "staging",
            "temporary",
            "final",
            "secondary",
            "unknown owner",
        ] {
            // Arrange
            let directory = tempfile::tempdir()?;
            let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
            hybrid.enable_ephemeral_sst_cache(256);
            event_loop.state.sequence = 1;
            event_loop
                .state
                .get_cf(0)
                .expect("family")
                .memtable
                .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
            let flush_id = event_loop.freeze_active_memtable(0)?.expect("frozen");
            let identity = FlushIdentity {
                flush_id,
                writer_epoch: event_loop.fencing.writer_epoch,
                cf_id: 0,
                sequence: 1,
            };
            let token = hybrid
                .reserve_for_flush_with_token(128)
                .expect("flush admission");
            let staging_path = directory.path().join("failed.sst");
            let name = crate::cloud_layout::file_name(0, 0, 1);
            let path = match residue {
                "staging" => Some(staging_path.clone()),
                "temporary" => Some(directory.path().join("failed.sst.tmp")),
                "final" => Some(event_loop.state.sst_dir.join(&name)),
                "secondary" => Some(directory.path().join("hybrid-local/sst").join(&name)),
                _ => None,
            };
            if let Some(path) = path {
                std::fs::create_dir_all(path.parent().unwrap())?;
                std::fs::write(path, b"retained output")?;
            }
            let (_, flush) = event_loop
                .state
                .immutable_flush_by_id_mut(flush_id)
                .expect("immutable");
            flush.sst_name = Some(name.clone());
            flush.built = Some(FlushBuildOutput {
                identity,
                staging_path,
                reservation: Some(token),
                file_meta: crate::runtime::FileMeta {
                    name,
                    level: 0,
                    size_bytes: 128,
                    content_crc32c: Some(1),
                    cf_id: 0,
                    smallest_key: Some(b"key".to_vec()),
                    largest_key: Some(b"key".to_vec()),
                    smallest_seq: Some(1),
                    largest_seq: Some(1),
                    key_bounds_complete: true,
                },
            });
            if residue == "unknown owner" {
                event_loop
                    .state
                    .get_cf_mut(0)
                    .expect("family")
                    .immutable_flushes
                    .clear();
            }
            event_loop.fencing.writer_epoch += 1;
            // Act
            event_loop.handle_flush_publish_completion(FlushPublishCompletion {
                identity,
                reservation: Some(token),
                publish_ns: 1,
                result: Err(crate::common::MidgeError::Fenced(
                    "publisher lost its lease".into(),
                )),
            });
            // Assert
            assert_eq!(
                hybrid.budget_snapshot().total_committed_bytes,
                if residue == "absent" { 0 } else { 128 },
                "residue: {residue}"
            );
        }
        Ok(())
    }

    #[test]
    fn should_keep_flush_admission_for_retry_when_publication_fails_after_output_is_built(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
        hybrid.enable_ephemeral_sst_cache(256);
        event_loop.state.sequence = 1;
        event_loop
            .state
            .get_cf(0)
            .expect("default CF")
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let flush_id = event_loop
            .freeze_active_memtable(0)?
            .expect("freeze memtable");
        let reservation = hybrid
            .reserve_for_flush_with_token(256)
            .expect("flush capacity");
        let identity = FlushIdentity {
            flush_id,
            writer_epoch: event_loop.fencing.writer_epoch,
            cf_id: 0,
            sequence: 1,
        };
        let staging_path = directory.path().join("built.sst");
        std::fs::write(&staging_path, [1_u8; 128])?;
        let (_, flush) = event_loop
            .state
            .immutable_flush_by_id_mut(flush_id)
            .expect("immutable");
        flush.built = Some(FlushBuildOutput {
            identity,
            staging_path: staging_path.clone(),
            reservation: Some(reservation),
            file_meta: crate::runtime::FileMeta {
                name: crate::cloud_layout::file_name(0, 0, 1),
                level: 0,
                size_bytes: 128,
                content_crc32c: Some(1),
                cf_id: 0,
                smallest_key: Some(b"key".to_vec()),
                largest_key: Some(b"key".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                key_bounds_complete: true,
            },
        });

        // Act
        event_loop.handle_flush_publish_completion(FlushPublishCompletion {
            identity,
            reservation: Some(reservation),
            publish_ns: 1,
            result: Err(crate::common::MidgeError::Timeout(
                "cloud unavailable".into(),
            )),
        });

        // Assert
        assert!(staging_path.exists());
        let (_, flush) = event_loop
            .state
            .immutable_flush_by_id(flush_id)
            .expect("retained immutable");
        assert_eq!(
            flush.built.as_ref().expect("retry output").reservation,
            Some(reservation)
        );
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 256);
        assert!(hybrid.reserve_for_flush_with_token(1).is_err());
        Ok(())
    }

    #[test]
    fn should_retain_flush_admission_when_failed_build_residue_cannot_be_removed(
    ) -> crate::common::MidgeResult<()> {
        for is_temp in [false, true] {
            // Arrange
            let directory = tempfile::tempdir()?;
            let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
            hybrid.enable_ephemeral_sst_cache(256);
            let reservation = hybrid
                .reserve_for_flush_with_token(256)
                .expect("flush capacity");
            let staging_path = directory.path().join("failed.sst");
            let residue_path = if is_temp {
                directory.path().join("failed.sst.tmp")
            } else {
                staging_path.clone()
            };
            // A nonempty directory reliably makes unlink fail without permissions or timing.
            std::fs::create_dir(&residue_path)?;
            std::fs::write(residue_path.join("retained-bytes"), [1_u8; 128])?;
            let completion = FlushBuildCompletion {
                identity: FlushIdentity {
                    flush_id: 99,
                    writer_epoch: 0,
                    cf_id: 0,
                    sequence: 1,
                },
                memtable: Arc::new(crate::memtable::SkipListMemtable::new()),
                staging_path,
                reservation: Some(reservation),
                build_ns: 1,
                result: Err(crate::common::MidgeError::NoSpace("build failed".into())),
            };

            // Act
            event_loop.handle_flush_build_completion(completion);

            // Assert
            assert!(residue_path.join("retained-bytes").exists());
            assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 256);
            assert!(hybrid.reserve_for_flush_with_token(1).is_err());
        }
        Ok(())
    }

    #[test]
    fn should_settle_storage_reservation_when_published_output_lost_immutable_owner(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let reservation = hybrid
            .reserve_for_flush_with_token(256)
            .expect("reserve flush storage");
        let identity = FlushIdentity {
            flush_id: 99,
            writer_epoch: 0,
            cf_id: 0,
            sequence: 1,
        };
        let delta = FlushPublicationDelta {
            identity,
            file_meta: crate::runtime::FileMeta {
                name: crate::cloud_layout::file_name(0, 0, 1),
                level: 0,
                size_bytes: 128,
                content_crc32c: Some(1),
                cf_id: 0,
                smallest_key: Some(b"key".to_vec()),
                largest_key: Some(b"key".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
                key_bounds_complete: true,
            },
            next_sst_seq: 2,
            cloud_metadata_published: false,
            persistence_anomaly: false,
            journal_checkpoint: None,
        };

        // Act
        event_loop.install_flush_publication(&delta, Some(reservation));

        // Assert
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 128);
        Ok(())
    }

    #[test]
    fn should_seal_active_wal_when_a_memtable_is_still_unflushed() -> crate::common::MidgeResult<()>
    {
        // Arrange: prune is the only local seal trigger, so a column family
        // that never goes quiet must not keep `wal.log` growing forever.
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        event_loop.wal_actor.append(
            &mut event_loop.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: 1,
                cf_id: 0,
                key: bytes::Bytes::from_static(b"unflushed"),
                value: Some(bytes::Bytes::from_static(b"value")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        let current = event_loop.state.wal.current_segment_id;
        let active = event_loop.state.wal_dir.join(crate::wal::ACTIVE_FILE_NAME);
        let active_len_before = std::fs::metadata(&active)?.len();
        assert!(
            event_loop
                .state
                .get_cf(0)
                .expect("default family")
                .memtable
                .size_bytes()
                > 0,
            "the appended record must be unflushed"
        );

        // Act
        event_loop.prune_local_wal_segments_covered_by_manifest();

        // Assert
        assert_eq!(
            event_loop.state.wal.current_segment_id,
            current + 1,
            "active wal.log was {active_len_before} bytes on disk before prune"
        );
        Ok(())
    }

    /// A local event loop whose WAL has already appended far more than the
    /// eventual-flush byte bound, with every memtable empty (#552).
    fn local_event_loop_after_long_wal_history(
        directory: &std::path::Path,
    ) -> crate::common::MidgeResult<EventLoop> {
        let state = crate::runtime::state::RuntimeState::new(directory.to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        let history = u64::try_from(event_loop.state.memtable_flush_threshold)
            .unwrap_or(u64::MAX)
            .saturating_mul(100);
        event_loop.state.wal.appended_bytes = history;
        Ok(event_loop)
    }

    #[test]
    fn should_not_flush_memtable_started_by_a_put_when_wal_history_exceeds_the_byte_bound(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: the byte gap counts from when this memtable started, not
        // from the start of the WAL.
        let directory = tempfile::tempdir()?;
        let mut event_loop = local_event_loop_after_long_wal_history(directory.path())?;
        event_loop.wal_actor.append(
            &mut event_loop.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: 1,
                cf_id: 0,
                key: bytes::Bytes::from_static(b"first"),
                value: Some(bytes::Bytes::from_static(b"value")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        // Act
        let frozen = event_loop.drain_auto_flush_memtables();

        // Assert
        assert_eq!(frozen, 0, "a fresh memtable must not be flushed at once");
        Ok(())
    }

    #[test]
    fn should_not_flush_memtable_started_by_a_range_delete_when_wal_history_exceeds_the_byte_bound(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let mut event_loop = local_event_loop_after_long_wal_history(directory.path())?;
        event_loop.wal_actor.append_delete_range(
            &mut event_loop.state,
            1,
            0,
            bytes::Bytes::from_static(b"a"),
            bytes::Bytes::from_static(b"z"),
            None,
        )?;

        // Act
        let frozen = event_loop.drain_auto_flush_memtables();

        // Assert
        assert_eq!(frozen, 0, "a fresh memtable must not be flushed at once");
        Ok(())
    }

    #[test]
    fn should_count_put_bytes_toward_the_local_wal_gap_when_appending(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let mut event_loop = local_event_loop_after_long_wal_history(directory.path())?;
        let before = event_loop.state.wal.appended_bytes;

        // Act
        event_loop.wal_actor.append(
            &mut event_loop.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: 1,
                cf_id: 0,
                key: bytes::Bytes::from_static(b"counted"),
                value: Some(bytes::Bytes::from_static(b"value")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        // Assert
        assert!(event_loop.state.wal.appended_bytes > before);
        Ok(())
    }

    #[test]
    fn should_not_rotate_local_wal_when_active_segment_is_empty() -> crate::common::MidgeResult<()>
    {
        // Arrange: nothing has been written since the last rotation.
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        let current = event_loop.state.wal.current_segment_id;

        // Act
        event_loop.prune_local_wal_segments_covered_by_manifest();

        // Assert
        assert_eq!(event_loop.state.wal.current_segment_id, current);
        Ok(())
    }

    #[test]
    fn should_mark_anomaly_when_invalid_wal_segment_follows_an_uncovered_one(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: segment 1 is valid but uncovered; segment 2 is corrupt.
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            1,
            1,
        );
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &crate::wal::encoding::encode(&record)?)?;
        let wal_dir = event_loop.state.wal_dir.clone();
        std::fs::write(wal_dir.join(crate::wal::segment_file_name(1)), frame)?;
        std::fs::write(
            wal_dir.join(crate::wal::segment_file_name(2)),
            b"not a WAL segment",
        )?;
        event_loop.state.wal.current_segment_id = 3;
        let cf = event_loop.state.get_cf_mut(0).expect("default family");
        cf.memtable
            .put_with_seq(b"unflushed".to_vec(), b"value".to_vec(), 1, None)?;
        cf.active_memtable_started_in_segment = 1;

        // Act
        event_loop.prune_local_wal_segments_covered_by_manifest();

        // Assert
        assert!(event_loop.state.persistence_anomaly_detected());
        assert!(wal_dir.join(crate::wal::segment_file_name(2)).exists());
        Ok(())
    }

    #[test]
    fn should_reprove_wal_segment_when_its_covering_sst_could_not_be_read(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: the manifest lists an SST covering the record, but it
        // cannot be read. That says nothing about coverage, so the failed
        // proof must not be remembered.
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        let record = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            5,
            1,
        );
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &crate::wal::encoding::encode(&record)?)?;
        std::fs::write(
            event_loop
                .state
                .wal_dir
                .join(crate::wal::segment_file_name(1)),
            frame,
        )?;
        event_loop
            .state
            .manifest
            .add_file(crate::metadata::FileMeta {
                name: crate::cloud_layout::file_name(0, 0, 9),
                cf_id: 0,
                size_bytes: 1,
                content_crc32c: Some(1),
                smallest_seq: Some(1),
                largest_seq: Some(10),
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"z".to_vec()),
                key_bounds_complete: true,
                ..Default::default()
            });
        event_loop.state.wal.current_segment_id = 2;
        let cf = event_loop.state.get_cf_mut(0).expect("default family");
        cf.memtable
            .put_with_seq(b"unflushed".to_vec(), b"value".to_vec(), 6, None)?;
        cf.active_memtable_started_in_segment = 1;
        RUNTIME_FILE_READS.with(|reads| reads.set(0));

        // Act
        event_loop.prune_local_wal_segments_covered_by_manifest();
        event_loop.prune_local_wal_segments_covered_by_manifest();

        // Assert: re-read on the second pass, and retained.
        assert_eq!(RUNTIME_FILE_READS.with(std::cell::Cell::get), 2);
        assert!(event_loop
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(1))
            .exists());
        Ok(())
    }

    #[test]
    fn should_not_reprove_uncovered_wal_segments_until_their_column_family_changes(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: three sealed segments above the recovery floor, none
        // covered by an SST of their column family (0).
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        for segment_id in 1..=3 {
            let record = crate::wal::WalRecord::new(
                crate::wal::WalOpKind::Put,
                bytes::Bytes::from(format!("key-{segment_id}")),
                Some(bytes::Bytes::from_static(b"value")),
                segment_id,
                1,
            );
            let payload = crate::wal::encoding::encode(&record)?;
            let mut frame = Vec::new();
            crate::wal::frame::append_frame(&mut frame, &payload)?;
            std::fs::write(
                event_loop
                    .state
                    .wal_dir
                    .join(crate::wal::segment_file_name(segment_id)),
                frame,
            )?;
        }
        event_loop.state.wal.current_segment_id = 4;
        let cf = event_loop.state.get_cf_mut(0).expect("default family");
        cf.memtable
            .put_with_seq(b"unflushed".to_vec(), b"value".to_vec(), 1, None)?;
        cf.active_memtable_started_in_segment = 1;
        RUNTIME_FILE_READS.with(|reads| reads.set(0));

        // Act
        event_loop.prune_local_wal_segments_covered_by_manifest();
        let first_pass = RUNTIME_FILE_READS.with(|reads| reads.replace(0));
        event_loop.prune_local_wal_segments_covered_by_manifest();
        let unchanged_pass = RUNTIME_FILE_READS.with(|reads| reads.replace(0));
        event_loop
            .state
            .manifest
            .add_file(crate::metadata::FileMeta {
                name: crate::cloud_layout::file_name(0, 0, 1),
                cf_id: 0,
                size_bytes: 1,
                ..Default::default()
            });
        event_loop.prune_local_wal_segments_covered_by_manifest();
        let changed_pass = RUNTIME_FILE_READS.with(std::cell::Cell::get);

        // Assert: every segment is considered, none is re-read until family
        // 0's SSTs change, and nothing uncovered is removed.
        assert_eq!((first_pass, unchanged_pass, changed_pass), (3, 0, 3));
        for segment_id in 1..=3 {
            assert!(event_loop
                .state
                .wal_dir
                .join(crate::wal::segment_file_name(segment_id))
                .exists());
        }
        Ok(())
    }

    #[test]
    fn should_not_checkpoint_manifest_on_event_loop_for_each_flush_name_reservation(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let snapshot = directory
            .path()
            .join(crate::metadata::files::MANIFEST_SNAPSHOT);
        let before = std::fs::read(&snapshot).ok();

        // Act
        let mut names = Vec::new();
        for _ in 0..10 {
            names.push(event_loop.reserve_flush_sst_seq(0)?);
        }

        // Assert: distinct names, no snapshot rewrite, one durable bump.
        let mut unique = names.clone();
        unique.dedup();
        assert_eq!(unique, names);
        assert_eq!(std::fs::read(&snapshot).ok(), before);
        let journaled = crate::metadata::ManifestPersistence::load(directory.path())
            .map_err(crate::common::MidgeError::Internal)?;
        let durable = journaled.next_sst_seqs.get(&0).copied().unwrap_or(1);
        assert!(
            names.iter().all(|seq| *seq < durable),
            "{names:?} vs {durable}"
        );
        let bumps = crate::metadata::journal::replay_journal(directory.path())?
            .into_iter()
            .filter(|edit| matches!(edit, crate::metadata::ManifestEdit::BumpNextSstSeq { .. }))
            .count();
        assert_eq!(bumps, 1);
        Ok(())
    }

    #[test]
    fn should_mirror_flush_sst_name_reservation_again_when_mirror_failed(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: the journal append succeeds but the cloud mirror fails,
        // so the cloud metadata never learns the reserved block.
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        event_loop.cloud_metadata_storage =
            Some(Arc::new(crate::storage::cloud::CloudStorage::new(
                Arc::new(crate::storage::cloud::MockCloudBackend::new()),
                String::new(),
            )));
        let lease_healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
        event_loop.fencing.lease_healthy = Some(Arc::clone(&lease_healthy));
        let failed = event_loop.reserve_flush_sst_seq(0);

        // Act: a retry inside the same block must still reach the mirror.
        let retried = event_loop.reserve_flush_sst_seq(0);
        lease_healthy.store(true, std::sync::atomic::Ordering::SeqCst);
        let recovered = event_loop.reserve_flush_sst_seq(0);

        // Assert
        assert!(failed.is_err());
        assert!(
            retried.is_err(),
            "a name was handed out without a mirrored reservation: {retried:?}"
        );
        assert!(recovered.is_ok(), "{recovered:?}");
        Ok(())
    }

    #[test]
    fn should_not_mirror_sst_name_reservation_while_metadata_reload_is_required(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: a failed reload leaves memory behind disk (#500), and the
        // on-disk intent log is unreadable.
        let directory = tempfile::tempdir()?;
        let (mut event_loop, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            String::new(),
        ));
        event_loop.cloud_metadata_storage = Some(Arc::clone(&cloud));
        std::fs::write(
            event_loop
                .state
                .db_path
                .join(crate::metadata::files::INTENT_LOG),
            b"not json",
        )?;
        assert!(event_loop.state.reload_persisted_metadata().is_err());

        // Act
        let result = event_loop.reserve_sst_name_durably(0, 1);

        // Assert: fenced, and nothing reached the authoritative mirror.
        assert!(
            matches!(result, Err(crate::common::MidgeError::Fenced(_))),
            "{result:?}"
        );
        let key = crate::cloud_layout::CloudObjectLayout::metadata_key(
            crate::metadata::files::INTENT_LOG,
        );
        let (tx, rx) = std::sync::mpsc::channel();
        cloud.submit_get(&key, tx);
        let uploaded = matches!(
            rx.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(crate::storage::cloud::CloudEvent::Get {
                result: crate::storage::cloud::CloudOutcome::Ok(_),
                ..
            })
        );
        assert!(!uploaded, "stale intent log was mirrored");
        Ok(())
    }

    #[test]
    fn should_not_reuse_reserved_sst_names_when_replacement_restores_from_cloud_metadata(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: hand out flush names and a compaction generation, then lose
        // the machine. The replacement sees only the cloud metadata mirror.
        let directory = tempfile::tempdir()?;
        let (mut first, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            String::new(),
        ));
        first.cloud_metadata_storage = Some(Arc::clone(&cloud));
        let mut used = Vec::new();
        for _ in 0..20 {
            used.push(first.reserve_flush_sst_seq(0)?);
        }
        let plan = first
            .assign_compaction_output_sequence(crate::compaction::CompactionPlan::new(0, 0, 1))?;
        drop(first);
        let replacement = tempfile::tempdir()?;
        for name in crate::metadata::files::CLOUD_MIRRORED {
            let (tx, rx) = std::sync::mpsc::channel();
            cloud.submit_get(
                &crate::cloud_layout::CloudObjectLayout::metadata_key(name),
                tx,
            );
            if let Ok(crate::storage::cloud::CloudEvent::Get {
                result: crate::storage::cloud::CloudOutcome::Ok(data),
                ..
            }) = rx.recv_timeout(std::time::Duration::from_secs(1))
            {
                std::fs::write(replacement.path().join(name), data)?;
            }
        }

        // Act
        let (mut second, _hybrid) = event_loop_with_hybrid_storage(&replacement)?;
        let first_name = second.reserve_flush_sst_seq(0)?;
        let next_generation = second.state.next_compaction_output_generation()?;

        // Assert
        assert!(
            used.iter().all(|seq| *seq < first_name),
            "{used:?} then {first_name}"
        );
        assert!(next_generation > plan.output_seq);
        Ok(())
    }

    #[test]
    fn should_not_reuse_reserved_flush_sst_name_when_reopened_after_crash(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: hand out names, then lose the in-memory cursor.
        let directory = tempfile::tempdir()?;
        let (mut first, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let mut used = Vec::new();
        for _ in 0..3 {
            used.push(first.reserve_flush_sst_seq(0)?);
        }
        drop(first);

        // Act
        let (mut reopened, _hybrid) = event_loop_with_hybrid_storage(&directory)?;
        let after_restart = reopened.reserve_flush_sst_seq(0)?;

        // Assert
        assert!(
            used.iter().all(|seq| *seq < after_restart),
            "{used:?} then {after_restart}"
        );
        Ok(())
    }

    #[test]
    fn should_validate_transactions_through_event_loop_read_resources(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);

        // Act
        let event_loop = EventLoop::new(
            state,
            false,
            Arc::new(crate::runtime::ResponseRouter::new()),
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;

        // Assert: validation shares the loop's reader and block caches (#492).
        assert!(event_loop.read_resources.is_some());
        assert!(event_loop.wal_actor.has_read_resources());
        Ok(())
    }

    #[test]
    fn should_mark_anomaly_when_local_wal_directory_sync_fails_after_pruning(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let mut state =
            crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let sync_fs = Arc::new(crate::io::MockFs::new());
        crate::io::Fs::open(
            sync_fs.as_ref(),
            &crate::io::FsPath::new(format!("wal/{}", crate::wal::ACTIVE_FILE_NAME)),
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: false,
            },
        )?;
        sync_fs.set_sync_dir_failure(true);
        state.fs = sync_fs.clone();
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;

        // Act
        event_loop.prune_local_wal_segments_covered_by_manifest();

        // Assert
        assert!(event_loop.state.persistence_anomaly_detected());
        assert_eq!(
            sync_fs.sync_dir_calls(),
            [(
                crate::io::FsPath::new("wal"),
                crate::io::Durability::Durable
            )]
        );
        Ok(())
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn should_retain_all_wal_state_when_pre_prune_sync_fails() -> crate::common::MidgeResult<()> {
        // Arrange
        let _failpoint_guard = crate::failpoints::test_failpoint_guard();
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
            ..crate::runtime::RuntimeConfig::default()
        };
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            config,
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        event_loop.wal_actor.append(
            &mut event_loop.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: 1,
                cf_id: 0,
                key: bytes::Bytes::from_static(b"published"),
                value: Some(bytes::Bytes::from_static(b"value")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        event_loop.sync_local_wal_before_prune_rotation()?;
        event_loop.rotate_local_wal_transition()?;
        event_loop.wal_actor.append(
            &mut event_loop.state,
            crate::runtime::actors::wal::AppendParams {
                request_id: 2,
                cf_id: 0,
                key: bytes::Bytes::from_static(b"buffered"),
                value: Some(bytes::Bytes::from_static(b"value")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;
        let segment_before = event_loop.state.wal.current_segment_id;
        let durable_before = event_loop.state.wal.local_durable_seq;
        let sealed_path = event_loop
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_before - 1));
        let scenario = fail::FailScenario::setup();
        fail::cfg("midge::wal::inject_no_space_on_sync", "return")
            .expect("configure WAL sync failure");

        // Act
        event_loop.prune_local_wal_segments_covered_by_manifest();
        fail::remove("midge::wal::inject_no_space_on_sync");
        scenario.teardown();

        // Assert
        assert_eq!(event_loop.state.wal.current_segment_id, segment_before);
        assert_eq!(event_loop.state.wal.local_durable_seq, durable_before);
        assert!(sealed_path.exists(), "failed sync must retain sealed WAL");
        assert!(
            event_loop
                .state
                .wal_dir
                .join(crate::wal::ACTIVE_FILE_NAME)
                .exists(),
            "failed sync must retain the active WAL writer file"
        );
        Ok(())
    }

    #[test]
    fn should_reject_publication_completion_after_lease_loss() -> crate::common::MidgeResult<()> {
        // Arrange
        let directory = tempfile::tempdir()?;
        let state = crate::runtime::state::RuntimeState::new(directory.path().to_path_buf(), false);
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let healthy = Arc::new(AtomicBool::new(true));
        let local = Arc::new(crate::storage::filesystem::FileSystem::new(
            directory.path().join("hybrid-local"),
        )?);
        let cloud = Arc::new(crate::storage::cloud::CloudStorage::new(
            Arc::new(crate::storage::cloud::MockCloudBackend::new()),
            String::new(),
        ));
        let hybrid = Arc::new(crate::storage::HybridStorage::with_policy(
            local,
            cloud,
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        ));
        let config = crate::runtime::RuntimeConfig {
            writer_epoch: 7,
            lease_healthy: Some(Arc::clone(&healthy)),
            hybrid_storage: Some(Arc::clone(&hybrid)),
            ..crate::runtime::RuntimeConfig::default()
        };
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            config,
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )?;
        event_loop.state.sequence = 1;
        event_loop
            .state
            .get_cf(0)
            .expect("default column family")
            .memtable
            .put_with_seq(b"key".to_vec(), b"value".to_vec(), 1, None)?;
        let flush_id = event_loop
            .freeze_active_memtable(0)?
            .expect("non-empty memtable must freeze");
        let identity = FlushIdentity {
            flush_id,
            writer_epoch: 7,
            cf_id: 0,
            sequence: 1,
        };
        let reservation = hybrid
            .reserve_for_flush_with_token(256)
            .expect("reserve flush storage");
        healthy.store(false, std::sync::atomic::Ordering::Release);
        let result =
            FlushWorkerResult::Publish(crate::runtime::actors::flush::FlushPublishCompletion {
                identity,
                reservation: Some(reservation),
                publish_ns: 10,
                result: Ok(FlushPublicationDelta {
                    identity,
                    file_meta: crate::runtime::FileMeta {
                        name: crate::cloud_layout::file_name(0, 0, 1),
                        level: 0,
                        size_bytes: 128,
                        content_crc32c: Some(1),
                        cf_id: 0,
                        smallest_key: Some(b"key".to_vec()),
                        largest_key: Some(b"key".to_vec()),
                        smallest_seq: Some(1),
                        largest_seq: Some(1),
                        key_bounds_complete: true,
                    },
                    next_sst_seq: 2,
                    cloud_metadata_published: false,
                    persistence_anomaly: false,
                    journal_checkpoint: None,
                }),
            });

        // Act
        event_loop.handle_flush_worker_result(result);

        // Assert
        assert!(event_loop.state.manifest.files.is_empty());
        assert_eq!(event_loop.state.wal.current_segment_id, 1);
        assert_eq!(
            hybrid.budget_snapshot().total_committed_bytes,
            128,
            "a published stale completion must settle its reservation to actual SST bytes"
        );
        assert_eq!(
            event_loop
                .state
                .get_cf(0)
                .expect("default column family")
                .immutable_flushes
                .len(),
            1,
            "the stale runtime must retain the immutable without reclaiming it"
        );
        Ok(())
    }
}
