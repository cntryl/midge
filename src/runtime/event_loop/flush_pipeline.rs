use super::EventLoop;
use crate::runtime::actors::flush::{
    FlushBuildCompletion, FlushBuildOutput, FlushIdentity, FlushPublicationDelta,
    FlushPublishCompletion, FlushPublishTask, FlushWorkerResult,
};
use crate::runtime::state::{ImmutableFlush, ImmutableFlushPhase};
use crate::runtime::RuntimeResponse;
use crate::sst::Memtable;
use std::sync::Arc;
use std::time::Duration;

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
            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                telemetry.metrics().record_write_stall_memory();
            }
            self.refresh_write_stall_timing();
            return Err(crate::common::MidgeError::WriteStall(format!(
                "immutable memtable queue is full for column family {cf_id}"
            )));
        }

        let current_segment_id = self.state.wal.current_segment_id;
        let sequence = self.state.sequence;
        let frozen = {
            let cf = self.state.get_cf_mut(cf_id).ok_or_else(|| {
                crate::common::MidgeError::Internal(format!(
                    "column family {cf_id} disappeared during freeze"
                ))
            })?;
            let frozen = std::mem::replace(
                &mut cf.memtable,
                Arc::new(crate::sst::SkipListMemtable::new()),
            );
            cf.active_memtable_started_in_segment = current_segment_id;
            frozen
        };
        let flush = self
            .state
            .track_new_immutable_flush(cf_id, Arc::clone(&frozen), sequence)
            .ok_or_else(|| {
                crate::common::MidgeError::ResourceLimit(
                    "flush identity space exhausted".to_string(),
                )
            })?;
        crate::failpoints::fail_point!("midge::flush_worker::after_freeze");
        self.publish_snapshot();
        Ok(Some(flush.flush_id))
    }

    pub(super) fn schedule_next_flush_worker(&mut self) {
        if self.shutting_down || self.state.is_memory_mode() || self.flush_actor.is_inflight() {
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
                    build.file_meta.size_bytes,
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
        let estimated_size = u64::try_from(flush.memtable.size_bytes()).unwrap_or(u64::MAX);
        let reservation = match crate::runtime::actors::flush::FlushActor::reserve_flush(
            self.hybrid_storage.as_ref(),
            cf_id,
            estimated_size,
        ) {
            Ok(reservation) => reservation,
            Err(error) => {
                self.fail_flush_pipeline(flush.flush_id, None, &error, false);
                return;
            }
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
            reservation,
        ) {
            self.fail_flush_pipeline(flush.flush_id, reservation, &error, false);
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
        self.publication_gate.active = true;
        let task = FlushPublishTask {
            build,
            sst_name,
            sst_seq,
            db_path: self.state.db_path.clone(),
            sst_dir: self.state.sst_dir.clone(),
            fs: Arc::clone(&self.state.fs),
            recovery_policy: self.state.recovery_policy(),
            hybrid_storage: self.hybrid_storage.clone(),
            cloud_metadata_storage: self.cloud_metadata_storage.clone(),
            lease_healthy: self.lease_healthy.clone(),
            leader_store: self.leader_store.clone(),
            leader_holder_id: self.leader_holder_id.clone(),
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

    fn handle_flush_build_completion(&mut self, completion: FlushBuildCompletion) -> bool {
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
            self.fail_flush_pipeline(
                completion.identity.flush_id,
                completion.reservation,
                &error,
                false,
            );
            return false;
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

    fn prepare_flush_publication(
        &mut self,
        identity: FlushIdentity,
        staging_path: std::path::PathBuf,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
        file_meta: crate::runtime::FileMeta,
    ) {
        let sst_seq = self
            .state
            .manifest
            .next_sst_seqs
            .get(&identity.cf_id)
            .copied()
            .unwrap_or(1);
        let next_sst_seq = sst_seq.saturating_add(1);
        self.state
            .manifest
            .next_sst_seqs
            .entry(identity.cf_id)
            .and_modify(|next| *next = (*next).max(next_sst_seq))
            .or_insert(next_sst_seq);
        let sst_name = crate::sst::file_name(identity.cf_id, 0, sst_seq);
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
        if identity.writer_epoch != self.writer_epoch {
            return Err(crate::common::MidgeError::Fenced(format!(
                "flush {} epoch {} does not match runtime epoch {}",
                identity.flush_id, identity.writer_epoch, self.writer_epoch
            )));
        }
        if let Some(store) = &self.leader_store {
            store
                .validate_epoch(
                    self.leader_holder_id.as_deref().unwrap_or_default(),
                    identity.writer_epoch,
                )
                .map_err(|error| crate::common::MidgeError::Fenced(error.to_string()))?;
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
            ..Default::default()
        });
        self.state.manifest.last_persisted_sequence = self
            .state
            .manifest
            .last_persisted_sequence
            .max(delta.identity.sequence);
        if delta.persistence_anomaly {
            self.state.mark_persistence_anomaly();
        }
        if let Some(removed_size) = self.state.complete_immutable_flush(cf_id, &frozen) {
            self.state.total_memtable_bytes =
                self.state.total_memtable_bytes.saturating_sub(removed_size);
        }
        if let (Some(hybrid), Some(token)) = (&self.hybrid_storage, reservation) {
            hybrid.flush_completed_with_token(token, delta.file_meta.size_bytes);
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

    fn settle_stale_publish_reservation(&self, completion: &FlushPublishCompletion) {
        let (Some(hybrid), Some(token)) = (&self.hybrid_storage, completion.reservation) else {
            return;
        };
        match &completion.result {
            Ok(delta) => hybrid.flush_completed_with_token(token, delta.file_meta.size_bytes),
            Err(_) => hybrid.flush_failed_with_token(token),
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
        if let Err(error) = self.sync_local_wal_before_prune_rotation() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "retaining local WAL because buffered records could not be synced");
            return;
        }
        if let Err(error) = self.wal_actor.rotate(&mut self.state) {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "retaining local WAL because the active segment could not be sealed");
            return;
        }

        let mut sealed_segments = match std::fs::read_dir(&self.state.wal_dir) {
            Ok(entries) => entries
                .flatten()
                .filter_map(|entry| {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    crate::wal::parse_segment_id(&name).map(|segment_id| (segment_id, entry.path()))
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

        for (segment_id, path) in sealed_segments {
            if let Err(error) = self.validate_runtime_lease_for_wal_prune() {
                tracing::warn!(segment_id, %error, "stopped local WAL pruning after lease validation failed");
                return;
            }
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.state.mark_persistence_anomaly();
                    tracing::warn!(segment_id, %error, "retaining unreadable local WAL segment");
                    continue;
                }
            };
            if bytes.is_empty() {
                if let Err(error) = std::fs::remove_file(&path) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        self.state.mark_persistence_anomaly();
                        tracing::warn!(segment_id, %error, "failed to remove empty local WAL segment");
                    }
                }
                continue;
            }
            let key = path.to_string_lossy();
            let readback = match crate::wal::cloud_segment::inspect_bytes(&key, &bytes) {
                Ok(readback) => readback,
                Err(error) => {
                    self.state.mark_persistence_anomaly();
                    tracing::warn!(segment_id, %error, "retaining invalid local WAL segment");
                    continue;
                }
            };
            if !crate::runtime::hybrid_persistence::wal_data_records_covered_by_manifest(
                &readback.data_records,
                &self.state.manifest,
            ) {
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => tracing::debug!(segment_id, "removed manifest-covered local WAL segment"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    self.state.mark_persistence_anomaly();
                    tracing::warn!(segment_id, %error, "failed to remove manifest-covered local WAL segment");
                }
            }
        }

        if let Err(error) = self.state.fs.sync_dir(
            &crate::io::FsPath::new("wal"),
            crate::io::Durability::Durable,
        ) {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "failed to sync local WAL directory after pruning");
        }
    }

    fn validate_runtime_lease_for_wal_prune(&self) -> crate::common::MidgeResult<()> {
        self.check_lease_health()?;
        if let Some(store) = &self.leader_store {
            store
                .validate_epoch(
                    self.leader_holder_id.as_deref().unwrap_or_default(),
                    self.writer_epoch,
                )
                .map_err(|error| crate::common::MidgeError::Fenced(error.to_string()))?;
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
        // This token is process-local capacity accounting, not a durability
        // mutation. Release it even after fencing while retaining all durable
        // objects and the immutable for proof-based recovery or retry.
        crate::runtime::actors::flush::FlushActor::release_reservation(
            self.hybrid_storage.as_ref(),
            reservation,
        );
        if let Some((_, flush)) = self.state.immutable_flush_by_id_mut(flush_id) {
            if let Some(build) = &mut flush.built {
                build.reservation = None;
            }
        }
        self.flush_actor.finish_pipeline();
        if publication_phase {
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
                } if deferred_cf == cf_id => self.respond(
                    request_id,
                    RuntimeResponse::Error {
                        request_id,
                        error: clone_flush_error(error),
                    },
                ),
                other => retained.push_back(other),
            }
        }
        self.publication_gate.deferred_messages = retained;
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
                        error: clone_flush_error(error),
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
        let mut frozen_count = 0usize;
        let mut attempted_cfs = std::collections::HashSet::new();
        while let Some(candidate) = self
            .state
            .next_flush_candidate_skipping(self.wal_actor.is_cloud_async(), &attempted_cfs)
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

fn clone_flush_error(error: &crate::common::MidgeError) -> crate::common::MidgeError {
    match error {
        crate::common::MidgeError::Fenced(message) => {
            crate::common::MidgeError::Fenced(message.clone())
        }
        crate::common::MidgeError::NoSpace(message) => {
            crate::common::MidgeError::NoSpace(message.clone())
        }
        crate::common::MidgeError::WriteStall(message) => {
            crate::common::MidgeError::WriteStall(message.clone())
        }
        crate::common::MidgeError::Corruption(message) => {
            crate::common::MidgeError::Corruption(message.clone())
        }
        crate::common::MidgeError::Timeout(message) => {
            crate::common::MidgeError::Timeout(message.clone())
        }
        _ => crate::common::MidgeError::Internal(error.to_string()),
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
            None,
        )?;
        Ok((event_loop, hybrid))
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
            memtable: Arc::new(crate::sst::SkipListMemtable::new()),
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
                name: crate::sst::file_name(0, 0, 1),
                level: 0,
                size_bytes: 128,
                content_crc32c: Some(1),
                cf_id: 0,
                smallest_key: Some(b"key".to_vec()),
                largest_key: Some(b"key".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(1),
            },
            next_sst_seq: 2,
            cloud_metadata_published: false,
            persistence_anomaly: false,
        };

        // Act
        event_loop.install_flush_publication(&delta, Some(reservation));

        // Assert
        assert_eq!(hybrid.budget_snapshot().total_committed_bytes, 128);
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
        sync_fs.set_sync_dir_failure(true);
        state.fs = sync_fs.clone();
        let router = Arc::new(crate::runtime::ResponseRouter::new());
        let mut event_loop = EventLoop::new(
            state,
            false,
            router,
            crate::runtime::RuntimeConfig::default(),
            None,
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
        let mut event_loop = EventLoop::new(state, false, router, config, None)?;
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
        event_loop.wal_actor.sync(&mut event_loop.state)?;
        event_loop.wal_actor.rotate(&mut event_loop.state)?;
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
        let mut event_loop = EventLoop::new(state, false, router, config, None)?;
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
                        name: crate::sst::file_name(0, 0, 1),
                        level: 0,
                        size_bytes: 128,
                        content_crc32c: Some(1),
                        cf_id: 0,
                        smallest_key: Some(b"key".to_vec()),
                        largest_key: Some(b"key".to_vec()),
                        smallest_seq: Some(1),
                        largest_seq: Some(1),
                    },
                    next_sst_seq: 2,
                    cloud_metadata_published: false,
                    persistence_anomaly: false,
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
