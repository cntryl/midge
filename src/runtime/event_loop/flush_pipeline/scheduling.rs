use super::{
    Arc, EventLoop, FlushBuildCompletion, FlushBuildOutput, FlushIdentity, FlushPublishTask,
    FlushWorkerResult, ImmutableFlush, ImmutableFlushPhase, Origin,
};

impl EventLoop {
    pub(in crate::runtime::event_loop) fn column_family_flush_pipeline_active(
        &self,
        cf_id: u32,
    ) -> bool {
        self.state
            .get_cf(cf_id)
            .is_some_and(|cf| !cf.immutable_flushes.is_empty())
    }

    pub(in crate::runtime::event_loop) fn column_family_publication_pipeline_active(
        &self,
        cf_id: u32,
    ) -> bool {
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

    pub(in crate::runtime::event_loop) fn ordinary_flush_origin(&self) -> Origin {
        if self.actors.cloud_coordinator.hybrid_storage.is_some() {
            Origin::CloudFlush
        } else {
            Origin::OrdinaryLocalFlush
        }
    }

    pub(in crate::runtime::event_loop) fn freeze_active_memtable(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> crate::common::MidgeResult<Option<u64>> {
        let origin = self.ordinary_flush_origin();
        self.freeze_active_memtable_for(cf_id, origin)
    }

    pub(in crate::runtime::event_loop) fn freeze_active_memtable_for(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        origin: Origin,
    ) -> crate::common::MidgeResult<Option<u64>> {
        let cf = self.state.get_cf(cf_id).ok_or_else(|| {
            crate::common::MidgeError::InvalidArgument(format!(
                "column family {cf_id} does not exist"
            ))
        })?;
        if cf.memtable.size_bytes() == 0 {
            return Ok(None);
        }
        // A flush frontier at or past a partly applied unit's commit would
        // make restart skip the unit; only replay can apply it whole.
        if let Some(commit_seq) = cf.partially_applied_commit() {
            return Err(crate::common::MidgeError::RecoveryFailed(format!(
                "column family {cf_id} holds a partly applied transaction committed at \
                 sequence {commit_seq}; restart before flushing it"
            )));
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

        let open_segment = self.state.wal.open_segment();

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
            .track_new_immutable_flush_for(cf_id, Arc::clone(&frozen), sequence, origin)
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
        cf.active_memtable_started_in_segment = open_segment;
        cf.active_memtable_started_at_wal_bytes = appended_bytes;
        crate::failpoints::fail_point!("midge::flush_worker::after_freeze");
        self.publish_snapshot();
        Ok(Some(flush.flush_id))
    }

    pub(in crate::runtime::event_loop) fn schedule_next_flush_worker(&mut self) {
        self.schedule_next_flush_worker_with_shutdown(false);
    }

    pub(in crate::runtime::event_loop) fn schedule_next_flush_worker_during_shutdown(&mut self) {
        self.schedule_next_flush_worker_with_shutdown(true);
    }

    /// Whether a queued flush cannot start right now. The run loop uses the
    /// same condition, so a queued flush it cannot start does not count as
    /// actionable work and spin the loop.
    pub(in crate::runtime::event_loop) fn flush_start_blocked(
        &self,
        allow_during_shutdown: bool,
    ) -> bool {
        (self.shutdown.shutting_down && !allow_during_shutdown)
            || self.state.is_memory_mode()
            || (!self.actors.flush_actor.has_capacity()
                && !self.state.has_due_immutable_flush_matching(|cf, id| {
                    self.actors.flush_actor.owns_pipeline(cf, id)
                }))
            || (self.pipeline.inline_flush_worker && self.publication.publication_gate.is_active())
            || (!allow_during_shutdown && self.routing.pending_msg.is_some())
            || (!allow_during_shutdown
                && self.cloud_maintenance_enabled()
                && self.cloud_maintenance_control_blocked())
    }

    pub(super) fn schedule_next_flush_worker_with_shutdown(&mut self, allow_during_shutdown: bool) {
        if !allow_during_shutdown
            && self.cloud_maintenance_enabled()
            && !self.actors.cloud_coordinator.cloud_maintenance.dispatching
        {
            self.schedule_cloud_maintenance();
            return;
        }
        if self.flush_start_blocked(allow_during_shutdown) {
            return;
        }
        let actor = &self.actors.flush_actor;
        let Some(mut flush) = self
            .state
            .begin_next_immutable_flush_matching(|cf, id| actor.can_start_or_resume(cf, id))
        else {
            return;
        };
        let Some((cf_id, _)) = self.state.immutable_flush_by_id(flush.flush_id) else {
            return;
        };
        let identity = FlushIdentity {
            flush_id: flush.flush_id,
            writer_epoch: flush.writer_epoch,
            cf_id,
            sequence: flush.sequence,
        };
        let (sst_name, sst_seq) = match self.ensure_flush_sst_identity(identity) {
            Ok(identity) => identity,
            Err(crate::common::MidgeError::Busy(_))
                if self.actors.cloud_coordinator.metadata_mirror.is_running() =>
            {
                // Reservation owns its own completion. Retain the immutable
                // and original flush waiters until its exact name is covered.
                if let Some((_, current)) = self.state.immutable_flush_by_id_mut(flush.flush_id) {
                    current.phase = ImmutableFlushPhase::Queued;
                }
                self.actors.cloud_coordinator.metadata_mirror.pending_flush = Some(flush.flush_id);
                return;
            }
            Err(error) => {
                self.fail_flush_pipeline(flush.flush_id, None, &error);
                return;
            }
        };
        flush.sst_name = Some(sst_name);
        flush.sst_seq = Some(sst_seq);
        if let Some(mut build) = flush.built.clone() {
            if build.reservation.is_none() {
                let Some((cf_id, _)) = self.state.immutable_flush_by_id(flush.flush_id) else {
                    return;
                };
                let reservation = match crate::runtime::actors::flush::FlushActor::reserve_flush(
                    self.actors.cloud_coordinator.hybrid_storage.as_ref(),
                    cf_id,
                    build.file_meta.size_bytes.saturating_mul(2),
                ) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        self.fail_flush_pipeline(flush.flush_id, None, &error);
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

        let staging_path = std::path::Path::new(crate::sst::fs::FLUSH_STAGING_DIR).join(format!(
            "{}-{}.sst",
            identity.writer_epoch, identity.flush_id
        ));
        if let Err(error) = self.actors.flush_actor.submit_build(
            identity,
            Arc::clone(&flush.memtable),
            staging_path,
            self.actors.cloud_coordinator.hybrid_storage.clone(),
        ) {
            self.fail_flush_pipeline(flush.flush_id, None, &error);
        }
    }

    pub(super) fn schedule_flush_publication(
        &mut self,
        flush: &ImmutableFlush,
        build: FlushBuildOutput,
    ) {
        let (Some(sst_name), Some(sst_seq)) = (flush.sst_name.clone(), flush.sst_seq) else {
            self.fail_flush_pipeline(
                flush.flush_id,
                build.reservation,
                &crate::common::MidgeError::Internal(
                    "built flush has no canonical SST identity".to_string(),
                ),
            );
            return;
        };
        // The loop later validates and applies the returned edit to its live
        // manifest. The worker only finalizes/uploads its owned SST.
        if let Err(error) = self.state.retry_metadata_reload() {
            self.fail_flush_pipeline(flush.flush_id, build.reservation, &error);
            return;
        }
        let publication_owner =
            crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush {
                flush_id: flush.flush_id,
            };
        if self.pipeline.inline_flush_worker
            && !self
                .publication
                .publication_gate
                .try_acquire(publication_owner.clone())
        {
            if let Some((_, current)) = self.state.immutable_flush_by_id_mut(flush.flush_id) {
                current.phase = ImmutableFlushPhase::Queued;
            }
            return;
        }
        if let Err(error) = self.validate_flush_completion(build.identity) {
            self.fail_flush_pipeline(flush.flush_id, build.reservation, &error);
            return;
        }
        let task = FlushPublishTask {
            build,
            sst_name,
            sst_seq,
            fs: Arc::clone(&self.state.fs),
            storage: Arc::new(crate::runtime::actors::flush::HybridFlushStorage::new(
                self.actors.cloud_coordinator.hybrid_storage.clone(),
                self.actors.cloud_coordinator.cloud_metadata_storage.clone(),
            )),
            lease_validity: self.authority.fencing.lease_validity.clone(),
            lease_healthy: self.authority.fencing.lease_healthy.clone(),
            leader_store: self.authority.fencing.leader_store.clone(),
            leader_holder_id: self.authority.fencing.leader_holder_id.clone(),
        };
        let submitted_at = std::time::Instant::now();
        match self.actors.flush_actor.submit_publish(task) {
            Ok(()) => self
                .state
                .accept_flush_publication_attempt(flush.flush_id, submitted_at),
            Err(error) => {
                self.state.metadata_accounting().publication_attempt(
                    flush.accounting.origin,
                    self.state.metadata_medium(),
                    submitted_at.elapsed(),
                    true,
                );
                self.publication
                    .publication_gate
                    .release(&publication_owner);
                self.fail_flush_pipeline(flush.flush_id, None, &error);
            }
        }
    }

    pub(in crate::runtime::event_loop) fn handle_flush_worker_result(
        &mut self,
        result: FlushWorkerResult,
    ) {
        let identity = result.identity();
        #[cfg(test)]
        let fixture = self.pipeline.inline_flush_worker
            && !self.actors.flush_actor.has_pipeline(identity.flush_id);
        #[cfg(not(test))]
        let fixture = false;
        if !fixture && !self.actors.flush_actor.completion_is_ready(&result) {
            tracing::debug!(flush_id = identity.flush_id, "ignored unowned flush result");
            return;
        }
        let owner = crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush {
            flush_id: identity.flush_id,
        };
        let local_turn = !matches!(&result, FlushWorkerResult::Build(_));
        let acquired_here = local_turn && !self.publication.publication_gate.is_owned_by(&owner);
        if acquired_here && !self.publication.publication_gate.try_acquire(owner.clone()) {
            // One terminal result per admitted physical phase; its pipeline
            // retains capacity until this exact callback can take a local turn.
            if !self
                .pipeline
                .deferred_flush_results
                .iter()
                .any(|held| held.identity().flush_id == identity.flush_id)
            {
                self.pipeline.deferred_flush_results.push_back(result);
            }
            return;
        }
        if !self.actors.flush_actor.take_completion(&result) {
            // Inline unit fixtures also exercise settlement without submitting
            // a physical task. An existing owner always requires its terminal
            // proof, including under Inline mode; production has no bypass.
            if !fixture {
                if acquired_here {
                    self.publication.publication_gate.release(&owner);
                }
                tracing::debug!(
                    flush_id = identity.flush_id,
                    "ignored stale, duplicate or early flush worker completion"
                );
                return;
            }
        }
        let should_continue = match result {
            FlushWorkerResult::Build(completion) => self.handle_flush_build_completion(completion),
            FlushWorkerResult::Publish(completion) => {
                self.handle_flush_publish_completion(completion)
            }
            FlushWorkerResult::Mirror(completion) => {
                self.handle_flush_mirror_completion(completion)
            }
        };
        if local_turn && !self.pipeline.inline_flush_worker {
            self.publication.publication_gate.release(&owner);
        }
        if !should_continue {
            return;
        }
        self.restore_publication_deferred_message();
        self.schedule_next_flush_worker();
    }

    pub(super) fn handle_flush_build_completion(
        &mut self,
        mut completion: FlushBuildCompletion,
    ) -> bool {
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
            self.fail_flush_pipeline(completion.identity.flush_id, completion.reservation, &error);
            return false;
        }
        if let Err(error) = self.validate_flush_completion(completion.identity) {
            self.cleanup_failed_flush_build(&mut completion);
            self.fail_flush_pipeline(completion.identity.flush_id, completion.reservation, &error);
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
            ),
        }
        true
    }

    pub(super) fn cleanup_failed_flush_build(&self, completion: &mut FlushBuildCompletion) {
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
}
