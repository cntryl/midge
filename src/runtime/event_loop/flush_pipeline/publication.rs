use super::{
    Arc, EventLoop, FlushBuildOutput, FlushIdentity, FlushManifestPublication,
    FlushMirrorCompletion, FlushMirrorTask, FlushPublicationDelta, FlushPublishCompletion,
    ImmutableFlushPhase, Origin,
};

impl EventLoop {
    pub(super) fn runtime_fs_path(
        &self,
        path: &std::path::Path,
    ) -> crate::common::MidgeResult<crate::io::FsPath> {
        crate::sst::fs::fs_relative_sst_path(&self.state.fs, path)
    }

    /// Picks the SST sequence for a flush output and, when the output can
    /// reach remote storage before it is published, makes the name durable.
    #[cfg(test)]
    pub(in crate::runtime::event_loop) fn reserve_flush_sst_seq(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> crate::common::MidgeResult<u64> {
        self.reserve_flush_sst_seq_for(cf_id, Origin::Unclassified)
    }

    pub(super) fn reserve_flush_sst_seq_for(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        origin: Origin,
    ) -> crate::common::MidgeResult<u64> {
        let sst_seq = self.next_sst_name(cf_id)?;
        if self.actors.cloud_coordinator.hybrid_storage.is_some() {
            // The publish worker uploads before it journals AddSst, so the
            // name must already be durable (as compaction does) or a crash in
            // that window leaves an orphan whose name the next flush reuses.
            // Names come from a cursor inside a durably reserved block, so
            // most flushes reserve nothing (#491). A restart resumes at the
            // durable reservation, past every name this session handed out.
            self.reserve_sst_name_durably_for(cf_id, sst_seq, origin)?;
        }
        self.consume_sst_name(cf_id, sst_seq)?;
        Ok(sst_seq)
    }

    pub(super) fn ensure_flush_sst_identity(
        &mut self,
        identity: FlushIdentity,
    ) -> crate::common::MidgeResult<(String, u64)> {
        let (cf_id, flush) = self
            .state
            .immutable_flush_by_id(identity.flush_id)
            .ok_or_else(|| {
                crate::common::MidgeError::Fenced(format!(
                    "flush {} lost immutable ownership before name reservation",
                    identity.flush_id
                ))
            })?;
        if cf_id != identity.cf_id
            || flush.writer_epoch != identity.writer_epoch
            || flush.sequence != identity.sequence
        {
            return Err(crate::common::MidgeError::Fenced(
                "flush identity changed before name reservation".into(),
            ));
        }
        match (&flush.sst_name, flush.sst_seq) {
            (Some(name), Some(seq)) if *name == crate::cloud_layout::file_name(cf_id, 0, seq) => {
                return Ok((name.clone(), seq));
            }
            (None, None) => {}
            _ => {
                return Err(crate::common::MidgeError::Internal(
                    "flush has an inconsistent canonical SST identity".into(),
                ));
            }
        }
        if self.publication.publication_gate.is_active() {
            return Err(crate::common::MidgeError::Busy(
                "SST name reservation waits for the publication owner".into(),
            ));
        }
        // Reserve while this event-loop turn owns metadata, before build can
        // overlap compute. A later publisher may park the build without a new
        // journal append or mirror. Retries retain this canonical identity.
        let origin = flush.accounting.origin;
        let sst_seq = self.reserve_flush_sst_seq_for(cf_id, origin)?;
        let sst_name = crate::cloud_layout::file_name(cf_id, 0, sst_seq);
        let (_, flush) = self
            .state
            .immutable_flush_by_id_mut(identity.flush_id)
            .expect("immutable ownership is stable during the event-loop turn");
        flush.sst_name = Some(sst_name.clone());
        flush.sst_seq = Some(sst_seq);
        Ok((sst_name, sst_seq))
    }

    pub(super) fn prepare_flush_publication(
        &mut self,
        identity: FlushIdentity,
        staging_path: std::path::PathBuf,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
        file_meta: crate::runtime::FileMeta,
    ) {
        let (sst_name, sst_seq) = match self.ensure_flush_sst_identity(identity) {
            Ok(identity) => identity,
            Err(error) => {
                self.fail_flush_pipeline(identity.flush_id, reservation, &error);
                return;
            }
        };
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
                self.fail_flush_pipeline(identity.flush_id, reservation, &error);
                return;
            };
            flush.sst_name = Some(sst_name);
            flush.sst_seq = Some(sst_seq);
            flush.built = Some(build.clone());
            flush.phase = ImmutableFlushPhase::Built;
            flush.clone()
        };
        // Inline compatibility publication still serializes its live-file
        // mirror. Background workers publish captured cuts independently.
        if self.pipeline.inline_flush_worker && self.publication.publication_gate.is_active() {
            if let Some((_, current)) = self.state.immutable_flush_by_id_mut(identity.flush_id) {
                current.phase = ImmutableFlushPhase::Queued;
            }
            return;
        }
        if let Some((_, current)) = self.state.immutable_flush_by_id_mut(identity.flush_id) {
            current.phase = ImmutableFlushPhase::Publishing;
        }
        self.schedule_flush_publication(&flush, build);
    }

    pub(super) fn handle_flush_publish_completion(
        &mut self,
        completion: FlushPublishCompletion,
    ) -> bool {
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
                );
                return true;
            }
            self.state
                .finish_flush_publication_attempt(completion.identity.flush_id, true);
            self.settle_stale_publish_reservation(&completion);
            self.actors
                .flush_actor
                .finish_pipeline(completion.identity.flush_id);
            self.publication.publication_gate.release(
                &crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush {
                    flush_id: completion.identity.flush_id,
                },
            );
            self.fail_flush_waiters(
                completion.identity.cf_id,
                completion.identity.sequence,
                &error,
            );
            return false;
        }
        match completion.result {
            Ok(delta) => {
                let frozen_at = self
                    .state
                    .immutable_flush_by_id(completion.identity.flush_id)
                    .map(|(_, flush)| flush.started_at);
                if let Err(error) = self.commit_flush_metadata(&delta) {
                    self.state.fence_metadata_until_reloaded();
                    self.fail_flush_pipeline(
                        completion.identity.flush_id,
                        completion.reservation,
                        &error,
                    );
                    return true;
                }
                if let Some(frozen_at) = frozen_at {
                    self.state
                        .diagnostics
                        .record(|m| m.record_flush_latency(frozen_at.elapsed()));
                }
                if let Err(error) = self.submit_flush_mirror(delta, completion.reservation) {
                    self.fail_flush_pipeline(
                        completion.identity.flush_id,
                        completion.reservation,
                        &error,
                    );
                    return true;
                }
                return false;
            }
            Err(error) => self.fail_flush_pipeline(
                completion.identity.flush_id,
                completion.reservation,
                &error,
            ),
        }
        true
    }

    pub(super) fn commit_flush_metadata(
        &mut self,
        delta: &FlushPublicationDelta,
    ) -> crate::common::MidgeResult<()> {
        self.validate_flush_completion(delta.identity)?;
        let origin = self
            .state
            .immutable_flush_by_id(delta.identity.flush_id)
            .expect("validated flush immutable exists")
            .1
            .accounting
            .origin;
        self.state.record_flush_publication_intent(
            delta.identity.cf_id,
            delta.identity.sequence,
            &delta.file_meta,
        )?;
        crate::failpoints::fail_point!("midge::flush::after_publication_intent_before_manifest");
        self.check_lease_health()?;
        self.state
            .commit_flush_publication_for(FlushManifestPublication {
                origin,
                cf_id: delta.identity.cf_id,
                sequence: delta.identity.sequence,
                file_meta: &delta.file_meta,
                next_sst_seq: delta.next_sst_seq,
                require_snapshot: self.actors.cloud_coordinator.hybrid_storage.is_some(),
            })?;
        Ok(())
    }

    pub(super) fn submit_flush_mirror(
        &mut self,
        delta: FlushPublicationDelta,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
    ) -> crate::common::MidgeResult<()> {
        let metadata_cut = if !self.pipeline.inline_flush_worker
            && self
                .actors
                .cloud_coordinator
                .cloud_metadata_storage
                .is_some()
        {
            let deadline = crate::common::OperationDeadline::from_budget(
                self.routing.runtime_response_timeout,
            );
            Some(
                self.publication
                    .metadata_publication_lock
                    .capture_control_cut(&self.state.fs, delta.identity.writer_epoch, &deadline)?,
            )
        } else {
            None
        };
        let task = FlushMirrorTask {
            delta,
            reservation,
            fs: Arc::clone(&self.state.fs),
            metadata_cut,
            storage: Arc::new(crate::runtime::actors::flush::HybridFlushStorage::new(
                self.actors.cloud_coordinator.hybrid_storage.clone(),
                self.actors.cloud_coordinator.cloud_metadata_storage.clone(),
            )),
            metadata_publication_lock: self.publication.metadata_publication_lock.clone(),
            lease_validity: self.authority.fencing.lease_validity.clone(),
            lease_healthy: self.authority.fencing.lease_healthy.clone(),
            leader_store: self.authority.fencing.leader_store.clone(),
            leader_holder_id: self.authority.fencing.leader_holder_id.clone(),
            runtime_response_timeout: self.routing.runtime_response_timeout,
        };
        self.actors.flush_actor.submit_mirror(task)
    }

    pub(super) fn handle_flush_mirror_completion(
        &mut self,
        completion: FlushMirrorCompletion,
    ) -> bool {
        if let Err(error) = self.validate_flush_completion(completion.delta.identity) {
            self.fail_flush_pipeline(
                completion.delta.identity.flush_id,
                completion.reservation,
                &error,
            );
            return true;
        }
        match completion.result {
            Ok(cloud_metadata_published) => {
                self.publication.publication_gate.release(
                    &crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush {
                        flush_id: completion.delta.identity.flush_id,
                    },
                );
                self.install_flush_publication(
                    &completion.delta,
                    completion.reservation,
                    cloud_metadata_published,
                );
            }
            Err(error) => self.fail_flush_pipeline(
                completion.delta.identity.flush_id,
                completion.reservation,
                &error,
            ),
        }
        true
    }

    pub(super) fn validate_flush_completion(
        &self,
        identity: FlushIdentity,
    ) -> crate::common::MidgeResult<()> {
        self.check_lease_health()?;
        if identity.writer_epoch != self.authority.fencing.writer_epoch {
            return Err(crate::common::MidgeError::Fenced(format!(
                "flush {} epoch {} does not match runtime epoch {}",
                identity.flush_id, identity.writer_epoch, self.authority.fencing.writer_epoch
            )));
        }
        if let Some(store) = &self.authority.fencing.leader_store {
            store
                .validate_epoch(
                    self.authority
                        .fencing
                        .leader_holder_id
                        .as_deref()
                        .unwrap_or_default(),
                    identity.writer_epoch,
                )
                .map_err(|error| {
                    error.into_validation_error(&format!(
                        "flush {} writer validation",
                        identity.flush_id
                    ))
                })?;
        }
        self.check_lease_health()?;
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

    pub(super) fn install_flush_publication(
        &mut self,
        delta: &FlushPublicationDelta,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
        cloud_metadata_published: bool,
    ) {
        let Some((cf_id, flush)) = self.state.immutable_flush_by_id(delta.identity.flush_id) else {
            if let (Some(hybrid), Some(token)) =
                (&self.actors.cloud_coordinator.hybrid_storage, reservation)
            {
                hybrid.flush_completed_with_token(token, delta.file_meta.size_bytes);
            }
            self.actors
                .flush_actor
                .finish_pipeline(delta.identity.flush_id);
            return;
        };
        let accounting = flush.accounting;
        let frozen = Arc::clone(&flush.memtable);
        let observed_attempt = self
            .state
            .finish_flush_publication_attempt(delta.identity.flush_id, false);
        if let Some(removed_size) = self.state.complete_immutable_flush(cf_id, &frozen) {
            if let Some(started) = accounting.started {
                self.state.metadata_accounting().flush_committed(
                    accounting.origin,
                    self.state.metadata_medium(),
                    delta.file_meta.size_bytes,
                    started.elapsed(),
                );
            } else {
                self.state
                    .metadata_accounting()
                    .invalidate_missing_publication_start();
            }
            if !observed_attempt {
                self.state
                    .metadata_accounting()
                    .invalidate_missing_publication_start();
            }
            self.state.total_memtable_bytes =
                self.state.total_memtable_bytes.saturating_sub(removed_size);
        } else {
            self.state
                .metadata_accounting()
                .invalidate_missing_publication_start();
        }
        if let Err(error) = self.refresh_cloud_flush_headroom() {
            tracing::warn!(%error, "flush staging headroom awaits output retirement");
        }
        if let (Some(hybrid), Some(token)) =
            (&self.actors.cloud_coordinator.hybrid_storage, reservation)
        {
            hybrid.flush_completed_with_token(token, delta.file_meta.size_bytes);
        }
        if cloud_metadata_published {
            self.evict_published_sst_cache(std::slice::from_ref(&delta.file_meta.name));
        }
        self.actors
            .flush_actor
            .finish_pipeline(delta.identity.flush_id);
        self.publish_snapshot();
        self.refresh_write_stall_timing();
        self.wake_write_stall_waiters();
        self.complete_flush_waiters(cf_id);
        self.schedule_compaction_after_flush_publication(&delta.file_meta.name);
        crate::failpoints::fail_point!("midge::flush_worker::before_wal_prune");
        if cloud_metadata_published {
            self.prune_cloud_wal_segments_covered_by_manifest();
        } else {
            self.prune_local_wal_segments_covered_by_manifest();
        }
    }
}
