use super::{Duration, EventLoop, FlushPublishCompletion, FsError, RuntimeResponse};

impl EventLoop {
    pub(super) fn settle_stale_publish_reservation(&mut self, completion: &FlushPublishCompletion) {
        let (Some(hybrid), Some(token)) = (
            self.actors.cloud_coordinator.hybrid_storage.clone(),
            completion.reservation,
        ) else {
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
            crate::sst::fs::sst_location(name),
        ];
        let primary_absent = paths.iter().all(|path| {
            self.runtime_fs_path(path)
                .and_then(|path| self.state.fs.exists(&path).map_err(FsError::into_midge))
                .is_ok_and(|exists| !exists)
        });
        if !primary_absent {
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

    pub(in crate::runtime::event_loop) fn fail_flush_pipeline(
        &mut self,
        flush_id: u64,
        reservation: Option<crate::storage::hybrid::actor::StorageReservationToken>,
        error: &crate::common::MidgeError,
    ) {
        self.forget_flush_mirror_retry(flush_id);
        self.state.finish_flush_publication_attempt(flush_id, true);
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
                self.actors.cloud_coordinator.hybrid_storage.as_ref(),
                reservation,
            );
        }
        self.actors.flush_actor.finish_pipeline(flush_id);
        self.publication.publication_gate.release(
            &crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush { flush_id },
        );
        let retry_after = self.state.mark_immutable_flush_failed(flush_id);
        tracing::warn!(flush_id, ?retry_after, %error, "flush pipeline failed; immutable retained");
        if let Some((cf_id, sequence)) = identity {
            self.fail_flush_waiters(cf_id, sequence, error);
            self.fail_deferred_column_family_drops(cf_id, error);
        }
        self.refresh_write_stall_timing();
    }

    pub(super) fn fail_deferred_column_family_drops(
        &mut self,
        cf_id: u32,
        error: &crate::common::MidgeError,
    ) {
        let mut retained = std::collections::VecDeque::new();
        for message in self.publication.publication_gate.take_deferred() {
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
        for message in retained {
            self.publication.publication_gate.defer(message);
        }
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
    pub(super) fn deferred_drop_failure(
        error: &crate::common::MidgeError,
    ) -> crate::common::MidgeError {
        match error {
            crate::common::MidgeError::Busy(message) => crate::common::MidgeError::Aborted(
                format!("column family drop abandoned by a failed flush: {message}"),
            ),
            other => other.replay(),
        }
    }

    pub(in crate::runtime::event_loop) fn flush_frontier_satisfied(
        &self,
        cf_id: u32,
        frontier: u64,
    ) -> bool {
        self.state.get_cf(cf_id).is_none_or(|cf| {
            cf.immutable_flushes
                .iter()
                .all(|flush| flush.sequence > frontier)
        })
    }

    pub(super) fn complete_flush_waiters(&mut self, cf_id: u32) {
        let Some(waiters) = self.publication.flush_barrier_waiters.remove(&cf_id) else {
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
            self.publication
                .flush_barrier_waiters
                .insert(cf_id, pending);
        }
    }

    pub(super) fn fail_flush_waiters(
        &mut self,
        cf_id: u32,
        failed_sequence: u64,
        error: &crate::common::MidgeError,
    ) {
        let Some(waiters) = self.publication.flush_barrier_waiters.remove(&cf_id) else {
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
            self.publication
                .flush_barrier_waiters
                .insert(cf_id, pending);
        }
    }

    pub(in crate::runtime::event_loop) fn drain_flush_worker_results(&mut self) {
        if !self.publication.publication_gate.is_active() {
            let ready = std::mem::take(&mut self.pipeline.deferred_flush_results);
            for result in ready {
                self.handle_flush_worker_result(result);
            }
        }
        while let Ok(result) = self.pipeline.flush_worker_result_rx.try_recv() {
            self.handle_flush_worker_result(result);
        }
    }

    pub(in crate::runtime::event_loop) fn drain_inline_flush_worker(&mut self) {
        if !self.pipeline.inline_flush_worker {
            return;
        }
        while self.actors.flush_actor.is_inflight() {
            match self
                .pipeline
                .flush_worker_result_rx
                .recv_timeout(Duration::from_secs(5))
            {
                Ok(result) => self.handle_flush_worker_result(result),
                Err(_) => break,
            }
        }
    }

    pub(in crate::runtime::event_loop) fn drain_auto_flush_memtables(&mut self) -> usize {
        if self.wal_group_in_flight() {
            return 0;
        }
        if self.state.is_memory_mode() || self.shutdown.shutting_down {
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
        let rule = if self.actors.wal_actor.is_cloud_async() {
            crate::runtime::state::EventualFlush::SegmentGap
        } else {
            crate::runtime::state::EventualFlush::WalBytes
        };
        while let Some(candidate) = self.state.next_flush_candidate_skipping(
            rule,
            &attempted_cfs,
            self.actors
                .cloud_coordinator
                .cloud_wal
                .oldest_published_facts(),
        ) {
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

    pub(in crate::runtime::event_loop) fn refresh_write_stall_timing(&mut self) {
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
