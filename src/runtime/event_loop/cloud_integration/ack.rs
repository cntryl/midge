//! Cloud acknowledgement, failure, and backpressure handling.

use super::super::durability_sync::CompletionSource;
use super::super::EventLoop;
use crate::common::OperationDeadline;
use crate::runtime::hybrid_persistence::HybridPersistence;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;

impl EventLoop {
    pub(crate) fn tick_hybrid_storage(&mut self) {
        self.tick_hybrid_storage_with_deadline(None);
    }

    pub(in crate::runtime::event_loop) fn tick_hybrid_storage_within(
        &mut self,
        deadline: &OperationDeadline,
    ) {
        self.tick_hybrid_storage_with_deadline(Some(deadline));
    }

    fn tick_hybrid_storage_with_deadline(&mut self, deadline: Option<&OperationDeadline>) {
        self.reap_cloud_wal_prune_worker();
        let Some(storage) = &self.hybrid_storage else {
            return;
        };

        // Drive async storage uploads.
        // In push-channel mode, completion events are delivered via `hybrid_storage_events`.
        // In polling mode, `process_uploads()` returns completion events.
        let storage_events = storage.process_uploads();
        for event in storage_events {
            self.handle_storage_event_with_deadline(event, deadline);
        }
        self.wake_write_stall_waiters();
    }

    pub(crate) fn drain_hybrid_storage_events(&mut self) {
        self.drain_hybrid_storage_events_with_deadline(None);
    }

    pub(in crate::runtime::event_loop) fn drain_hybrid_storage_events_within(
        &mut self,
        deadline: &OperationDeadline,
    ) {
        self.drain_hybrid_storage_events_with_deadline(Some(deadline));
    }

    fn drain_hybrid_storage_events_with_deadline(&mut self, deadline: Option<&OperationDeadline>) {
        let Some(rx) = &self.hybrid_storage_events else {
            return;
        };

        let rx = rx.clone();

        while let Ok(event) = rx.try_recv() {
            self.handle_storage_event_with_deadline(event, deadline);
        }
    }

    pub(crate) fn handle_storage_event(&mut self, event: crate::storage::StorageEvent) {
        self.handle_storage_event_with_deadline(event, None);
    }

    fn handle_storage_event_with_deadline(
        &mut self,
        event: crate::storage::StorageEvent,
        deadline: Option<&OperationDeadline>,
    ) {
        match event {
            crate::storage::StorageEvent::CloudAck {
                segment_id,
                max_sequence,
            } => {
                self.handle_storage_event_cloud_ack(segment_id, max_sequence, deadline);
            }
            crate::storage::StorageEvent::CloudFail {
                segment_id,
                error,
                terminal,
                failure_kind,
            } => {
                if terminal {
                    let error = match failure_kind {
                        crate::storage::CloudUploadFailureKind::Timeout => {
                            crate::common::MidgeError::Timeout(format!(
                                "Cloud durability timed out after storage retries: {error}"
                            ))
                        }
                        crate::storage::CloudUploadFailureKind::Other => {
                            crate::common::MidgeError::Internal(format!(
                                "Cloud durability failed after storage retries: {error}"
                            ))
                        }
                    };
                    self.handle_cloud_upload_failure(segment_id, &error, true);
                } else {
                    tracing::warn!(
                        segment_id,
                        error = %error,
                        "Cloud WAL upload attempt failed; storage queue retains retry ownership"
                    );
                }
            }
            crate::storage::StorageEvent::CloudWalPruneComplete { segment_id, result } => {
                self.handle_storage_event_cloud_wal_prune_complete(segment_id, result);
            }
            crate::storage::StorageEvent::CloudWalPruneAttemptFailed { segment_id, error } => {
                self.cloud_wal.prune_inflight.remove(&segment_id);
                tracing::debug!(
                    segment_id,
                    error = %error,
                    "Cloud WAL prune preflight failed; retaining authority for retry"
                );
            }
            crate::storage::StorageEvent::BackpressureOn => {
                tracing::warn!("storage backpressure activated — pausing flushes");
                self.state.set_write_stalled(true);
            }
            crate::storage::StorageEvent::BackpressureOff => {
                tracing::info!("storage backpressure released — resuming normal operation");
                if self.state.write_stalled() {
                    self.state.set_write_stalled(false);
                }
                self.wake_write_stall_waiters();
                self.drain_auto_flush_memtables();
            }
            _ => {}
        }
    }

    fn defer_cloud_ack_for_memory(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
        error: &crate::common::MidgeError,
        deadline: &OperationDeadline,
    ) -> bool {
        let contention = self
            .hybrid_storage
            .as_ref()
            .and_then(|storage| storage.maintenance_memory())
            .and_then(|budget| budget.take_contention(error));
        if !deadline.is_expired()
            && contention.is_some_and(|contention| {
                self.hybrid_storage
                    .as_ref()
                    .and_then(|storage| storage.maintenance_memory())
                    .is_some_and(|budget| contention.is_blocked_by(&budget))
            })
        {
            // Shared maintenance pressure is backpressure, not a failed
            // accepted write. Keep its waiter and sealed local WAL while
            // the existing upload backlog owns a bounded retry.
            self.cloud_wal_prune_progress.discard_idle_proofs();
            self.cloud_wal
                .upload_backlog
                .insert(segment_id, max_sequence);
            self.cloud_wal.defer_upload_retry();
            tracing::debug!(segment_id, %error, "deferring WAL catalog publication for shared memory");
            return true;
        }
        false
    }

    fn handle_storage_event_cloud_ack(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
        attempt_deadline: Option<&OperationDeadline>,
    ) {
        let deadline = attempt_deadline.copied().unwrap_or_else(|| {
            self.cloud_ack_deadline(segment_id)
                .unwrap_or_else(OperationDeadline::unbounded)
        });
        if let Err(error) = self.validate_runtime_writer_lease_within(&deadline) {
            self.handle_cloud_upload_failure(
                segment_id,
                &Self::cloud_ack_error(
                    "writer lease validation failed before cloud WAL acknowledgement",
                    error,
                ),
                true,
            );
            return;
        }
        if let Err(error) =
            self.verify_remote_wal_segment_before_ack(segment_id, max_sequence, &deadline)
        {
            if self.defer_cloud_ack_for_memory(segment_id, max_sequence, &error, &deadline) {
                return;
            }
            self.handle_cloud_upload_failure(
                segment_id,
                &Self::cloud_ack_error("cloud WAL readback validation failed", error),
                true,
            );
            return;
        }
        if let Err(error) = self.validate_runtime_writer_lease_within(&deadline) {
            self.handle_cloud_upload_failure(
                segment_id,
                &Self::cloud_ack_error(
                    "writer lease validation failed after cloud WAL publication",
                    error,
                ),
                true,
            );
            return;
        }

        self.state.cloud.pending_uploads.retain(|item| {
            crate::wal::parse_segment_id(item).is_none_or(|pending| pending != segment_id)
        });
        self.cloud_wal
            .acked_segments
            .insert(segment_id, max_sequence);
        if let Err(error) = self.wal_transition.note_acknowledged(segment_id) {
            if self.state.wal.cloud_durable_seq >= max_sequence {
                tracing::debug!(segment_id, "ignored duplicate cloud WAL acknowledgement");
                return;
            }
            self.handle_cloud_upload_failure(segment_id, &error, true);
            return;
        }

        let ready_segments = match self
            .durability
            .contiguous_acked_cloud_segments(&self.cloud_wal.acked_segments)
        {
            Ok(ready_segments) => ready_segments,
            Err(error) => {
                self.cloud_wal.acked_segments.remove(&segment_id);
                self.handle_cloud_upload_failure(
                    segment_id,
                    &crate::common::MidgeError::Internal(error),
                    true,
                );
                return;
            }
        };

        let Some((durable_segment_id, durable_max_sequence)) = ready_segments.last().copied()
        else {
            tracing::debug!(
                segment_id,
                max_sequence,
                "CloudAck buffered behind an earlier unacked WAL segment"
            );
            return;
        };

        self.commit_contiguous_cloud_ack(&ready_segments, durable_segment_id, durable_max_sequence);
    }

    fn commit_contiguous_cloud_ack(
        &mut self,
        ready_segments: &[(u64, u64)],
        durable_segment_id: u64,
        durable_max_sequence: u64,
    ) {
        if let Err(error) = self.wal_transition.begin_ack(durable_segment_id) {
            self.fence_cloud_ack_transition(ready_segments, &error);
            return;
        }
        if let Err(error) = WalTransitionBoundary::BeforeAccountingTransfer.check() {
            self.fence_cloud_ack_transition(ready_segments, &error);
            return;
        }

        self.state.wal.cloud_durable_seq =
            self.state.wal.cloud_durable_seq.max(durable_max_sequence);
        tracing::debug!(
            segment_id = durable_segment_id,
            cloud_durable_seq = self.state.wal.cloud_durable_seq,
            "Cloud upload complete"
        );
        if let Err(error) = WalTransitionBoundary::AfterAccountingTransfer.check() {
            self.fence_cloud_ack_transition(ready_segments, &error);
            return;
        }

        if let Err(error) = WalTransitionBoundary::BeforeWaiterCompletion.check() {
            self.fence_cloud_ack_transition(ready_segments, &error);
            return;
        }
        for (seg_id, _) in ready_segments {
            let waiters = self.durability.complete_waiters_at(*seg_id);
            self.complete_durability_waiters(waiters, CompletionSource::CloudAck);
            if let Err(error) = self.wal_transition.note_cloud_durable(*seg_id) {
                self.fence_cloud_ack_transition(ready_segments, &error);
                return;
            }
        }

        if let Err(error) = self.wal_transition.finish_ack(durable_segment_id) {
            self.fence_cloud_ack_transition(ready_segments, &error);
            return;
        }

        for (seg_id, _) in ready_segments {
            if let Some(enqueued_at) = self.durability.retire_cloud_segment(*seg_id) {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_ack_latency_us(
                        Self::elapsed_micros_to_u64(enqueued_at.elapsed()),
                    );
                }
            }
        }

        for (ready_segment_id, _) in ready_segments {
            if self.remove_cloud_durable_local_wal_segment(*ready_segment_id) {
                if let Err(error) = self.wal_transition.retire_cloud_durable(*ready_segment_id) {
                    self.state.mark_persistence_anomaly();
                    self.wal_actor
                        .fence_transition(&mut self.state, error.to_string());
                    self.wal_transition
                        .fence(error.to_string(), Some(*ready_segment_id));
                    tracing::error!(%error, segment_id = *ready_segment_id, "cloud WAL obligation retirement fenced");
                    return;
                }
            }
        }
        if let Err(error) = WalTransitionBoundary::AfterCommitBeforeReturn.check() {
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "cloud WAL acknowledgement committed before injected return boundary");
        }
        self.prune_cloud_wal_segments_covered_by_manifest();
        self.drain_auto_flush_memtables();
    }

    fn fence_cloud_ack_transition(
        &mut self,
        ready_segments: &[(u64, u64)],
        error: &crate::common::MidgeError,
    ) {
        let first_segment = ready_segments.first().map(|(segment_id, _)| *segment_id);
        self.state.mark_persistence_anomaly();
        self.wal_actor
            .fence_transition(&mut self.state, error.to_string());
        self.wal_transition.fence(error.to_string(), first_segment);
        if let Some(first_segment) = first_segment {
            let waiters = self.durability.drain_waiters_at_or_after(first_segment);
            self.fail_durability_waiters(waiters, error);
        }
        tracing::error!(%error, ?first_segment, "cloud WAL acknowledgement transition fenced");
    }

    fn handle_storage_event_cloud_wal_prune_complete(
        &mut self,
        segment_id: u64,
        result: crate::storage::StorageOutcome<()>,
    ) {
        self.cloud_wal.prune_inflight.remove(&segment_id);
        match result {
            crate::storage::StorageOutcome::Ok(()) => {
                self.cloud_wal.acked_segments.remove(&segment_id);
                self.next_background_compaction_check = std::time::Instant::now();
                tracing::debug!(segment_id, "Pruned cloud-covered remote WAL segment");
            }
            crate::storage::StorageOutcome::Err(error) => {
                // The authoritative catalog entry is retired before physical
                // deletion is attempted. A failed delete is therefore a safe
                // storage leak, not a recovery obligation that may be retried
                // through a now-absent catalog entry.
                self.cloud_wal.acked_segments.remove(&segment_id);
                self.state.mark_persistence_anomaly();
                tracing::warn!(
                    segment_id,
                    error = %error,
                    "Cloud WAL authority was retired but physical deletion failed; retaining an ignored orphan"
                );
            }
        }
    }

    /// Shared cloud budget for the work that answers `segment_id`.
    ///
    /// Derived from the latest live caller waiting on this segment or on a
    /// later segment that depends on this frontier gap. Time already spent
    /// queued is charged against that caller's budget, while an older expired
    /// caller cannot prematurely fail a newer dependent waiter. Once every
    /// caller has abandoned, the accepted WAL obligation continues through
    /// bounded maintenance attempts so provider latency cannot monopolize the
    /// event loop.
    pub(super) fn cloud_ack_deadline(&self, segment_id: u64) -> Option<OperationDeadline> {
        let request_ids = self
            .durability
            .cloud_durability_request_ids_at_or_after(segment_id);
        if request_ids.is_empty() {
            return Some(OperationDeadline::from_budget(
                self.runtime_response_timeout,
            ));
        }
        let latest_start = request_ids
            .iter()
            .filter_map(|request_id| self.router.registered_at(*request_id))
            .max();
        latest_start.map_or_else(
            || {
                Some(OperationDeadline::from_budget(
                    self.runtime_response_timeout,
                ))
            },
            |latest_start| {
                Some(OperationDeadline::from_start(
                    latest_start,
                    self.runtime_response_timeout,
                ))
            },
        )
    }

    fn verify_remote_wal_segment_before_ack(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
        deadline: &OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let Some(storage) = self.hybrid_storage.as_ref() else {
            return Err(crate::common::MidgeError::Internal(
                "CloudAck received without HybridStorage".to_string(),
            ));
        };
        let local_path = self
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        storage.publish_remote_wal_segment(
            segment_id,
            max_sequence,
            &local_path,
            self.state.writer_epoch,
            deadline,
        )
    }

    fn cloud_ack_error(
        context: &str,
        error: crate::common::MidgeError,
    ) -> crate::common::MidgeError {
        match error {
            crate::common::MidgeError::Timeout(message) => {
                crate::common::MidgeError::Timeout(format!("{context}: {message}"))
            }
            crate::common::MidgeError::Fenced(message) => {
                crate::common::MidgeError::Fenced(format!("{context}: {message}"))
            }
            other => crate::common::MidgeError::Internal(format!("{context}: {other}")),
        }
    }

    fn handle_cloud_upload_failure(
        &mut self,
        segment_id: u64,
        error: &crate::common::MidgeError,
        requeue_publication: bool,
    ) {
        self.state.cloud.pending_uploads.retain(|item| {
            crate::wal::parse_segment_id(item).is_none_or(|pending| pending != segment_id)
        });
        self.state.mark_persistence_anomaly();
        self.cloud_wal.acked_segments.remove(&segment_id);
        if let Err(registration_error) = self.wal_transition.note_requeued(segment_id) {
            self.wal_actor
                .fence_transition(&mut self.state, registration_error.to_string());
            self.wal_transition
                .fence(registration_error.to_string(), Some(segment_id));
            tracing::error!(%registration_error, segment_id, "cloud WAL failure could not preserve transition ownership");
        }

        // Keep the segment in the inflight frontier and preserve its request
        // identities. The accepted local WAL remains owned for callerless
        // retry, so invalidating those identities would let a client retry
        // create a second mutation that could also become durable.
        let failed_max_sequence = self.durability.cloud_segment_max_sequence(segment_id);

        // Let WAL actor handle its internal failure handling and drop pending writes.
        tracing::error!(segment_id, error = %error, "Cloud upload failed");

        if requeue_publication {
            if let Some(max_seq) = failed_max_sequence {
                let local_path = self
                    .state
                    .wal_dir
                    .join(crate::wal::segment_file_name(segment_id));
                if local_path.exists() {
                    self.cloud_wal.upload_backlog.insert(segment_id, max_seq);
                    self.cloud_wal.defer_upload_retry();
                } else {
                    tracing::error!(
                        segment_id,
                        path = %local_path.display(),
                        "could not requeue failed cloud WAL publication because its local segment is missing"
                    );
                }
            } else {
                tracing::error!(
                    segment_id,
                    "could not requeue failed cloud WAL publication because its inflight sequence maximum is unknown"
                );
            }
        }

        // A failure at this segment blocks its own and every later cloud
        // durability generation, but an earlier sealed generation can still
        // close independently when its acknowledgement arrives.
        let waiters = self.durability.drain_waiters_at_or_after(segment_id);
        self.fail_durability_waiters(waiters, error);

        // Keep all inflight segments. A later ACK may already be buffered, but
        // it cannot advance the frontier until this failed segment is retried
        // successfully.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::MidgeError;
    use crate::runtime::event_loop::tests::create_test_cloud_event_loop;

    #[test]
    fn should_reject_unrelated_resource_failure_when_shared_budget_is_in_use() {
        // Arrange
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )
        .unwrap();
        let budget = el
            .hybrid_storage
            .as_ref()
            .unwrap()
            .maintenance_memory()
            .unwrap();
        let _held = budget.reserve(1, "unrelated retained memory").unwrap();
        let error = MidgeError::ResourceLimit("identity space exhausted".into());

        // Act
        let deferred = el.defer_cloud_ack_for_memory(1, 1, &error, &OperationDeadline::unbounded());

        // Assert
        assert!(!deferred);
        assert!(el.cloud_wal.upload_backlog.is_empty());
    }

    #[test]
    fn should_reject_oversized_reservation_when_shared_budget_is_in_use() {
        // Arrange
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )
        .unwrap();
        let budget = el
            .hybrid_storage
            .as_ref()
            .unwrap()
            .maintenance_memory()
            .unwrap();
        let _held = budget.reserve(1, "retained memory").unwrap();
        let error = budget
            .reserve(budget.limit() + 1, "oversized catalog")
            .unwrap_err();

        // Act
        let deferred = el.defer_cloud_ack_for_memory(1, 1, &error, &OperationDeadline::unbounded());

        // Assert
        assert!(!deferred);
        assert!(el.cloud_wal.upload_backlog.is_empty());
    }

    #[test]
    fn should_reject_contention_when_it_belongs_to_another_budget() {
        // Arrange
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )
        .unwrap();
        let shared = el
            .hybrid_storage
            .as_ref()
            .unwrap()
            .maintenance_memory()
            .unwrap();
        let _shared_held = shared
            .reserve(shared.limit(), "shared maintenance")
            .unwrap();
        let other =
            crate::common::resource_budget::ResourceBudget::new(10).with_contention_errors();
        let _other_held = other.reserve(10, "other pool").unwrap();
        let error = other.reserve(1, "other request").unwrap_err();

        // Act
        let deferred = el.defer_cloud_ack_for_memory(1, 1, &error, &OperationDeadline::unbounded());

        // Assert
        assert!(!deferred);
        assert!(el.cloud_wal.upload_backlog.is_empty());
    }

    #[test]
    fn should_reject_contention_when_ack_deadline_has_expired() {
        // Arrange
        let mut el = create_test_cloud_event_loop(
            crate::storage::hybrid::policy::StorageBudgetPolicy::default(),
        )
        .unwrap();
        let budget = el
            .hybrid_storage
            .as_ref()
            .unwrap()
            .maintenance_memory()
            .unwrap()
            .with_contention_errors();
        let _held = budget
            .reserve(budget.limit(), "active maintenance")
            .unwrap();
        let error = budget.reserve(1, "catalog request").unwrap_err();
        let deadline = OperationDeadline::from_budget(std::time::Duration::ZERO);

        // Act
        let deferred = el.defer_cloud_ack_for_memory(1, 1, &error, &deadline);

        // Assert
        assert!(!deferred);
        assert!(el.cloud_wal.upload_backlog.is_empty());
    }
}
