//! Cloud WAL segment sealing and flush scheduling.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::HybridPersistence;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use std::time::Instant;

struct CloudSealPlan {
    segment_id: u64,
    next_segment_id: u64,
    expected_max_sequence: u64,
    bytes_buffered: u64,
}

impl EventLoop {
    pub(crate) fn seal_current_cloud_segment(
        &mut self,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        self.seal_current_cloud_segment_inner(false, &crate::common::OperationDeadline::unbounded())
    }

    pub(in crate::runtime::event_loop) fn seal_current_cloud_segment_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        self.seal_current_cloud_segment_inner(false, deadline)
    }

    pub(in crate::runtime::event_loop) fn seal_recovered_cloud_active_segment(
        &mut self,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        self.seal_current_cloud_segment_inner(true, &crate::common::OperationDeadline::unbounded())
    }

    fn seal_current_cloud_segment_inner(
        &mut self,
        recovered_active: bool,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        let result = self.try_seal_current_cloud_segment(recovered_active, deadline);
        if result.is_err() && self.durability.cloud_seal_retry_needed() {
            self.durability.defer_cloud_seal_retry();
        }
        result
    }

    fn try_seal_current_cloud_segment(
        &mut self,
        recovered_active: bool,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        let Some(plan) = self.prepare_cloud_seal(recovered_active, deadline)? else {
            return Ok(None);
        };
        let CloudSealPlan {
            segment_id,
            next_segment_id,
            expected_max_sequence,
            bytes_buffered,
        } = plan;
        let ticket =
            self.wal_transition
                .begin_seal(segment_id, next_segment_id, expected_max_sequence)?;
        let seal_start = Instant::now();
        let max_sequence =
            match self
                .wal_actor
                .flush_for_cloud_upload_within(&mut self.state, deadline, &ticket)
            {
                Ok(max_sequence) => max_sequence,
                Err(error) => {
                    self.settle_failed_seal_step(ticket, &error);
                    return Err(error);
                }
            };
        if max_sequence != expected_max_sequence {
            let error = crate::common::MidgeError::Fenced(format!(
                "cloud WAL accounting changed during seal: expected max sequence {expected_max_sequence}, flushed {max_sequence}"
            ));
            self.fence_wal_transition(&error, None);
            return Err(error);
        }
        if let Err(error) = Self::after_cloud_flush_boundary() {
            return self.cancel_reversible_cloud_seal(ticket, error);
        }
        // Revalidate after the potentially slow flush but before the active
        // file is irreversibly rotated. A failure here leaves the same active
        // segment intact for a later retry.
        if let Err(error) = self.validate_runtime_writer_lease_within(deadline) {
            return self.cancel_reversible_cloud_seal(ticket, error);
        }
        let receipt = match self.wal_actor.rotate(&mut self.state, &ticket) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.settle_failed_seal_step(ticket, &error);
                tracing::error!(error = %error, "CloudAsync: WAL rotate failed");
                return Err(error);
            }
        };
        self.commit_rotated_cloud_seal(ticket, receipt)?;

        // From this point on the sealed file is a tracked obligation. Even a
        // lease or queue failure cannot make a later segment skip it.
        crate::failpoints::fail_point!(
            "midge::cloud::inject_fail_after_wal_rotate_before_enqueue",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: cloud seal failed after WAL rotate before enqueue".to_string(),
            ))
        );
        self.try_drain_cloud_wal_upload_backlog_within(deadline)?;

        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry.metrics().record_cloud_async_wal_segment_sealed(
                bytes_buffered,
                Self::elapsed_micros_to_u64(seal_start.elapsed()),
            );
        }

        Ok(Some((segment_id, max_sequence)))
    }

    fn prepare_cloud_seal(
        &mut self,
        recovered_active: bool,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<CloudSealPlan>> {
        if !self.wal_actor.is_cloud_async() {
            return Ok(None);
        }
        let Some(storage) = self.hybrid_storage.clone() else {
            return Err(crate::common::MidgeError::Internal(
                "CloudAsync requires HybridStorage".to_string(),
            ));
        };
        if self.state.is_memory_mode() || self.state.wal.pending_writes == 0 {
            return Ok(None);
        }
        if !recovered_active && !self.cloud_wal.upload_backlog.is_empty() {
            return Err(crate::common::MidgeError::WriteStall(
                "older CloudAsync WAL segments are still awaiting upload admission".to_string(),
            ));
        }
        self.durability.mark_cloud_seal_retry_needed();
        self.validate_runtime_writer_lease_within(deadline)?;

        let segment_id = self.state.wal.current_segment_id;
        let next_segment_id = segment_id.checked_add(1).ok_or_else(|| {
            crate::common::MidgeError::ResourceLimit(
                "WAL segment identity space exhausted".to_string(),
            )
        })?;
        let expected_max_sequence = self.wal_actor.current_segment_max_sequence();
        if self.durability.current_key() != segment_id {
            let error = crate::common::MidgeError::Internal(format!(
                "durability generation drift: expected current key {segment_id}"
            ));
            self.fence_wal_transition(&error, None);
            self.fail_durability_waiters_after_generation_drift(segment_id, &error);
            return Err(error);
        }
        let bytes_buffered = self.wal_actor.bytes_since_sync() as u64;
        let local_path = self
            .state
            .wal_dir
            .join(crate::wal::segment_file_name(segment_id));
        let existing_bytes = std::fs::metadata(&local_path).map_or(0, |metadata| metadata.len());
        if !recovered_active {
            storage.ensure_wal_upload_capacity(existing_bytes.max(bytes_buffered))?;
        }
        Ok(Some(CloudSealPlan {
            segment_id,
            next_segment_id,
            expected_max_sequence,
            bytes_buffered,
        }))
    }

    /// Register the rotation receipt with every component that observes it.
    ///
    /// The receipt proves the active file was renamed, so from here every
    /// failure is post-irreversible and resolves through
    /// `finish_failed_cloud_seal_transition`: the sealed segment keeps a
    /// runtime owner, accounting is transferred, and the runtime fences.
    fn commit_rotated_cloud_seal(
        &mut self,
        ticket: crate::runtime::wal_transition::WalSealTicket,
        receipt: crate::runtime::actors::wal::WalRotationReceipt,
    ) -> crate::common::MidgeResult<()> {
        if let Err(error) = self.try_commit_rotated_cloud_seal(&ticket, receipt) {
            self.finish_failed_cloud_seal_transition(receipt, &error);
            return Err(error);
        }
        if let Err(error) = self.wal_transition.finish_seal(ticket, receipt) {
            self.finish_failed_cloud_seal_transition(receipt, &error);
            return Err(error);
        }
        Ok(())
    }

    fn try_commit_rotated_cloud_seal(
        &mut self,
        ticket: &crate::runtime::wal_transition::WalSealTicket,
        receipt: crate::runtime::actors::wal::WalRotationReceipt,
    ) -> crate::common::MidgeResult<()> {
        let segment_id = receipt.sealed_segment;
        let max_sequence = receipt.max_sequence;
        self.wal_transition.note_sealed(ticket, receipt)?;
        WalTransitionBoundary::BeforeCoordinatorCommit.check()?;
        #[cfg(feature = "failpoints")]
        if crate::failpoints::is_active("midge::cloud::inject_coordinator_drift_after_wal_rotation")
        {
            let Some(drifted_generation) = receipt.next_segment.checked_add(100) else {
                return Err(crate::common::MidgeError::ResourceLimit(
                    "injected WAL coordinator generation overflow".to_string(),
                ));
            };
            self.durability
                .rotate_from_to(segment_id, drifted_generation)?;
        }
        self.durability
            .rotate_from_to(segment_id, receipt.next_segment)?;
        WalTransitionBoundary::AfterCoordinatorCommit.check()?;
        WalTransitionBoundary::BeforeSegmentRegistration.check()?;
        self.durability
            .record_cloud_segment_inflight(segment_id, max_sequence);
        self.cloud_wal
            .upload_backlog
            .insert(segment_id, max_sequence);
        self.wal_transition.note_queued(segment_id)?;
        WalTransitionBoundary::AfterSegmentRegistration.check()?;
        WalTransitionBoundary::BeforeAccountingTransfer.check()?;
        self.wal_actor
            .complete_cloud_upload_seal(&mut self.state, receipt);
        self.durability.record_cloud_flush();
        self.durability.clear_cloud_seal_retry_needed();
        WalTransitionBoundary::AfterAccountingTransfer.check()
    }

    /// Deterministic terminal state for a cloud seal that failed after the
    /// active file was renamed.
    ///
    /// Idempotent: every step either overwrites with the same value or is a
    /// no-op once applied, so it is safe whether the failure happened before
    /// or after any individual registration step.
    pub(in crate::runtime::event_loop) fn finish_failed_cloud_seal_transition(
        &mut self,
        receipt: crate::runtime::actors::wal::WalRotationReceipt,
        error: &crate::common::MidgeError,
    ) {
        let segment_id = receipt.sealed_segment;
        let max_sequence = receipt.max_sequence;
        self.durability
            .record_cloud_segment_inflight(segment_id, max_sequence);
        self.cloud_wal
            .upload_backlog
            .insert(segment_id, max_sequence);
        if let Err(registration_error) = self.wal_transition.note_queued(segment_id) {
            tracing::error!(%registration_error, segment_id, "failed to retain sealed WAL transition obligation");
        }
        self.wal_actor
            .complete_cloud_upload_seal(&mut self.state, receipt);
        self.durability.record_cloud_flush();
        self.durability.clear_cloud_seal_retry_needed();
        self.fence_wal_transition(error, Some(segment_id));
        self.fail_durability_waiters_after_generation_drift(receipt.next_segment, error);
        tracing::error!(
            %error,
            segment_id,
            next_segment_id = receipt.next_segment,
            "CloudAsync WAL seal fenced after irreversible rotation"
        );
    }

    fn cancel_reversible_cloud_seal(
        &mut self,
        ticket: crate::runtime::wal_transition::WalSealTicket,
        original_error: crate::common::MidgeError,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        if let Err(cancel_error) = self.wal_actor.cancel_reversible_cloud_flush(&ticket) {
            let segment_id = ticket.segment_id();
            self.fence_wal_transition(&cancel_error, None);
            self.fail_durability_waiters_after_generation_drift(segment_id, &cancel_error);
            return Err(cancel_error);
        }
        self.wal_transition.abandon_prepared_seal(ticket);
        Err(original_error)
    }

    fn after_cloud_flush_boundary() -> crate::common::MidgeResult<()> {
        crate::failpoints::fail_point!(
            "midge::cloud::inject_fail_after_wal_flush_before_rotate",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: cloud seal failed after WAL flush before rotate".to_string(),
            ))
        );
        Ok(())
    }

    fn try_drain_cloud_wal_upload_backlog_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let Some(storage) = self.hybrid_storage.clone() else {
            if self.cloud_wal.upload_backlog.is_empty() {
                return Ok(());
            }
            return Err(crate::common::MidgeError::Internal(
                "CloudAsync WAL upload backlog requires HybridStorage".to_string(),
            ));
        };

        if self.cloud_wal.upload_backlog.is_empty() {
            return Ok(());
        }
        if !self.cloud_wal.uploads_ready() {
            return Ok(());
        }
        self.cloud_wal.begin_upload_attempt();
        // Validate once per drain pass, not once per segment. Under a provider
        // leader store each validation is a cloud GET, and the durable guarantee
        // is not this poll: `WalPublicationCatalog::require_epoch` plus the
        // catalog compare-exchange reject a fenced writer's publish at the object
        // store regardless. This check is a fast-fail, so paying for it per
        // segment buys nothing and multiplies the stall on a deep backlog.
        self.validate_runtime_writer_lease_within(deadline)?;

        loop {
            let Some((&segment_id, &max_sequence)) =
                self.cloud_wal.upload_backlog.first_key_value()
            else {
                return Ok(());
            };
            let local_path = self
                .state
                .wal_dir
                .join(crate::wal::segment_file_name(segment_id));
            let resource = match storage.enqueue_wal_segment(segment_id, &local_path, max_sequence)
            {
                Ok(resource) => resource,
                Err(crate::common::MidgeError::WriteStall(_)) => {
                    // Capacity remains owned by already admitted uploads. Keep
                    // this segment in the runtime backlog and avoid repeatedly
                    // paying lease validation and WAL readback while the queue
                    // is still full.
                    self.cloud_wal.defer_upload_retry();
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            self.cloud_wal.upload_backlog.remove(&segment_id);
            if !self.state.cloud.pending_uploads.contains(&resource) {
                self.state.cloud.pending_uploads.push(resource);
            }
        }
    }

    pub(in crate::runtime::event_loop) fn drain_cloud_wal_upload_backlog(&mut self) {
        let deadline = crate::common::OperationDeadline::from_budget(self.runtime_response_timeout);
        self.drain_cloud_wal_upload_backlog_within(&deadline);
    }

    pub(in crate::runtime::event_loop) fn drain_cloud_wal_upload_backlog_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) {
        if let Err(error) = self.try_drain_cloud_wal_upload_backlog_within(deadline) {
            self.cloud_wal.defer_upload_retry();
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "could not admit recovered CloudAsync WAL upload");
        }
    }
}

impl EventLoop {
    pub(crate) fn maybe_flush_cloud_async_wal(&mut self) {
        if !self.wal_actor.is_cloud_async() {
            return;
        }
        if self.hybrid_storage.is_none() {
            return;
        }

        if self.state.is_memory_mode() {
            return;
        }

        // No pending local records to ship.
        if self.state.wal.pending_writes == 0 {
            return;
        }

        let pending_writes = self.state.wal.pending_writes;
        let bytes_buffered = self.wal_actor.bytes_since_sync();

        if !self
            .durability
            .should_flush_cloud_async(pending_writes, bytes_buffered)
        {
            return;
        }

        let seal_result = self.seal_current_cloud_segment();
        let Ok(Some((segment_id, max_sequence))) = seal_result else {
            if let Err(error) = seal_result {
                tracing::error!(error = %error, "CloudAsync: forced WAL seal failed");
            }
            return;
        };

        if std::env::var_os("MIDGE_TRACE_CLOUD_ASYNC").is_some() {
            // Throttle: log every 1000 segments to avoid noise.
            if segment_id.is_multiple_of(1000) {
                eprintln!(
                    "[midge] CloudAsync flush: segment_id={segment_id} max_sequence={max_sequence} pending_cloud={} ",
                    self.state.wal.pending_writes > 0
                );
            }
        }

        self.drain_auto_flush_memtables();
    }
}
