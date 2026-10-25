//! Cloud WAL segment sealing and flush scheduling.

use super::super::EventLoop;
use crate::runtime::hybrid_persistence::CloudPersistence;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use std::time::Instant;

struct CloudSealPlan {
    segment_id: u64,
    next_segment_id: u64,
    expected_max_sequence: u64,
    bytes_buffered: u64,
}

pub(in crate::runtime::event_loop) struct PendingCloudSeal {
    ticket: crate::runtime::wal_transition::WalSealTicket,
    stage: Option<CloudSealStage>,
    plan: CloudSealPlan,
    deadline: crate::common::OperationDeadline,
    started: Instant,
    failed: bool,
    waiters: Vec<CloudSealWaiter>,
}

enum CloudSealStage {
    PrefixSync {
        physical: crate::wal::traits::WalPendingSync,
        expected: crate::wal::traits::WalSyncPrefix,
    },
    FooterAppend {
        physical: crate::wal::footer_completion::WalPendingFooter,
        prepared: crate::runtime::wal_history::PreparedRotation,
    },
    FooterSync {
        physical: crate::wal::traits::WalPendingSync,
        expected: crate::wal::traits::WalSyncPrefix,
        prepared: crate::runtime::wal_history::PreparedRotation,
    },
}

struct CloudSealWaiter {
    request_id: u64,
    sequence: u64,
    wait_for_ack: bool,
}

impl EventLoop {
    pub(in crate::runtime::event_loop) fn start_cloud_seal_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<bool> {
        if self.pipeline.pending_cloud_seal.is_some() {
            return Ok(true);
        }
        let Some(plan) = self.prepare_cloud_seal(deadline)? else {
            return Ok(false);
        };
        let ticket = self.pipeline.wal_transition.begin_seal(
            plan.segment_id,
            plan.next_segment_id,
            plan.expected_max_sequence,
        )?;
        let submitted =
            self.actors
                .wal_actor
                .submit_cloud_upload_flush(&mut self.state, deadline, &ticket);
        let (physical, expected, submitted_max) = match submitted {
            Ok(submitted) => submitted,
            Err(error) => {
                self.settle_failed_seal_step(ticket, &error);
                return Err(error);
            }
        };
        if submitted_max != plan.expected_max_sequence {
            let error = crate::common::MidgeError::Internal(
                "CloudAsync seal prefix changed during sync admission".into(),
            );
            self.fence_wal_transition(&error, None);
            return Err(error);
        }
        self.pipeline.pending_cloud_seal = Some(PendingCloudSeal {
            ticket,
            stage: Some(CloudSealStage::PrefixSync { physical, expected }),
            plan,
            deadline: deadline.clone(),
            started: Instant::now(),
            failed: false,
            waiters: Vec::new(),
        });
        Ok(true)
    }

    pub(in crate::runtime::event_loop) fn start_cloud_seal_for_request(
        &mut self,
        deadline: &crate::common::OperationDeadline,
        request_id: u64,
        sequence: u64,
        wait_for_ack: bool,
    ) -> crate::common::MidgeResult<bool> {
        let started = self.start_cloud_seal_within(deadline)?;
        if started {
            if let Some(pending) = self.pipeline.pending_cloud_seal.as_mut() {
                pending.waiters.push(CloudSealWaiter {
                    request_id,
                    sequence,
                    wait_for_ack,
                });
            }
        }
        Ok(started)
    }

    pub(in crate::runtime::event_loop) fn progress_cloud_seal(&mut self) {
        let Some(mut pending) = self.pipeline.pending_cloud_seal.take() else {
            return;
        };
        if !pending.failed && pending.deadline.is_expired() {
            let error = crate::common::MidgeError::OutcomeUnknown(
                "CloudAsync WAL seal deadline expired; admitted sync remains owned until completion".into(),
            );
            self.fence_wal_transition(&error, None);
            for waiter in &pending.waiters {
                self.respond(
                    waiter.request_id,
                    crate::runtime::RuntimeResponse::Error {
                        request_id: waiter.request_id,
                        error: error.replay(),
                    },
                );
            }
            pending.failed = true;
        }
        if pending.failed {
            self.progress_failed_cloud_seal(pending);
            return;
        }
        let Some(stage) = pending.stage.take() else {
            return;
        };
        match stage {
            CloudSealStage::PrefixSync { physical, expected } => {
                self.progress_cloud_seal_prefix(pending, physical, expected)
            }
            CloudSealStage::FooterAppend { physical, prepared } => {
                self.progress_cloud_seal_footer_append(pending, physical, prepared)
            }
            CloudSealStage::FooterSync {
                physical,
                expected,
                prepared,
            } => self.progress_cloud_seal_footer_sync(pending, physical, expected, prepared),
        }
    }

    fn progress_failed_cloud_seal(&mut self, mut pending: PendingCloudSeal) {
        let Some(stage) = pending.stage.take() else {
            return;
        };
        let terminal = match &stage {
            CloudSealStage::PrefixSync { physical, .. }
            | CloudSealStage::FooterSync { physical, .. } => physical.try_complete().is_some(),
            CloudSealStage::FooterAppend { physical, .. } => physical.try_complete().is_some(),
        };
        if terminal {
            drop(stage);
            self.actors
                .wal_actor
                .release_fenced_writer_after_completion();
        } else {
            pending.stage = Some(stage);
            self.pipeline.pending_cloud_seal = Some(pending);
        }
    }

    fn progress_cloud_seal_prefix(
        &mut self,
        mut pending: PendingCloudSeal,
        physical: crate::wal::traits::WalPendingSync,
        expected: crate::wal::traits::WalSyncPrefix,
    ) {
        let Some(completion) = physical.try_complete() else {
            pending.stage = Some(CloudSealStage::PrefixSync { physical, expected });
            self.pipeline.pending_cloud_seal = Some(pending);
            return;
        };
        drop(physical);
        if let Err(error) =
            self.actors
                .wal_actor
                .finish_cloud_upload_flush(&mut self.state, expected, completion)
        {
            self.fence_wal_transition(&error, None);
            self.actors
                .wal_actor
                .release_fenced_writer_after_completion();
            self.fail_cloud_seal_waiters(pending.waiters, &error);
            return;
        }
        if let Err(error) = Self::after_cloud_flush_boundary() {
            let error = match self.cancel_reversible_cloud_seal(pending.ticket, error) {
                Ok(_) => unreachable!("a failed cloud flush boundary cannot succeed"),
                Err(error) => error,
            };
            self.fail_cloud_seal_waiters(pending.waiters, &error);
            return;
        }
        if let Err(error) = self.validate_runtime_writer_lease_within(&pending.deadline) {
            if let Err(cancel_error) = self
                .actors
                .wal_actor
                .cancel_reversible_cloud_flush(&pending.ticket)
            {
                self.fence_wal_transition(&cancel_error, None);
            } else {
                self.pipeline
                    .wal_transition
                    .abandon_prepared_seal(pending.ticket);
                if self.actors.wal_actor.is_fenced() {
                    self.fence_wal_transition(&error, None);
                }
            }
            self.fail_cloud_seal_waiters(pending.waiters, &error);
            return;
        }
        match self.actors.wal_actor.submit_cloud_rotation_footer(
            &mut self.state,
            &pending.deadline,
            &pending.ticket,
        ) {
            Ok((physical, prepared)) => {
                pending.stage = Some(CloudSealStage::FooterAppend { physical, prepared });
                self.pipeline.pending_cloud_seal = Some(pending);
            }
            Err(error) => {
                self.settle_failed_seal_step(pending.ticket, &error);
                self.fail_cloud_seal_waiters(pending.waiters, &error);
            }
        }
    }

    fn progress_cloud_seal_footer_append(
        &mut self,
        mut pending: PendingCloudSeal,
        physical: crate::wal::footer_completion::WalPendingFooter,
        prepared: crate::runtime::wal_history::PreparedRotation,
    ) {
        let Some(completion) = physical.try_complete() else {
            pending.stage = Some(CloudSealStage::FooterAppend { physical, prepared });
            self.pipeline.pending_cloud_seal = Some(pending);
            return;
        };
        drop(physical);
        let footer = match completion {
            Ok(completion) => completion.prefix,
            Err(error) => {
                self.actors
                    .wal_actor
                    .fence_transition(&mut self.state, error.to_string());
                self.fence_wal_transition(&error, None);
                self.actors
                    .wal_actor
                    .release_fenced_writer_after_completion();
                self.fail_cloud_seal_waiters(pending.waiters, &error);
                return;
            }
        };
        match self.actors.wal_actor.submit_cloud_footer_sync(
            &mut self.state,
            &pending.deadline,
            &pending.ticket,
            footer,
        ) {
            Ok((physical, expected)) => {
                pending.stage = Some(CloudSealStage::FooterSync {
                    physical,
                    expected,
                    prepared,
                });
                self.pipeline.pending_cloud_seal = Some(pending);
            }
            Err(error) => {
                self.fence_wal_transition(&error, None);
                self.actors
                    .wal_actor
                    .release_fenced_writer_after_completion();
                self.fail_cloud_seal_waiters(pending.waiters, &error);
            }
        }
    }

    fn progress_cloud_seal_footer_sync(
        &mut self,
        mut pending: PendingCloudSeal,
        physical: crate::wal::traits::WalPendingSync,
        expected: crate::wal::traits::WalSyncPrefix,
        prepared: crate::runtime::wal_history::PreparedRotation,
    ) {
        let Some(completion) = physical.try_complete() else {
            pending.stage = Some(CloudSealStage::FooterSync {
                physical,
                expected,
                prepared,
            });
            self.pipeline.pending_cloud_seal = Some(pending);
            return;
        };
        drop(physical);
        let receipt = match self.actors.wal_actor.finish_cloud_rotation_footer(
            &mut self.state,
            &pending.ticket,
            prepared,
            completion,
            expected,
        ) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.fence_wal_transition(&error, None);
                self.actors
                    .wal_actor
                    .release_fenced_writer_after_completion();
                self.fail_cloud_seal_waiters(pending.waiters, &error);
                return;
            }
        };
        let plan = pending.plan;
        let deadline = pending.deadline;
        let started = pending.started;
        let waiters = pending.waiters;
        if let Err(error) = self.commit_rotated_cloud_seal(pending.ticket, receipt) {
            self.fail_cloud_seal_waiters(waiters, &error);
            return;
        }
        self.finish_cloud_seal_upload(plan, deadline, started, waiters);
    }

    fn fail_cloud_seal_waiters(
        &mut self,
        waiters: Vec<CloudSealWaiter>,
        error: &crate::common::MidgeError,
    ) {
        for waiter in waiters {
            self.respond(
                waiter.request_id,
                crate::runtime::RuntimeResponse::Error {
                    request_id: waiter.request_id,
                    error: error.replay(),
                },
            );
        }
    }

    fn finish_cloud_seal_upload(
        &mut self,
        plan: CloudSealPlan,
        deadline: crate::common::OperationDeadline,
        started: Instant,
        waiters: Vec<CloudSealWaiter>,
    ) {
        if crate::failpoints::is_active("midge::cloud::inject_fail_after_wal_rotate_before_enqueue")
        {
            self.state.mark_persistence_anomaly();
            for waiter in waiters {
                self.respond(
                    waiter.request_id,
                    crate::runtime::RuntimeResponse::Error {
                        request_id: waiter.request_id,
                        error: crate::common::MidgeError::Internal(
                            "failpoint: cloud seal failed after WAL rotate before enqueue".into(),
                        ),
                    },
                );
            }
            self.actors
                .cloud_coordinator
                .cloud_wal
                .defer_uploads_after_failure();
            return;
        }
        if let Err(error) = self.try_drain_cloud_wal_upload_backlog_within(&deadline) {
            self.actors
                .cloud_coordinator
                .cloud_wal
                .defer_uploads_after_failure();
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "could not admit sealed CloudAsync WAL upload");
            self.fail_cloud_seal_waiters(waiters, &error);
            return;
        }
        self.state.diagnostics.record(|metrics| {
            metrics.record_cloud_async_wal_segment_sealed(
                plan.bytes_buffered,
                Self::elapsed_micros_to_u64(started.elapsed()),
            );
        });
        for waiter in waiters {
            if waiter.sequence > plan.expected_max_sequence {
                self.respond(
                    waiter.request_id,
                    crate::runtime::RuntimeResponse::Error {
                        request_id: waiter.request_id,
                        error: crate::common::MidgeError::Internal(format!(
                        "sealed WAL segment through sequence {}, requested cloud durability for {}",
                        plan.expected_max_sequence, waiter.sequence
                    )),
                    },
                );
            } else if !waiter.wait_for_ack
                || self.state.wal.frontiers.cloud_durable() >= waiter.sequence
            {
                self.drain_auto_flush_memtables();
                self.respond(
                    waiter.request_id,
                    crate::runtime::RuntimeResponse::Ok {
                        request_id: waiter.request_id,
                    },
                );
            } else {
                self.drain_auto_flush_memtables();
                self.pipeline.durability.queue_waiter_for_key(
                    plan.segment_id,
                    crate::runtime::durability::DurabilityWaiter::CloudDurability {
                        request_id: waiter.request_id,
                    },
                );
            }
        }
    }

    /// Seal the active cloud WAL segment on the event loop thread.
    ///
    /// Everything queued behind this call - reads, writes, acks, shutdown -
    /// waits for it, so it gets the budget the runtime promises its callers
    /// instead of waiting on the provider indefinitely. A timeout is a normal
    /// failure: the caller logs it and the seal is attempted again later.
    #[cfg(test)]
    pub(crate) fn seal_current_cloud_segment(
        &mut self,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        let deadline = self.event_loop_cloud_deadline();
        self.seal_current_cloud_segment_within(&deadline)
    }

    pub(in crate::runtime::event_loop) fn seal_current_cloud_segment_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        let result = self.try_seal_current_cloud_segment(deadline);
        if result.is_err() && self.pipeline.durability.cloud_seal_retry_needed() {
            self.pipeline.durability.defer_cloud_seal_retry();
        }
        result
    }

    fn try_seal_current_cloud_segment(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        let Some(plan) = self.prepare_cloud_seal(deadline)? else {
            return Ok(None);
        };
        let CloudSealPlan {
            segment_id,
            next_segment_id,
            expected_max_sequence,
            bytes_buffered,
        } = plan;
        let ticket = self.pipeline.wal_transition.begin_seal(
            segment_id,
            next_segment_id,
            expected_max_sequence,
        )?;
        let seal_start = Instant::now();
        let flushed =
            self.actors
                .wal_actor
                .flush_for_cloud_upload_within(&mut self.state, deadline, &ticket);
        let max_sequence = match flushed {
            Ok(max_sequence) => max_sequence,
            Err(error) => {
                self.settle_failed_seal_step(ticket, &error);
                return Err(error);
            }
        };
        if max_sequence != expected_max_sequence {
            let error = crate::common::MidgeError::Internal(format!(
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
        let receipt = match self.actors.wal_actor.rotate(&mut self.state, &ticket) {
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

        self.state.diagnostics.record(|m| {
            m.record_cloud_async_wal_segment_sealed(
                bytes_buffered,
                Self::elapsed_micros_to_u64(seal_start.elapsed()),
            );
        });

        Ok(Some((segment_id, max_sequence)))
    }

    fn prepare_cloud_seal(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<Option<CloudSealPlan>> {
        if !self.actors.wal_actor.is_cloud_async() {
            return Ok(None);
        }
        // A fenced protocol refuses before reading the ledger, which may
        // already have recorded the rotation that fenced it.
        self.pipeline.wal_transition.ensure_ready()?;
        let Some(storage) = self.actors.cloud_coordinator.hybrid_storage.clone() else {
            return Err(crate::common::MidgeError::Internal(
                "CloudAsync requires HybridStorage".to_string(),
            ));
        };
        if self.state.is_memory_mode() || self.state.wal.pending_writes == 0 {
            return Ok(None);
        }
        if !self.actors.cloud_coordinator.cloud_wal.backlog().is_empty() {
            return Err(crate::common::MidgeError::WriteStall(
                "older CloudAsync WAL segments are still awaiting upload admission".to_string(),
            ));
        }
        self.pipeline.durability.mark_cloud_seal_retry_needed();
        self.validate_runtime_writer_lease_within(deadline)?;

        let segment_id = self.state.wal.open_segment();
        let next_segment_id = self.state.wal.history.ledger().next_segment_id();
        let expected_max_sequence = self.actors.wal_actor.open_segment_max_sequence();
        if self.pipeline.durability.current_key() != segment_id {
            let error = crate::common::MidgeError::Internal(format!(
                "durability generation drift: expected current key {segment_id}"
            ));
            self.fence_wal_transition(&error, None);
            self.fail_durability_waiters_after_generation_drift(segment_id, &error);
            return Err(error);
        }
        let bytes_buffered = self.actors.wal_actor.bytes_since_sync() as u64;
        let existing_bytes = self
            .state
            .fs
            .metadata(&crate::wal::segment_key(segment_id))
            .map_or(0, |metadata| metadata.len);
        storage.ensure_wal_upload_capacity(existing_bytes.max(bytes_buffered))?;
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
        if let Err(error) = self.pipeline.wal_transition.finish_seal(ticket, receipt) {
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
        let segment_id = receipt.sealed_segment();
        let max_sequence = receipt.max_sequence();
        self.pipeline.wal_transition.note_sealed(ticket, receipt)?;
        WalTransitionBoundary::BeforeCoordinatorCommit.check()?;
        #[cfg(feature = "failpoints")]
        if crate::failpoints::is_active("midge::cloud::inject_coordinator_drift_after_wal_rotation")
        {
            let Some(drifted_generation) = receipt.next_segment().checked_add(100) else {
                return Err(crate::common::MidgeError::ResourceLimit(
                    "injected WAL coordinator generation overflow".to_string(),
                ));
            };
            self.pipeline
                .durability
                .rotate_from_to(segment_id, drifted_generation)?;
        }
        self.pipeline
            .durability
            .rotate_from_to(segment_id, receipt.next_segment())?;
        WalTransitionBoundary::AfterCoordinatorCommit.check()?;
        WalTransitionBoundary::BeforeSegmentRegistration.check()?;
        self.pipeline
            .durability
            .record_cloud_segment_inflight(segment_id, max_sequence);
        self.actors
            .cloud_coordinator
            .cloud_wal
            .queue_sealed(segment_id, max_sequence);
        self.pipeline.wal_transition.note_queued(segment_id)?;
        WalTransitionBoundary::AfterSegmentRegistration.check()?;
        WalTransitionBoundary::BeforeAccountingTransfer.check()?;
        self.actors
            .wal_actor
            .complete_cloud_upload_seal(&mut self.state, receipt);
        self.pipeline.durability.record_cloud_flush();
        self.pipeline.durability.clear_cloud_seal_retry_needed();
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
        let segment_id = receipt.sealed_segment();
        let max_sequence = receipt.max_sequence();
        self.pipeline
            .durability
            .record_cloud_segment_inflight(segment_id, max_sequence);
        self.actors
            .cloud_coordinator
            .cloud_wal
            .queue_sealed(segment_id, max_sequence);
        if let Err(registration_error) = self.pipeline.wal_transition.note_queued(segment_id) {
            tracing::error!(%registration_error, segment_id, "failed to retain sealed WAL transition obligation");
        }
        self.actors
            .wal_actor
            .complete_cloud_upload_seal(&mut self.state, receipt);
        self.pipeline.durability.record_cloud_flush();
        self.pipeline.durability.clear_cloud_seal_retry_needed();
        self.fence_wal_transition(error, Some(segment_id));
        self.fail_durability_waiters_after_generation_drift(receipt.next_segment(), error);
        tracing::error!(
            %error,
            segment_id,
            next_segment_id = receipt.next_segment(),
            "CloudAsync WAL seal fenced after irreversible rotation"
        );
    }

    fn cancel_reversible_cloud_seal(
        &mut self,
        ticket: crate::runtime::wal_transition::WalSealTicket,
        original_error: crate::common::MidgeError,
    ) -> crate::common::MidgeResult<Option<(u64, u64)>> {
        if let Err(cancel_error) = self.actors.wal_actor.cancel_reversible_cloud_flush(&ticket) {
            let segment_id = ticket.segment_id();
            self.fence_wal_transition(&cancel_error, None);
            self.fail_durability_waiters_after_generation_drift(segment_id, &cancel_error);
            return Err(cancel_error);
        }
        self.pipeline.wal_transition.abandon_prepared_seal(ticket);
        Err(original_error)
    }

    // The failpoint expands to an early return only with the `failpoints`
    // feature; the Result is the production-shaped boundary contract.
    #[allow(clippy::unnecessary_wraps)]
    fn after_cloud_flush_boundary() -> crate::common::MidgeResult<()> {
        crate::failpoints::fail_point!(
            "midge::cloud::inject_fail_after_wal_flush_before_rotate",
            |_| Err(crate::common::MidgeError::Internal(
                "failpoint: cloud seal failed after WAL flush before rotate".to_string(),
            ))
        );
        Ok(())
    }

    pub(in crate::runtime::event_loop) fn try_drain_cloud_wal_upload_backlog_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let Some(storage) = self.actors.cloud_coordinator.hybrid_storage.clone() else {
            if self.actors.cloud_coordinator.cloud_wal.backlog().is_empty() {
                return Ok(());
            }
            return Err(crate::common::MidgeError::Internal(
                "CloudAsync WAL upload backlog requires HybridStorage".to_string(),
            ));
        };
        let storage = CloudPersistence::new(storage);

        if self.actors.cloud_coordinator.cloud_wal.backlog().is_empty() {
            return Ok(());
        }
        if !self.actors.cloud_coordinator.cloud_wal.backlog().ready() {
            return Ok(());
        }
        self.actors
            .cloud_coordinator
            .cloud_wal
            .begin_upload_attempt();
        // Check authority once per drain pass. Provider stores use the local
        // monotonic lease window; `WalPublicationCatalog::require_epoch` and
        // the catalog CAS enforce cross-process fencing at publication. A
        // deep backlog does not require a lease-object GET per segment.
        self.validate_runtime_writer_lease_within(deadline)?;

        loop {
            let Some((segment_id, max_sequence)) =
                self.actors.cloud_coordinator.cloud_wal.backlog().first()
            else {
                return Ok(());
            };
            let source = crate::io::FsLocation::new(
                std::sync::Arc::clone(&self.state.fs),
                crate::wal::segment_key(segment_id),
            );
            let resource = match storage.enqueue_wal_segment(&source, segment_id, max_sequence) {
                Ok(resource) => resource,
                Err(crate::common::MidgeError::WriteStall(_)) => {
                    // Capacity remains owned by already admitted uploads. Keep
                    // this segment in the runtime backlog and avoid repeatedly
                    // paying lease validation and WAL readback while the queue
                    // is still full.
                    self.actors
                        .cloud_coordinator
                        .cloud_wal
                        .defer_uploads_for_memory();
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            self.actors
                .cloud_coordinator
                .cloud_wal
                .admitted_upload(segment_id);
            if !self.state.cloud.pending_uploads.contains(&resource) {
                self.state.cloud.pending_uploads.push(resource);
            }
        }
    }

    /// Whether a new `CloudAsync` write may enter the WAL: refused while the
    /// runtime backlog still owns sealed segments, and while the WAL the
    /// writer owes the cloud fills the upload queue's limits.
    pub(in crate::runtime::event_loop) fn check_cloud_wal_admission(
        &self,
    ) -> crate::common::MidgeResult<()> {
        if !self.actors.wal_actor.is_cloud_async() {
            return Ok(());
        }
        let Some(storage) = &self.actors.cloud_coordinator.hybrid_storage else {
            return Err(crate::common::MidgeError::Internal(
                "CloudAsync requires HybridStorage".to_string(),
            ));
        };
        if !self.actors.cloud_coordinator.cloud_wal.backlog().is_empty() {
            return Err(crate::common::MidgeError::WriteStall(
                "CloudAsync WAL recovery uploads are still draining".to_string(),
            ));
        }
        storage.ensure_wal_write_admission(self.actors.cloud_coordinator.held_uploads())
    }

    pub(in crate::runtime::event_loop) fn drain_cloud_wal_upload_backlog(&mut self) {
        let deadline =
            crate::common::OperationDeadline::from_budget(self.routing.runtime_response_timeout);
        self.drain_cloud_wal_upload_backlog_within(&deadline);
    }

    pub(in crate::runtime::event_loop) fn drain_cloud_wal_upload_backlog_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) {
        if let Err(error) = self.try_drain_cloud_wal_upload_backlog_within(deadline) {
            self.actors
                .cloud_coordinator
                .cloud_wal
                .defer_uploads_after_failure();
            self.state.mark_persistence_anomaly();
            tracing::warn!(%error, "could not admit recovered CloudAsync WAL upload");
        }
    }
}

impl EventLoop {
    pub(crate) fn maybe_flush_cloud_async_wal(&mut self) {
        if !self.actors.wal_actor.is_cloud_async() {
            return;
        }
        if self.actors.cloud_coordinator.hybrid_storage.is_none() {
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
        let bytes_buffered = self.actors.wal_actor.bytes_since_sync();

        if !self
            .pipeline
            .durability
            .should_flush_cloud_async(pending_writes, bytes_buffered)
        {
            return;
        }

        let deadline = self.event_loop_cloud_deadline();
        let seal_result = self.start_cloud_seal_within(&deadline);
        let Ok(true) = seal_result else {
            if let Err(error) = seal_result {
                tracing::error!(error = %error, "CloudAsync: forced WAL seal failed");
            }
            return;
        };
    }
}
