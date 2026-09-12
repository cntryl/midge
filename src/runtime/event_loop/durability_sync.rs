//! Durability synchronization — WAL sync, group commit, durability waiter completion
//!
//! Contains the ack policy logic, WAL sync triggers, and the shared
//! `complete_durability_waiters` helper that deduplicates the waiter-completion
//! pattern used by cloud ack, WAL sync, and forced sync code paths.

use super::super::durability::DurabilityWaiter;
use super::super::RuntimeMsg;
use super::super::RuntimeResponse;
use super::EventLoop;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use crossbeam::channel::Receiver;

/// Describes the source of a durability completion so the shared helper
/// can apply the correct side-effects (confirm variant, barrier clearing,
/// idempotency cleanup).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionSource {
    /// Cloud upload acknowledged — uses cloud frontier for confirm.
    CloudAck,
    /// Local WAL sync completed (group commit).
    WalSync,
    /// Sealed generation completed (forced sync / DDL barrier).
    SealedGeneration,
}

impl EventLoop {
    #[inline]
    pub(super) fn should_ack_immediately(&self, deferred: bool) -> bool {
        // Ack policy:
        // - Runtime write-path acks are immediate for all local modes and CloudAsync.
        // - CloudStrict blocking is enforced by the engine commit path via
        //   SealWalForCloud(wait_for_ack=true) before commit returns.
        // - Batched/Strict runtime writes still ack immediately; durability is enforced
        //   by explicit sync/flush barriers and read-path durability frontiers.
        //
        // In Batched mode, deferring the ack until fsync would serialize callers and
        // defeat group commit (and can make tests look hung).
        //
        // CRITICAL: Background CloudAsync must NOT wait for cloud confirmation.
        // CloudStrict waits happen in commit finalization, not in this runtime helper.
        if self.wal_actor.is_cloud_async() {
            // CloudAsync background mode: always ack immediately.
            // Cloud upload runs asynchronously; commits never block on upload.
            //
            // NOTE: WriteOptions::cloud_strict() is handled at the transaction commit
            // layer (engine/api/transaction.rs) which issues an explicit WalSync +
            // flush-and-upload sequence. By the time we reach should_ack_immediately,
            // the commit path has already ensured cloud durability for cloud_strict
            // writes. Therefore, runtime-level ack policy is always "immediate" for
            // CloudAsync — the blocking wait happens in the commit path, not here.
            return true;
        }

        // Non-CloudAsync always acks immediately.
        // `deferred` still matters for whether we queue confirm-only waiters.
        let _ = deferred;
        true
    }

    #[inline]
    pub(super) fn maybe_queue_confirm_only_waiter(
        &self,
        deferred: bool,
        request_id: u64,
        is_transaction: bool,
    ) {
        // If we already waited for durability (deferred==false for local, or cloud-backed
        // strict durability handled earlier in the commit path)
        // then the request will be confirmed at response time.
        if !deferred {
            return;
        }

        // Only queue confirm-only waiters when we are acknowledging before durability.
        if !self.should_ack_immediately(deferred) {
            return;
        }

        if is_transaction {
            self.durability
                .queue_waiter(DurabilityWaiter::ConfirmTransactionApply { request_id });
        } else {
            self.durability
                .queue_waiter(DurabilityWaiter::ConfirmWalAppend { request_id });
        }
    }

    /// Shared helper: complete a batch of durability waiters.
    ///
    /// This replaces three near-identical match blocks that were duplicated across
    /// cloud ack handling, WAL sync completion, and forced sync paths. The
    /// `source` parameter controls which side-effects apply:
    ///
    /// - `CloudAck`: uses `confirm_sequences_at(cloud_durable_seq)`, no transaction barrier clear
    /// - `WalSync`: uses `confirm_sequences(request_id)`, clears transaction barrier
    /// - `SealedGeneration`: same as `WalSync` plus `cleanup_old_idempotency_entries()`
    pub(super) fn complete_durability_waiters(
        &mut self,
        waiters: Vec<DurabilityWaiter>,
        source: CompletionSource,
    ) {
        for w in waiters {
            match w {
                #[cfg(test)]
                DurabilityWaiter::WalAppend {
                    request_id,
                    sequence,
                } => {
                    self.confirm_for_source(request_id, source);
                    self.respond(
                        request_id,
                        RuntimeResponse::WalAppended {
                            request_id,
                            sequence,
                        },
                    );
                }
                DurabilityWaiter::ConfirmWalAppend { request_id } => {
                    self.confirm_for_source(request_id, source);
                }
                DurabilityWaiter::TransactionApply {
                    request_id,
                    last_sequence,
                    op_count,
                    touched_cfs,
                } => {
                    if source != CompletionSource::CloudAck {
                        self.state.clear_pending_transaction_barrier();
                    }
                    self.confirm_for_source(request_id, source);
                    self.respond(
                        request_id,
                        RuntimeResponse::TransactionApplied {
                            request_id,
                            last_sequence,
                            op_count,
                            write_stall_hint: self.write_stall_hint_for_cfs(&touched_cfs),
                        },
                    );
                }
                DurabilityWaiter::ConfirmTransactionApply { request_id } => {
                    if source != CompletionSource::CloudAck {
                        self.state.clear_pending_transaction_barrier();
                    }
                    self.confirm_for_source(request_id, source);
                }
                DurabilityWaiter::CloudDurability { request_id } => {
                    self.respond(request_id, RuntimeResponse::Ok { request_id });
                }
                #[cfg(test)]
                DurabilityWaiter::Read {
                    request_id,
                    cf_id,
                    key,
                    sequence,
                } => {
                    let value = self.handle_read(cf_id, &key, sequence);
                    self.respond(request_id, RuntimeResponse::ReadValue { request_id, value });
                }
                #[cfg(test)]
                DurabilityWaiter::RangeScan {
                    request_id,
                    cf_id,
                    start,
                    end,
                    sequence,
                } => {
                    let results = self.handle_range_scan(cf_id, &start, &end, sequence);
                    self.respond(
                        request_id,
                        RuntimeResponse::RangeScanResults {
                            request_id,
                            results,
                        },
                    );
                }
            }

            if source == CompletionSource::SealedGeneration {
                self.state.cleanup_old_idempotency_entries();
            }
        }
    }

    /// Apply the correct sequence-confirmation call for the given source.
    #[inline]
    fn confirm_for_source(&mut self, request_id: u64, source: CompletionSource) {
        match source {
            CompletionSource::CloudAck => {
                self.state
                    .confirm_sequences_at(request_id, self.state.wal.cloud_durable_seq);
            }
            CompletionSource::WalSync | CompletionSource::SealedGeneration => {
                self.state.confirm_sequences(request_id);
            }
        }
    }

    pub(in crate::runtime::event_loop) fn fail_durability_waiters_after_generation_drift(
        &mut self,
        next_generation: u64,
        error: &crate::common::MidgeError,
    ) {
        self.state.mark_persistence_anomaly();
        let waiters = self.durability.drain_all_waiters_and_reset(next_generation);
        self.fail_durability_waiters(waiters, error);
    }

    pub(in crate::runtime::event_loop) fn fail_durability_waiters(
        &mut self,
        waiters: Vec<DurabilityWaiter>,
        error: &crate::common::MidgeError,
    ) {
        for waiter in waiters {
            let (request_id, clears_transaction_barrier, already_acknowledged) = match waiter {
                DurabilityWaiter::TransactionApply { request_id, .. } => (request_id, true, false),
                DurabilityWaiter::ConfirmTransactionApply { request_id } => {
                    (request_id, true, true)
                }
                DurabilityWaiter::ConfirmWalAppend { request_id } => (request_id, false, true),
                DurabilityWaiter::CloudDurability { request_id } => (request_id, false, false),
                #[cfg(test)]
                DurabilityWaiter::WalAppend { request_id, .. }
                | DurabilityWaiter::Read { request_id, .. }
                | DurabilityWaiter::RangeScan { request_id, .. } => (request_id, false, false),
            };
            if clears_transaction_barrier {
                self.state.clear_pending_transaction_barrier();
            }
            if already_acknowledged {
                continue;
            }
            self.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: error.replay(),
                },
            );
        }
    }

    /// Fsync the WAL and advance its local generation together with the
    /// generation-keyed durability coordinator.
    ///
    /// The actor remains non-operational after fsync until this method commits
    /// both its sync receipt and the coordinator generation. No caller can
    /// observe or bypass only half of the transition.
    pub(super) fn sync_wal_generation(
        &mut self,
        source: CompletionSource,
    ) -> crate::common::MidgeResult<()> {
        let sealed_generation = self.durability.current_key();
        let next_generation = self.next_wal_generation(sealed_generation)?;
        self.wal_transition.ensure_ready()?;
        if self.wal_actor.is_cloud_async() && sealed_generation != self.state.wal.current_segment_id
        {
            let error = crate::common::MidgeError::Fenced(format!(
                "cloud WAL generation drift: coordinator {sealed_generation}, active segment {}",
                self.state.wal.current_segment_id
            ));
            self.wal_actor
                .fence_transition(&mut self.state, error.to_string());
            self.wal_transition.fence(error.to_string(), None);
            return Err(error);
        }
        self.wal_transition.begin_sync(sealed_generation)?;
        let receipt = match self.wal_actor.begin_sync_transition(&mut self.state) {
            Ok(receipt) => receipt,
            Err(error) => {
                if self.wal_actor.is_fenced() {
                    self.wal_transition.fence(error.to_string(), None);
                    self.fail_durability_waiters_after_generation_drift(sealed_generation, &error);
                } else {
                    self.wal_transition.finish_sync(sealed_generation)?;
                }
                return Err(error);
            }
        };
        if self.wal_actor.is_cloud_async() {
            let result = self
                .wal_actor
                .commit_sync_transition(&mut self.state, receipt);
            if let Err(error) = &result {
                self.wal_transition.fence(error.to_string(), None);
                self.fail_durability_waiters_after_generation_drift(sealed_generation, error);
            } else if let Err(error) = self.wal_transition.finish_sync(sealed_generation) {
                self.wal_actor
                    .fence_transition(&mut self.state, error.to_string());
                self.wal_transition.fence(error.to_string(), None);
                return Err(error);
            }
            return result;
        }

        let next_generation = next_generation.expect("local generation preflighted");
        if let Err(error) = WalTransitionBoundary::BeforeCoordinatorCommit.check() {
            self.wal_actor
                .fence_transition(&mut self.state, error.to_string());
            self.wal_transition.fence(error.to_string(), None);
            self.fail_durability_waiters_after_generation_drift(sealed_generation, &error);
            return Err(error);
        }
        if let Err(error) = self
            .durability
            .rotate_from_to(sealed_generation, next_generation)
        {
            self.wal_actor
                .fence_transition(&mut self.state, error.to_string());
            self.wal_transition.fence(error.to_string(), None);
            tracing::error!(%error, "durability generation drift after WAL sync");
            self.fail_durability_waiters_after_generation_drift(next_generation, &error);
            return Err(error);
        }
        if let Err(error) = WalTransitionBoundary::AfterCoordinatorCommit.check() {
            self.wal_actor
                .fence_transition(&mut self.state, error.to_string());
            self.wal_transition.fence(error.to_string(), None);
            self.fail_durability_waiters_after_generation_drift(next_generation, &error);
            return Err(error);
        }

        if let Err(error) = self
            .wal_actor
            .commit_sync_transition(&mut self.state, receipt)
        {
            self.wal_actor
                .fence_transition(&mut self.state, error.to_string());
            self.wal_transition.fence(error.to_string(), None);
            self.fail_durability_waiters_after_generation_drift(next_generation, &error);
            return Err(error);
        }

        if let Err(error) = WalTransitionBoundary::BeforeWaiterCompletion.check() {
            self.wal_actor
                .fence_transition(&mut self.state, error.to_string());
            self.wal_transition.fence(error.to_string(), None);
            self.fail_durability_waiters_after_generation_drift(next_generation, &error);
            return Err(error);
        }
        let completed = self.durability.complete_waiters_at(sealed_generation);
        self.complete_durability_waiters(completed, source);
        if let Err(error) = self.wal_transition.finish_sync(sealed_generation) {
            self.wal_actor
                .fence_transition(&mut self.state, error.to_string());
            self.wal_transition.fence(error.to_string(), None);
            return Err(error);
        }
        WalTransitionBoundary::AfterCommitBeforeReturn.check()
    }

    fn next_wal_generation(&self, current: u64) -> crate::common::MidgeResult<Option<u64>> {
        if self.wal_actor.is_cloud_async() {
            return Ok(None);
        }
        current.checked_add(1).map(Some).ok_or_else(|| {
            crate::common::MidgeError::ResourceLimit(
                "WAL durability generation space exhausted".to_string(),
            )
        })
    }

    /// Sync batched WAL if threshold exceeded or if there are pending writes.
    /// In group commit mode, this completes all waiters for the sealed generation.
    pub(super) fn sync_batched_wal_if_needed(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        const MAX_DRAIN_WRITES_BEFORE_SYNC: usize = 4096;

        if self.wal_actor.is_cloud_async() {
            return; // CloudAsync has separate logic
        }

        // Sync if any of these conditions are true:
        // 1. Byte threshold exceeded
        // 2. Time threshold exceeded
        // NOTE: Do NOT unconditionally sync just because there are pending writes; that
        // defeats group commit—let the batch window (time/bytes) determine when to sync.
        // Durable waiters will be satisfied when the batch window elapses.
        let has_pending_waiters = self.durability.has_pending_waiters();

        let should_sync = self.wal_actor.should_sync_batch();

        if !should_sync {
            return;
        }

        // Skip no-op syncs when truly idle: no buffered data and no waiters.
        // This prevents the WAL actor from spinning at ~12 Hz doing empty fsyncs
        // when the engine has no work. Reset the sync timer so the time-based
        // threshold doesn't immediately re-trigger.
        if !self.wal_actor.has_pending_data() && !has_pending_waiters {
            self.wal_actor.reset_sync_timer();
            return;
        }

        // 🔑 CRITICAL INVARIANT: If we have pending waiters, we MUST seal a generation.
        // Even with zero bytes, the durability guarantee requires advancing the generation.
        // Drain any available writes to maximize group commit.
        let _ = self.drain_pending_writes(msg_rx, MAX_DRAIN_WRITES_BEFORE_SYNC);

        // Always sync: the paired operation advances the generation even with
        // zero bytes and completes every waiter sealed into the old generation.
        if let Err(error) = self.sync_wal_generation(CompletionSource::WalSync) {
            tracing::warn!(%error, "failed to sync batched WAL");
        }
    }

    /// Force WAL sync even if no pending writes (for DDL durability barriers).
    /// Required before CF metadata mutations to guarantee durability fences.
    /// CRITICAL: Must drain pending writes first so they are included in the sync.
    pub(super) fn force_wal_sync(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> crate::common::MidgeResult<()> {
        const MAX_DRAIN: usize = 4096;

        // 🔑 Drain any pending writes so they are included in this sync.
        // An unresolved remote DDL decision fences those writes in queue order
        // until a DDL retry reconciles the durable prepare.
        if !self.fencing.ddl_authority_ambiguous {
            let _ = self.drain_pending_writes(msg_rx, MAX_DRAIN);
        }

        self.sync_current_wal()
    }

    /// Establish a WAL barrier without consuming messages ordered after the
    /// current operation. DDL drop uses this so a later write cannot be
    /// coalesced ahead of the drop and then silently discarded by it.
    pub(super) fn sync_current_wal(&mut self) -> crate::common::MidgeResult<()> {
        if self.wal_actor.is_cloud_async() {
            return Ok(()); // CloudAsync has separate logic
        }

        // Always sync to establish the durability barrier, even when there are
        // no pending writes or waiters.
        self.sync_wal_generation(CompletionSource::SealedGeneration)
    }

    /// Make all records already appended to the local WAL durable before a
    /// flush-triggered rotation can expose sealed segments to pruning.
    pub(super) fn sync_local_wal_before_prune_rotation(
        &mut self,
    ) -> crate::common::MidgeResult<()> {
        if self.wal_actor.is_cloud_async() {
            return Ok(());
        }
        self.sync_wal_generation(CompletionSource::SealedGeneration)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_test_state;
    use super::*;
    use crate::runtime::{ResponseRouter, RuntimeConfig};
    use crate::wal::policy::BatchConfig;
    use std::sync::Arc;

    struct PanickingSyncWriter(std::sync::atomic::AtomicBool);

    impl crate::wal::WalWriter for PanickingSyncWriter {
        fn append_record(
            &self,
            _record: &crate::wal::WalRecord,
        ) -> crate::common::MidgeResult<u64> {
            Ok(0)
        }

        fn append_batch(
            &self,
            _records: &[crate::wal::WalRecord],
        ) -> crate::common::MidgeResult<u64> {
            Ok(0)
        }

        fn flush(&self) -> crate::common::MidgeResult<()> {
            Ok(())
        }

        fn sync(&self) -> crate::common::MidgeResult<()> {
            assert!(
                !self.0.swap(false, std::sync::atomic::Ordering::SeqCst),
                "injected sync panic"
            );
            Ok(())
        }

        fn current_pos(&self) -> u64 {
            0
        }

        fn close(&self) -> crate::common::MidgeResult<()> {
            Ok(())
        }
    }

    fn create_event_loop_with_policy(
        policy: crate::wal::DurabilityPolicy,
    ) -> crate::common::MidgeResult<EventLoop> {
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());
        let config = RuntimeConfig {
            wal_durability_policy: policy,
            ..RuntimeConfig::default()
        };
        EventLoop::new(state, false, router, config, None)
    }

    #[test]
    fn should_ack_immediately_for_local_mode_regardless_of_deferred_flag() {
        // Arrange
        let event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");

        // Act
        let ack_not_deferred = event_loop.should_ack_immediately(false);
        let ack_deferred = event_loop.should_ack_immediately(true);

        // Assert
        assert!(ack_not_deferred);
        assert!(ack_deferred);
    }

    #[test]
    fn should_produce_single_terminal_failure_when_wal_fsync_panics() {
        // Arrange
        let mut event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");
        event_loop
            .wal_actor
            .replace_writer_for_test(Box::new(PanickingSyncWriter(
                std::sync::atomic::AtomicBool::new(true),
            )));
        let response = event_loop.router.register(77, "CloudDurability");
        event_loop
            .durability
            .queue_waiter(DurabilityWaiter::CloudDurability { request_id: 77 });
        event_loop.state.wal.pending_writes = 1;
        let generation = event_loop.durability.current_key();

        // Act
        let result = event_loop.sync_wal_generation(CompletionSource::WalSync);

        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::Internal(_))
        ));
        assert_eq!(event_loop.durability.current_key(), generation);
        assert!(!event_loop.durability.has_pending_waiters());
        assert!(event_loop.wal_actor.is_fenced());
        assert!(event_loop.wal_transition.is_fenced());
        assert!(event_loop.state.persistence_anomaly_detected());
        assert!(matches!(
            response.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(RuntimeResponse::Error {
                request_id: 77,
                error: crate::common::MidgeError::Internal(message),
            }) if message.contains("WAL fsync panicked")
        ));
        assert!(
            response.try_recv().is_err(),
            "waiter must terminate exactly once"
        );
        assert!(matches!(
            event_loop
                .sync_wal_generation(CompletionSource::WalSync)
                .expect_err("fenced WAL must reject a later sync"),
            crate::common::MidgeError::Fenced(_)
        ));
    }

    #[cfg(feature = "failpoints")]
    #[test]
    #[allow(clippy::too_many_lines)]
    fn should_resolve_every_local_sync_boundary_without_partial_operational_state() {
        // Arrange
        let _test_guard = crate::failpoints::test_failpoint_guard();
        let scenario = fail::FailScenario::setup();
        let cases = [
            (
                WalTransitionBoundary::BeforeFsync,
                0,
                0,
                false,
                false,
                false,
            ),
            (WalTransitionBoundary::AfterFsync, 0, 0, true, true, false),
            (
                WalTransitionBoundary::BeforeCoordinatorCommit,
                0,
                0,
                true,
                true,
                false,
            ),
            (
                WalTransitionBoundary::AfterCoordinatorCommit,
                1,
                0,
                true,
                true,
                false,
            ),
            (
                WalTransitionBoundary::BeforeWaiterCompletion,
                1,
                9,
                true,
                true,
                false,
            ),
            (
                WalTransitionBoundary::AfterCommitBeforeReturn,
                1,
                9,
                false,
                false,
                true,
            ),
        ];

        for (boundary, expected_generation, expected_frontier, fenced, degraded, succeeded) in cases
        {
            let mut event_loop =
                create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
                    .expect("create event loop");
            event_loop
                .wal_actor
                .replace_writer_for_test(Box::new(PanickingSyncWriter(
                    std::sync::atomic::AtomicBool::new(false),
                )));
            event_loop.state.sequence = 9;
            event_loop.state.wal.pending_writes = 1;
            let request_id = 91_000 + expected_generation + expected_frontier;
            let response = event_loop.router.register(request_id, "CloudDurability");
            event_loop
                .durability
                .queue_waiter(DurabilityWaiter::CloudDurability { request_id });
            fail::cfg(boundary.failpoint_name(), "return")
                .expect("configure local WAL transition boundary");

            // Act
            let error = event_loop
                .sync_wal_generation(CompletionSource::WalSync)
                .expect_err("configured transition boundary must interrupt sync");
            fail::remove(boundary.failpoint_name());

            // Assert
            assert!(matches!(error, crate::common::MidgeError::Internal(_)));
            assert_eq!(event_loop.durability.current_key(), expected_generation);
            assert_eq!(event_loop.state.wal.local_durable_seq, expected_frontier);
            assert_eq!(event_loop.state.wal.last_synced_seq, expected_frontier);
            assert_eq!(event_loop.wal_actor.is_fenced(), fenced);
            assert_eq!(event_loop.wal_transition.is_fenced(), fenced);
            assert_eq!(event_loop.state.persistence_anomaly_detected(), degraded);

            if boundary == WalTransitionBoundary::BeforeFsync {
                assert!(event_loop.durability.has_pending_waiters());
                assert!(response.try_recv().is_err());
                event_loop
                    .sync_wal_generation(CompletionSource::WalSync)
                    .expect("pre-fsync failure must be retryable");
                assert!(matches!(
                    response.recv_timeout(std::time::Duration::from_secs(1)),
                    Ok(RuntimeResponse::Ok { request_id: actual }) if actual == request_id
                ));
            } else {
                assert!(!event_loop.durability.has_pending_waiters());
                let terminal = response
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .expect("durability waiter terminal outcome");
                if succeeded {
                    assert!(matches!(
                        terminal,
                        RuntimeResponse::Ok { request_id: actual } if actual == request_id
                    ));
                } else {
                    assert!(matches!(
                        terminal,
                        RuntimeResponse::Error { request_id: actual, .. }
                            if actual == request_id
                    ));
                }
            }
            assert!(
                response.try_recv().is_err(),
                "waiter must terminate exactly once"
            );
        }

        scenario.teardown();
    }

    #[test]
    fn should_reject_exhausted_local_generation_before_fsync_or_state_change() {
        // Arrange
        let mut event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");
        let _ = event_loop.durability.drain_all_waiters_and_reset(u64::MAX);
        event_loop.state.sequence = 9;
        event_loop.state.wal.pending_writes = 1;

        // Act
        let error = event_loop
            .sync_wal_generation(CompletionSource::WalSync)
            .expect_err("exhausted generation must reject sync");

        // Assert
        assert!(matches!(
            error,
            crate::common::MidgeError::ResourceLimit(message)
                if message.contains("generation space exhausted")
        ));
        assert_eq!(event_loop.durability.current_key(), u64::MAX);
        assert_eq!(event_loop.state.wal.local_durable_seq, 0);
        assert_eq!(event_loop.state.wal.pending_writes, 1);
        assert!(!event_loop.wal_actor.is_fenced());
        assert!(!event_loop.wal_transition.is_fenced());
        assert!(!event_loop.state.persistence_anomaly_detected());
    }

    #[test]
    fn should_complete_sealed_waiter_before_explicit_wal_barrier_acknowledgement() {
        // Arrange
        let mut event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");
        event_loop
            .durability
            .queue_waiter(DurabilityWaiter::ConfirmWalAppend { request_id: 77 });
        let barrier_request_id = 78;
        let barrier_response = event_loop.router.register(barrier_request_id, "WalSync");

        // Act
        super::super::wal::WalCoordinator::sync(&mut event_loop, barrier_request_id);
        let response = barrier_response
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("explicit WAL barrier response");

        // Assert
        assert!(matches!(
            response,
            RuntimeResponse::Ok { request_id } if request_id == barrier_request_id
        ));
        assert_eq!(event_loop.durability.waiters_fanned_out(), 1);
        assert!(!event_loop.durability.has_pending_waiters());
        assert_eq!(event_loop.durability.current_key(), 1);
        assert!(!event_loop.state.persistence_anomaly_detected());
    }

    #[test]
    fn should_advance_the_coordinator_as_the_single_local_generation_owner() {
        // Arrange
        let state = create_test_state();
        let router = Arc::new(ResponseRouter::new());
        let config = RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
            wal_batch_config: BatchConfig {
                max_delay_ms: 0,
                max_bytes: usize::MAX,
            },
            ..RuntimeConfig::default()
        };
        let mut event_loop =
            EventLoop::new(state, false, router, config, None).expect("create event loop");
        let first_request = 88;
        let first_response = event_loop.router.register(first_request, "TestRequest");
        event_loop
            .durability
            .queue_waiter(DurabilityWaiter::CloudDurability {
                request_id: first_request,
            });
        event_loop
            .wal_actor
            .append(
                &mut event_loop.state,
                crate::runtime::actors::wal::AppendParams {
                    request_id: first_request,
                    cf_id: 0,
                    key: bytes::Bytes::from_static(b"drift-first"),
                    value: Some(bytes::Bytes::from_static(b"value")),
                    insert_only: false,
                    ttl_seconds: None,
                },
            )
            .expect("append first record");
        let (_msg_tx, msg_rx) = crossbeam::channel::unbounded();

        // Act
        event_loop.sync_batched_wal_if_needed(&msg_rx);
        let first = first_response
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("generation drift response");
        // Assert
        assert!(matches!(
            first,
            RuntimeResponse::Ok {
                request_id,
            } if request_id == first_request
        ));
        assert_eq!(event_loop.durability.current_key(), 1);
        assert!(!event_loop.state.persistence_anomaly_detected());
        assert!(!event_loop.durability.has_pending_waiters());
    }

    #[test]
    fn should_ack_immediately_for_cloud_async_mode_regardless_of_deferred_flag() {
        // Arrange
        let event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::CloudAsync)
            .expect("create event loop");

        // Act
        let ack_not_deferred = event_loop.should_ack_immediately(false);
        let ack_deferred = event_loop.should_ack_immediately(true);

        // Assert
        assert!(ack_not_deferred);
        assert!(ack_deferred);
    }

    #[test]
    fn should_queue_confirm_only_waiter_when_deferred_write_is_acked_immediately() {
        // Arrange
        let event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");
        assert!(!event_loop.durability.has_pending_waiters());

        // Act
        event_loop.maybe_queue_confirm_only_waiter(true, 42, false);

        // Assert
        assert!(event_loop.durability.has_pending_waiters());
        let waiters = event_loop.durability.drain_all_waiters();
        assert_eq!(waiters.len(), 1);
        match &waiters[0] {
            DurabilityWaiter::ConfirmWalAppend { request_id } => {
                assert_eq!(*request_id, 42);
            }
            other => panic!("unexpected waiter variant: {other:?}"),
        }
    }

    #[test]
    fn should_not_queue_confirm_only_waiter_when_not_deferred() {
        // Arrange
        let event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");
        assert!(!event_loop.durability.has_pending_waiters());

        // Act
        event_loop.maybe_queue_confirm_only_waiter(false, 7, false);

        // Assert
        assert!(!event_loop.durability.has_pending_waiters());
    }

    #[test]
    fn should_queue_confirm_only_transaction_waiter_when_deferred_txn_is_acked_immediately() {
        // Arrange
        let event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");
        assert!(!event_loop.durability.has_pending_waiters());

        // Act
        event_loop.maybe_queue_confirm_only_waiter(true, 99, true);

        // Assert
        assert!(event_loop.durability.has_pending_waiters());
        let waiters = event_loop.durability.drain_all_waiters();
        assert_eq!(waiters.len(), 1);
        match &waiters[0] {
            DurabilityWaiter::ConfirmTransactionApply { request_id } => {
                assert_eq!(*request_id, 99);
            }
            other => panic!("unexpected waiter variant: {other:?}"),
        }
    }

    #[test]
    fn should_not_queue_confirm_only_transaction_waiter_when_not_deferred() {
        // Arrange
        let event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");
        assert!(!event_loop.durability.has_pending_waiters());

        // Act
        event_loop.maybe_queue_confirm_only_waiter(false, 100, true);

        // Assert
        assert!(!event_loop.durability.has_pending_waiters());
    }

    #[test]
    fn should_complete_transaction_with_stall_hint_from_waiter_column_family() {
        // Arrange
        let mut event_loop = create_event_loop_with_policy(crate::wal::DurabilityPolicy::Batched)
            .expect("create event loop");
        let secondary_cf = event_loop
            .state
            .create_cf("delayed-stall-cf".to_string())
            .expect("create secondary column family");
        event_loop.state.max_immutable_memtables = 1;
        event_loop
            .state
            .get_cf_mut(secondary_cf)
            .expect("secondary column family")
            .immutable_memtables
            .push(Arc::new(crate::sst::SkipListMemtable::new()));
        let response_rx = event_loop.router.register(101, "TestRequest");
        let waiter = DurabilityWaiter::TransactionApply {
            request_id: 101,
            last_sequence: 9,
            op_count: 1,
            touched_cfs: vec![secondary_cf],
        };

        // Act
        event_loop.complete_durability_waiters(vec![waiter], CompletionSource::WalSync);

        // Assert
        match response_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("transaction response")
        {
            RuntimeResponse::TransactionApplied {
                write_stall_hint, ..
            } => assert!(write_stall_hint),
            other => panic!("unexpected response: {other:?}"),
        }
    }
}
