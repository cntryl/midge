use super::compaction::CompactionCoordinator;
use super::dispatch::RuntimeDispatcher;
use super::{EventLoop, HandleOutcome, HYBRID_STORAGE_POLL_INTERVAL};
use crate::runtime::RuntimeMsg;
use crossbeam::channel::{Receiver, TryRecvError};
use std::time::Duration;

impl EventLoop {
    pub(super) fn has_actionable_work(&self) -> bool {
        if self.wal_group_in_flight() {
            return self.pipeline.wal_commits.has_ready_work()
                || self.pipeline.memtable_apply.is_ready()
                || (self.routing.pending_msg.is_some() && !self.pending_message_waits_for_wal());
        }
        if CompactionCoordinator::manual_compaction_wait_timeout(self) == Some(Duration::ZERO) {
            return true;
        }
        if self.routing.pending_msg.is_some() {
            return true;
        }

        if self.publication.verification_barrier.token.is_some() {
            // Verification freezes layout maintenance, but group-commit fsync
            // does not change the layout being verified.
            let due_sync = self.actors.wal_actor.should_sync_batch()
                && (self.actors.wal_actor.has_pending_data()
                    || self.pipeline.durability.has_pending_waiters());
            return due_sync
                || self
                    .actors
                    .cloud_coordinator
                    .hybrid_storage_events
                    .as_ref()
                    .is_some_and(|rx| !rx.is_empty());
        }

        if self
            .actors
            .cloud_coordinator
            .cloud_wal_prune_worker
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            return true;
        }

        if !self.pipeline.flush_worker_result_rx.is_empty() {
            return true;
        }
        if self
            .actors
            .cloud_coordinator
            .metadata_mirror
            .has_ready_result()
        {
            return true;
        }
        if !self
            .publication
            .publication_gate
            .blocks_validation_completion()
            && self.pipeline.validation.has_ready_result()
        {
            return true;
        }

        if !self.publication.compaction_publish_result_rx.is_empty() {
            return true;
        }
        if !self.publication.publication_gate.is_active()
            && self.actors.compaction_actor.has_ready_compute()
        {
            return true;
        }

        if self.actors.wal_actor.should_sync_batch() {
            return true;
        }

        if self.pipeline.durability.cloud_seal_retry_due() && self.state.wal.pending_writes > 0 {
            return true;
        }

        if self.actors.cloud_coordinator.cloud_wal.backlog().ready() {
            return true;
        }

        if self.background_maintenance_timeout() == Duration::ZERO {
            return true;
        }
        if self.metadata_retention_timeout() == Some(Duration::ZERO) {
            return true;
        }

        if self.state.has_due_immutable_flush_matching(|cf, id| {
            self.actors.flush_actor.can_start_or_resume(cf, id)
        }) && !self.flush_start_blocked(false)
        {
            return true;
        }

        if let Some(rx) = &self.actors.cloud_coordinator.hybrid_storage_events {
            if !rx.is_empty() {
                return true;
            }
        }

        false
    }

    pub(super) fn idle_progress_timeout(&self) -> Option<Duration> {
        if self.wal_group_in_flight() {
            return Some(Duration::from_millis(1));
        }
        // Lease loss can occur without a storage completion or pressure
        // change. Keep pending admission and flush barrier waiters observing
        // terminal authority even when no worker completion can arrive.
        let authority_poll = (!self.pipeline.write_stall_waiters.is_empty()
            || !self.publication.flush_barrier_waiters.is_empty())
        .then_some(Duration::from_millis(100));
        if self.publication.verification_barrier.is_active() {
            // Verification deliberately freezes maintenance. Ignoring due
            // retry deadlines here makes the run loop block for the release
            // message instead of repeatedly timing out at zero duration. The
            // batched WAL sync deadline still applies.
            return [
                self.actors.wal_actor.sync_deadline_timeout(),
                authority_poll,
                CompactionCoordinator::manual_compaction_wait_timeout(self),
            ]
            .into_iter()
            .flatten()
            .min();
        }

        [
            self.actors.wal_actor.sync_deadline_timeout(),
            self.retry_deadlines().min(),
            self.actors
                .cloud_coordinator
                .hybrid_storage
                .as_ref()
                .and_then(|storage| storage.next_upload_turn(HYBRID_STORAGE_POLL_INTERVAL)),
            // Terminal storage events can arrive before the prune thread
            // exits. Keep observing the handle after its last event so the
            // publication gate and queued manual requests cannot lose a wake.
            self.actors
                .cloud_coordinator
                .cloud_wal_prune_worker
                .as_ref()
                .map(|_| HYBRID_STORAGE_POLL_INTERVAL),
            self.actors
                .flush_actor
                .is_inflight()
                .then_some(Duration::from_millis(1)),
            self.actors
                .cloud_coordinator
                .metadata_mirror
                .is_running()
                .then_some(Duration::from_millis(1)),
            self.pipeline
                .validation
                .is_pending()
                .then_some(Duration::from_millis(1)),
            self.actors
                .compaction_publish_actor
                .is_inflight()
                .then_some(Duration::from_millis(1)),
            self.actors
                .compaction_actor
                .has_pending_compute()
                .then_some(Duration::from_millis(1)),
            authority_poll,
            self.metadata_retention_timeout(),
            CompactionCoordinator::manual_compaction_wait_timeout(self),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// All runtime-owned retry schedules observed by the idle run loop.
    fn retry_deadlines(&self) -> impl Iterator<Item = Duration> + '_ {
        [
            self.pipeline
                .durability
                .cloud_seal_deadline_timeout(self.state.wal.pending_writes),
            self.actors.gc_actor.retry_deadline_timeout(),
            self.actors
                .cloud_coordinator
                .cloud_wal
                .backlog()
                .retry_timeout(),
            self.state.flush_retry_deadline_timeout(),
            Some(self.background_maintenance_timeout()),
        ]
        .into_iter()
        .flatten()
    }

    pub(super) fn progress_pass(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        CompactionCoordinator::expire_manual_compaction_waiters(self);
        if self.publication.verification_barrier.token.is_some() {
            self.wake_write_stall_waiters();
            self.wake_flush_waiters_for_terminal_fencing();
            // Mutations stay deferred behind the barrier, so sync only what
            // is already in the WAL; do not drain queued writes into it.
            // Cloud acknowledgements still complete their waiters.
            self.sync_batched_wal_without_draining();
            self.drain_hybrid_storage_events();
            return;
        }
        self.background_progress(Some(msg_rx));
    }

    /// The one list of background progress steps. The idle pass and the
    /// request fairness slot both run it, so a busy queue cannot starve a
    /// step the idle pass would have run. The idle pass may drain queued
    /// writes into its batched sync; the fairness slot runs after dispatch
    /// and leaves the queue in order.
    pub(super) fn background_progress(&mut self, drain_writes_from: Option<&Receiver<RuntimeMsg>>) {
        self.progress_wal_pipeline();
        if self.wal_group_in_flight() {
            self.drain_validation_results(&crossbeam::channel::never());
            return;
        }
        CompactionCoordinator::expire_manual_compaction_waiters(self);
        self.wake_write_stall_waiters();
        self.reap_compaction_compute_results();
        self.wake_flush_waiters_for_terminal_fencing();
        CompactionCoordinator::drain_publish_results(self);
        self.drain_flush_worker_results();
        if let Some(msg_rx) = drain_writes_from {
            self.drain_validation_results(msg_rx);
        } else {
            self.drain_validation_results(&crossbeam::channel::never());
        }
        match drain_writes_from {
            Some(msg_rx) => self.sync_batched_wal_if_needed(msg_rx),
            None => self.sync_batched_wal_without_draining(),
        }
        self.maybe_flush_cloud_async_wal();
        self.drain_cloud_wal_upload_backlog();
        self.tick_hybrid_storage();
        self.drain_hybrid_storage_events();
        self.drain_cloud_wal_upload_backlog();
        let hybrid_storage = self.actors.cloud_coordinator.hybrid_storage.clone();
        self.actors
            .gc_actor
            .retry_failed_cloud_deletes_if_due(&mut self.state, hybrid_storage);
        self.retry_manifest_reclamation_if_due();
        self.drain_auto_flush_memtables();
        self.run_background_compaction_maintenance_if_due();
        self.schedule_metadata_retention_if_due();
        self.prefetch_flush_sst_names_if_due();
    }

    pub(super) fn retry_manifest_reclamation_if_due(&mut self) {
        if !self.actors.gc_actor.manifest_reclamation_retry_due() {
            return;
        }

        // Do not interleave with a flush/prune publication snapshot. Deferring
        // re-arms the idle wakeup instead of turning a busy publication gate
        // into a spin loop.
        if self.publication.publication_gate.is_active() {
            self.actors.gc_actor.defer_manifest_reclamation_retry();
            return;
        }

        let deadline =
            crate::common::OperationDeadline::from_budget(self.routing.runtime_response_timeout);
        let _ = self.retry_gc_within(&deadline);
    }

    pub(super) fn record_wake_batch(&mut self, batch: usize) {
        self.state.diagnostics.record(|m| {
            m.record_event_loop_wake();
            m.record_event_loop_batch(batch as u64);
        });

        self.diagnostics.record_wake_batch(batch);
    }

    pub(super) fn process_one(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        if self.publication.verification_barrier.token.is_none()
            && !self.wal_group_in_flight()
            && msg.is_mutation()
        {
            CompactionCoordinator::drain_publish_results(self);
            self.drain_flush_worker_results();
            self.maybe_flush_cloud_async_wal();
            self.tick_hybrid_storage();
            self.drain_hybrid_storage_events();
            self.run_background_compaction_maintenance_if_due();
        }
        let outcome = self.handle_runtime_msg(msg, msg_rx);
        if outcome == HandleOutcome::Continue {
            self.run_request_fairness_slot();
        }
        outcome
    }

    pub(super) fn process_restored_one(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        // A restored message owns the publication turn that just became
        // available. Running maintenance before dispatch can start another
        // flush and re-defer the same request forever under steady flush debt.
        let outcome = self.handle_runtime_msg(msg, msg_rx);
        if outcome == HandleOutcome::Continue {
            self.run_request_fairness_slot();
        }
        outcome
    }

    pub(super) fn process_pending_message(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> Option<HandleOutcome> {
        if self.pending_message_waits_for_wal() {
            return None;
        }
        let message = self.routing.pending_msg.take()?;
        Some(self.process_restored_one(message, msg_rx))
    }

    pub(super) fn run_request_fairness_slot(&mut self) {
        CompactionCoordinator::expire_manual_compaction_waiters(self);
        if self.publication.verification_barrier.token.is_some() {
            // Allowed control traffic can keep the idle timer from firing.
            // Admission waiters still observe fencing while layout is frozen.
            self.wake_write_stall_waiters();
            self.wake_flush_waiters_for_terminal_fencing();
            return;
        }
        // A continuously non-empty request queue must not starve background
        // durability and storage progress. Run this bounded slot only after
        // dispatch so a restored control request keeps the publication turn
        // that made it eligible.
        self.background_progress(None);
    }

    pub(super) fn handle_runtime_msg(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        RuntimeDispatcher::handle(self, msg, msg_rx)
    }

    pub(super) fn process_wake_msg(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
        max_drain: usize,
    ) -> HandleOutcome {
        let mut batch = 1usize;
        let outcome = self.process_one(msg, msg_rx);

        if outcome == HandleOutcome::Break {
            self.record_wake_batch(batch);
            return outcome;
        }

        let drained = if self.publication.verification_barrier.token.is_some()
            || self.routing.pending_msg.is_some()
        {
            0
        } else {
            self.drain_pending_writes(msg_rx, max_drain)
        };
        batch += drained;
        self.record_wake_batch(batch);
        outcome
    }

    pub(super) fn restore_publication_deferred_message(&mut self) {
        if !self.shutdown.shutting_down
            && !self.publication.publication_gate.is_active()
            && self.publication.verification_barrier.token.is_none()
            && self.routing.pending_msg.is_none()
        {
            if let Some(index) = self
                .publication
                .publication_gate
                .next_restorable_index(|cf_id| {
                    self.column_family_publication_pipeline_active(cf_id)
                })
            {
                self.routing.pending_msg = self.publication.publication_gate.finish_at(index);
            }
        }
    }

    pub(super) fn run_actionable_pass(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
        max_drain_writes: usize,
    ) -> Option<HandleOutcome> {
        if !self.has_actionable_work() {
            return None;
        }
        match msg_rx.try_recv() {
            Ok(msg) => return Some(self.process_wake_msg(msg, msg_rx, max_drain_writes)),
            Err(TryRecvError::Disconnected) => return Some(HandleOutcome::Break),
            Err(TryRecvError::Empty) => {}
        }
        if !self.wal_group_in_flight() {
            if let Some(storage_rx) = &self.actors.cloud_coordinator.hybrid_storage_events {
                if let Ok(event) = storage_rx.try_recv() {
                    self.handle_storage_event(event);
                    return Some(HandleOutcome::Continue);
                }
            }
        }
        self.progress_pass(msg_rx);
        std::thread::sleep(Duration::from_micros(50));
        Some(HandleOutcome::Continue)
    }

    /// Main event loop — runs until Shutdown message or channel close.
    pub fn run(&mut self, msg_rx: &Receiver<RuntimeMsg>, worker_msg_rx: &Receiver<RuntimeMsg>) {
        // Bound write coalescing by a fairness quantum. Thousands of local
        // writes are cheap, but the same wake on cloud durability can consume
        // an entire control-request deadline before yielding.
        const MAX_DRAIN_WRITES_ON_WAKE: usize = 64;

        loop {
            // Worker completions can remain continuously ready. Settle the
            // ordered WAL prefix before admitting another worker message so
            // a receipt or its deadline cannot be starved by that traffic.
            self.progress_wal_pipeline();
            if let Ok(message) = worker_msg_rx.try_recv() {
                if RuntimeDispatcher::handle(self, message, msg_rx) == HandleOutcome::Break {
                    break;
                }
                continue;
            }
            self.restore_verification_deferred_message();
            self.restore_publication_deferred_message();
            if let Some(outcome) = self.process_pending_message(msg_rx) {
                if outcome == HandleOutcome::Break {
                    break;
                }
                continue;
            }

            if let Some(outcome) = self.run_actionable_pass(msg_rx, MAX_DRAIN_WRITES_ON_WAKE) {
                if outcome == HandleOutcome::Break {
                    break;
                }
                continue;
            }

            let idle_timeout = self.idle_progress_timeout();
            // Storage events are selected under the barrier too: the handler
            // takes only the ones that cannot change the verified layout and
            // defers the rest.
            // Cloud acknowledgements can retire a local WAL segment. Leave
            // their existing queue ownership intact while a Strict root owns
            // that segment, then resume event delivery after settlement.
            let selectable_storage_rx = (!self.wal_group_in_flight())
                .then(|| self.actors.cloud_coordinator.hybrid_storage_events.clone())
                .flatten();
            let msg = if let Some(storage_rx) = selectable_storage_rx {
                if let Some(timeout) = idle_timeout {
                    crossbeam::channel::select! {
                        recv(worker_msg_rx) -> msg => msg.ok(),
                        recv(msg_rx) -> msg => msg.ok(),
                        recv(storage_rx) -> ev => {
                            match ev {
                                Ok(ev) => {
                                    self.handle_storage_event(ev);
                                }
                                Err(_) => {
                                    self.actors.cloud_coordinator.disconnect_storage_events();
                                }
                            }
                            continue;
                        }
                        default(timeout) => {
                            self.progress_pass(msg_rx);
                            continue;
                        }
                    }
                } else {
                    crossbeam::channel::select! {
                        recv(worker_msg_rx) -> msg => msg.ok(),
                        recv(msg_rx) -> msg => msg.ok(),
                        recv(storage_rx) -> ev => {
                            match ev {
                                Ok(ev) => {
                                    self.handle_storage_event(ev);
                                }
                                Err(_) => {
                                    self.actors.cloud_coordinator.disconnect_storage_events();
                                }
                            }
                            continue;
                        }
                    }
                }
            } else if let Some(timeout) = idle_timeout {
                crossbeam::channel::select! {
                    recv(worker_msg_rx) -> msg => msg.ok(),
                    recv(msg_rx) -> msg => msg.ok(),
                    default(timeout) => {
                        self.progress_pass(msg_rx);
                        continue;
                    }
                }
            } else {
                crossbeam::channel::select! {
                    recv(worker_msg_rx) -> msg => msg.ok(),
                    recv(msg_rx) -> msg => msg.ok(),
                }
            };

            let Some(msg) = msg else {
                break;
            };

            let outcome = self.process_wake_msg(msg, msg_rx, MAX_DRAIN_WRITES_ON_WAKE);
            if outcome == HandleOutcome::Break {
                break;
            }
        }

        tracing::debug!("Runtime message channel closed — exiting event loop");
    }
}
