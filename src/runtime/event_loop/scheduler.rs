use super::compaction::CompactionCoordinator;
use super::dispatch::RuntimeDispatcher;
use super::{EventLoop, HandleOutcome, HYBRID_STORAGE_POLL_INTERVAL};
use crate::runtime::RuntimeMsg;
use crossbeam::channel::{Receiver, TryRecvError};
use std::time::Duration;

impl EventLoop {
    pub(super) fn has_actionable_work(&self) -> bool {
        if self.pending_msg.is_some() {
            return true;
        }

        if self.verification_barrier.token.is_some() {
            // Verification freezes layout maintenance, but group-commit fsync
            // does not change the layout being verified.
            let due_sync = self.wal_actor.should_sync_batch()
                && (self.wal_actor.has_pending_data() || self.durability.has_pending_waiters());
            return due_sync
                || self
                    .cloud_coordinator
                    .hybrid_storage_events
                    .as_ref()
                    .is_some_and(|rx| !rx.is_empty());
        }

        if self
            .cloud_coordinator
            .cloud_wal_prune_worker
            .as_ref()
            .is_some_and(std::thread::JoinHandle::is_finished)
        {
            return true;
        }

        if !self.flush_worker_result_rx.is_empty() {
            return true;
        }

        if !self.compaction_publish_result_rx.is_empty() {
            return true;
        }

        if self.wal_actor.should_sync_batch() {
            return true;
        }

        if self.durability.cloud_seal_retry_due() && self.state.wal.pending_writes > 0 {
            return true;
        }

        if self.cloud_coordinator.cloud_wal.uploads_ready() {
            return true;
        }

        if self.background_maintenance_timeout() == Duration::ZERO {
            return true;
        }

        if self.state.has_due_immutable_flush() && !self.flush_start_blocked(false) {
            return true;
        }

        if let Some(rx) = &self.cloud_coordinator.hybrid_storage_events {
            if !rx.is_empty() {
                return true;
            }
        }

        false
    }

    pub(super) fn idle_progress_timeout(&self) -> Option<Duration> {
        if self.verification_barrier.is_active() {
            // Verification deliberately freezes maintenance. Ignoring due
            // retry deadlines here makes the run loop block for the release
            // message instead of repeatedly timing out at zero duration. The
            // batched WAL sync deadline still applies.
            return self.wal_actor.sync_deadline_timeout();
        }

        [
            self.wal_actor.sync_deadline_timeout(),
            self.retry_deadlines().min(),
            self.cloud_coordinator
                .hybrid_storage
                .as_ref()
                .and_then(|storage| {
                    (storage.pending_upload_count() > 0).then_some(HYBRID_STORAGE_POLL_INTERVAL)
                }),
            // Terminal storage events can arrive before the prune thread
            // exits. Keep observing the handle after its last event so the
            // publication gate and queued manual requests cannot lose a wake.
            self.cloud_coordinator
                .cloud_wal_prune_worker
                .as_ref()
                .map(|_| HYBRID_STORAGE_POLL_INTERVAL),
            self.flush_actor
                .is_inflight()
                .then_some(Duration::from_millis(1)),
            self.compaction_publish_actor
                .is_inflight()
                .then_some(Duration::from_millis(1)),
            Some(self.background_maintenance_timeout()),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// All runtime-owned retry schedules observed by the idle run loop.
    fn retry_deadlines(&self) -> impl Iterator<Item = Duration> + '_ {
        [
            self.durability
                .cloud_seal_deadline_timeout(self.state.wal.pending_writes),
            self.gc_actor.retry_deadline_timeout(),
            self.cloud_coordinator
                .cloud_wal
                .upload_retry_deadline_timeout(),
            self.state.flush_retry_deadline_timeout(),
        ]
        .into_iter()
        .flatten()
    }

    pub(super) fn progress_pass(&mut self, msg_rx: &Receiver<RuntimeMsg>) {
        if self.verification_barrier.token.is_some() {
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
        CompactionCoordinator::drain_publish_results(self);
        self.drain_flush_worker_results();
        match drain_writes_from {
            Some(msg_rx) => self.sync_batched_wal_if_needed(msg_rx),
            None => self.sync_batched_wal_without_draining(),
        }
        self.maybe_flush_cloud_async_wal();
        self.drain_cloud_wal_upload_backlog();
        self.tick_hybrid_storage();
        self.drain_hybrid_storage_events();
        self.drain_cloud_wal_upload_backlog();
        let hybrid_storage = self.cloud_coordinator.hybrid_storage.clone();
        self.gc_actor
            .retry_failed_cloud_deletes_if_due(&mut self.state, hybrid_storage);
        self.retry_manifest_reclamation_if_due();
        self.drain_auto_flush_memtables();
        self.run_background_compaction_maintenance_if_due();
    }

    pub(super) fn retry_manifest_reclamation_if_due(&mut self) {
        if !self.gc_actor.manifest_reclamation_retry_due() {
            return;
        }

        // Do not interleave with a flush/prune publication snapshot. Deferring
        // re-arms the idle wakeup instead of turning a busy publication gate
        // into a spin loop.
        if self.publication_gate.is_active() {
            self.gc_actor.defer_manifest_reclamation_retry();
            return;
        }

        let deadline = crate::common::OperationDeadline::from_budget(self.runtime_response_timeout);
        let _ = self.retry_gc_within(&deadline);
    }

    pub(super) fn record_wake_batch(&mut self, batch: usize) {
        self.state.diagnostics.record(|m| {
            m.record_event_loop_wake();
            m.record_event_loop_batch(batch as u64);
        });

        if self.loop_debug {
            const LOOP_DEBUG_EVERY: u64 = 256;
            self.loop_debug_wakes += 1;
            self.loop_debug_batch_total += batch as u64;

            if self.loop_debug_wakes.is_multiple_of(LOOP_DEBUG_EVERY) {
                let avg_batch = self
                    .loop_debug_batch_total
                    .to_string()
                    .parse::<f64>()
                    .unwrap_or(0.0)
                    / self
                        .loop_debug_wakes
                        .to_string()
                        .parse::<f64>()
                        .unwrap_or(1.0);
                eprintln!(
                    "[midge] loop_stats wakes={} avg_batch={:.2}",
                    self.loop_debug_wakes, avg_batch
                );
            }
        }
    }

    pub(super) fn process_one(
        &mut self,
        msg: RuntimeMsg,
        msg_rx: &Receiver<RuntimeMsg>,
    ) -> HandleOutcome {
        if self.verification_barrier.token.is_none() && msg.is_mutation() {
            CompactionCoordinator::drain_publish_results(self);
            self.drain_flush_worker_results();
            self.maybe_flush_cloud_async_wal();
            self.tick_hybrid_storage();
            self.drain_hybrid_storage_events();
            self.run_background_compaction_maintenance_if_due();
        }
        let outcome = self.handle_runtime_msg(msg, msg_rx);
        if outcome == HandleOutcome::Continue && self.verification_barrier.token.is_none() {
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
        if outcome == HandleOutcome::Continue && self.verification_barrier.token.is_none() {
            self.run_request_fairness_slot();
        }
        outcome
    }

    pub(super) fn run_request_fairness_slot(&mut self) {
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

        let drained = if self.verification_barrier.token.is_some() || self.pending_msg.is_some() {
            0
        } else {
            self.drain_pending_writes(msg_rx, max_drain)
        };
        batch += drained;
        self.record_wake_batch(batch);
        outcome
    }

    pub(super) fn restore_publication_deferred_message(&mut self) {
        if !self.shutting_down
            && !self.publication_gate.is_active()
            && self.verification_barrier.token.is_none()
            && self.pending_msg.is_none()
        {
            if let Some(index) = self.publication_gate.next_restorable_index(|cf_id| {
                self.column_family_publication_pipeline_active(cf_id)
            }) {
                self.pending_msg = self.publication_gate.finish_at(index);
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
        if let Some(storage_rx) = &self.cloud_coordinator.hybrid_storage_events {
            if let Ok(event) = storage_rx.try_recv() {
                self.handle_storage_event(event);
                return Some(HandleOutcome::Continue);
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
            if let Ok(message) = worker_msg_rx.try_recv() {
                if RuntimeDispatcher::handle(self, message, msg_rx) == HandleOutcome::Break {
                    break;
                }
                continue;
            }
            self.restore_verification_deferred_message();
            self.restore_publication_deferred_message();
            if let Some(pending) = self.pending_msg.take() {
                let outcome = self.process_restored_one(pending, msg_rx);
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
            let selectable_storage_rx = self.cloud_coordinator.hybrid_storage_events.clone();
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
                                    self.cloud_coordinator.hybrid_storage_events = None;
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
                                    self.cloud_coordinator.hybrid_storage_events = None;
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
