use super::{EventLoop, HandleOutcome};
use crate::common::MidgeError;
use crate::runtime::durability::DurabilityWaiter;
use crate::runtime::{RuntimeMsg, RuntimeResponse};

impl EventLoop {
    pub(super) fn handle_shutdown_request(&mut self, request_id: Option<u64>) -> HandleOutcome {
        if self.verification_barrier.token.is_some() {
            let message = request_id.map_or(RuntimeMsg::Shutdown, |request_id| {
                RuntimeMsg::ShutdownWithResponse { request_id }
            });
            self.defer_verification_message(message);
            return HandleOutcome::Continue;
        }
        self.handle_shutdown(request_id)
    }

    pub(super) fn handle_shutdown(&mut self, request_id: Option<u64>) -> HandleOutcome {
        tracing::info!("Runtime shutting down");
        let mut shutdown_error = None;
        self.shutting_down = true;

        // Caller-bearing work held outside the runtime queue cannot make
        // progress once terminal shutdown owns the event loop. Reject it
        // before joining potentially stalled storage workers so those callers
        // observe shutdown promptly instead of inheriting the worker budget.
        self.fail_shutdown_held_work();

        // Drain the currently owned flush pipeline before releasing the writer
        // epoch. No new immutable is scheduled once shutdown admission closes.
        while self.flush_actor.is_inflight() {
            match self
                .flush_worker_result_rx
                .recv_timeout(std::time::Duration::from_millis(25))
            {
                Ok(result) => self.handle_flush_worker_result(result),
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
            }
        }
        if let Err(error) = self.flush_actor.shutdown_and_join() {
            if shutdown_error.is_none() {
                shutdown_error = Some(error);
            }
        }

        // Stop compaction before the runtime drains cloud work. Its worker
        // owns staged SST output and must finish while this lease epoch is
        // still valid.
        self.compaction_actor
            .cancel_and_join_worker(&mut self.state, self.hybrid_storage.as_ref());

        let cloud_shutdown_deadline =
            crate::common::OperationDeadline::from_budget(self.shutdown_cloud_drain_timeout);
        if self.wal_actor.is_cloud_async() && self.state.wal.pending_writes > 0 {
            match self.seal_current_cloud_segment_within(&cloud_shutdown_deadline) {
                Ok(Some((segment_id, _max_sequence))) => {
                    tracing::info!(segment_id, "Enqueued final CloudAsync segment on shutdown");
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "Failed to seal CloudAsync segment during shutdown"
                    );
                    shutdown_error = Some(error);
                }
            }
        }

        if self.wal_actor.is_cloud_async() {
            if let Some(storage) = &self.hybrid_storage {
                let storage = storage.clone();

                while (storage.pending_upload_count() > 0 || self.cloud_wal.has_pending_uploads())
                    && !cloud_shutdown_deadline.is_expired()
                {
                    // UploadQueue and the runtime backlog are two ownership
                    // domains for the same accepted WAL obligation. Terminal
                    // storage failure transfers work back to the latter, so
                    // shutdown must keep admitting it until durability closes
                    // or the configured drain deadline expires.
                    self.drain_cloud_wal_upload_backlog_within(&cloud_shutdown_deadline);
                    self.tick_hybrid_storage_within(&cloud_shutdown_deadline);
                    self.drain_hybrid_storage_events_within(&cloud_shutdown_deadline);
                    if !cloud_shutdown_deadline.is_expired() {
                        self.drain_cloud_wal_upload_backlog_within(&cloud_shutdown_deadline);
                    }

                    let sleep_for = cloud_shutdown_deadline
                        .remaining()
                        .min(std::time::Duration::from_millis(10));
                    if !sleep_for.is_zero() {
                        std::thread::sleep(sleep_for);
                    }
                }

                let storage_pending = storage.pending_upload_count();
                let runtime_pending = self.cloud_wal.upload_backlog.len();
                if storage_pending > 0 || runtime_pending > 0 {
                    tracing::warn!(
                        storage_pending,
                        runtime_pending,
                        "Shutdown timeout: CloudAsync uploads remain owned"
                    );
                    if shutdown_error.is_none() {
                        shutdown_error = Some(MidgeError::Internal(format!(
                            "shutdown timed out with {storage_pending} storage-owned and {runtime_pending} runtime-owned cloud uploads"
                        )));
                    }
                } else {
                    tracing::info!("All CloudAsync uploads completed on shutdown");
                }
            }
        }

        // GC and remote WAL-prune workers can mutate local/cloud storage.
        // Join them before the event loop exits; Engine releases its lease
        // only after this runtime has quiesced.
        self.gc_actor.shutdown_workers();
        self.join_cloud_wal_prune_worker();
        if let Some(storage) = &self.hybrid_storage {
            storage.shutdown_background_workers();
        }

        // Flush completion and other worker progress can restore a deferred
        // caller into `pending_msg` while shutdown drains. The run loop exits
        // immediately after this method, so explicitly reject every held
        // caller before acknowledging shutdown rather than silently dropping
        // its response channel.
        self.fail_shutdown_held_work();

        if let Some(request_id) = request_id {
            let response = match shutdown_error {
                Some(error) => RuntimeResponse::Error { request_id, error },
                None => RuntimeResponse::Ok { request_id },
            };
            self.respond(request_id, response);
        }

        HandleOutcome::Break
    }

    pub(super) fn fail_shutdown_held_work(&mut self) {
        let mut messages = Vec::new();
        if let Some(message) = self.pending_msg.take() {
            messages.push(message);
        }
        messages.extend(self.verification_barrier.deferred_messages.drain(..));
        messages.extend(self.publication_gate.deferred_messages.drain(..));
        for message in messages {
            self.fail_shutdown_message(message);
        }

        let mut routed_request_ids = std::collections::BTreeSet::new();
        routed_request_ids.extend(
            self.flush_barrier_waiters
                .drain()
                .flat_map(|(_, waiters)| waiters)
                .map(|waiter| waiter.request_id),
        );
        {
            let mut pending = self.state.pending_compaction_waits.lock();
            routed_request_ids.extend(pending.keys().copied());
            pending.clear();
        }
        routed_request_ids.extend(self.write_stall_waiters.keys().copied());
        self.write_stall_waiters.clear();
        self.write_stall_waiter_queues.clear();
        let durability_waiters = self.durability.drain_all_waiters();
        routed_request_ids.extend(
            durability_waiters
                .iter()
                .filter_map(shutdown_waiter_request_id),
        );
        routed_request_ids.extend(self.inline_responses.borrow().keys().copied());

        for request_id in routed_request_ids {
            self.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: shutdown_error(),
                },
            );
        }
    }

    fn fail_shutdown_message(&self, message: RuntimeMsg) {
        let Some(request_id) = message.request_id() else {
            return;
        };
        let inline_response = match message {
            RuntimeMsg::ApplyTransaction { response_tx, .. }
            | RuntimeMsg::ApplySpilledTransaction { response_tx, .. } => response_tx,
            _ => None,
        };
        let response = RuntimeResponse::Error {
            request_id,
            error: shutdown_error(),
        };
        if let Some(response_tx) = inline_response {
            let _ = response_tx.send(response);
        } else {
            self.respond(request_id, response);
        }
    }
}

fn shutdown_waiter_request_id(waiter: &DurabilityWaiter) -> Option<u64> {
    match waiter {
        DurabilityWaiter::TransactionApply { request_id, .. }
        | DurabilityWaiter::CloudDurability { request_id } => Some(*request_id),
        DurabilityWaiter::ConfirmWalAppend { .. }
        | DurabilityWaiter::ConfirmTransactionApply { .. } => None,
        #[cfg(test)]
        DurabilityWaiter::WalAppend { request_id, .. }
        | DurabilityWaiter::Read { request_id, .. }
        | DurabilityWaiter::RangeScan { request_id, .. } => Some(*request_id),
    }
}

fn shutdown_error() -> MidgeError {
    MidgeError::Busy("runtime is shutting down".to_string())
}
