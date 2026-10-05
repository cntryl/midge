use super::{EventLoop, HandleOutcome};
use crate::common::MidgeError;
use crate::runtime::durability::{DurabilityWaiter, TestDurabilityWaiter};
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
        let shutdown_started = std::time::Instant::now();
        self.trace_shutdown_start();
        let mut shutdown_error = None;
        self.shutting_down = true;

        // Local mode does not checkpoint memtables on shutdown, so the WAL
        // tail is the only copy of buffered commits from the last batch
        // window. Make it durable, and complete the waiters it covers, before
        // held work is rejected below. CloudAsync seals its segment later.
        if let Err(error) =
            trace_shutdown_phase("sync-current-wal", None, || self.sync_current_wal())
        {
            tracing::error!(error = %error, "Failed to sync local WAL during shutdown");
            shutdown_error = Some(error);
        }

        // Caller-bearing work held outside the runtime queue cannot make
        // progress once terminal shutdown owns the event loop. Reject it
        // before joining potentially stalled storage workers so those callers
        // observe shutdown promptly instead of inheriting the worker budget.
        self.fail_shutdown_held_work();

        // A compaction publisher may be between its durable intent, manifest
        // installation, and mirrored clear, holding the publication gate that
        // parks finished flushes. Settle it first so the flush drain below can
        // publish them, and so it completes while this lease epoch is valid.
        if let Err(error) = trace_shutdown_phase("compaction-publication-drain", None, || {
            self.drain_shutdown_compaction_publication()
        }) {
            if shutdown_error.is_none() {
                shutdown_error = Some(error);
            }
        }

        let cloud_async = self.wal_actor.is_cloud_async();
        let cloud_shutdown_deadline =
            crate::common::OperationDeadline::from_budget(self.shutdown_cloud_drain_timeout);

        // Finish work already admitted to the flush pipeline before sealing
        // the final WAL generation. Cloud shutdown uses the same bounded
        // durability budget as upload drain; local shutdown retains its
        // existing behavior.
        if cloud_async {
            if let Err(error) = trace_shutdown_phase(
                "cloud-flush-pipeline-drain",
                Some(&cloud_shutdown_deadline),
                || self.drain_shutdown_flush_pipeline_within(&cloud_shutdown_deadline),
            ) {
                shutdown_error = Some(error);
            }
        } else {
            trace_shutdown_phase("local-flush-pipeline-drain", None, || {
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
            });
        }

        // Establish authoritative remote WAL durability before replacing that
        // recovery authority with an SST/manifest checkpoint.
        if cloud_async && self.state.wal.pending_writes > 0 {
            match trace_shutdown_phase(
                "final-cloud-wal-seal",
                Some(&cloud_shutdown_deadline),
                || self.seal_current_cloud_segment_within(&cloud_shutdown_deadline),
            ) {
                Ok(Some((segment_id, _max_sequence))) => {
                    tracing::info!(segment_id, "Enqueued final CloudAsync segment on shutdown");
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "Failed to seal CloudAsync segment during shutdown"
                    );
                    if shutdown_error.is_none() {
                        shutdown_error = Some(error);
                    }
                }
            }
        }

        if cloud_async {
            if let Some(error) =
                trace_shutdown_phase("cloud-upload-drain", Some(&cloud_shutdown_deadline), || {
                    self.drain_shutdown_cloud_uploads_within(&cloud_shutdown_deadline)
                })
            {
                if shutdown_error.is_none() {
                    shutdown_error = Some(error);
                }
            }
        }

        // Short-lived cloud writers can remain below the normal memtable
        // threshold forever. Checkpoint their active memtables during a clean
        // shutdown so reopen does not have to replay an ever-growing catalog
        // of otherwise uncovered WAL segments. If remote WAL durability or an
        // earlier flush failed, retain WAL authority and report the shutdown
        // failure instead of attempting the authority switch.
        if cloud_async && shutdown_error.is_none() {
            if let Err(error) = trace_shutdown_phase(
                "active-cloud-memtable-checkpoint",
                Some(&cloud_shutdown_deadline),
                || self.checkpoint_active_cloud_memtables_within(&cloud_shutdown_deadline),
            ) {
                shutdown_error = Some(error);
            }
        }

        self.join_shutdown_storage_workers(
            cloud_async,
            &cloud_shutdown_deadline,
            &mut shutdown_error,
        );
        self.state.invalidate_unsettled_flush_accounting();

        // Flush completion and other worker progress can restore a deferred
        // caller into `pending_msg` while shutdown drains. The run loop exits
        // immediately after this method, so explicitly reject every held
        // caller before acknowledging shutdown rather than silently dropping
        // its response channel.
        self.fail_shutdown_held_work();

        self.trace_shutdown_completion(shutdown_started, shutdown_error.is_none());
        if let Some(request_id) = request_id {
            let response = match shutdown_error {
                Some(error) => RuntimeResponse::Error { request_id, error },
                None => RuntimeResponse::Ok { request_id },
            };
            self.respond(request_id, response);
        }

        HandleOutcome::Break
    }

    fn trace_shutdown_start(&self) {
        tracing::info!(
            writer_epoch = self.state.writer_epoch,
            cloud_async = self.wal_actor.is_cloud_async(),
            immutable_flushes_pending = self
                .state
                .column_families
                .values()
                .map(|cf| cf.immutable_flushes.len())
                .sum::<usize>(),
            flush_worker_inflight = self.flush_actor.is_inflight(),
            compaction_publisher_inflight = self.compaction_publish_actor.is_inflight(),
            wal_pending_writes = self.state.wal.pending_writes,
            runtime_uploads_pending = self.cloud_coordinator.cloud_wal.upload_backlog.len(),
            storage_uploads_pending = self
                .cloud_coordinator
                .hybrid_storage
                .as_ref()
                .map_or(0, |storage| storage.pending_upload_count()),
            wal_segments_acked = self.cloud_coordinator.cloud_wal.acked_segments.len(),
            wal_prunes_inflight = self.cloud_coordinator.cloud_wal.prune_inflight.len(),
            wal_preflight_worker_owned = self.cloud_coordinator.cloud_wal_prune_worker.is_some(),
            "Runtime shutdown started"
        );
    }

    fn trace_shutdown_completion(&self, started: std::time::Instant, successful: bool) {
        tracing::info!(
            writer_epoch = self.state.writer_epoch,
            elapsed_ms = started.elapsed().as_millis(),
            successful,
            "Runtime shutdown completed"
        );
    }

    fn join_shutdown_storage_workers(
        &mut self,
        cloud_async: bool,
        cloud_shutdown_deadline: &crate::common::OperationDeadline,
        shutdown_error: &mut Option<MidgeError>,
    ) {
        let join_deadline = cloud_async.then_some(cloud_shutdown_deadline);
        if let Err(error) = trace_shutdown_phase("flush-worker-join", join_deadline, || {
            self.flush_actor.shutdown_and_join()
        }) {
            if shutdown_error.is_none() {
                *shutdown_error = Some(error);
            }
        }

        // Stop compaction only after the final checkpoint has settled. Its
        // worker owns staged SST output and must finish while this lease epoch
        // is still valid.
        let compaction_storage = self
            .cloud_coordinator
            .hybrid_storage
            .as_ref()
            .map(|storage| {
                std::sync::Arc::clone(storage)
                    as std::sync::Arc<dyn crate::runtime::actors::compaction::CompactionStorage>
            });
        trace_shutdown_phase("compaction-worker-join", join_deadline, || {
            self.compaction_actor
                .cancel_and_join_worker(&mut self.state, compaction_storage.as_ref());
        });

        // GC and remote WAL-prune workers can mutate local/cloud storage.
        // Join them before the event loop exits; Engine releases its lease
        // only after this runtime has quiesced.
        trace_shutdown_phase("sst-gc-worker-join", join_deadline, || {
            self.gc_actor.shutdown_workers();
        });
        trace_shutdown_phase("wal-preflight-worker-join", join_deadline, || {
            self.join_cloud_wal_prune_worker();
        });
        if cloud_async {
            self.drain_hybrid_storage_events_within(cloud_shutdown_deadline);
        }
        if let Some(storage) = &self.cloud_coordinator.hybrid_storage {
            trace_shutdown_phase("storage-prune-worker-join", join_deadline, || {
                storage.shutdown_background_workers();
            });
        }
    }

    fn drain_shutdown_cloud_uploads_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> Option<MidgeError> {
        let storage = self.cloud_coordinator.hybrid_storage.as_ref()?.clone();
        while (storage.pending_upload_count() > 0
            || self.cloud_coordinator.cloud_wal.has_pending_uploads())
            && !deadline.is_expired()
        {
            // UploadQueue and the runtime backlog are two ownership domains
            // for the same accepted WAL obligation. Terminal storage failure
            // transfers work back to the latter, so shutdown must keep
            // admitting it until durability closes or the configured drain
            // deadline expires.
            self.drain_cloud_wal_upload_backlog_within(deadline);
            self.tick_hybrid_storage_within(deadline);
            self.drain_hybrid_storage_events_within(deadline);
            if !deadline.is_expired() {
                self.drain_cloud_wal_upload_backlog_within(deadline);
            }

            let sleep_for = deadline
                .remaining()
                .min(std::time::Duration::from_millis(10));
            if !sleep_for.is_zero() {
                std::thread::sleep(sleep_for);
            }
        }

        let storage_pending = storage.pending_upload_count();
        let runtime_pending = self.cloud_coordinator.cloud_wal.upload_backlog.len();
        if storage_pending > 0 || runtime_pending > 0 {
            crate::failpoints::fail_point!("midge::shutdown::after_cloud_upload_drain_timeout");
            tracing::warn!(
                storage_pending,
                runtime_pending,
                "Shutdown timeout: CloudAsync uploads remain owned"
            );
            Some(MidgeError::Timeout(format!(
                "shutdown timed out with {storage_pending} storage-owned and {runtime_pending} runtime-owned cloud uploads"
            )))
        } else {
            tracing::info!("All CloudAsync uploads completed on shutdown");
            None
        }
    }

    fn drain_shutdown_compaction_publication(&mut self) -> crate::common::MidgeResult<()> {
        while self.compaction_publish_actor.is_inflight() {
            match self
                .compaction_publish_result_rx
                .recv_timeout(std::time::Duration::from_millis(25))
            {
                Ok(completion) => {
                    crate::runtime::event_loop::compaction::CompactionCoordinator::handle_publication_completion(
                        self,
                        completion,
                    );
                }
                Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                    return Err(MidgeError::Internal(
                        "compaction publication worker disconnected during shutdown".to_string(),
                    ));
                }
            }
        }
        if self.compaction_publication.is_active() {
            return Err(MidgeError::Internal(
                "compaction publication was left without a worker completion during shutdown"
                    .to_string(),
            ));
        }
        self.compaction_publish_actor.shutdown_and_join()
    }

    fn checkpoint_active_cloud_memtables_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        let mut cf_ids: Vec<_> = self.state.column_families.keys().copied().collect();
        cf_ids.sort_unstable();
        for cf_id in cf_ids {
            if deadline.is_expired() {
                return Err(MidgeError::Timeout(
                    "cloud shutdown checkpoint exceeded the durability deadline".to_string(),
                ));
            }
            self.freeze_active_memtable_for(cf_id, crate::metadata::accounting::Origin::Shutdown)?;
        }
        self.drain_shutdown_flush_pipeline_within(deadline)
    }

    pub(super) fn drain_shutdown_flush_pipeline_within(
        &mut self,
        deadline: &crate::common::OperationDeadline,
    ) -> crate::common::MidgeResult<()> {
        loop {
            let pending = self
                .state
                .column_families
                .values()
                .map(|cf| cf.immutable_flushes.len())
                .sum::<usize>();
            if pending == 0 && !self.flush_actor.is_inflight() {
                return Ok(());
            }
            if deadline.is_expired() {
                return Err(MidgeError::Timeout(format!(
                    "cloud shutdown checkpoint timed out with {pending} immutable memtable(s) pending"
                )));
            }

            self.schedule_next_flush_worker_during_shutdown();
            if self.flush_actor.is_inflight() {
                let wait_for = deadline
                    .remaining()
                    .min(std::time::Duration::from_millis(25));
                match self.flush_worker_result_rx.recv_timeout(wait_for) {
                    Ok(result) => self.handle_flush_worker_result(result),
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                        return Err(MidgeError::Internal(
                            "cloud shutdown checkpoint flush worker disconnected".to_string(),
                        ));
                    }
                }
                continue;
            }

            if self.publication_gate.is_active() {
                self.reap_cloud_wal_prune_worker();
                let sleep_for = deadline
                    .remaining()
                    .min(std::time::Duration::from_millis(10));
                if !sleep_for.is_zero() {
                    std::thread::sleep(sleep_for);
                }
                continue;
            }

            if let Some(retry_after) = self.state.flush_retry_deadline_timeout() {
                let sleep_for = deadline.remaining().min(retry_after);
                if !sleep_for.is_zero() {
                    std::thread::sleep(sleep_for);
                }
                continue;
            }

            return Err(MidgeError::Internal(format!(
                "cloud shutdown checkpoint stalled with {pending} immutable memtable(s) pending"
            )));
        }
    }

    pub(super) fn fail_shutdown_held_work(&mut self) {
        let mut messages = Vec::new();
        if let Some(message) = self.pending_msg.take() {
            messages.push(message);
        }
        messages.extend(self.verification_barrier.deferred_messages.drain(..));
        messages.extend(self.publication_gate.take_deferred());
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
        routed_request_ids
            .extend(std::mem::take(&mut self.state.pending_compaction_waits).into_keys());
        routed_request_ids.extend(self.write_stall_waiters.drain());
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

// This records the inherited budget without starting a new deadline or
// changing the requirement to join accepted workers before releasing fencing.
fn trace_shutdown_phase<T>(
    phase: &'static str,
    deadline: Option<&crate::common::OperationDeadline>,
    operation: impl FnOnce() -> T,
) -> T {
    let started = std::time::Instant::now();
    tracing::info!(
        phase,
        cloud_deadline_remaining_ms = ?deadline.map(|deadline| deadline.remaining().as_millis()),
        "Runtime shutdown phase started"
    );
    let result = operation();
    tracing::info!(
        phase,
        elapsed_ms = started.elapsed().as_millis(),
        cloud_deadline_remaining_ms = ?deadline.map(|deadline| deadline.remaining().as_millis()),
        "Runtime shutdown phase completed"
    );
    result
}

fn shutdown_waiter_request_id(waiter: &DurabilityWaiter) -> Option<u64> {
    match waiter {
        DurabilityWaiter::Test(waiter) => Some(shutdown_test_waiter_request_id(waiter)),
        DurabilityWaiter::CloudDurability { request_id } => Some(*request_id),
        DurabilityWaiter::ConfirmTransactionApply { .. } => None,
    }
}

#[cfg(test)]
fn shutdown_test_waiter_request_id(waiter: &TestDurabilityWaiter) -> u64 {
    match waiter {
        TestDurabilityWaiter::WalAppend { request_id, .. }
        | TestDurabilityWaiter::Read { request_id, .. }
        | TestDurabilityWaiter::RangeScan { request_id, .. } => *request_id,
    }
}

#[cfg(not(test))]
fn shutdown_test_waiter_request_id(waiter: &TestDurabilityWaiter) -> u64 {
    match *waiter {}
}

fn shutdown_error() -> MidgeError {
    MidgeError::Busy("runtime is shutting down".to_string())
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_test_state;
    use super::*;
    use crate::runtime::{ResponseRouter, RuntimeConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    struct CountingSyncWriter(Arc<AtomicUsize>);

    impl crate::wal::WalWriter for CountingSyncWriter {
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

        fn sync(&self) -> crate::common::MidgeResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn current_pos(&self) -> u64 {
            0
        }
    }

    fn local_event_loop_with_counting_writer() -> (EventLoop, Arc<AtomicUsize>) {
        let router = Arc::new(ResponseRouter::new());
        let config = RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::Batched,
            ..RuntimeConfig::default()
        };
        let mut event_loop = EventLoop::new(
            create_test_state(),
            false,
            router,
            config,
            crate::runtime::event_loop::FlushWorkerMode::Inline,
        )
        .expect("create event loop");
        let syncs = Arc::new(AtomicUsize::new(0));
        event_loop
            .wal_actor
            .replace_writer_for_test(Box::new(CountingSyncWriter(Arc::clone(&syncs))));
        (event_loop, syncs)
    }

    #[test]
    fn should_fsync_wal_on_clean_shutdown_when_buffered_commits_are_pending() {
        // Arrange
        let (mut event_loop, syncs) = local_event_loop_with_counting_writer();
        event_loop.state.wal.pending_writes = 1;
        let response = event_loop.router.register(1, "Shutdown");

        // Act
        let outcome = event_loop.handle_shutdown(Some(1));

        // Assert
        assert!(matches!(outcome, HandleOutcome::Break));
        assert!(
            syncs.load(Ordering::SeqCst) >= 1,
            "shutdown must fsync the WAL tail"
        );
        assert!(matches!(
            response.recv_timeout(Duration::from_secs(1)),
            Ok(RuntimeResponse::Ok { request_id: 1 })
        ));
    }

    #[test]
    fn should_complete_wal_durability_waiters_when_local_shutdown_syncs() {
        // Arrange
        let (mut event_loop, _syncs) = local_event_loop_with_counting_writer();
        event_loop.state.wal.pending_writes = 1;
        let waiter = event_loop.router.register(7, "WalAppend");
        event_loop.durability.queue_waiter(DurabilityWaiter::Test(
            TestDurabilityWaiter::WalAppend {
                request_id: 7,
                sequence: 1,
            },
        ));

        // Act
        event_loop.handle_shutdown(None);

        // Assert
        assert!(matches!(
            waiter.recv_timeout(Duration::from_secs(1)),
            Ok(RuntimeResponse::WalAppended {
                request_id: 7,
                sequence: 1,
            })
        ));
    }

    #[test]
    fn should_extract_request_ids_from_test_durability_waiters_during_shutdown() {
        // Arrange
        let waiters = [
            DurabilityWaiter::Test(TestDurabilityWaiter::WalAppend {
                request_id: 8,
                sequence: 1,
            }),
            DurabilityWaiter::Test(TestDurabilityWaiter::Read {
                request_id: 9,
                cf_id: 0,
                key: b"key".to_vec(),
                sequence: 1,
            }),
            DurabilityWaiter::Test(TestDurabilityWaiter::RangeScan {
                request_id: 10,
                cf_id: 0,
                start: b"a".to_vec(),
                end: b"z".to_vec(),
                sequence: 1,
            }),
        ];

        // Act
        let request_ids = waiters
            .iter()
            .map(shutdown_waiter_request_id)
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(request_ids, vec![Some(8), Some(9), Some(10)]);
    }
}
