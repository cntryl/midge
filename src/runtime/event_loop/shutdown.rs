use super::{EventLoop, HandleOutcome};
use crate::common::MidgeError;
use crate::runtime::{RuntimeMsg, RuntimeResponse};

impl EventLoop {
    pub(super) fn handle_shutdown_request(&mut self, request_id: Option<u64>) -> HandleOutcome {
        if self.verification_barrier_token.is_some() {
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
        self.compaction_actor.cancel_and_join_worker();

        if self.wal_actor.is_cloud_async() && self.state.wal.pending_writes > 0 {
            match self.seal_current_cloud_segment() {
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
                let shutdown_start = std::time::Instant::now();
                let shutdown_timeout = std::time::Duration::from_secs(30);
                let mut last_pending = usize::MAX;
                let mut stagnant_rounds = 0usize;

                while storage.pending_upload_count() > 0
                    && shutdown_start.elapsed() < shutdown_timeout
                {
                    self.tick_hybrid_storage();
                    self.drain_hybrid_storage_events();

                    let pending = storage.pending_upload_count();
                    if pending < last_pending {
                        last_pending = pending;
                        stagnant_rounds = 0;
                    } else if self.state.persistence_anomaly_detected() {
                        stagnant_rounds = stagnant_rounds.saturating_add(1);
                        if stagnant_rounds >= 25 {
                            tracing::warn!(
                                pending,
                                "aborting cloud shutdown wait after repeated failed upload progress"
                            );
                            break;
                        }
                    }

                    std::thread::sleep(std::time::Duration::from_millis(10));
                }

                if storage.pending_upload_count() > 0 {
                    tracing::warn!(
                        pending = storage.pending_upload_count(),
                        "Shutdown timeout: {} pending CloudAsync uploads not completed",
                        storage.pending_upload_count()
                    );
                    if shutdown_error.is_none() {
                        shutdown_error = Some(MidgeError::Internal(format!(
                            "shutdown timed out with {} pending cloud uploads",
                            storage.pending_upload_count()
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
        if let Some(storage) = &self.hybrid_storage {
            storage.shutdown_background_workers();
        }

        if let Some(request_id) = request_id {
            let response = match shutdown_error {
                Some(error) => RuntimeResponse::Error { request_id, error },
                None => RuntimeResponse::Ok { request_id },
            };
            self.respond(request_id, response);
        }

        HandleOutcome::Break
    }
}
