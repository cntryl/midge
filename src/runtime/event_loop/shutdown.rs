use super::{EventLoop, HandleOutcome};

impl EventLoop {
    pub(super) fn handle_shutdown(&mut self) -> HandleOutcome {
        tracing::info!("Runtime shutting down");

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
                    } else if self.state.persistence_anomaly_detected {
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
                } else {
                    tracing::info!("All CloudAsync uploads completed on shutdown");
                }
            }
        }

        HandleOutcome::Break
    }
}
