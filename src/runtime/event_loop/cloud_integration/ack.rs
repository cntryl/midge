//! Cloud acknowledgement, failure, and backpressure handling.

use super::super::durability_sync::CompletionSource;
use super::super::EventLoop;
use crate::common::OperationDeadline;
use crate::runtime::hybrid_persistence::HybridPersistence;

impl EventLoop {
    pub(crate) fn tick_hybrid_storage(&mut self) {
        let Some(storage) = &self.hybrid_storage else {
            return;
        };

        // Drive async storage uploads.
        // In push-channel mode, completion events are delivered via `hybrid_storage_events`.
        // In polling mode, `process_uploads()` returns completion events.
        let storage_events = storage.process_uploads();
        for event in storage_events {
            self.handle_storage_event(event);
        }
        self.wake_write_stall_waiters();
    }

    pub(crate) fn drain_hybrid_storage_events(&mut self) {
        let Some(rx) = &self.hybrid_storage_events else {
            return;
        };

        let rx = rx.clone();

        while let Ok(event) = rx.try_recv() {
            self.handle_storage_event(event);
        }
    }

    pub(crate) fn handle_storage_event(&mut self, event: crate::storage::StorageEvent) {
        match event {
            crate::storage::StorageEvent::CloudAck {
                segment_id,
                max_sequence,
            } => {
                self.handle_storage_event_cloud_ack(segment_id, max_sequence);
            }
            crate::storage::StorageEvent::CloudFail { segment_id, error } => {
                self.handle_cloud_upload_failure(
                    segment_id,
                    &crate::common::MidgeError::Internal(format!(
                        "Cloud durability failed: {error}"
                    )),
                    false,
                );
            }
            crate::storage::StorageEvent::CloudWalPruneComplete { segment_id, result } => {
                self.handle_storage_event_cloud_wal_prune_complete(segment_id, result);
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

    fn handle_storage_event_cloud_ack(&mut self, segment_id: u64, max_sequence: u64) {
        let deadline = self
            .cloud_ack_deadline(segment_id)
            .unwrap_or_else(OperationDeadline::unbounded);
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

        let ready_segments = match self
            .durability
            .take_contiguous_acked_cloud_segments(&self.cloud_wal.acked_segments)
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

        self.state.wal.cloud_durable_seq =
            self.state.wal.cloud_durable_seq.max(durable_max_sequence);
        tracing::debug!(
            segment_id = durable_segment_id,
            cloud_durable_seq = self.state.wal.cloud_durable_seq,
            "Cloud upload complete"
        );

        for (ready_segment_id, _) in &ready_segments {
            self.remove_cloud_durable_local_wal_segment(*ready_segment_id);
        }

        for (seg_id, _) in ready_segments {
            if let Some(enqueued_at) = self.durability.take_cloud_segment_timing(seg_id) {
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cloud_async_wal_ack_latency_us(
                        Self::elapsed_micros_to_u64(enqueued_at.elapsed()),
                    );
                }
            }

            let waiters = self.durability.complete_waiters_at(seg_id);
            self.complete_durability_waiters(waiters, CompletionSource::CloudAck);
        }
        self.prune_cloud_wal_segments_covered_by_manifest();
        self.drain_auto_flush_memtables();
    }

    fn handle_storage_event_cloud_wal_prune_complete(
        &mut self,
        segment_id: u64,
        result: crate::storage::StorageOutcome<()>,
    ) {
        self.cloud_wal.prune_inflight.remove(&segment_id);
        match result {
            crate::storage::StorageOutcome::Ok(()) => {
                self.cloud_wal.prune_retries.remove(&segment_id);
                self.cloud_wal.acked_segments.remove(&segment_id);
                self.next_background_compaction_check = std::time::Instant::now();
                tracing::debug!(segment_id, "Pruned cloud-covered remote WAL segment");
            }
            crate::storage::StorageOutcome::Err(error) => {
                // The authoritative catalog entry is retired before physical
                // deletion is attempted. A failed delete is therefore a safe
                // storage leak, not a recovery obligation that may be retried
                // through a now-absent catalog entry.
                self.cloud_wal.prune_retries.remove(&segment_id);
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
    /// Derived from the latest live caller waiting on the segment. Time already
    /// spent queued is charged against that caller's budget, while an older
    /// expired caller cannot prematurely fail a newer waiter. Once every caller
    /// has abandoned, the accepted WAL obligation continues as callerless work.
    pub(super) fn cloud_ack_deadline(&self, segment_id: u64) -> Option<OperationDeadline> {
        let request_ids = self.durability.cloud_durability_request_ids_at(segment_id);
        if request_ids.is_empty() {
            // Background acknowledgement with no caller attached (CloudAsync).
            // Nobody is waiting on a response, so there is no budget to honour
            // and no timeout to make ambiguous. Deliberately left unbounded:
            // imposing a budget here would newly fail slow-but-healthy
            // background publication that previously succeeded, without any
            // caller benefiting. Bounding this work is part of moving cloud I/O
            // off the event loop, not of honouring a caller's deadline.
            return Some(OperationDeadline::unbounded());
        }
        let latest_start = request_ids
            .iter()
            .filter_map(|request_id| self.router.registered_at(*request_id))
            .max();
        latest_start.map_or_else(
            || Some(OperationDeadline::unbounded()),
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

        // Attempt to recover the failed segment's max_sequence so we can
        // invalidate idempotency allocations that were part of it. Keep the
        // segment in the inflight frontier: the bounded storage queue retries
        // upload failures itself, while acknowledgement/publication failures
        // are explicitly requeued below. Later ACKs must not skip this gap.
        let failed_max_seq = self.durability.cloud_segment_max_sequence(segment_id);

        // Let WAL actor handle its internal failure handling and drop pending writes.
        tracing::error!(segment_id, error = %error, "Cloud upload failed");

        // If we know the max_sequence for the failed segment, invalidate idempotency
        // allocations up to that sequence so retries will allocate fresh sequences.
        if let Some(max_seq) = failed_max_seq {
            self.state.invalidate_idempotency_allocations_up_to(max_seq);
            if requeue_publication {
                let local_path = self
                    .state
                    .wal_dir
                    .join(crate::wal::segment_file_name(segment_id));
                if local_path.exists() {
                    self.cloud_wal.upload_backlog.insert(segment_id, max_seq);
                } else {
                    tracing::error!(
                        segment_id,
                        path = %local_path.display(),
                        "could not requeue failed cloud WAL publication because its local segment is missing"
                    );
                }
            }
        }

        let waiters = self.durability.drain_all_waiters();
        for w in waiters {
            let request_id = match w {
                super::super::super::durability::DurabilityWaiter::ConfirmWalAppend {
                    request_id,
                }
                | super::super::super::durability::DurabilityWaiter::TransactionApply {
                    request_id,
                    ..
                }
                | super::super::super::durability::DurabilityWaiter::ConfirmTransactionApply {
                    request_id,
                }
                | super::super::super::durability::DurabilityWaiter::CloudDurability {
                    request_id,
                } => request_id,
                #[cfg(test)]
                super::super::super::durability::DurabilityWaiter::WalAppend {
                    request_id, ..
                }
                | super::super::super::durability::DurabilityWaiter::Read { request_id, .. }
                | super::super::super::durability::DurabilityWaiter::RangeScan {
                    request_id, ..
                } => request_id,
            };
            self.respond(
                request_id,
                super::super::super::RuntimeResponse::Error {
                    request_id,
                    error: error.replay(),
                },
            );
        }

        // Keep all inflight segments. A later ACK may already be buffered, but
        // it cannot advance the frontier until this failed segment is retried
        // successfully.
    }
}
