//! Cloud acknowledgement, failure, and backpressure handling.

use super::super::durability_sync::CompletionSource;
use super::super::EventLoop;
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
                self.handle_cloud_upload_failure(segment_id, &error);
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
        if let Err(error) = self.verify_remote_wal_segment_before_ack(segment_id, max_sequence) {
            self.handle_cloud_upload_failure(
                segment_id,
                &format!("cloud WAL readback validation failed: {error}"),
            );
            return;
        }

        let resource = crate::wal::cloud_segment_object_key(segment_id);
        self.state
            .cloud
            .pending_uploads
            .retain(|item| item != &resource);
        self.cloud_acked_wal_segments
            .insert(segment_id, max_sequence);

        let ready_segments = match self
            .durability
            .take_contiguous_acked_cloud_segments(&self.cloud_acked_wal_segments)
        {
            Ok(ready_segments) => ready_segments,
            Err(error) => {
                self.cloud_acked_wal_segments.remove(&segment_id);
                self.handle_cloud_upload_failure(segment_id, &error);
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

        #[cfg(test)]
        let upload_complete_result = self.wal_actor.handle_cloud_upload_complete(
            &mut self.state,
            durable_segment_id,
            durable_max_sequence,
        );
        #[cfg(not(test))]
        let upload_complete_result: crate::common::MidgeResult<()> = {
            self.wal_actor.handle_cloud_upload_complete(
                &mut self.state,
                durable_segment_id,
                durable_max_sequence,
            );
            Ok(())
        };

        match upload_complete_result {
            Ok(()) => {
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
            Err(e) => {
                tracing::error!(error = %e, "failed to apply cloud ack");
            }
        }
    }

    fn handle_storage_event_cloud_wal_prune_complete(
        &mut self,
        segment_id: u64,
        result: crate::storage::StorageOutcome<()>,
    ) {
        self.cloud_wal_prune_inflight.remove(&segment_id);
        match result {
            crate::storage::StorageOutcome::Ok(()) => {
                self.cloud_acked_wal_segments.remove(&segment_id);
                tracing::debug!(segment_id, "Pruned cloud-covered remote WAL segment");
            }
            crate::storage::StorageOutcome::Err(error) => {
                tracing::warn!(
                    segment_id,
                    error = %error,
                    "Failed to prune cloud-covered remote WAL segment; will retry after a future checkpoint"
                );
            }
        }
    }

    fn verify_remote_wal_segment_before_ack(
        &mut self,
        segment_id: u64,
        max_sequence: u64,
    ) -> Result<(), String> {
        let Some(storage) = self.hybrid_storage.as_ref() else {
            return Err("CloudAck received without HybridStorage".to_string());
        };
        storage.verify_remote_wal_segment(segment_id, max_sequence)
    }

    fn handle_cloud_upload_failure(&mut self, segment_id: u64, error: &str) {
        let resource = crate::wal::cloud_segment_object_key(segment_id);
        self.state
            .cloud
            .pending_uploads
            .retain(|item| item != &resource);
        self.state.mark_persistence_anomaly();
        self.cloud_acked_wal_segments.remove(&segment_id);

        // Attempt to recover the failed segment's max_sequence so we can
        // invalidate idempotency allocations that were part of it. Keep the
        // segment in the inflight frontier: the bounded storage queue may
        // retry the same segment, and later ACKs must not skip this gap.
        let failed_max_seq = self.durability.cloud_segment_max_sequence(segment_id);

        // Let WAL actor handle its internal failure handling and drop pending writes.
        self.wal_actor.handle_cloud_upload_failed(segment_id, error);

        // If we know the max_sequence for the failed segment, invalidate idempotency
        // allocations up to that sequence so retries will allocate fresh sequences.
        if let Some(max_seq) = failed_max_seq {
            self.state.invalidate_idempotency_allocations_up_to(max_seq);
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
                    error: crate::common::MidgeError::Internal(format!(
                        "Cloud durability failed: {error}"
                    )),
                },
            );
        }

        // Keep all inflight segments. A later ACK may already be buffered, but
        // it cannot advance the frontier until this failed segment is retried
        // successfully.
    }
}
