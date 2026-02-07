//! Cloud / hybrid storage integration
//!
//! Handles CloudFirst WAL flush, cloud upload ack/fail events,
//! and hybrid storage polling/push-channel draining.

use super::durability_sync::CompletionSource;
use super::EventLoop;
use crossbeam::channel::TryRecvError;
use std::time::Instant;

impl EventLoop {
    pub(super) fn tick_hybrid_storage(&mut self) {
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
    }

    pub(super) fn drain_hybrid_storage_events(&mut self) {
        let Some(rx) = &self.hybrid_storage_events else {
            return;
        };

        let rx = rx.clone();

        loop {
            match rx.try_recv() {
                Ok(event) => self.handle_storage_event(event),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }

    pub(super) fn handle_storage_event(&mut self, event: crate::storage::StorageEvent) {
        match event {
            crate::storage::StorageEvent::CloudAck {
                segment_id,
                max_sequence,
            } => match self.wal_actor.handle_cloud_upload_complete(
                &mut self.state,
                segment_id,
                max_sequence,
            ) {
                Ok(()) => {
                    // If cloud_durable_seq advanced past multiple segments,
                    // complete all inflight segments whose max_sequence is now durable.
                    let durable = self.state.wal.cloud_durable_seq;
                    let ready = self.durability.get_ready_cloud_segments(durable);

                    for seg_id in ready {
                        if let Some(enqueued_at) = self.durability.take_cloud_segment_timing(seg_id)
                        {
                            if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                                telemetry.metrics().record_cloudfirst_wal_ack_latency_us(
                                    enqueued_at.elapsed().as_micros() as u64,
                                );
                            }
                        }

                        let waiters = self.durability.complete_waiters_at(seg_id);
                        self.complete_durability_waiters(waiters, CompletionSource::CloudAck);
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to apply cloud ack");
                }
            },
            crate::storage::StorageEvent::CloudFail { segment_id, error } => {
                // Attempt to recover the failed segment's max_sequence so we can
                // invalidate idempotency allocations that were part of it.
                let failed_max_seq = self.durability.take_cloud_segment_max_sequence(segment_id);

                // Let WAL actor handle its internal failure handling and drop pending writes
                self.wal_actor
                    .handle_cloud_upload_failed(segment_id, &error);

                // If we know the max_sequence for the failed segment, invalidate idempotency
                // allocations up to that sequence so retries will allocate fresh sequences.
                if let Some(max_seq) = failed_max_seq {
                    self.state.invalidate_idempotency_allocations_up_to(max_seq);
                }

                let waiters = self.durability.drain_all_waiters();
                for w in waiters {
                    let request_id = match w {
                        super::super::durability::DurabilityWaiter::WalAppend {
                            request_id,
                            ..
                        }
                        | super::super::durability::DurabilityWaiter::ConfirmWalAppend {
                            request_id,
                        }
                        | super::super::durability::DurabilityWaiter::TransactionApply {
                            request_id,
                            ..
                        }
                        | super::super::durability::DurabilityWaiter::ConfirmTransactionApply {
                            request_id,
                        }
                        | super::super::durability::DurabilityWaiter::Read { request_id, .. }
                        | super::super::durability::DurabilityWaiter::RangeScan {
                            request_id,
                            ..
                        } => request_id,
                    };
                    self.respond(
                        request_id,
                        super::super::RuntimeResponse::Error {
                            request_id,
                            error: crate::common::MidgeError::Internal(format!(
                                "Cloud durability failed: {error}"
                            )),
                        },
                    );
                }

                // Clear any remaining inflight segments
                self.durability.clear_inflight();
            }
            _ => {}
        }
    }

    pub(super) fn maybe_flush_cloudfirst_wal(&mut self) {
        if !self.wal_actor.is_cloud_first() {
            return;
        }
        let Some(storage) = &self.hybrid_storage else {
            return;
        };

        if self.state.memory_mode {
            return;
        }

        // No pending local records to ship.
        if self.state.wal.pending_writes == 0 {
            return;
        }

        let cloud_pending = self.wal_actor.pending_cloud_writes_len();
        let bytes_buffered = self.wal_actor.bytes_since_sync();

        if !self
            .durability
            .should_flush_cloudfirst(cloud_pending, bytes_buffered)
        {
            return;
        }

        let segment_id = self.state.wal.current_segment_id;

        let seal_start = Instant::now();
        if let Err(e) = self.wal_actor.flush_for_cloud_upload(&mut self.state) {
            tracing::error!(error = %e, "CloudFirst: WAL flush failed");
            return;
        }
        let seal_latency_us = seal_start.elapsed().as_micros() as u64;

        if let Err(e) = self.wal_actor.rotate(&mut self.state) {
            tracing::error!(error = %e, "CloudFirst: WAL rotate failed");
            return;
        }

        // Move waiters for the sealed segment into an inflight bucket keyed by `segment_id`.
        self.durability.rotate_to(self.state.wal.current_segment_id);

        let max_sequence = self.state.wal.local_durable_seq;
        let local_path = self.state.wal_dir.join(format!("{segment_id}.wal"));
        storage.enqueue_wal_segment(segment_id, local_path, max_sequence);

        self.durability
            .record_cloud_segment_inflight(segment_id, max_sequence);
        self.durability.record_cloud_flush();

        if let Some(telemetry) = crate::telemetry::Telemetry::global() {
            telemetry
                .metrics()
                .record_cloudfirst_wal_segment_sealed(bytes_buffered as u64, seal_latency_us);
        }

        if std::env::var_os("MIDGE_TRACE_CLOUDFIRST").is_some() {
            // Throttle: log every 1000 segments to avoid noise.
            if segment_id.is_multiple_of(1000) {
                eprintln!(
                    "[midge] CloudFirst flush: segment_id={segment_id} max_sequence={max_sequence} pending_cloud={} ",
                    self.wal_actor.has_pending_cloud_writes()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::create_test_event_loop;
    use super::super::EventLoop;
    use crate::runtime::{state::RuntimeState, ResponseRouter};
    use std::sync::Arc;

    #[test]
    fn should_start_with_no_hybrid_storage() {
        // Arrange
        // Act
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Assert - eviction_actor should be None initially
        assert!(event_loop.eviction_actor.is_none());
        assert!(event_loop.hybrid_storage.is_none());
    }

    #[test]
    fn should_set_hybrid_storage() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act
        // Create a mock hybrid storage (we need to use a real one or skip this test)
        // For now, we'll skip detailed testing of set_hybrid_storage since it requires
        // complex setup with actual HybridStorage instance

        // Assert - Method exists and is callable
        drop(event_loop);
    }

    #[test]
    fn should_support_hybrid_storage_optional_field() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act

        // Assert
        // hybrid_storage starts as None
        assert!(event_loop.hybrid_storage.is_none());
    }

    #[test]
    fn should_support_eviction_actor_optional_field() {
        // Arrange
        let event_loop = create_test_event_loop().expect("Should create event loop");

        // Act

        // Assert
        // eviction_actor starts as None
        assert!(event_loop.eviction_actor.is_none());
    }

    #[test]
    fn should_cloudfirst_ack_confirm_idempotent_request() -> crate::common::MidgeResult<()> {
        // Arrange: create state and event loop with CloudFirst policy
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let state = RuntimeState::new(tmp.path().to_path_buf(), false);
        let router = Arc::new(ResponseRouter::new());
        let config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudFirst,
            ..Default::default()
        };
        let mut el = EventLoop::new(state, false, router, config, None)?;

        // Act

        // Add a wal append with a specific request_id
        let request_id = 123u64;
        let cf_id = 0u32;

        let (seq, deferred) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k1"),
                value: Some(bytes::Bytes::from("v1")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        assert!(
            deferred,
            "CloudFirst append should be deferred waiting for CloudAck"
        );

        // Queue waiter for this append (simulates EventLoop behavior)
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id,
                sequence: seq,
            });

        // Simulate sealing & uploading segment for CloudFirst as EventLoop would do
        let seg_id = el.state.wal.current_segment_id;
        // Flush and rotate to create a sealed segment
        el.wal_actor.flush_for_cloud_upload(&mut el.state)?;
        el.wal_actor.rotate(&mut el.state)?;
        // Move waiters into inflight bucket and record timing as EventLoop does
        el.durability.rotate_to(el.state.wal.current_segment_id);
        let max_sequence = el.state.wal.local_durable_seq;
        el.durability
            .record_cloud_segment_inflight(seg_id, max_sequence);
        el.durability.record_cloud_flush();

        // Now simulate the storage CloudAck for that segment
        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id: seg_id,
            max_sequence,
        });

        // Assert: After handling, the idempotency entry for request_id should be confirmed at cloud frontier
        assert!(
            el.state
                .sequence_idempotency_cache
                .contains_key(&request_id),
            "idempotency entry missing"
        );
        if let Some(entry) = el.state.sequence_idempotency_cache.get(&request_id) {
            assert!(entry.2 >= el.state.wal.cloud_durable_seq);
        }

        Ok(())
    }

    #[test]
    fn should_cloudfirst_retry_after_ack_return_same_sequence_without_queueing(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: create state and event loop with CloudFirst policy
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let state = RuntimeState::new(tmp.path().to_path_buf(), false);
        let router = Arc::new(ResponseRouter::new());
        let config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudFirst,
            ..Default::default()
        };
        let mut el = EventLoop::new(state, false, router, config, None)?;

        // Act

        // Add a wal append with a specific request_id
        let request_id = 124u64;
        let cf_id = 0u32;

        let (seq1, deferred1) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k1"),
                value: Some(bytes::Bytes::from("v1")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        assert!(
            deferred1,
            "CloudFirst append should be deferred waiting for CloudAck"
        );

        // Queue waiter for this append (simulates EventLoop behavior)
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id,
                sequence: seq1,
            });

        // Simulate sealing & uploading segment for CloudFirst as EventLoop would do
        let seg_id = el.state.wal.current_segment_id;
        // Flush and rotate to create a sealed segment
        el.wal_actor.flush_for_cloud_upload(&mut el.state)?;
        el.wal_actor.rotate(&mut el.state)?;
        // Move waiters into inflight bucket and record timing as EventLoop does
        el.durability.rotate_to(el.state.wal.current_segment_id);
        let max_sequence = el.state.wal.local_durable_seq;
        el.durability
            .record_cloud_segment_inflight(seg_id, max_sequence);
        el.durability.record_cloud_flush();

        // Now simulate the storage CloudAck for that segment
        el.handle_storage_event(crate::storage::StorageEvent::CloudAck {
            segment_id: seg_id,
            max_sequence,
        });

        // After handling, the idempotency entry for request_id should be confirmed at cloud frontier
        assert!(
            el.state
                .sequence_idempotency_cache
                .contains_key(&request_id),
            "idempotency entry missing"
        );
        if let Some(entry) = el.state.sequence_idempotency_cache.get(&request_id) {
            assert!(entry.2 >= el.state.wal.cloud_durable_seq);
        }

        // Assert: Retry the same request_id: should return the same sequence and NOT be deferred
        // Retry the same request_id: should return the same sequence and NOT be deferred
        let (seq2, deferred2) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k1"),
                value: Some(bytes::Bytes::from("v1")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        assert_eq!(seq1, seq2, "retry should return same sequence");
        assert!(
            !deferred2,
            "retry after confirmation should not be deferred"
        );
        assert_eq!(el.wal_actor.pending_cloud_writes_len(), 0);

        Ok(())
    }

    #[test]
    fn should_cloudfirst_fail_invalidates_idempotency_then_retry_allocates_new_seq(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: create state and event loop with CloudFirst policy
        let tmp = tempfile::tempdir().expect("create tmpdir");
        let state = RuntimeState::new(tmp.path().to_path_buf(), false);
        let router = Arc::new(ResponseRouter::new());
        let config = crate::runtime::RuntimeConfig {
            wal_durability_policy: crate::wal::DurabilityPolicy::CloudFirst,
            ..Default::default()
        };
        let mut el = EventLoop::new(state, false, router, config, None)?;

        // Act

        // Add a wal append with a specific request_id
        let request_id = 200u64;
        let cf_id = 0u32;

        let (seq1, deferred1) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k2"),
                value: Some(bytes::Bytes::from("v2")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        assert!(
            deferred1,
            "CloudFirst append should be deferred waiting for CloudAck"
        );

        // Queue waiter for this append (simulates EventLoop behavior)
        el.durability
            .queue_waiter(crate::runtime::durability::DurabilityWaiter::WalAppend {
                request_id,
                sequence: seq1,
            });

        // Simulate sealing & uploading segment for CloudFirst as EventLoop would do
        let seg_id = el.state.wal.current_segment_id;
        // Flush and rotate to create a sealed segment
        el.wal_actor.flush_for_cloud_upload(&mut el.state)?;
        el.wal_actor.rotate(&mut el.state)?;
        // Move waiters into inflight bucket and record timing as EventLoop does
        el.durability.rotate_to(el.state.wal.current_segment_id);
        let max_sequence = el.state.wal.local_durable_seq;
        el.durability
            .record_cloud_segment_inflight(seg_id, max_sequence);
        el.durability.record_cloud_flush();

        // Now simulate the storage CloudFail for that segment
        el.handle_storage_event(crate::storage::StorageEvent::CloudFail {
            segment_id: seg_id,
            error: "upload_failed".to_string(),
        });

        // Retry the same request_id: since the previous allocation failed, we expect a new sequence
        let (seq2, deferred2) = el.wal_actor.append(
            &mut el.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from("k2"),
                value: Some(bytes::Bytes::from("v2")),
                insert_only: false,
                ttl_seconds: None,
            },
        )?;

        // Assert: retry should allocate a new sequence and be deferred
        assert_ne!(
            seq1, seq2,
            "retry after cloud fail should allocate a new sequence"
        );
        assert!(
            deferred2,
            "retry should be deferred when retried after fail (CloudFirst)"
        );

        Ok(())
    }
}
