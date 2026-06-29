use super::super::durability::DurabilityWaiter;
use super::{EventLoop, HandleOutcome};
use crate::runtime::{RuntimeMsg, RuntimeResponse, TransactionIsolationPolicy, TransactionOp};
use crate::wal::DurabilityPolicy;
use crossbeam::channel::Receiver;

pub(super) struct WalCoordinator;

impl WalCoordinator {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_transaction(
        event_loop: &mut EventLoop,
        msg_rx: &Receiver<RuntimeMsg>,
        request_id: u64,
        ops: Vec<TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
        start_sequence: Option<u64>,
        isolation_policy: TransactionIsolationPolicy,
    ) -> HandleOutcome {
        if !Self::accept_write(event_loop, request_id) {
            return HandleOutcome::Continue;
        }

        match event_loop.wal_actor.append_transaction(
            &mut event_loop.state,
            request_id,
            ops,
            durability_policy,
            start_sequence,
            isolation_policy,
        ) {
            Ok((last_sequence, op_count, deferred)) => {
                event_loop.publish_snapshot();

                if event_loop.should_ack_immediately(deferred) {
                    if event_loop.wal_actor.is_cloud_async() {
                        event_loop
                            .durability
                            .queue_waiter(DurabilityWaiter::ConfirmTransactionApply { request_id });
                    } else if deferred {
                        event_loop.maybe_queue_confirm_only_waiter(deferred, request_id, true);
                    } else {
                        event_loop.state.clear_pending_transaction_barrier();
                        event_loop.state.confirm_sequences(request_id);
                    }

                    event_loop.respond(
                        request_id,
                        RuntimeResponse::TransactionApplied {
                            request_id,
                            last_sequence,
                            op_count,
                            write_stall_hint: event_loop.state.should_stall_writes(0),
                        },
                    );
                } else {
                    event_loop
                        .durability
                        .queue_waiter(DurabilityWaiter::TransactionApply {
                            request_id,
                            last_sequence,
                            op_count,
                        });
                }
            }
            Err(error) => {
                event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            }
        }

        if !event_loop.wal_actor.is_cloud_async() {
            const MAX_DRAIN_WRITES_AFTER_BATCH: usize = 1024;
            let _ = event_loop.drain_pending_writes(msg_rx, MAX_DRAIN_WRITES_AFTER_BATCH);
            event_loop.sync_batched_wal_if_needed(msg_rx);
        }

        event_loop.maybe_flush_cloud_async_wal();
        event_loop.drain_auto_flush_memtables();
        HandleOutcome::Continue
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn append(
        event_loop: &mut EventLoop,
        msg_rx: &Receiver<RuntimeMsg>,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        ttl_seconds: Option<u64>,
        insert_only: bool,
    ) -> HandleOutcome {
        if !Self::accept_write(event_loop, request_id) {
            return HandleOutcome::Continue;
        }

        let result = event_loop.wal_actor.append(
            &mut event_loop.state,
            crate::runtime::actors::wal::AppendParams {
                request_id,
                cf_id,
                key: bytes::Bytes::from(key),
                value: value.map(bytes::Bytes::from),
                insert_only,
                ttl_seconds,
            },
        );
        match result {
            Ok((sequence, deferred)) => {
                Self::handle_append_success(event_loop, request_id, sequence, deferred);
            }
            Err(error) => {
                event_loop.respond(
                    request_id,
                    RuntimeResponse::Error {
                        request_id,
                        error: crate::common::MidgeError::Internal(error.to_string()),
                    },
                );
            }
        }

        event_loop.sync_batched_wal_if_needed(msg_rx);
        event_loop.maybe_flush_cloud_async_wal();
        event_loop.drain_auto_flush_memtables();
        HandleOutcome::Continue
    }

    pub(super) fn append_delete_range(
        event_loop: &mut EventLoop,
        msg_rx: &Receiver<RuntimeMsg>,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        durability_policy: Option<DurabilityPolicy>,
    ) -> HandleOutcome {
        if !Self::accept_write(event_loop, request_id) {
            return HandleOutcome::Continue;
        }

        let result = event_loop.wal_actor.append_delete_range(
            &mut event_loop.state,
            request_id,
            cf_id,
            bytes::Bytes::from(start_key),
            bytes::Bytes::from(end_key),
            durability_policy,
        );
        match result {
            Ok((sequence, deferred)) => {
                Self::handle_append_success(event_loop, request_id, sequence, deferred);
            }
            Err(error) => {
                event_loop.respond(
                    request_id,
                    RuntimeResponse::Error {
                        request_id,
                        error: crate::common::MidgeError::Internal(error.to_string()),
                    },
                );
            }
        }

        event_loop.sync_batched_wal_if_needed(msg_rx);
        event_loop.maybe_flush_cloud_async_wal();
        event_loop.drain_auto_flush_memtables();
        HandleOutcome::Continue
    }

    pub(super) fn sync(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        let result = event_loop.wal_actor.sync(&mut event_loop.state);
        let resp = result
            .map(|_| RuntimeResponse::Ok { request_id })
            .unwrap_or_else(|error| RuntimeResponse::Error {
                request_id,
                error: crate::common::MidgeError::Internal(error.to_string()),
            });
        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    pub(super) fn rotate(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        let result = event_loop.wal_actor.rotate(&mut event_loop.state);
        let resp = result
            .map(|_| RuntimeResponse::Ok { request_id })
            .unwrap_or_else(|error| RuntimeResponse::Error {
                request_id,
                error: crate::common::MidgeError::Internal(error.to_string()),
            });
        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    pub(super) fn seal_for_cloud(
        event_loop: &mut EventLoop,
        request_id: u64,
        sequence: u64,
        wait_for_ack: bool,
    ) -> HandleOutcome {
        if !event_loop.wal_actor.is_cloud_async() {
            let resp = if event_loop.state.wal.local_durable_seq >= sequence {
                RuntimeResponse::Ok { request_id }
            } else {
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::InvalidArgument(
                        "cloud durability requested outside cloud-backed mode".to_string(),
                    ),
                }
            };
            event_loop.respond(request_id, resp);
            return HandleOutcome::Continue;
        }

        if let Err(error) = event_loop.check_lease_health() {
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            return HandleOutcome::Continue;
        }

        if event_loop.state.wal.cloud_durable_seq >= sequence {
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
            return HandleOutcome::Continue;
        }

        let mut inflight_segment = event_loop
            .durability
            .inflight_segment_for_sequence(sequence);
        if inflight_segment.is_none()
            && (event_loop.state.wal.pending_writes > 0
                || event_loop.state.wal.local_durable_seq < sequence)
        {
            match event_loop.seal_current_cloud_segment() {
                Ok(Some((segment_id, max_sequence))) => {
                    if max_sequence < sequence {
                        event_loop.respond(
                            request_id,
                            RuntimeResponse::Error {
                                request_id,
                                error: crate::common::MidgeError::Internal(format!(
                                    "sealed WAL segment up to sequence {max_sequence}, but requested cloud durability for {sequence}"
                                )),
                            },
                        );
                        return HandleOutcome::Continue;
                    }
                    inflight_segment = Some(segment_id);
                }
                Ok(None) => {}
                Err(error) => {
                    event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
                    return HandleOutcome::Continue;
                }
            }
        }

        if !wait_for_ack || event_loop.state.wal.cloud_durable_seq >= sequence {
            event_loop.drain_auto_flush_memtables();
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        } else if let Some(segment_id) = inflight_segment {
            event_loop.drain_auto_flush_memtables();
            event_loop
                .durability
                .queue_waiter_for_key(segment_id, DurabilityWaiter::CloudDurability { request_id });
        } else {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(format!(
                        "no inflight cloud upload covers sequence {sequence}"
                    )),
                },
            );
        }
        HandleOutcome::Continue
    }

    pub(super) fn sync_complete(
        event_loop: &mut EventLoop,
        request_id: u64,
        segment_id: u64,
    ) -> HandleOutcome {
        event_loop
            .wal_actor
            .handle_sync_complete(&mut event_loop.state, segment_id);
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    fn accept_write(event_loop: &mut EventLoop, request_id: u64) -> bool {
        if let Err(error) = event_loop.check_lease_health() {
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            return false;
        }

        if event_loop.wal_actor.is_cloud_async() && event_loop.hybrid_storage.is_none() {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(
                        "CloudAsync requires HybridStorage".to_string(),
                    ),
                },
            );
            return false;
        }

        true
    }

    fn handle_append_success(
        event_loop: &mut EventLoop,
        request_id: u64,
        sequence: u64,
        deferred: bool,
    ) {
        event_loop.publish_snapshot();

        if event_loop.should_ack_immediately(deferred) {
            if event_loop.wal_actor.is_cloud_async() {
                event_loop.state.confirm_sequences(request_id);
            } else if deferred {
                event_loop.maybe_queue_confirm_only_waiter(deferred, request_id, false);
            } else {
                event_loop.state.confirm_sequences(request_id);
            }

            event_loop.respond(
                request_id,
                RuntimeResponse::WalAppended {
                    request_id,
                    sequence,
                },
            );
        } else {
            event_loop
                .durability
                .queue_waiter(DurabilityWaiter::WalAppend {
                    request_id,
                    sequence,
                });
        }
    }
}
