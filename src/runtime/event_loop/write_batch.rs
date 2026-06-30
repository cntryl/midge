//! Write batching and backpressure — group commit drain and write stall management
//!
//! Contains `drain_pending_writes` (opportunistic write coalescing for group commit)
//! and `wake_write_stall_waiters` (backpressure release).

use super::super::durability::DurabilityWaiter;
use super::super::{RuntimeMsg, RuntimeResponse};
use super::EventLoop;
use crossbeam::channel::{Receiver, TryRecvError};

enum WriteResult {
    WalAppend {
        sequence: u64,
        deferred: bool,
    },
    TransactionApplied {
        last_sequence: u64,
        op_count: usize,
        deferred: bool,
    },
}

impl WriteResult {
    fn deferred(&self) -> bool {
        match self {
            WriteResult::WalAppend { deferred, .. }
            | WriteResult::TransactionApplied { deferred, .. } => *deferred,
        }
    }
}

impl EventLoop {
    pub(super) fn wake_write_stall_waiters(&mut self) {
        // Avoid borrowing issues by snapshotting keys.
        let cf_ids: Vec<crate::types::ColumnFamilyId> =
            self.write_stall_waiter_queues.keys().copied().collect();

        for cf_id in cf_ids {
            if self.state.should_stall_writes(cf_id) {
                continue;
            }

            let Some(mut queue) = self.write_stall_waiter_queues.remove(&cf_id) else {
                continue;
            };

            while let Some(wait_request_id) = queue.pop_front() {
                // Only complete if still registered (not canceled/timeouts).
                if self.write_stall_waiters.remove(&wait_request_id).is_some() {
                    self.respond(
                        wait_request_id,
                        RuntimeResponse::Ok {
                            request_id: wait_request_id,
                        },
                    );
                }
            }
        }
    }

    /// Opportunistically drain pending *write* messages from the channel.
    ///
    /// This improves group commit by coalescing bursts of concurrent writers into a single WAL sync.
    /// If a non-write message is encountered, it is stashed in `self.pending_msg` to preserve FIFO
    /// semantics (since we cannot "un-recv" with crossbeam channels).
    pub(super) fn drain_pending_writes(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
        max: usize,
    ) -> usize {
        if self.wal_actor.is_cloud_async() {
            return 0;
        }

        // IMPORTANT: If we already have a buffered non-write message, do not `try_recv()`.
        // Otherwise we could consume another non-write message and have nowhere to stash it.
        // The stashed message is always consumed on the next main-loop recv (see event_loop mod).
        if self.pending_msg.is_some() {
            return 0;
        }

        let mut drained = 0usize;

        while drained < max {
            match self.drain_one_pending_write(msg_rx) {
                DrainOutcome::WriteHandled => drained += 1,
                DrainOutcome::StashedNonWrite | DrainOutcome::ChannelEmpty => break,
            }
        }

        if drained > 0 && self.trace_enabled {
            tracing::trace!(drained, "drained pending writes");
        }

        if drained > 0 {
            self.drain_auto_flush_memtables();
        }

        drained
    }

    fn drain_one_pending_write(&mut self, msg_rx: &Receiver<RuntimeMsg>) -> DrainOutcome {
        match msg_rx.try_recv() {
            Ok(RuntimeMsg::WalAppend {
                request_id,
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            }) => {
                let result = self.wal_actor.append(
                    &mut self.state,
                    crate::runtime::actors::wal::AppendParams {
                        request_id,
                        cf_id,
                        key: bytes::Bytes::from(key),
                        value: value.map(bytes::Bytes::from),
                        insert_only,
                        ttl_seconds,
                    },
                );
                self.finish_drained_write(
                    request_id,
                    result
                        .map(|(sequence, deferred)| WriteResult::WalAppend { sequence, deferred }),
                );
                DrainOutcome::WriteHandled
            }
            Ok(RuntimeMsg::WalAppendDeleteRange {
                request_id,
                cf_id,
                start_key,
                end_key,
                durability_policy,
            }) => {
                let result = self.wal_actor.append_delete_range(
                    &mut self.state,
                    request_id,
                    cf_id,
                    bytes::Bytes::from(start_key),
                    bytes::Bytes::from(end_key),
                    durability_policy,
                );
                self.finish_drained_write(
                    request_id,
                    result
                        .map(|(sequence, deferred)| WriteResult::WalAppend { sequence, deferred }),
                );
                DrainOutcome::WriteHandled
            }
            Ok(RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy,
                start_sequence,
                isolation_policy,
            }) => {
                let result = self
                    .wal_actor
                    .append_transaction(
                        &mut self.state,
                        request_id,
                        ops,
                        durability_policy,
                        start_sequence,
                        isolation_policy,
                    )
                    .map(
                        |(last_sequence, op_count, deferred)| WriteResult::TransactionApplied {
                            last_sequence,
                            op_count,
                            deferred,
                        },
                    );
                self.finish_drained_write(request_id, result);
                DrainOutcome::WriteHandled
            }
            Ok(other) => {
                if self.pending_msg.is_none() {
                    self.pending_msg = Some(other);
                }
                DrainOutcome::StashedNonWrite
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => DrainOutcome::ChannelEmpty,
        }
    }

    fn finish_drained_write(
        &mut self,
        request_id: u64,
        result: Result<WriteResult, crate::common::MidgeError>,
    ) {
        match result {
            Ok(result) => self.handle_write_success(request_id, &result),
            Err(error) => self.handle_write_error(request_id, error),
        }
    }

    fn handle_write_success(&mut self, request_id: u64, result: &WriteResult) {
        self.publish_snapshot();

        let is_transaction = matches!(result, WriteResult::TransactionApplied { .. });
        let deferred = result.deferred();
        if self.should_ack_immediately(deferred) {
            if deferred {
                self.maybe_queue_confirm_only_waiter(deferred, request_id, is_transaction);
            } else {
                if is_transaction {
                    self.state.clear_pending_transaction_barrier();
                }
                self.state.confirm_sequences(request_id);
            }

            match result {
                WriteResult::WalAppend { sequence, .. } => {
                    self.respond(
                        request_id,
                        RuntimeResponse::WalAppended {
                            request_id,
                            sequence: *sequence,
                        },
                    );
                }
                WriteResult::TransactionApplied {
                    last_sequence,
                    op_count,
                    ..
                } => {
                    self.respond(
                        request_id,
                        RuntimeResponse::TransactionApplied {
                            request_id,
                            last_sequence: *last_sequence,
                            op_count: *op_count,
                            write_stall_hint: self.state.should_stall_writes(0),
                        },
                    );
                }
            }
        } else {
            match result {
                WriteResult::WalAppend { sequence, .. } => {
                    self.durability.queue_waiter(DurabilityWaiter::WalAppend {
                        request_id,
                        sequence: *sequence,
                    });
                }
                WriteResult::TransactionApplied {
                    last_sequence,
                    op_count,
                    ..
                } => {
                    self.durability
                        .queue_waiter(DurabilityWaiter::TransactionApply {
                            request_id,
                            last_sequence: *last_sequence,
                            op_count: *op_count,
                        });
                }
            }
        }
    }

    fn handle_write_error(&mut self, request_id: u64, error: crate::common::MidgeError) {
        self.respond(request_id, RuntimeResponse::Error { request_id, error });
    }
}

enum DrainOutcome {
    WriteHandled,
    StashedNonWrite,
    ChannelEmpty,
}
