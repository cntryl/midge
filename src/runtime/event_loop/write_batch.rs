//! Write batching and backpressure — group commit drain and write stall management
//!
//! Contains `drain_pending_writes` (opportunistic write coalescing for group commit)
//! and `wake_write_stall_waiters` (backpressure release).

use super::super::durability::DurabilityWaiter;
use super::super::{RuntimeMsg, RuntimeResponse, TransactionOp};
use super::wal::ApplyTransactionRequest;
use super::EventLoop;
use crate::common::MidgeError;
use crossbeam::channel::{Receiver, TryRecvError};
use std::collections::HashSet;

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
            match self.drain_one_pending_write(msg_rx, max - drained) {
                DrainOutcome::WritesHandled(count) => drained += count,
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

    fn drain_one_pending_write(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
        max: usize,
    ) -> DrainOutcome {
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
                DrainOutcome::WritesHandled(1)
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
                DrainOutcome::WritesHandled(1)
            }
            Ok(RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy,
                start_sequence,
                isolation_policy,
            }) => {
                let handled = self.apply_transaction_with_coalescing(
                    msg_rx,
                    ApplyTransactionRequest {
                        request_id,
                        ops,
                        durability_policy,
                        start_sequence,
                        isolation_policy,
                    },
                    max,
                );
                DrainOutcome::WritesHandled(handled)
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

    pub(super) fn apply_transaction_with_coalescing(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
        initial: ApplyTransactionRequest,
        max: usize,
    ) -> usize {
        if max == 0 {
            return 0;
        }

        if !self
            .wal_actor
            .can_coalesce_transaction_append(initial.durability_policy)
        {
            self.apply_single_transaction_request(initial);
            return 1;
        }

        let mut batch = CoalescedTransactionBatch::default();

        match self.prepare_transaction_for_coalescing(initial, &mut batch.staged_touches) {
            PrepareOutcome::Prepared {
                request_id,
                prepared_transaction,
            } => {
                batch.push_prepared(request_id, prepared_transaction);
            }
            PrepareOutcome::Fallback(request) => {
                self.apply_single_transaction_request(request);
                return 1;
            }
            PrepareOutcome::Error { request_id, error } => {
                self.finish_drained_write(request_id, Err(error));
                return 1;
            }
        }

        self.collect_coalesced_transaction_batch(msg_rx, max, &mut batch);
        self.finish_coalesced_transaction_batch(batch)
    }

    fn collect_coalesced_transaction_batch(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
        max: usize,
        batch: &mut CoalescedTransactionBatch,
    ) {
        while batch.handled < max {
            match msg_rx.try_recv() {
                Ok(RuntimeMsg::ApplyTransaction {
                    request_id,
                    ops,
                    durability_policy,
                    start_sequence,
                    isolation_policy,
                }) => {
                    let request = ApplyTransactionRequest {
                        request_id,
                        ops,
                        durability_policy,
                        start_sequence,
                        isolation_policy,
                    };
                    match self
                        .prepare_transaction_for_coalescing(request, &mut batch.staged_touches)
                    {
                        PrepareOutcome::Prepared {
                            request_id,
                            prepared_transaction,
                        } => {
                            batch.push_prepared(request_id, prepared_transaction);
                        }
                        PrepareOutcome::Fallback(request) => {
                            batch.fallback_request = Some(request);
                            break;
                        }
                        PrepareOutcome::Error { request_id, error } => {
                            batch.deferred_error = Some((request_id, error));
                            break;
                        }
                    }
                }
                Ok(other) => {
                    if self.pending_msg.is_none() {
                        self.pending_msg = Some(other);
                    }
                    break;
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
    }

    fn finish_coalesced_transaction_batch(&mut self, batch: CoalescedTransactionBatch) -> usize {
        let CoalescedTransactionBatch {
            staged_touches: _,
            prepared,
            request_ids,
            mut handled,
            fallback_request,
            deferred_error,
        } = batch;

        match self
            .wal_actor
            .append_prepared_transactions(&mut self.state, prepared)
        {
            Ok(results) => {
                for result in results {
                    self.finish_drained_write(
                        result.request_id,
                        Ok(WriteResult::TransactionApplied {
                            last_sequence: result.last_sequence,
                            op_count: result.op_count,
                            deferred: result.deferred,
                        }),
                    );
                }
            }
            Err(error) => {
                for request_id in request_ids {
                    self.finish_drained_write(request_id, Err(duplicate_midge_error(&error)));
                }
            }
        }

        if let Some((request_id, error)) = deferred_error {
            self.finish_drained_write(request_id, Err(error));
            handled += 1;
        }

        if let Some(request) = fallback_request {
            self.apply_single_transaction_request(request);
            handled += 1;
        }

        handled
    }

    fn prepare_transaction_for_coalescing(
        &mut self,
        request: ApplyTransactionRequest,
        staged_touches: &mut StagedTransactionTouches,
    ) -> PrepareOutcome {
        if request.ops.is_empty()
            || !self
                .wal_actor
                .can_coalesce_transaction_append(request.durability_policy)
            || staged_touches.touches_ops(&request.ops)
        {
            return PrepareOutcome::Fallback(request);
        }

        let ops_for_staging = request.ops.clone();
        let request_id = request.request_id;
        let result = self.wal_actor.prepare_transaction_append(
            &mut self.state,
            crate::runtime::actors::wal::TransactionAppendParams {
                request_id,
                ops: request.ops,
                durability_policy: request.durability_policy,
                start_sequence: request.start_sequence,
                isolation_policy: request.isolation_policy,
            },
        );

        match result {
            Ok(prepared_transaction) => {
                staged_touches.record_ops(&ops_for_staging);
                PrepareOutcome::Prepared {
                    request_id,
                    prepared_transaction,
                }
            }
            Err(error) => PrepareOutcome::Error { request_id, error },
        }
    }

    fn apply_single_transaction_request(&mut self, request: ApplyTransactionRequest) {
        let ApplyTransactionRequest {
            request_id,
            ops,
            durability_policy,
            start_sequence,
            isolation_policy,
        } = request;
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
    WritesHandled(usize),
    StashedNonWrite,
    ChannelEmpty,
}

enum PrepareOutcome {
    Prepared {
        request_id: u64,
        prepared_transaction: crate::runtime::actors::wal::PreparedTransactionAppend,
    },
    Fallback(ApplyTransactionRequest),
    Error {
        request_id: u64,
        error: MidgeError,
    },
}

#[derive(Default)]
struct CoalescedTransactionBatch {
    staged_touches: StagedTransactionTouches,
    prepared: Vec<crate::runtime::actors::wal::PreparedTransactionAppend>,
    request_ids: Vec<u64>,
    handled: usize,
    fallback_request: Option<ApplyTransactionRequest>,
    deferred_error: Option<(u64, MidgeError)>,
}

impl CoalescedTransactionBatch {
    fn push_prepared(
        &mut self,
        request_id: u64,
        prepared_transaction: crate::runtime::actors::wal::PreparedTransactionAppend,
    ) {
        self.request_ids.push(request_id);
        self.prepared.push(prepared_transaction);
        self.handled += 1;
    }
}

#[derive(Default)]
struct StagedTransactionTouches {
    point_keys: HashSet<(crate::types::ColumnFamilyId, Vec<u8>)>,
    ranges: Vec<StagedRange>,
}

struct StagedRange {
    cf_id: crate::types::ColumnFamilyId,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
}

impl StagedTransactionTouches {
    fn touches_ops(&self, ops: &[TransactionOp]) -> bool {
        ops.iter().any(|op| match op {
            TransactionOp::Put { cf_id, key, .. } | TransactionOp::Delete { cf_id, key } => {
                self.touches_point(*cf_id, key)
            }
            TransactionOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
            } => self.touches_range(*cf_id, start_key, end_key),
        })
    }

    fn record_ops(&mut self, ops: &[TransactionOp]) {
        for op in ops {
            match op {
                TransactionOp::Put { cf_id, key, .. } | TransactionOp::Delete { cf_id, key } => {
                    self.point_keys.insert((*cf_id, key.to_vec()));
                }
                TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } => self.ranges.push(StagedRange {
                    cf_id: *cf_id,
                    start_key: start_key.to_vec(),
                    end_key: end_key.to_vec(),
                }),
            }
        }
    }

    fn touches_point(&self, cf_id: crate::types::ColumnFamilyId, key: &[u8]) -> bool {
        self.point_keys.contains(&(cf_id, key.to_vec()))
            || self.ranges.iter().any(|range| {
                range.cf_id == cf_id
                    && key >= range.start_key.as_slice()
                    && key < range.end_key.as_slice()
            })
    }

    fn touches_range(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        start_key: &[u8],
        end_key: &[u8],
    ) -> bool {
        self.point_keys.iter().any(|(point_cf_id, key)| {
            *point_cf_id == cf_id && key.as_slice() >= start_key && key.as_slice() < end_key
        }) || self.ranges.iter().any(|range| {
            range.cf_id == cf_id
                && range.start_key.as_slice() < end_key
                && range.end_key.as_slice() > start_key
        })
    }
}

fn duplicate_midge_error(error: &MidgeError) -> MidgeError {
    match error {
        MidgeError::Io(io_error) => {
            MidgeError::Io(std::io::Error::new(io_error.kind(), io_error.to_string()))
        }
        MidgeError::NotFound => MidgeError::NotFound,
        MidgeError::InvalidArgument(message) => MidgeError::InvalidArgument(message.clone()),
        MidgeError::Corruption(message) => MidgeError::Corruption(message.clone()),
        MidgeError::NotSupported(message) => MidgeError::NotSupported(message.clone()),
        MidgeError::Internal(message) => MidgeError::Internal(message.clone()),
        MidgeError::InvalidPath => MidgeError::InvalidPath,
        MidgeError::NoSpace(message) => MidgeError::NoSpace(message.clone()),
        MidgeError::RecoveryFailed(message) => MidgeError::RecoveryFailed(message.clone()),
        MidgeError::CompatibilityError(message) => MidgeError::CompatibilityError(message.clone()),
        MidgeError::WriteStall(message) => MidgeError::WriteStall(message.clone()),
        MidgeError::MemoryModeViolation(message) => {
            MidgeError::MemoryModeViolation(message.clone())
        }
        MidgeError::Fenced(message) => MidgeError::Fenced(message.clone()),
        MidgeError::WriteConflict(message) => MidgeError::WriteConflict(message.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn should_detect_point_and_range_touches_when_staging_transactions() {
        // Arrange
        let mut touches = StagedTransactionTouches::default();
        touches.record_ops(&[
            TransactionOp::Put {
                cf_id: 0,
                key: Bytes::from_static(b"point"),
                value: Bytes::from_static(b"value"),
                ttl_seconds: None,
                insert_only: false,
            },
            TransactionOp::DeleteRange {
                cf_id: 0,
                start_key: Bytes::from_static(b"range-a"),
                end_key: Bytes::from_static(b"range-z"),
            },
        ]);

        // Assert
        assert!(touches.touches_ops(&[TransactionOp::Delete {
            cf_id: 0,
            key: Bytes::from_static(b"point"),
        }]));
        assert!(touches.touches_ops(&[TransactionOp::Put {
            cf_id: 0,
            key: Bytes::from_static(b"range-m"),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: true,
        }]));
        assert!(touches.touches_ops(&[TransactionOp::DeleteRange {
            cf_id: 0,
            start_key: Bytes::from_static(b"range-y"),
            end_key: Bytes::from_static(b"zz"),
        }]));
        assert!(!touches.touches_ops(&[TransactionOp::Put {
            cf_id: 1,
            key: Bytes::from_static(b"point"),
            value: Bytes::from_static(b"value"),
            ttl_seconds: None,
            insert_only: false,
        }]));
        assert!(!touches.touches_ops(&[TransactionOp::DeleteRange {
            cf_id: 0,
            start_key: Bytes::from_static(b"range-z"),
            end_key: Bytes::from_static(b"zz"),
        }]));
    }
}
