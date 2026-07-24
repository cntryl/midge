//! Write batching and backpressure — group commit drain and write stall management
//!
//! Contains `drain_pending_writes` (opportunistic write coalescing for group commit)
//! and `wake_write_stall_waiters` (backpressure release).

use super::super::durability::DurabilityWaiter;
use super::super::{ConflictPolicy, RuntimeMsg, RuntimeResponse, TransactionOp};
#[cfg(test)]
use super::snapshot::SnapshotCoordinator;
use super::wal::ApplyTransactionRequest;
use super::EventLoop;
use crate::common::MidgeError;
use crossbeam::channel::{Receiver, TryRecvError};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const EXPLICIT_TRANSACTION_COALESCE_TARGET: usize = 8;
const EXPLICIT_TRANSACTION_COALESCE_COLD_WINDOW: Duration = Duration::from_micros(25);
const EXPLICIT_TRANSACTION_COALESCE_BUSY_WINDOW: Duration = Duration::from_micros(100);
const EXPLICIT_TRANSACTION_COALESCE_IDLE_WAIT: Duration = Duration::from_micros(5);
const LOCAL_STRICT_GROUP_COMMIT_CANDIDATE_WINDOWS_US: [u64; 5] = [0, 10, 25, 50, 100];
const LOCAL_STRICT_GROUP_COMMIT_SELECTED_WINDOW_INDEX: usize = 4;
const LOCAL_STRICT_GROUP_COMMIT_WINDOW: Duration = Duration::from_micros(
    LOCAL_STRICT_GROUP_COMMIT_CANDIDATE_WINDOWS_US[LOCAL_STRICT_GROUP_COMMIT_SELECTED_WINDOW_INDEX],
);

fn local_strict_group_commit_collect_window(queue_is_empty: bool) -> Option<Duration> {
    (!queue_is_empty).then_some(LOCAL_STRICT_GROUP_COMMIT_WINDOW)
}

enum WriteResult {
    #[cfg(test)]
    WalAppend { sequence: u64, deferred: bool },
    TransactionApplied {
        last_sequence: u64,
        op_count: usize,
        deferred: bool,
        touched_cfs: Vec<crate::types::ColumnFamilyId>,
    },
}

impl WriteResult {
    fn deferred(&self) -> bool {
        match self {
            #[cfg(test)]
            WriteResult::WalAppend { deferred, .. } => *deferred,
            WriteResult::TransactionApplied { deferred, .. } => *deferred,
        }
    }
}

impl EventLoop {
    pub(super) fn transaction_cf_ids(ops: &[TransactionOp]) -> Vec<crate::types::ColumnFamilyId> {
        let mut seen = HashSet::new();
        ops.iter()
            .filter_map(|op| {
                let cf_id = match op {
                    TransactionOp::Put { cf_id, .. }
                    | TransactionOp::Delete { cf_id, .. }
                    | TransactionOp::DeleteRange { cf_id, .. } => *cf_id,
                };
                seen.insert(cf_id).then_some(cf_id)
            })
            .collect()
    }

    pub(super) fn write_stall_hint_for_cfs(&self, cf_ids: &[crate::types::ColumnFamilyId]) -> bool {
        cf_ids.iter().any(|cf_id| self.should_stall_writes(*cf_id))
    }

    pub(super) fn should_stall_writes(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        self.state.should_stall_writes(cf_id)
            || self
                .hybrid_storage
                .as_ref()
                .is_some_and(|storage| storage.is_wal_upload_stalled())
    }

    pub(super) fn wake_write_stall_waiters(&mut self) {
        // Avoid borrowing issues by snapshotting keys.
        let cf_ids: Vec<crate::types::ColumnFamilyId> =
            self.write_stall_waiter_queues.keys().copied().collect();

        for cf_id in cf_ids {
            if self.should_stall_writes(cf_id) {
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
            if self.pending_msg.is_some() {
                break;
            }

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
        if self.pending_msg.is_some() {
            return DrainOutcome::StashedNonWrite;
        }

        match msg_rx.try_recv() {
            #[cfg(test)]
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
            #[cfg(test)]
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
                conflict_policy,
                response_tx,
            }) => {
                if let Some(response_tx) = response_tx {
                    self.register_inline_response(request_id, response_tx);
                }

                let handled = self.apply_transaction_with_coalescing(
                    msg_rx,
                    ApplyTransactionRequest {
                        request_id,
                        ops,
                        durability_policy,
                        start_sequence,
                        conflict_policy,
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

        let Some(coalescing_key) = self.transaction_coalescing_key(&initial) else {
            self.apply_single_transaction_request(initial);
            return 1;
        };

        let mut batch = CoalescedTransactionBatch::new(coalescing_key);

        match self.prepare_transaction_for_coalescing(
            initial,
            batch.coalescing_key,
            &mut batch.staged_touches,
        ) {
            PrepareOutcome::Prepared {
                request_id,
                prepared_transaction,
                touched_cfs,
            } => {
                batch.push_prepared(request_id, prepared_transaction, touched_cfs);
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

        let collect_window = match coalescing_key.durability_policy {
            crate::wal::DurabilityPolicy::Strict => {
                local_strict_group_commit_collect_window(msg_rx.is_empty())
            }
            crate::wal::DurabilityPolicy::Batched if coalescing_key.has_start_sequence => {
                Some(if msg_rx.is_empty() {
                    EXPLICIT_TRANSACTION_COALESCE_COLD_WINDOW
                } else {
                    EXPLICIT_TRANSACTION_COALESCE_BUSY_WINDOW
                })
            }
            _ => None,
        };
        if matches!(
            coalescing_key.durability_policy,
            crate::wal::DurabilityPolicy::Strict
        ) {
            crate::failpoints::fail_point!("midge::runtime::strict_group_before_collect");
        }
        self.collect_coalesced_transaction_batch(msg_rx, max, &mut batch, collect_window);
        self.finish_coalesced_transaction_batch(batch)
    }

    fn transaction_coalescing_key(
        &self,
        request: &ApplyTransactionRequest,
    ) -> Option<TransactionCoalescingKey> {
        self.wal_actor
            .coalesced_transaction_durability(request.durability_policy)
            .map(|durability_policy| TransactionCoalescingKey {
                durability_policy,
                has_start_sequence: request.start_sequence.is_some(),
                conflict_policy: request.conflict_policy,
            })
    }

    fn collect_coalesced_transaction_batch(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
        max: usize,
        batch: &mut CoalescedTransactionBatch,
        explicit_collect_window: Option<Duration>,
    ) {
        let collect_until = explicit_collect_window.map(|window| Instant::now() + window);

        while batch.handled < max {
            match msg_rx.try_recv() {
                Ok(msg) => {
                    if !self.collect_coalesced_transaction_msg(msg, batch) {
                        break;
                    }
                }
                Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => {
                    if collect_until.is_none()
                        || batch.handled >= EXPLICIT_TRANSACTION_COALESCE_TARGET
                    {
                        break;
                    }

                    let Some(collect_until) = collect_until else {
                        break;
                    };
                    let now = Instant::now();
                    if now >= collect_until {
                        break;
                    }

                    let wait_for = collect_until
                        .saturating_duration_since(now)
                        .min(EXPLICIT_TRANSACTION_COALESCE_IDLE_WAIT);
                    match msg_rx.recv_timeout(wait_for) {
                        Ok(msg) => {
                            if !self.collect_coalesced_transaction_msg(msg, batch) {
                                break;
                            }
                        }
                        Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam::channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            }
        }
    }

    fn collect_coalesced_transaction_msg(
        &mut self,
        msg: RuntimeMsg,
        batch: &mut CoalescedTransactionBatch,
    ) -> bool {
        match msg {
            RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy,
                start_sequence,
                conflict_policy,
                response_tx,
            } => {
                if let Some(response_tx) = response_tx {
                    self.register_inline_response(request_id, response_tx);
                }

                let request = ApplyTransactionRequest {
                    request_id,
                    ops,
                    durability_policy,
                    start_sequence,
                    conflict_policy,
                };
                match self.prepare_transaction_for_coalescing(
                    request,
                    batch.coalescing_key,
                    &mut batch.staged_touches,
                ) {
                    PrepareOutcome::Prepared {
                        request_id,
                        prepared_transaction,
                        touched_cfs,
                    } => {
                        batch.push_prepared(request_id, prepared_transaction, touched_cfs);
                        true
                    }
                    PrepareOutcome::Fallback(request) => {
                        batch.fallback_request = Some(request);
                        false
                    }
                    PrepareOutcome::Error { request_id, error } => {
                        batch.deferred_error = Some((request_id, error));
                        false
                    }
                }
            }
            other => self.handle_interleavable_coalescing_message(other),
        }
    }

    fn handle_interleavable_coalescing_message(&mut self, msg: RuntimeMsg) -> bool {
        match msg {
            // Snapshot bookkeeping is actor-owned but does not observe or advance
            // data/WAL state. Handling it here keeps transaction lifecycle traffic
            // from fragmenting an already ordered ApplyTransaction batch.
            #[cfg(test)]
            RuntimeMsg::RegisterSnapshot {
                request_id,
                snapshot_id,
                sequence,
                pinned_sst_names,
            } => {
                let _ = SnapshotCoordinator::register(
                    self,
                    request_id,
                    snapshot_id,
                    sequence,
                    pinned_sst_names,
                );
                true
            }
            #[cfg(test)]
            RuntimeMsg::UnregisterSnapshot { snapshot_id } => {
                let _ = SnapshotCoordinator::unregister(self, snapshot_id);
                true
            }
            other => {
                if self.pending_msg.is_none() {
                    self.pending_msg = Some(other);
                }
                false
            }
        }
    }

    fn finish_coalesced_transaction_batch(&mut self, batch: CoalescedTransactionBatch) -> usize {
        let CoalescedTransactionBatch {
            coalescing_key: _,
            staged_touches: _,
            prepared,
            mut touched_cfs,
            request_ids,
            mut handled,
            fallback_request,
            deferred_error,
        } = batch;

        let append_result = self
            .wal_actor
            .append_prepared_transactions(&mut self.state, prepared);

        match append_result {
            Ok(results) => {
                if !results.is_empty() {
                    self.publish_snapshot();
                }
                for result in results {
                    let touched_cfs = touched_cfs.remove(&result.request_id).unwrap_or_default();
                    self.finish_drained_write_after_publish(
                        result.request_id,
                        Ok(WriteResult::TransactionApplied {
                            last_sequence: result.last_sequence,
                            op_count: result.op_count,
                            deferred: result.deferred,
                            touched_cfs,
                        }),
                    );
                }

                if let Some((request_id, error)) = deferred_error {
                    self.finish_drained_write(request_id, Err(error));
                    handled += 1;
                }

                if let Some(request) = fallback_request {
                    self.apply_single_transaction_request(request);
                    handled += 1;
                }
            }
            Err(error) => {
                for request_id in request_ids {
                    self.finish_drained_write(request_id, Err(duplicate_midge_error(&error)));
                }

                if let Some((request_id, error)) = deferred_error {
                    self.finish_drained_write(request_id, Err(error));
                    handled += 1;
                }

                if let Some(request) = fallback_request {
                    self.finish_drained_write(request.request_id, Err(error));
                    handled += 1;
                }
            }
        }

        handled
    }

    fn prepare_transaction_for_coalescing(
        &mut self,
        request: ApplyTransactionRequest,
        coalescing_key: TransactionCoalescingKey,
        staged_touches: &mut StagedTransactionTouches,
    ) -> PrepareOutcome {
        if request.ops.is_empty()
            || self.transaction_coalescing_key(&request) != Some(coalescing_key)
            || staged_touches.touches_ops(&request.ops)
        {
            return PrepareOutcome::Fallback(request);
        }

        let request_id = request.request_id;
        let touched_cfs = Self::transaction_cf_ids(&request.ops);
        staged_touches.record_ops(&request.ops);
        let result = self.wal_actor.prepare_transaction_append(
            &mut self.state,
            crate::runtime::actors::wal::TransactionAppendParams {
                request_id,
                ops: request.ops,
                durability_policy: request.durability_policy,
                start_sequence: request.start_sequence,
                conflict_policy: request.conflict_policy,
            },
        );

        match result {
            Ok(prepared_transaction) => PrepareOutcome::Prepared {
                request_id,
                prepared_transaction: Box::new(prepared_transaction),
                touched_cfs,
            },
            Err(error) => PrepareOutcome::Error { request_id, error },
        }
    }

    fn apply_single_transaction_request(&mut self, request: ApplyTransactionRequest) {
        let ApplyTransactionRequest {
            request_id,
            ops,
            durability_policy,
            start_sequence,
            conflict_policy,
        } = request;
        let touched_cfs = Self::transaction_cf_ids(&ops);
        let result = self
            .wal_actor
            .append_transaction(
                &mut self.state,
                request_id,
                ops,
                durability_policy,
                start_sequence,
                conflict_policy,
            )
            .map(
                |(last_sequence, op_count, deferred)| WriteResult::TransactionApplied {
                    last_sequence,
                    op_count,
                    deferred,
                    touched_cfs,
                },
            );
        self.finish_drained_write(request_id, result);
    }

    fn finish_drained_write(
        &mut self,
        request_id: u64,
        result: Result<WriteResult, crate::common::MidgeError>,
    ) {
        if result.is_ok() {
            self.publish_snapshot();
        }
        self.finish_drained_write_after_publish(request_id, result);
    }

    fn finish_drained_write_after_publish(
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
                #[cfg(test)]
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
                    touched_cfs,
                    ..
                } => {
                    self.respond(
                        request_id,
                        RuntimeResponse::TransactionApplied {
                            request_id,
                            last_sequence: *last_sequence,
                            op_count: *op_count,
                            write_stall_hint: self.write_stall_hint_for_cfs(touched_cfs),
                        },
                    );
                }
            }
        } else {
            match result {
                #[cfg(test)]
                WriteResult::WalAppend { sequence, .. } => {
                    self.durability.queue_waiter(DurabilityWaiter::WalAppend {
                        request_id,
                        sequence: *sequence,
                    });
                }
                WriteResult::TransactionApplied {
                    last_sequence,
                    op_count,
                    touched_cfs,
                    ..
                } => {
                    self.durability
                        .queue_waiter(DurabilityWaiter::TransactionApply {
                            request_id,
                            last_sequence: *last_sequence,
                            op_count: *op_count,
                            touched_cfs: touched_cfs.clone(),
                        });
                }
            }
        }
    }

    fn handle_write_error(&mut self, request_id: u64, error: crate::common::MidgeError) {
        if matches!(error, crate::common::MidgeError::Timeout(_)) {
            self.state.mark_persistence_anomaly();
            if let Some(healthy) = &self.lease_healthy {
                healthy.store(false, std::sync::atomic::Ordering::Release);
            }
            tracing::error!(
                request_id,
                "storage acknowledgement timed out; runtime fenced from further writes"
            );
        }
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
        prepared_transaction: Box<crate::runtime::actors::wal::PreparedTransactionAppend>,
        touched_cfs: Vec<crate::types::ColumnFamilyId>,
    },
    Fallback(ApplyTransactionRequest),
    Error {
        request_id: u64,
        error: MidgeError,
    },
}

struct CoalescedTransactionBatch {
    coalescing_key: TransactionCoalescingKey,
    staged_touches: StagedTransactionTouches,
    prepared: Vec<crate::runtime::actors::wal::PreparedTransactionAppend>,
    touched_cfs: HashMap<u64, Vec<crate::types::ColumnFamilyId>>,
    request_ids: Vec<u64>,
    handled: usize,
    fallback_request: Option<ApplyTransactionRequest>,
    deferred_error: Option<(u64, MidgeError)>,
}

impl CoalescedTransactionBatch {
    fn new(coalescing_key: TransactionCoalescingKey) -> Self {
        Self {
            coalescing_key,
            staged_touches: StagedTransactionTouches::default(),
            prepared: Vec::new(),
            touched_cfs: HashMap::new(),
            request_ids: Vec::new(),
            handled: 0,
            fallback_request: None,
            deferred_error: None,
        }
    }

    fn push_prepared(
        &mut self,
        request_id: u64,
        prepared_transaction: Box<crate::runtime::actors::wal::PreparedTransactionAppend>,
        touched_cfs: Vec<crate::types::ColumnFamilyId>,
    ) {
        self.request_ids.push(request_id);
        self.touched_cfs.insert(request_id, touched_cfs);
        self.prepared.push(*prepared_transaction);
        self.handled += 1;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct TransactionCoalescingKey {
    durability_policy: crate::wal::DurabilityPolicy,
    has_start_sequence: bool,
    conflict_policy: ConflictPolicy,
}

#[derive(Default)]
struct StagedTransactionTouches {
    point_keys: HashMap<crate::types::ColumnFamilyId, HashSet<Vec<u8>>>,
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
                    self.point_keys
                        .entry(*cf_id)
                        .or_default()
                        .insert(key.to_vec());
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
        self.point_keys
            .get(&cf_id)
            .is_some_and(|keys| keys.contains(key))
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
        self.point_keys.get(&cf_id).is_some_and(|keys| {
            keys.iter()
                .any(|key| key.as_slice() >= start_key && key.as_slice() < end_key)
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
        MidgeError::Aborted(message) => MidgeError::Aborted(message.clone()),
        MidgeError::Busy(message) => MidgeError::Busy(message.clone()),
        MidgeError::Timeout(message) => MidgeError::Timeout(message.clone()),
        MidgeError::ResourceLimit(message) => MidgeError::ResourceLimit(message.clone()),
    }
}

#[cfg(test)]
mod tests;
