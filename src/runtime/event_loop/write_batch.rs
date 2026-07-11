//! Write batching and backpressure — group commit drain and write stall management
//!
//! Contains `drain_pending_writes` (opportunistic write coalescing for group commit)
//! and `wake_write_stall_waiters` (backpressure release).

use super::super::durability::DurabilityWaiter;
use super::super::{RuntimeMsg, RuntimeResponse, TransactionOp};
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

        if !self
            .wal_actor
            .can_coalesce_transaction_append(initial.durability_policy)
        {
            self.apply_single_transaction_request(initial);
            return 1;
        }

        let initial_is_explicit = initial.start_sequence.is_some();
        let mut batch = CoalescedTransactionBatch::default();

        match self.prepare_transaction_for_coalescing(initial, &mut batch.staged_touches) {
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

        let explicit_collect_window = initial_is_explicit.then(|| {
            if msg_rx.is_empty() {
                EXPLICIT_TRANSACTION_COALESCE_COLD_WINDOW
            } else {
                EXPLICIT_TRANSACTION_COALESCE_BUSY_WINDOW
            }
        });
        self.collect_coalesced_transaction_batch(msg_rx, max, &mut batch, explicit_collect_window);
        self.finish_coalesced_transaction_batch(batch)
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
                match self.prepare_transaction_for_coalescing(request, &mut batch.staged_touches) {
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
                for result in results {
                    let touched_cfs = touched_cfs.remove(&result.request_id).unwrap_or_default();
                    self.finish_drained_write(
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

#[derive(Default)]
struct CoalescedTransactionBatch {
    staged_touches: StagedTransactionTouches,
    prepared: Vec<crate::runtime::actors::wal::PreparedTransactionAppend>,
    touched_cfs: HashMap<u64, Vec<crate::types::ColumnFamilyId>>,
    request_ids: Vec<u64>,
    handled: usize,
    fallback_request: Option<ApplyTransactionRequest>,
    deferred_error: Option<(u64, MidgeError)>,
}

impl CoalescedTransactionBatch {
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
mod tests {
    use super::*;
    use crate::common::{MidgeError, MidgeResult};
    use crate::runtime::event_loop::EventLoop;
    use crate::runtime::state::RuntimeState;
    use crate::runtime::{ConflictPolicy, ResponseRouter, RuntimeConfig};
    use crate::wal::DurabilityPolicy;
    use bytes::Bytes;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Duration;
    use tempfile::TempDir;

    const RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
    const NO_RESPONSE_TIMEOUT: Duration = Duration::from_millis(25);

    static FAILPOINT_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct EventLoopFixture {
        _temp_dir: TempDir,
        event_loop: EventLoop,
        router: Arc<ResponseRouter>,
    }

    impl EventLoopFixture {
        fn batched() -> MidgeResult<Self> {
            Self::with_policy(DurabilityPolicy::Batched)
        }

        fn with_policy(policy: DurabilityPolicy) -> MidgeResult<Self> {
            let temp_dir = tempfile::tempdir().map_err(MidgeError::Io)?;
            let state = RuntimeState::new(temp_dir.path().to_path_buf(), false);
            let router = Arc::new(ResponseRouter::new());
            let event_loop = EventLoop::new(
                state,
                false,
                Arc::clone(&router),
                RuntimeConfig {
                    wal_durability_policy: policy,
                    ..RuntimeConfig::default()
                },
                None,
            )?;

            Ok(Self {
                _temp_dir: temp_dir,
                event_loop,
                router,
            })
        }

        fn register(&self, request_id: u64) -> crossbeam::channel::Receiver<RuntimeResponse> {
            self.router.register(request_id)
        }
    }

    fn failpoint_test_lock() -> &'static Mutex<()> {
        FAILPOINT_TEST_LOCK.get_or_init(|| Mutex::new(()))
    }

    struct TxnAppendBatchNoSpaceFailpointGuard;

    impl TxnAppendBatchNoSpaceFailpointGuard {
        fn setup(request_id: u64) -> Self {
            crate::runtime::actors::wal::set_txn_append_batch_no_space_failpoint_request_id(Some(
                request_id,
            ));
            fail::cfg("midge::wal::inject_no_space_on_txn_append_batch", "return")
                .expect("configure txn append batch no-space failpoint");
            Self
        }
    }

    impl Drop for TxnAppendBatchNoSpaceFailpointGuard {
        fn drop(&mut self) {
            fail::remove("midge::wal::inject_no_space_on_txn_append_batch");
            crate::runtime::actors::wal::set_txn_append_batch_no_space_failpoint_request_id(None);
        }
    }

    fn txn_request(
        request_id: u64,
        ops: Vec<TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
    ) -> ApplyTransactionRequest {
        ApplyTransactionRequest {
            request_id,
            ops,
            durability_policy,
            start_sequence: None,
            conflict_policy: ConflictPolicy::LastWriteWins,
        }
    }

    fn txn_msg(
        request_id: u64,
        ops: Vec<TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
    ) -> RuntimeMsg {
        RuntimeMsg::ApplyTransaction {
            request_id,
            ops,
            durability_policy,
            start_sequence: None,
            conflict_policy: ConflictPolicy::LastWriteWins,
            response_tx: None,
        }
    }

    fn inline_txn_msg(
        request_id: u64,
        ops: Vec<TransactionOp>,
        durability_policy: Option<DurabilityPolicy>,
    ) -> (RuntimeMsg, crossbeam::channel::Receiver<RuntimeResponse>) {
        let (response_tx, response_rx) = crossbeam::channel::bounded(1);
        (
            RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                durability_policy,
                start_sequence: None,
                conflict_policy: ConflictPolicy::LastWriteWins,
                response_tx: Some(response_tx),
            },
            response_rx,
        )
    }

    fn put_op(
        cf_id: crate::types::ColumnFamilyId,
        key: &'static [u8],
        value: &'static [u8],
    ) -> TransactionOp {
        TransactionOp::Put {
            cf_id,
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
            ttl_seconds: None,
            insert_only: false,
        }
    }

    fn insert_only_put_op(
        cf_id: crate::types::ColumnFamilyId,
        key: &'static [u8],
        value: &'static [u8],
    ) -> TransactionOp {
        TransactionOp::Put {
            cf_id,
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
            ttl_seconds: None,
            insert_only: true,
        }
    }

    fn ttl_put_op(
        cf_id: crate::types::ColumnFamilyId,
        key: &'static [u8],
        value: &'static [u8],
        ttl_seconds: u64,
    ) -> TransactionOp {
        TransactionOp::Put {
            cf_id,
            key: Bytes::from_static(key),
            value: Bytes::from_static(value),
            ttl_seconds: Some(ttl_seconds),
            insert_only: false,
        }
    }

    fn delete_op(cf_id: crate::types::ColumnFamilyId, key: &'static [u8]) -> TransactionOp {
        TransactionOp::Delete {
            cf_id,
            key: Bytes::from_static(key),
        }
    }

    fn delete_range_op(
        cf_id: crate::types::ColumnFamilyId,
        start_key: &'static [u8],
        end_key: &'static [u8],
    ) -> TransactionOp {
        TransactionOp::DeleteRange {
            cf_id,
            start_key: Bytes::from_static(start_key),
            end_key: Bytes::from_static(end_key),
        }
    }

    fn recv_response(rx: &crossbeam::channel::Receiver<RuntimeResponse>) -> RuntimeResponse {
        rx.recv_timeout(RESPONSE_TIMEOUT)
            .expect("response should arrive before timeout")
    }

    fn expect_transaction_applied(
        rx: &crossbeam::channel::Receiver<RuntimeResponse>,
        expected_request_id: u64,
    ) -> (u64, usize) {
        match recv_response(rx) {
            RuntimeResponse::TransactionApplied {
                request_id,
                last_sequence,
                op_count,
                ..
            } => {
                assert_eq!(request_id, expected_request_id);
                (last_sequence, op_count)
            }
            other => panic!("unexpected response for {expected_request_id}: {other:?}"),
        }
    }

    fn expect_ok(rx: &crossbeam::channel::Receiver<RuntimeResponse>, expected_request_id: u64) {
        match recv_response(rx) {
            RuntimeResponse::Ok { request_id } => {
                assert_eq!(request_id, expected_request_id);
            }
            other => panic!("unexpected response for {expected_request_id}: {other:?}"),
        }
    }

    fn expect_error<F>(
        rx: &crossbeam::channel::Receiver<RuntimeResponse>,
        expected_request_id: u64,
        predicate: F,
    ) where
        F: FnOnce(&MidgeError) -> bool,
    {
        match recv_response(rx) {
            RuntimeResponse::Error { request_id, error } => {
                assert_eq!(request_id, expected_request_id);
                assert!(
                    predicate(&error),
                    "unexpected error for {expected_request_id}: {error:?}"
                );
            }
            other => panic!("unexpected response for {expected_request_id}: {other:?}"),
        }
    }

    fn assert_no_response(rx: &crossbeam::channel::Receiver<RuntimeResponse>) {
        match rx.recv_timeout(NO_RESPONSE_TIMEOUT) {
            Err(crossbeam::channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam::channel::RecvTimeoutError::Disconnected) => {
                panic!("response channel disconnected before timeout")
            }
            Ok(response) => panic!("unexpected response: {response:?}"),
        }
    }

    fn assert_no_additional_response(rx: &crossbeam::channel::Receiver<RuntimeResponse>) {
        match rx.recv_timeout(NO_RESPONSE_TIMEOUT) {
            Err(
                crossbeam::channel::RecvTimeoutError::Timeout
                | crossbeam::channel::RecvTimeoutError::Disconnected,
            ) => {}
            Ok(response) => panic!("unexpected additional response: {response:?}"),
        }
    }

    fn assert_memtable_value(
        event_loop: &EventLoop,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
        expected: Option<&[u8]>,
    ) {
        let snapshot = event_loop
            .create_read_snapshot(cf_id)
            .expect("column family should exist");
        let actual = snapshot
            .get(key, u64::MAX)
            .expect("snapshot lookup should succeed");
        assert_eq!(actual.as_deref(), expected);
    }

    fn seed_memtable_value(
        event_loop: &mut EventLoop,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
        value: &[u8],
        sequence: u64,
    ) -> MidgeResult<()> {
        event_loop
            .state
            .get_cf(cf_id)
            .expect("column family should exist")
            .memtable
            .put_with_seq(key.to_vec(), value.to_vec(), sequence, None)
    }

    #[test]
    fn should_coalesce_independent_buffered_transactions_into_one_wal_append() -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(10);
        let second_rx = fixture.register(11);
        let third_rx = fixture.register(12);

        msg_tx
            .send(txn_msg(
                11,
                vec![put_op(0, b"coalesce-b", b"value-b")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue second transaction");
        msg_tx
            .send(txn_msg(
                12,
                vec![put_op(0, b"coalesce-c", b"value-c")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue third transaction");

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                10,
                vec![put_op(0, b"coalesce-a", b"value-a")],
                Some(DurabilityPolicy::Batched),
            ),
            1024,
        );

        // Assert
        assert_eq!(handled, 3);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
        assert_eq!(fixture.event_loop.state.wal.pending_writes, 3);
        assert_eq!(fixture.event_loop.wal_actor.pending_sync_count(), 3);
        assert_eq!(fixture.event_loop.state.pending_txn_min_seq, Some(1));

        assert_eq!(expect_transaction_applied(&first_rx, 10), (3, 1));
        assert_eq!(expect_transaction_applied(&second_rx, 11), (6, 1));
        assert_eq!(expect_transaction_applied(&third_rx, 12), (9, 1));

        assert_memtable_value(&fixture.event_loop, 0, b"coalesce-a", Some(b"value-a"));
        assert_memtable_value(&fixture.event_loop, 0, b"coalesce-b", Some(b"value-b"));
        assert_memtable_value(&fixture.event_loop, 0, b"coalesce-c", Some(b"value-c"));

        Ok(())
    }

    #[test]
    fn should_coalesce_mixed_transaction_contents_across_column_families() -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let secondary_cf = fixture
            .event_loop
            .state
            .create_cf("secondary".to_string())?;
        seed_memtable_value(&mut fixture.event_loop, 0, b"delete-me", b"old", 1)?;
        seed_memtable_value(&mut fixture.event_loop, secondary_cf, b"range-m", b"old", 2)?;
        seed_memtable_value(
            &mut fixture.event_loop,
            secondary_cf,
            b"range-z",
            b"keep",
            3,
        )?;
        fixture.event_loop.state.sequence = 10;

        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(20);
        let second_rx = fixture.register(21);
        let third_rx = fixture.register(22);
        let fourth_rx = fixture.register(23);

        msg_tx
            .send(txn_msg(
                21,
                vec![insert_only_put_op(secondary_cf, b"insert-key", b"inserted")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue insert-only transaction");
        msg_tx
            .send(txn_msg(
                22,
                vec![delete_op(0, b"delete-me")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue delete transaction");
        msg_tx
            .send(txn_msg(
                23,
                vec![delete_range_op(secondary_cf, b"range-a", b"range-z")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue delete-range transaction");

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                20,
                vec![ttl_put_op(0, b"ttl-key", b"ttl-value", 600)],
                Some(DurabilityPolicy::Batched),
            ),
            1024,
        );

        // Assert
        assert_eq!(handled, 4);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
        assert_eq!(fixture.event_loop.state.wal.pending_writes, 4);

        assert_eq!(expect_transaction_applied(&first_rx, 20).1, 1);
        assert_eq!(expect_transaction_applied(&second_rx, 21).1, 1);
        assert_eq!(expect_transaction_applied(&third_rx, 22).1, 1);
        assert_eq!(expect_transaction_applied(&fourth_rx, 23).1, 1);

        assert_memtable_value(&fixture.event_loop, 0, b"ttl-key", Some(b"ttl-value"));
        assert_memtable_value(
            &fixture.event_loop,
            secondary_cf,
            b"insert-key",
            Some(b"inserted"),
        );
        assert_memtable_value(&fixture.event_loop, 0, b"delete-me", None);
        assert_memtable_value(&fixture.event_loop, secondary_cf, b"range-m", None);
        assert_memtable_value(&fixture.event_loop, secondary_cf, b"range-z", Some(b"keep"));

        Ok(())
    }

    #[test]
    fn should_fall_back_from_coalescing_when_point_range_overlap() -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(30);
        let fallback_rx = fixture.register(31);

        msg_tx
            .send(txn_msg(
                31,
                vec![delete_range_op(0, b"overlap-a", b"overlap-z")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue overlapping delete range");

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                30,
                vec![put_op(0, b"overlap-m", b"value")],
                Some(DurabilityPolicy::Batched),
            ),
            1024,
        );

        // Assert
        assert_eq!(handled, 2);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 2);
        assert_eq!(expect_transaction_applied(&first_rx, 30).1, 1);
        assert_eq!(expect_transaction_applied(&fallback_rx, 31).1, 1);
        assert_memtable_value(&fixture.event_loop, 0, b"overlap-m", None);

        Ok(())
    }

    #[test]
    fn should_return_inline_responses_when_same_key_lww_transaction_falls_back_from_coalescing(
    ) -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let (first_msg, first_rx) = inline_txn_msg(
            34,
            vec![put_op(0, b"same-key-inline", b"first")],
            Some(DurabilityPolicy::Batched),
        );
        let (fallback_msg, fallback_rx) = inline_txn_msg(
            35,
            vec![put_op(0, b"same-key-inline", b"second")],
            Some(DurabilityPolicy::Batched),
        );

        msg_tx.send(first_msg).expect("queue first transaction");
        msg_tx
            .send(fallback_msg)
            .expect("queue same-key fallback transaction");

        // Act
        let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

        // Assert
        assert_eq!(handled, 2);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 2);
        assert_eq!(expect_transaction_applied(&first_rx, 34).1, 1);
        assert_eq!(expect_transaction_applied(&fallback_rx, 35).1, 1);
        assert_no_additional_response(&first_rx);
        assert_no_additional_response(&fallback_rx);
        assert_memtable_value(&fixture.event_loop, 0, b"same-key-inline", Some(b"second"));

        Ok(())
    }

    #[test]
    fn should_return_inline_error_when_same_key_insert_only_fallback_rejects_from_coalescing(
    ) -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let (first_msg, first_rx) = inline_txn_msg(
            36,
            vec![put_op(0, b"same-key-insert-inline", b"first")],
            Some(DurabilityPolicy::Batched),
        );
        let (fallback_msg, fallback_rx) = inline_txn_msg(
            37,
            vec![insert_only_put_op(0, b"same-key-insert-inline", b"second")],
            Some(DurabilityPolicy::Batched),
        );

        msg_tx.send(first_msg).expect("queue first transaction");
        msg_tx
            .send(fallback_msg)
            .expect("queue same-key insert-only fallback transaction");

        // Act
        let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

        // Assert
        assert_eq!(handled, 2);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
        assert_eq!(expect_transaction_applied(&first_rx, 36).1, 1);
        expect_error(
            &fallback_rx,
            37,
            |error| matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists"),
        );
        assert_no_additional_response(&first_rx);
        assert_no_additional_response(&fallback_rx);
        assert_memtable_value(
            &fixture.event_loop,
            0,
            b"same-key-insert-inline",
            Some(b"first"),
        );

        Ok(())
    }

    #[test]
    fn should_coalesce_across_snapshot_bookkeeping_messages() -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(42);
        let register_rx = fixture.register(43);
        let second_rx = fixture.register(44);

        msg_tx
            .send(RuntimeMsg::RegisterSnapshot {
                request_id: 43,
                snapshot_id: 900,
                sequence: 0,
                pinned_sst_names: Vec::new(),
            })
            .expect("queue snapshot registration");
        msg_tx
            .send(txn_msg(
                44,
                vec![put_op(0, b"after-register", b"value-b")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue second transaction");

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                42,
                vec![put_op(0, b"before-register", b"value-a")],
                Some(DurabilityPolicy::Batched),
            ),
            1024,
        );

        // Assert
        assert_eq!(handled, 2);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
        assert_eq!(expect_transaction_applied(&first_rx, 42).1, 1);
        expect_ok(&register_rx, 43);
        assert_eq!(expect_transaction_applied(&second_rx, 44).1, 1);
        assert_memtable_value(&fixture.event_loop, 0, b"before-register", Some(b"value-a"));
        assert_memtable_value(&fixture.event_loop, 0, b"after-register", Some(b"value-b"));

        Ok(())
    }

    #[test]
    fn should_not_drop_non_write_after_coalescing_stashes_pending_message() -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let (txn_msg, txn_rx) = inline_txn_msg(
            38,
            vec![put_op(0, b"coalesce-before-non-write", b"value")],
            Some(DurabilityPolicy::Batched),
        );

        msg_tx.send(txn_msg).expect("queue transaction");
        msg_tx
            .send(RuntimeMsg::Noop { request_id: 39 })
            .expect("queue first non-write");
        msg_tx
            .send(RuntimeMsg::Noop { request_id: 40 })
            .expect("queue second non-write");

        // Act
        let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

        // Assert
        assert_eq!(handled, 1);
        assert_eq!(expect_transaction_applied(&txn_rx, 38).1, 1);
        match fixture.event_loop.pending_msg.take() {
            Some(RuntimeMsg::Noop { request_id }) => assert_eq!(request_id, 39),
            other => panic!("expected first non-write to be stashed, got {other:?}"),
        }
        match msg_rx.try_recv() {
            Ok(RuntimeMsg::Noop { request_id }) => assert_eq!(request_id, 40),
            other => panic!("expected second non-write to remain queued, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn should_error_insert_only_fallback_after_first_transaction_publishes() -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(40);
        let fallback_rx = fixture.register(41);

        msg_tx
            .send(txn_msg(
                41,
                vec![insert_only_put_op(0, b"insert-conflict", b"second")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue overlapping insert-only transaction");

        // Act
        let handled = fixture.event_loop.apply_transaction_with_coalescing(
            &msg_rx,
            txn_request(
                40,
                vec![put_op(0, b"insert-conflict", b"first")],
                Some(DurabilityPolicy::Batched),
            ),
            1024,
        );

        // Assert
        assert_eq!(handled, 2);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
        assert_eq!(expect_transaction_applied(&first_rx, 40).1, 1);
        expect_error(
            &fallback_rx,
            41,
            |error| matches!(error, MidgeError::InvalidArgument(message) if message == "key already exists"),
        );
        assert_memtable_value(&fixture.event_loop, 0, b"insert-conflict", Some(b"first"));

        Ok(())
    }

    #[test]
    fn should_not_enter_coalesced_path_for_non_buffered_durability() -> MidgeResult<()> {
        for (policy_name, durability_policy, expected_append_calls) in [
            ("strict", DurabilityPolicy::Strict, 1),
            ("best_effort", DurabilityPolicy::BestEffort, 0),
            ("cloud_effective", DurabilityPolicy::CloudAsync, 1),
        ] {
            // Arrange
            let mut fixture = EventLoopFixture::batched()?;
            let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
            let first_rx = fixture.register(50);
            let queued_rx = fixture.register(51);
            msg_tx
                .send(txn_msg(
                    51,
                    vec![put_op(0, b"queued-buffered", b"value")],
                    Some(DurabilityPolicy::Batched),
                ))
                .expect("queue transaction that must not be drained");

            // Act
            let handled = fixture.event_loop.apply_transaction_with_coalescing(
                &msg_rx,
                txn_request(
                    50,
                    vec![put_op(0, b"non-coalesced", b"value")],
                    Some(durability_policy),
                ),
                1024,
            );

            // Assert
            assert_eq!(
                handled, 1,
                "{policy_name} should handle only the initial request"
            );
            assert_eq!(
                fixture.event_loop.wal_actor.append_calls(),
                expected_append_calls,
                "{policy_name} should not batch with the queued request"
            );
            assert_eq!(expect_transaction_applied(&first_rx, 50).1, 1);
            assert_no_response(&queued_rx);
        }

        Ok(())
    }

    #[test]
    fn should_fail_all_event_loop_buffered_transactions_when_append_hits_no_space(
    ) -> MidgeResult<()> {
        // Arrange
        let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let first_rx = fixture.register(60);
        let second_rx = fixture.register(61);

        msg_tx
            .send(txn_msg(
                61,
                vec![put_op(0, b"failed-b", b"value-b")],
                Some(DurabilityPolicy::Batched),
            ))
            .expect("queue second transaction");

        {
            let scenario = fail::FailScenario::setup();
            let failpoint_guard = TxnAppendBatchNoSpaceFailpointGuard::setup(60);

            // Act
            let handled = fixture.event_loop.apply_transaction_with_coalescing(
                &msg_rx,
                txn_request(
                    60,
                    vec![put_op(0, b"failed-a", b"value-a")],
                    Some(DurabilityPolicy::Batched),
                ),
                1024,
            );

            // Assert
            assert_eq!(handled, 2);
            expect_error(&first_rx, 60, |error| {
                matches!(error, MidgeError::NoSpace(_))
            });
            expect_error(&second_rx, 61, |error| {
                matches!(error, MidgeError::NoSpace(_))
            });
            assert_eq!(fixture.event_loop.wal_actor.append_calls(), 0);
            assert_eq!(fixture.event_loop.state.wal.pending_writes, 0);
            assert_eq!(fixture.event_loop.wal_actor.pending_sync_count(), 0);
            assert_eq!(fixture.event_loop.wal_actor.bytes_since_sync(), 0);
            assert_eq!(fixture.event_loop.state.pending_txn_min_seq, None);
            assert!(fixture
                .event_loop
                .state
                .get_cf(0)
                .expect("default column family should exist")
                .memtable
                .iter_all(u64::MAX)
                .is_empty());
            assert!(
                fixture.event_loop.state.sequence > 0,
                "failed preparation may leave sequence gaps"
            );

            drop(failpoint_guard);
            scenario.teardown();
        }

        let recovery_rx = fixture.register(62);
        let (_unused_tx, empty_rx) = crossbeam::channel::unbounded();
        let recovery_handled = fixture.event_loop.apply_transaction_with_coalescing(
            &empty_rx,
            txn_request(
                62,
                vec![put_op(0, b"recovered", b"value")],
                Some(DurabilityPolicy::Batched),
            ),
            1024,
        );

        assert_eq!(recovery_handled, 1);
        assert_eq!(expect_transaction_applied(&recovery_rx, 62).1, 1);
        assert_eq!(fixture.event_loop.wal_actor.append_calls(), 1);
        assert_memtable_value(&fixture.event_loop, 0, b"recovered", Some(b"value"));

        Ok(())
    }

    #[test]
    fn should_fail_same_key_fallback_when_coalesced_prefix_append_fails() -> MidgeResult<()> {
        // Arrange
        let _guard = failpoint_test_lock().lock().expect("lock failpoint tests");
        let mut fixture = EventLoopFixture::batched()?;
        let (msg_tx, msg_rx) = crossbeam::channel::unbounded();
        let (first_msg, first_rx) = inline_txn_msg(
            80,
            vec![put_op(0, b"failed-same-key", b"first")],
            Some(DurabilityPolicy::Batched),
        );
        let (fallback_msg, fallback_rx) = inline_txn_msg(
            81,
            vec![put_op(0, b"failed-same-key", b"second")],
            Some(DurabilityPolicy::Batched),
        );

        msg_tx.send(first_msg).expect("queue first transaction");
        msg_tx
            .send(fallback_msg)
            .expect("queue same-key fallback transaction");

        {
            let scenario = fail::FailScenario::setup();
            let failpoint_guard = TxnAppendBatchNoSpaceFailpointGuard::setup(80);

            // Act
            let handled = fixture.event_loop.drain_pending_writes(&msg_rx, 1024);

            // Assert
            assert_eq!(handled, 2);
            expect_error(&first_rx, 80, |error| {
                matches!(error, MidgeError::NoSpace(_))
            });
            expect_error(&fallback_rx, 81, |error| {
                matches!(error, MidgeError::NoSpace(_))
            });
            assert_no_additional_response(&first_rx);
            assert_no_additional_response(&fallback_rx);
            assert_eq!(fixture.event_loop.wal_actor.append_calls(), 0);
            assert_eq!(fixture.event_loop.state.wal.pending_writes, 0);
            assert_memtable_value(&fixture.event_loop, 0, b"failed-same-key", None);

            drop(failpoint_guard);
            scenario.teardown();
        }

        Ok(())
    }

    #[test]
    fn should_detect_overlapping_point_range_touches_when_staging_transactions() {
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

        // Act
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

    #[test]
    fn should_remove_cancelled_write_stall_waiter_from_all_indexes() -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        fixture.event_loop.state.set_write_stalled(true);
        fixture.event_loop.handle_wait_for_write_stall_clear(900, 0);
        assert_eq!(fixture.event_loop.write_stall_waiters.get(&900), Some(&0));
        assert_eq!(
            fixture
                .event_loop
                .write_stall_waiter_queues
                .get(&0)
                .map(std::collections::VecDeque::len),
            Some(1)
        );

        // Act
        fixture
            .event_loop
            .handle_cancel_wait_for_write_stall_clear(900);

        // Assert
        assert!(!fixture.event_loop.write_stall_waiters.contains_key(&900));
        assert!(
            fixture
                .event_loop
                .write_stall_waiter_queues
                .get(&0)
                .is_none_or(std::collections::VecDeque::is_empty),
            "cancellation must remove the request from both waiter indexes"
        );
        Ok(())
    }

    #[test]
    fn should_report_stall_hint_for_transaction_actual_column_family() -> MidgeResult<()> {
        // Arrange
        let mut fixture = EventLoopFixture::batched()?;
        let secondary_cf = fixture
            .event_loop
            .state
            .create_cf("stalled-secondary".to_string())?;
        fixture.event_loop.state.max_immutable_memtables = 1;
        fixture
            .event_loop
            .state
            .get_cf_mut(secondary_cf)
            .expect("secondary column family")
            .immutable_memtables
            .push(Arc::new(crate::sst::SkipListMemtable::new()));
        assert!(!fixture.event_loop.state.should_stall_writes(0));
        assert!(fixture.event_loop.state.should_stall_writes(secondary_cf));
        let response_rx = fixture.register(901);
        let (_tx, empty_rx) = crossbeam::channel::unbounded();

        // Act
        fixture.event_loop.apply_transaction_with_coalescing(
            &empty_rx,
            txn_request(
                901,
                vec![put_op(secondary_cf, b"secondary-key", b"value")],
                Some(DurabilityPolicy::Batched),
            ),
            1,
        );

        // Assert
        match recv_response(&response_rx) {
            RuntimeResponse::TransactionApplied {
                write_stall_hint, ..
            } => assert!(
                write_stall_hint,
                "response must compute pressure from the transaction's CF, not CF 0"
            ),
            other => panic!("unexpected response: {other:?}"),
        }
        Ok(())
    }
}
