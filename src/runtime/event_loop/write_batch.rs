//! Write batching and backpressure — group commit drain and write stall management
//!
//! Contains `drain_pending_writes` (opportunistic write coalescing for group commit)
//! and `wake_write_stall_waiters` (backpressure release).

use super::super::{ConflictPolicy, RuntimeMsg, RuntimeResponse, TransactionOp};
use super::durability_sync::AppliedTransaction;
use super::wal::ApplyTransactionRequest;
use super::EventLoop;
use crate::common::MidgeError;
use crate::runtime::actors::wal::StagedWriteOverlay;
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

/// Outcome of one applied transaction, owned by the drain path until it is
/// acknowledged.
///
/// The drain path handles exactly one kind of message — `ApplyTransaction` — so
/// this is a struct rather than a message-shaped enum: there is no second,
/// test-only production write to model.
struct WriteResult {
    last_sequence: u64,
    op_count: usize,
    deferred: bool,
    touched_cfs: Vec<crate::types::ColumnFamilyId>,
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
            || !self.actors.cloud_coordinator.cloud_wal.backlog().is_empty()
            || self
                .actors
                .cloud_coordinator
                .hybrid_storage
                .as_ref()
                .is_some_and(|storage| {
                    storage.is_wal_upload_stalled(self.actors.cloud_coordinator.held_uploads())
                })
    }

    pub(super) fn write_pressure_mask(&self, cf_id: crate::types::ColumnFamilyId) -> u8 {
        #[cfg(any(test, feature = "internal-testing"))]
        {
            use crate::diagnostics::{
                PRESSURE_CLOUD_PENDING, PRESSURE_L0, PRESSURE_MEMORY, PRESSURE_RUNTIME,
                PRESSURE_UPLOAD_STALLED,
            };
            let mut mask = 0;
            if self.state.write_stalled() {
                mask |= PRESSURE_RUNTIME;
            }
            if self.state.l0_write_slot_unavailable(cf_id) {
                mask |= PRESSURE_L0;
            }
            if self.state.is_immutable_memtable_queue_full(cf_id)
                || self.state.is_total_memtable_hard_limit_exceeded()
            {
                mask |= PRESSURE_MEMORY;
            }
            if !self.actors.cloud_coordinator.cloud_wal.backlog().is_empty() {
                mask |= PRESSURE_CLOUD_PENDING;
            }
            if self
                .actors
                .cloud_coordinator
                .hybrid_storage
                .as_ref()
                .is_some_and(|storage| {
                    storage.is_wal_upload_stalled(self.actors.cloud_coordinator.held_uploads())
                })
            {
                mask |= PRESSURE_UPLOAD_STALLED;
            }
            mask
        }
        #[cfg(not(any(test, feature = "internal-testing")))]
        {
            let _ = cf_id;
            0
        }
    }

    pub(super) fn ensure_l0_write_admission(
        &self,
        cf_ids: &[crate::types::ColumnFamilyId],
    ) -> Result<(), MidgeError> {
        for cf_id in cf_ids {
            if self.state.l0_write_slot_unavailable(*cf_id) {
                self.state.diagnostics.record_write_admission_rejection(
                    crate::diagnostics::WriteAdmissionRejection::L0,
                );
                self.state.diagnostics.record(|m| {
                    m.record_write_stall_compaction();
                });
                return Err(MidgeError::WriteStall(format!(
                    "column family {cf_id} has no free L0 slot ({}/{})",
                    self.state.l0_slot_usage(*cf_id),
                    self.state.l0_hard_ceiling()
                )));
            }
        }
        Ok(())
    }

    pub(super) fn wake_write_stall_waiters(&mut self) {
        // Avoid borrowing issues by snapshotting keys.
        let cf_ids: Vec<crate::types::ColumnFamilyId> =
            self.pipeline.write_stall_waiters.column_families();

        for cf_id in cf_ids {
            if self.check_lease_health().is_ok() && self.should_stall_writes(cf_id) {
                continue;
            }

            for wait_request_id in self.pipeline.write_stall_waiters.take_column_family(cf_id) {
                let response = match self.check_lease_health() {
                    Ok(()) => RuntimeResponse::Ok {
                        request_id: wait_request_id,
                    },
                    Err(error) => RuntimeResponse::Error {
                        request_id: wait_request_id,
                        error,
                    },
                };
                self.respond(wait_request_id, response);
            }
        }
    }

    /// Opportunistically drain pending *write* messages from the channel.
    ///
    /// This improves group commit by coalescing bursts of concurrent writers into a single WAL sync.
    /// If a non-write message is encountered, it is stashed in `self.routing.pending_msg` to preserve FIFO
    /// semantics (since we cannot "un-recv" with crossbeam channels).
    pub(super) fn drain_pending_writes(
        &mut self,
        msg_rx: &Receiver<RuntimeMsg>,
        max: usize,
    ) -> usize {
        if self.actors.wal_actor.is_cloud_async() {
            return 0;
        }
        // A fenced runtime must not pull writes around dispatch, which is the
        // only place a rejected write is answered in queue order.
        if self.check_lease_health().is_err() {
            return 0;
        }

        // IMPORTANT: If we already have a buffered non-write message, do not `try_recv()`.
        // Otherwise we could consume another non-write message and have nowhere to stash it.
        // The stashed message is always consumed on the next main-loop recv (see event_loop mod).
        if self.routing.pending_msg.is_some() {
            return 0;
        }

        let mut drained = 0usize;

        while drained < max {
            if self.routing.pending_msg.is_some() {
                break;
            }

            match self.drain_one_pending_write(msg_rx, max - drained) {
                DrainOutcome::WritesHandled(count) => drained += count,
                DrainOutcome::StashedNonWrite | DrainOutcome::ChannelEmpty => break,
            }
        }

        if drained > 0 && self.diagnostics.trace_enabled {
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
        if self.routing.pending_msg.is_some() {
            return DrainOutcome::StashedNonWrite;
        }

        match msg_rx.try_recv() {
            Ok(RuntimeMsg::ApplyTransaction {
                request_id,
                ops,
                assertions,
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
                        assertions,
                        durability_policy,
                        start_sequence,
                        conflict_policy,
                        validation: None,
                    },
                    max,
                );
                DrainOutcome::WritesHandled(handled)
            }
            Ok(other) => {
                if self.routing.pending_msg.is_none() {
                    self.routing.pending_msg = Some(other);
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
        let Some(initial) = self.prepare_detached_validation(initial) else {
            return 1;
        };

        let Some(coalescing_key) = self.transaction_coalescing_key(&initial) else {
            self.apply_single_transaction_request(initial);
            return 1;
        };

        let mut batch = CoalescedTransactionBatch::new(coalescing_key);

        match self.prepare_transaction_for_coalescing(
            initial,
            batch.coalescing_key,
            &mut batch.staged_writes,
            &mut batch.staged_growth,
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
        self.actors
            .wal_actor
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
            // A prior progress pass may have filled the single FIFO lookahead
            // slot before dispatching the current transaction. Do not consume
            // another message until the run loop restores that request.
            if self.routing.pending_msg.is_some() {
                break;
            }
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
                assertions,
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
                    assertions,
                    durability_policy,
                    start_sequence,
                    conflict_policy,
                    validation: None,
                };
                match self.prepare_transaction_for_coalescing(
                    request,
                    batch.coalescing_key,
                    &mut batch.staged_writes,
                    &mut batch.staged_growth,
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
            // Anything that is not another coalescable `ApplyTransaction` ends the
            // batch: it is stashed for the main loop, which runs it through
            // `RuntimeDispatcher::handle` and therefore through both runtime gates.
            other => {
                if self.routing.pending_msg.is_none() {
                    self.routing.pending_msg = Some(other);
                }
                false
            }
        }
    }

    fn finish_coalesced_transaction_batch(&mut self, batch: CoalescedTransactionBatch) -> usize {
        let CoalescedTransactionBatch {
            coalescing_key: _,
            staged_writes: _,
            staged_growth: _,
            prepared,
            mut touched_cfs,
            request_ids,
            mut handled,
            fallback_request,
            deferred_error,
        } = batch;

        let (applied, failure) = match self
            .actors
            .wal_actor
            .append_prepared_transaction_group(&mut self.state, prepared)
        {
            Ok(outcome) => (outcome.applied, outcome.failure),
            Err(error) => (Vec::new(), Some(error)),
        };

        // Units applied before a failure are visible. A synced one is durable
        // and replay keeps it whole, so it is acknowledged like any other. A
        // deferred (`Batched`) one is written but not synced when the failure
        // fences the WAL, and no later sync will run, so its durability is
        // unknown and its caller gets `Timeout`, not an acknowledgement.
        if !applied.is_empty() {
            let families = applied
                .iter()
                .filter_map(|result| touched_cfs.get(&result.request_id))
                .flatten()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            self.publish_snapshots_for(&families);
        }
        let applied_ids = applied
            .iter()
            .map(|result| result.request_id)
            .collect::<std::collections::HashSet<_>>();
        for result in applied {
            if failure.is_some() && result.deferred {
                self.settle_applied_write(result.request_id, true);
                self.handle_write_error(
                    result.request_id,
                    MidgeError::OutcomeUnknown(
                        "the transaction is applied but the WAL was fenced before its \
                         frame was synced; it may be lost in a crash"
                            .to_string(),
                    ),
                );
                continue;
            }
            let touched_cfs = touched_cfs.remove(&result.request_id).unwrap_or_default();
            self.finish_drained_write_after_publish(
                result.request_id,
                Ok(WriteResult {
                    last_sequence: result.last_sequence,
                    op_count: result.op_count,
                    deferred: result.deferred,
                    touched_cfs,
                }),
            );
        }

        match failure {
            None => {
                if let Some((request_id, error)) = deferred_error {
                    self.finish_drained_write(request_id, Err(error));
                    handled += 1;
                }

                if let Some(request) = fallback_request {
                    self.apply_single_transaction_request(request);
                    handled += 1;
                }
            }
            Some(error) => {
                for request_id in request_ids {
                    if !applied_ids.contains(&request_id) {
                        self.finish_drained_write(request_id, Err(error.replay()));
                    }
                }

                if let Some((request_id, error)) = deferred_error {
                    self.finish_drained_write(request_id, Err(error));
                    handled += 1;
                }

                if let Some(request) = fallback_request {
                    self.finish_drained_write(request.request_id, Err(not_attempted(error)));
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
        staged_writes: &mut StagedWriteOverlay,
        staged_growth: &mut HashMap<u32, usize>,
    ) -> PrepareOutcome {
        if request.ops.is_empty()
            || self.transaction_coalescing_key(&request) != Some(coalescing_key)
            || self.validation_would_defer(&request)
        {
            return PrepareOutcome::Fallback(request);
        }

        let request_id = request.request_id;
        let touched_cfs = Self::transaction_cf_ids(&request.ops);
        if let Err(error) = self.admit_drained_transaction(&touched_cfs) {
            return PrepareOutcome::Error { request_id, error };
        }
        match self.prepare_coalesced_cloud_memtables(&request.ops, staged_growth) {
            Ok(true) => {}
            Ok(false) => return PrepareOutcome::Fallback(request),
            Err(error) => return PrepareOutcome::Error { request_id, error },
        }
        if self.validation_would_defer(&request) {
            return PrepareOutcome::Fallback(request);
        }
        let result = self.actors.wal_actor.prepare_transaction_append_with_proof(
            &mut self.state,
            crate::runtime::actors::wal::TransactionAppendParams {
                request_id,
                ops: request.ops,
                assertions: request.assertions,
                durability_policy: request.durability_policy,
                start_sequence: request.start_sequence,
                conflict_policy: request.conflict_policy,
            },
            staged_writes,
            request.validation.as_deref(),
        );

        match result {
            Ok(prepared_transaction) => {
                staged_writes.record(&prepared_transaction);
                PrepareOutcome::Prepared {
                    request_id,
                    prepared_transaction: Box::new(prepared_transaction),
                    touched_cfs,
                }
            }
            Err(error) => PrepareOutcome::Error { request_id, error },
        }
    }

    /// Admission for writes that bypass dispatch: the lease can be lost while
    /// a drain is running, so each write re-checks the fence before L0.
    fn admit_drained_transaction(
        &self,
        touched_cfs: &[crate::types::ColumnFamilyId],
    ) -> crate::common::MidgeResult<()> {
        self.check_lease_health()?;
        self.ensure_l0_write_admission(touched_cfs)
    }

    fn apply_single_transaction_request(&mut self, request: ApplyTransactionRequest) {
        let Some(request) = self.prepare_detached_validation(request) else {
            return;
        };
        let request_id = request.request_id;
        let touched_cfs = Self::transaction_cf_ids(&request.ops);
        if let Err(error) = self
            .admit_drained_transaction(&touched_cfs)
            .and_then(|()| self.prepare_cloud_transaction_memtables(&request.ops))
        {
            self.finish_drained_write(request_id, Err(error));
            return;
        }
        let Some(request) = self.prepare_detached_validation(request) else {
            return;
        };
        let ApplyTransactionRequest {
            request_id,
            ops,
            assertions,
            durability_policy,
            start_sequence,
            conflict_policy,
            validation,
        } = request;
        let result = self
            .actors
            .wal_actor
            .append_transaction_with_proof(
                &mut self.state,
                crate::runtime::actors::wal::TransactionAppendParams {
                    request_id,
                    ops,
                    assertions,
                    durability_policy,
                    start_sequence,
                    conflict_policy,
                },
                validation.as_deref(),
            )
            .map(|(last_sequence, op_count, deferred)| WriteResult {
                last_sequence,
                op_count,
                deferred,
                touched_cfs,
            });
        self.finish_drained_write(request_id, result);
    }

    fn finish_drained_write(
        &mut self,
        request_id: u64,
        result: Result<WriteResult, crate::common::MidgeError>,
    ) {
        if let Ok(result) = &result {
            self.publish_snapshots_for(&result.touched_cfs);
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
        let WriteResult {
            last_sequence,
            op_count,
            deferred,
            touched_cfs,
        } = result;
        self.ack_applied_transaction(
            request_id,
            &AppliedTransaction {
                last_sequence: *last_sequence,
                op_count: *op_count,
                deferred: *deferred,
                touched_cfs,
            },
        );
    }

    fn handle_write_error(&mut self, request_id: u64, error: crate::common::MidgeError) {
        // A WAL append failure fences the actor itself, so ask it rather than
        // looking for "wal writer" in the message: a NoSpace raised elsewhere
        // (storage admission) is backpressure, not lost durability.
        let must_fence = matches!(
            error,
            crate::common::MidgeError::Timeout(_) | crate::common::MidgeError::OutcomeUnknown(_)
        ) || self.actors.wal_actor.is_fenced();
        if must_fence {
            self.state.mark_persistence_anomaly();
            if let Some(healthy) = &self.authority.fencing.lease_healthy {
                healthy.store(false, std::sync::atomic::Ordering::Release);
            }
            tracing::error!(
                request_id,
                error = %error,
                "storage persistence became ambiguous; runtime fenced from further writes"
            );
        }
        self.respond(request_id, RuntimeResponse::Error { request_id, error });
    }
}

/// The error for a request a failed group never prepared: it was neither
/// logged nor applied. The group's `OutcomeUnknown` speaks only for logged units,
/// so it becomes `Fenced`, which says the request is not applied; any other
/// group error already makes no claim that it was.
fn not_attempted(group_error: MidgeError) -> MidgeError {
    match group_error {
        MidgeError::OutcomeUnknown(reason) => MidgeError::Fenced(format!(
            "the transaction was not logged or applied because an earlier \
             transaction in its batch failed: {reason}"
        )),
        other => other,
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
    staged_writes: StagedWriteOverlay,
    staged_growth: HashMap<u32, usize>,
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
            staged_writes: StagedWriteOverlay::default(),
            staged_growth: HashMap::new(),
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

#[cfg(test)]
mod tests;
