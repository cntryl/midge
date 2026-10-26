//! Ordered strict synchronization. A later write never publishes across an
//! unapplied earlier group, including an explicitly `BestEffort` write.

use super::durability_sync::CompletionSource;
use super::write_batch::CoalescedTransactionBatch;
use super::EventLoop;
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::actors::wal::{PendingStrictIo, StrictIoCompletion};
use crate::runtime::wal_transition::WalSyncTicket;
use crate::runtime::{RuntimeMsg, RuntimeResponse, TransactionOp};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::time::Duration;

pub(super) const MAX_INFLIGHT_GROUP_BYTES: usize = 16 * 1024 * 1024;
const MAX_DEFERRED_CALLERS: usize = 128;

struct PendingGroup {
    batch: CoalescedTransactionBatch,
    spill: Option<PendingSpilledCommit>,
    io: Option<PendingStrictIo>,
    ticket: WalSyncTicket,
    generation: u64,
    next_generation: Option<u64>,
    ready: RefCell<Option<StrictIoCompletion>>,
    failed: bool,
    reservation: ResourceReservation,
}

struct PendingSpilledCommit {
    source: crate::runtime::transaction_spill::TransactionOpSource,
    prepared: crate::runtime::actors::wal::PreparedSpilledAppend,
    workspace: ResourceBudget,
}

struct StartedGroup {
    io: PendingStrictIo,
    ticket: WalSyncTicket,
    generation: u64,
    next_generation: Option<u64>,
    reservation: ResourceReservation,
}

impl PendingGroup {
    fn ready(&self) -> bool {
        let mut ready = self.ready.borrow_mut();
        if ready.is_none() {
            *ready = self.io.as_ref().and_then(PendingStrictIo::poll);
        }
        ready.is_some() || (!self.failed && self.deadline_expired())
    }

    fn deadline_expired(&self) -> bool {
        self.io
            .as_ref()
            .is_some_and(PendingStrictIo::deadline_expired)
    }
}

struct DeferredMutation {
    message: RuntimeMsg,
    reservation: Option<ResourceReservation>,
}

pub(super) struct WalCommitPipeline {
    pending: Option<PendingGroup>,
    deferred: VecDeque<DeferredMutation>,
    memory: ResourceBudget,
    queued_memory: ResourceBudget,
    admitting: Vec<ResourceReservation>,
    completion_wake_tx: crossbeam::channel::Sender<()>,
    completion_wake_rx: crossbeam::channel::Receiver<()>,
}

impl Default for WalCommitPipeline {
    fn default() -> Self {
        let (completion_wake_tx, completion_wake_rx) = crossbeam::channel::bounded(1);
        Self {
            pending: None,
            deferred: VecDeque::new(),
            memory: ResourceBudget::new(MAX_INFLIGHT_GROUP_BYTES),
            queued_memory: ResourceBudget::new(MAX_INFLIGHT_GROUP_BYTES),
            admitting: Vec::new(),
            completion_wake_tx,
            completion_wake_rx,
        }
    }
}

impl WalCommitPipeline {
    pub(super) fn completion_wake_sender(&self) -> crossbeam::channel::Sender<()> {
        self.completion_wake_tx.clone()
    }

    pub(super) fn completion_wake_receiver(&self) -> crossbeam::channel::Receiver<()> {
        self.completion_wake_rx.clone()
    }

    pub(super) fn pending_deadline_timeout(&self) -> Option<Duration> {
        self.pending
            .as_ref()
            .filter(|pending| !pending.failed)
            .and_then(|pending| pending.io.as_ref())
            .and_then(PendingStrictIo::deadline_timeout)
    }

    pub(super) fn has_pending_group(&self) -> bool {
        self.pending.is_some()
    }

    pub(super) fn has_ready_work(&self) -> bool {
        self.pending.as_ref().is_some_and(PendingGroup::ready)
    }

    pub(super) fn release_admitting(&mut self) {
        self.admitting.clear();
    }
}

fn mutation_bytes(message: &RuntimeMsg) -> usize {
    let base = std::mem::size_of::<RuntimeMsg>();
    match message {
        RuntimeMsg::ApplyTransaction {
            ops, assertions, ..
        } => ops
            .iter()
            .fold(base, |total, op| {
                let payload = match op {
                    TransactionOp::Put { key, value, .. } => key.len().saturating_add(value.len()),
                    TransactionOp::Delete { key, .. } => key.len(),
                    TransactionOp::DeleteRange {
                        start_key, end_key, ..
                    } => start_key.len().saturating_add(end_key.len()),
                };
                total
                    .saturating_add(std::mem::size_of::<TransactionOp>())
                    .saturating_add(payload)
            })
            .saturating_add(assertions.iter().fold(0usize, |total, assertion| {
                total
                    .saturating_add(std::mem::size_of_val(assertion))
                    .saturating_add(assertion.key.len())
            })),
        RuntimeMsg::ApplySpilledTransaction { source, .. } => {
            base.saturating_add(source.retained_memory_bytes())
        }
        _ => base,
    }
}

fn requires_applied_prefix(message: &RuntimeMsg) -> bool {
    !matches!(
        message.verification_barrier_action(),
        crate::runtime::protocol::VerificationBarrierAction::Allow
    ) || matches!(
        message,
        RuntimeMsg::Shutdown
            | RuntimeMsg::ShutdownWithResponse { .. }
            | RuntimeMsg::BeginStorageVerification { .. }
            | RuntimeMsg::BeginBackupCapture { .. }
    )
}

impl EventLoop {
    pub(super) fn wal_group_in_flight(&self) -> bool {
        self.pipeline.wal_commits.has_pending_group()
            || self.pipeline.memtable_apply.is_pending()
            || self.pipeline.pending_cloud_seal.is_some()
    }

    pub(super) fn pending_message_waits_for_wal(&self) -> bool {
        self.wal_group_in_flight()
            && self
                .routing
                .pending_msg
                .as_ref()
                .is_some_and(requires_applied_prefix)
    }

    pub(super) fn has_deferred_wal_messages(&self) -> bool {
        !self.pipeline.wal_commits.deferred.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn deferred_wal_request_ids(&self) -> Vec<u64> {
        self.pipeline
            .wal_commits
            .deferred
            .iter()
            .filter_map(|pending| pending.message.request_id())
            .collect()
    }

    /// Take only leading resident transactions. A spill or layout/control
    /// barrier stays in place and prevents collection from the newer inbox.
    pub(super) fn take_deferred_transaction_for_wal_group(&mut self) -> Option<RuntimeMsg> {
        if !self
            .pipeline
            .wal_commits
            .deferred
            .front()
            .is_some_and(|pending| matches!(pending.message, RuntimeMsg::ApplyTransaction { .. }))
        {
            return None;
        }
        self.take_deferred_wal_message()
    }

    fn take_deferred_wal_message(&mut self) -> Option<RuntimeMsg> {
        let pending = self.pipeline.wal_commits.deferred.pop_front()?;
        if let Some(reservation) = pending.reservation {
            self.pipeline.wal_commits.admitting.push(reservation);
        }
        Some(pending.message)
    }

    pub(super) fn defer_transaction_behind_wal(
        &mut self,
        request: super::wal::ApplyTransactionRequest,
    ) -> Option<super::wal::ApplyTransactionRequest> {
        if !self.wal_group_in_flight() {
            return Some(request);
        }
        self.defer_wal_pipeline_message(RuntimeMsg::ApplyTransaction {
            request_id: request.request_id,
            ops: request.ops,
            assertions: request.assertions,
            durability_policy: request.durability_policy,
            start_sequence: request.start_sequence,
            conflict_policy: request.conflict_policy,
            response_tx: None,
        });
        None
    }

    pub(super) fn gate_message_for_wal_pipeline(
        &mut self,
        message: RuntimeMsg,
    ) -> Option<RuntimeMsg> {
        if !self.wal_group_in_flight() || !requires_applied_prefix(&message) {
            return Some(message);
        }
        self.defer_wal_pipeline_message(message);
        None
    }

    fn defer_wal_pipeline_message(&mut self, message: RuntimeMsg) {
        if (self
            .pipeline
            .wal_commits
            .pending
            .as_ref()
            .is_some_and(|group| group.failed)
            || self.pipeline.memtable_apply.terminal_prefix()
            || self.state.partial_apply_prefix)
            && message.is_mutation()
        {
            self.reject_pipeline_message(
                &message,
                MidgeError::Fenced(
                    "strict WAL prefix failed before this write was admitted".into(),
                ),
            );
            return;
        }
        if matches!(message, RuntimeMsg::RetryGc)
            && self
                .pipeline
                .wal_commits
                .deferred
                .iter()
                .any(|pending| matches!(pending.message, RuntimeMsg::RetryGc))
        {
            return;
        }
        if matches!(message, RuntimeMsg::Shutdown)
            && self
                .pipeline
                .wal_commits
                .deferred
                .iter()
                .any(|pending| matches!(pending.message, RuntimeMsg::Shutdown))
        {
            return;
        }
        let request_id = message.request_id();
        let shutdown = matches!(
            message,
            RuntimeMsg::Shutdown | RuntimeMsg::ShutdownWithResponse { .. }
        );
        let caller_count = self
            .pipeline
            .wal_commits
            .deferred
            .iter()
            .filter(|pending| pending.message.request_id().is_some())
            .count();
        if request_id.is_some() && !shutdown && caller_count >= MAX_DEFERRED_CALLERS {
            self.reject_pipeline_message(
                &message,
                MidgeError::WriteStall("ordered WAL admission queue is full".into()),
            );
            return;
        }
        let reservation = if message.is_mutation() || (request_id.is_some() && !shutdown) {
            if let Ok(reservation) = self
                .pipeline
                .wal_commits
                .queued_memory
                .reserve(mutation_bytes(&message), "ordered WAL deferred writes")
            {
                Some(reservation)
            } else {
                self.reject_pipeline_message(
                    &message,
                    MidgeError::WriteStall("ordered WAL admission byte limit is full".into()),
                );
                return;
            }
        } else {
            None
        };
        self.pipeline
            .wal_commits
            .deferred
            .push_back(DeferredMutation {
                message,
                reservation,
            });
    }

    fn reject_pipeline_message(&self, message: &RuntimeMsg, error: MidgeError) {
        let response = RuntimeResponse::Error {
            request_id: message.request_id().unwrap_or_default(),
            error,
        };
        match message {
            RuntimeMsg::ApplyTransaction {
                response_tx: Some(sender),
                ..
            }
            | RuntimeMsg::ApplySpilledTransaction {
                response_tx: Some(sender),
                ..
            } => {
                let _ = sender.send(response);
            }
            _ => {
                if let Some(request_id) = message.request_id() {
                    self.respond(request_id, response);
                }
            }
        }
    }

    pub(super) fn start_strict_pipeline_batch(
        &mut self,
        batch: CoalescedTransactionBatch,
        retained: usize,
    ) -> usize {
        let handled = batch.accepted_request_count();
        let result = self.start_strict_pipeline_io(&batch, retained);
        match result {
            Ok(StartedGroup {
                io,
                ticket,
                generation,
                next_generation,
                reservation,
            }) => {
                self.pipeline.wal_commits.pending = Some(PendingGroup {
                    batch,
                    spill: None,
                    io: Some(io),
                    ticket,
                    generation,
                    next_generation,
                    ready: RefCell::new(None),
                    failed: false,
                    reservation,
                });
                if self.pipeline.inline_flush_worker {
                    // Deterministic fixtures execute the same receipt protocol
                    // inline, rather than bypassing its transitions.
                    while self.wal_group_in_flight() {
                        self.progress_wal_pipeline();
                        if self.wal_group_in_flight() {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
            }
            Err(error) => {
                self.finish_coalesced_transaction_outcome(batch, Err(error), None);
            }
        }
        handled
    }

    pub(super) fn start_spilled_strict_pipeline(
        &mut self,
        request_id: u64,
        source: crate::runtime::transaction_spill::TransactionOpSource,
        params: &crate::runtime::actors::wal::SpilledTransactionAppendParams,
        proof: Option<&std::sync::Arc<crate::runtime::actors::wal::SpilledValidationProof>>,
        touched_cfs: Vec<crate::types::ColumnFamilyId>,
    ) {
        let workspace = match source.streaming_memory_bound() {
            Ok(bound) => ResourceBudget::new(bound),
            Err(error) => {
                let batch = CoalescedTransactionBatch::spilled_pipeline_placeholder(
                    request_id,
                    touched_cfs,
                );
                self.finish_coalesced_transaction_outcome(batch, Err(error), None);
                return;
            }
        };
        let retained = source
            .retained_memory_bytes()
            .saturating_add(std::mem::size_of::<PendingSpilledCommit>());
        let batch =
            CoalescedTransactionBatch::spilled_pipeline_placeholder(request_id, touched_cfs);
        let reservation = match self
            .pipeline
            .wal_commits
            .memory
            .reserve(retained, "strict spilled WAL in-flight group")
        {
            Ok(reservation) => reservation,
            Err(error) => {
                self.finish_coalesced_transaction_outcome(batch, Err(error), None);
                return;
            }
        };
        let generation = self.pipeline.durability.current_key();
        let next_generation = match self.next_wal_generation(generation) {
            Ok(next_generation) => next_generation,
            Err(error) => {
                self.finish_coalesced_transaction_outcome(batch, Err(error), None);
                return;
            }
        };
        let ticket = match self.pipeline.wal_transition.begin_sync(generation) {
            Ok(ticket) => ticket,
            Err(error) => {
                self.finish_coalesced_transaction_outcome(batch, Err(error), None);
                return;
            }
        };
        let submission = self.actors.wal_actor.begin_spilled_strict_group_submission(
            &mut self.state,
            &source,
            params,
            proof.map(AsRef::as_ref),
            &ticket,
            &workspace,
        );
        let (sync, prepared) = match submission {
            Ok(result) => result,
            Err(error) => {
                if self.actors.wal_actor.is_fenced() {
                    self.fence_wal_transition(&error, None);
                } else if let Err(finish) = self.pipeline.wal_transition.finish_sync(ticket) {
                    self.fence_wal_transition(&finish, None);
                }
                self.finish_coalesced_transaction_outcome(batch, Err(error), None);
                return;
            }
        };
        self.pipeline.wal_commits.pending = Some(PendingGroup {
            batch,
            spill: Some(PendingSpilledCommit {
                source,
                prepared,
                workspace,
            }),
            io: Some(PendingStrictIo::Sync(sync)),
            ticket,
            generation,
            next_generation,
            ready: RefCell::new(None),
            failed: false,
            reservation,
        });
        if self.pipeline.inline_flush_worker {
            while self.wal_group_in_flight() {
                self.progress_wal_pipeline();
                if self.wal_group_in_flight() {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }

    fn start_strict_pipeline_io(
        &mut self,
        batch: &CoalescedTransactionBatch,
        retained: usize,
    ) -> MidgeResult<StartedGroup> {
        let reservation = self
            .pipeline
            .wal_commits
            .memory
            .reserve(retained, "strict WAL in-flight group")?;
        let generation = self.pipeline.durability.current_key();
        let next_generation = self.next_wal_generation(generation)?;
        let ticket = self.pipeline.wal_transition.begin_sync(generation)?;
        match self.actors.wal_actor.begin_strict_group_submission(
            &mut self.state,
            &batch.prepared,
            &ticket,
        ) {
            Ok(io) => Ok(StartedGroup {
                io,
                ticket,
                generation,
                next_generation,
                reservation,
            }),
            Err(error) => {
                if self.actors.wal_actor.is_fenced() {
                    self.fence_wal_transition(&error, None);
                } else if let Err(finish) = self.pipeline.wal_transition.finish_sync(ticket) {
                    self.fence_wal_transition(&finish, None);
                }
                Err(error)
            }
        }
    }

    pub(super) fn progress_wal_pipeline(&mut self) {
        self.progress_memtable_apply_group();
        if self.pipeline.memtable_apply.is_pending() {
            return;
        }
        let Some(mut group) = self.pipeline.wal_commits.pending.take() else {
            self.restore_wal_pipeline_message();
            return;
        };
        let completion = group
            .ready
            .borrow_mut()
            .take()
            .or_else(|| group.io.as_ref().and_then(PendingStrictIo::poll));
        if !group.failed && group.deadline_expired() {
            let error = MidgeError::OutcomeUnknown("strict WAL synchronization expired; admitted storage effects remain owned until completion".into());
            self.fence_wal_transition(&error, None);
            self.fail_coalesced_transaction_responses(&group.batch, &error);
            let deferred = std::mem::take(&mut self.pipeline.wal_commits.deferred);
            for pending in deferred {
                if pending.message.is_mutation() {
                    self.reject_pipeline_message(
                        &pending.message,
                        MidgeError::Fenced(
                            "strict WAL prefix failed before this write was admitted".into(),
                        ),
                    );
                } else {
                    self.pipeline.wal_commits.deferred.push_back(pending);
                }
            }
            group.failed = true;
        }
        let Some(completion) = completion else {
            self.pipeline.wal_commits.pending = Some(group);
            return;
        };
        if group.failed {
            self.actors
                .wal_actor
                .release_fenced_writer_after_completion();
            drop(group);
            self.restore_wal_pipeline_message();
            return;
        }
        let receipt = match completion {
            StrictIoCompletion::Append(completion) => {
                let Some(PendingStrictIo::Append(append)) = group.io.take() else {
                    unreachable!("append receipt belongs to the parked append stage");
                };
                match self.actors.wal_actor.finish_sequenced_group_append(
                    &mut self.state,
                    append,
                    &group.ticket,
                    completion,
                ) {
                    Ok(sync) => {
                        group.io = Some(PendingStrictIo::Sync(sync));
                        self.pipeline.wal_commits.pending = Some(group);
                        return;
                    }
                    Err(error) => Err(error),
                }
            }
            StrictIoCompletion::Sync(completion) => {
                let Some(PendingStrictIo::Sync(sync)) = group.io.as_ref() else {
                    unreachable!("sync receipt belongs to the parked sync stage");
                };
                self.actors.wal_actor.finish_strict_group_submission(
                    &mut self.state,
                    sync,
                    &group.ticket,
                    completion,
                )
            }
        };
        self.complete_wal_pipeline_group(group, receipt);
    }

    fn complete_wal_pipeline_group(
        &mut self,
        group: PendingGroup,
        receipt: MidgeResult<crate::runtime::actors::wal::WalSyncReceipt>,
    ) {
        let completed = receipt.and_then(|receipt| {
            self.commit_synced_generation(
                CompletionSource::WalSync,
                group.generation,
                group.next_generation,
                group.ticket,
                receipt,
            )
            .map_err(|error| crate::common::error::classify_commit_error(error, false))
        });
        let PendingGroup {
            batch,
            spill,
            io,
            reservation,
            ..
        } = group;
        drop(io); // release lexical native root before apply/transitions
        if let Some(spill) = spill {
            let request_id = spill.prepared.request_id;
            match completed.and_then(|()| {
                self.actors
                    .wal_actor
                    .apply_spilled_strict_after_pipeline_sync(
                        &mut self.state,
                        &spill.source,
                        spill.prepared,
                        &spill.workspace,
                    )
            }) {
                Ok((last_sequence, op_count, touched_cfs)) => {
                    self.publish_snapshots_for(&touched_cfs);
                    self.ack_applied_transaction(
                        request_id,
                        &super::durability_sync::AppliedTransaction {
                            last_sequence,
                            op_count,
                            deferred: false,
                            touched_cfs: &touched_cfs,
                        },
                    );
                    drop(reservation);
                    self.pipeline.wal_commits.release_admitting();
                    self.restore_wal_pipeline_message();
                }
                Err(error) => {
                    self.fence_wal_transition(&error, None);
                    self.actors
                        .wal_actor
                        .release_fenced_writer_after_completion();
                    self.finish_coalesced_transaction_outcome(batch, Err(error), Some(reservation));
                    self.restore_wal_pipeline_message();
                }
            }
            return;
        }
        let error = match completed {
            Ok(()) => {
                self.begin_memtable_apply_group(batch, reservation);
                return;
            }
            Err(error) => error,
        };
        self.fence_wal_transition(&error, None);
        self.actors
            .wal_actor
            .release_fenced_writer_after_completion();
        self.finish_coalesced_transaction_outcome(batch, Err(error), Some(reservation));
        self.restore_wal_pipeline_message();
    }

    fn restore_wal_pipeline_message(&mut self) {
        if self.wal_group_in_flight() || self.routing.pending_msg.is_some() {
            return;
        }
        self.pipeline.wal_commits.admitting.clear();
        self.routing.pending_msg = self.take_deferred_wal_message();
    }
}

#[cfg(all(test, feature = "failpoints"))]
pub(super) mod tests;
