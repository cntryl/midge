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
    io: Option<PendingStrictIo>,
    ticket: WalSyncTicket,
    generation: u64,
    next_generation: Option<u64>,
    ready: RefCell<Option<StrictIoCompletion>>,
    failed: bool,
    reservation: ResourceReservation,
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
}

impl Default for WalCommitPipeline {
    fn default() -> Self {
        Self {
            pending: None,
            deferred: VecDeque::new(),
            memory: ResourceBudget::new(MAX_INFLIGHT_GROUP_BYTES),
            queued_memory: ResourceBudget::new(MAX_INFLIGHT_GROUP_BYTES),
            admitting: Vec::new(),
        }
    }
}

impl WalCommitPipeline {
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
        self.pipeline.wal_commits.pending.is_some() || self.pipeline.memtable_apply.is_pending()
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
        let batch = group.batch;
        drop(group.io); // release lexical native root before apply/transitions
        let error = match completed {
            Ok(()) => {
                self.begin_memtable_apply_group(batch, group.reservation);
                return;
            }
            Err(error) => error,
        };
        self.fence_wal_transition(&error, None);
        self.actors
            .wal_actor
            .release_fenced_writer_after_completion();
        self.finish_coalesced_transaction_outcome(batch, Err(error), Some(group.reservation));
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
