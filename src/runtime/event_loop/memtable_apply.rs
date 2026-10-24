//! Bounded concurrent memtable effects, one ordered group visibility cut.

use super::write_batch::CoalescedTransactionBatch;
use super::EventLoop;
use crate::common::resource_budget::ResourceReservation;
use crate::common::{MidgeError, OperationDeadline};
use crate::runtime::actors::wal::{MemtableApplyAuthority, MemtableApplyPlan};
use crossbeam::channel::{Receiver, Sender, TrySendError};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

mod pool;
#[cfg(all(test, feature = "failpoints"))]
mod tests;
use pool::{ApplyCompletion, ApplyPool, ApplyWork};

struct ApplyingGroup {
    batch: CoalescedTransactionBatch,
    plan: MemtableApplyPlan,
    authority: MemtableApplyAuthority,
    completed: Receiver<ApplyCompletion>,
    sender: Sender<ApplyCompletion>,
    in_flight: HashSet<usize>,
    cancelled: Arc<AtomicBool>,
    wake_queued: Arc<AtomicBool>,
    deadline: OperationDeadline,
    failure: Option<MidgeError>,
    responses_failed: bool,
    reservation: ResourceReservation,
}

#[derive(Default)]
pub(super) struct MemtableApplyCoordinator {
    pool: Option<ApplyPool>,
    pending: Option<ApplyingGroup>,
    terminal_prefix: bool,
}

impl MemtableApplyCoordinator {
    pub(super) fn is_pending(&self) -> bool {
        self.pending.is_some()
    }
    pub(super) fn terminal_prefix(&self) -> bool {
        self.terminal_prefix
    }
    pub(super) fn is_ready(&self) -> bool {
        self.pending.as_ref().is_some_and(|group| {
            !group.completed.is_empty() || (!group.responses_failed && group.deadline.is_expired())
        })
    }
}

impl Drop for MemtableApplyCoordinator {
    fn drop(&mut self) {
        if let Some(group) = &self.pending {
            group.cancelled.store(true, Ordering::Release);
        }
        // Join while the pending plan and memory reservation remain owned.
        // Never let field-drop order release admitted inputs before effects.
        self.pool.take();
    }
}

impl EventLoop {
    pub(super) fn begin_memtable_apply_group(
        &mut self,
        mut batch: CoalescedTransactionBatch,
        reservation: ResourceReservation,
    ) {
        if self.pipeline.memtable_apply.pool.is_none() {
            match ApplyPool::new() {
                Ok(pool) => self.pipeline.memtable_apply.pool = Some(pool),
                Err(error) => {
                    self.fail_unstarted_memtable_apply(batch, reservation, error);
                    return;
                }
            }
        }
        let prepared = std::mem::take(&mut batch.prepared);
        match self
            .actors
            .wal_actor
            .prepare_detached_memtable_apply(&self.state, prepared)
        {
            Ok(plan) => self.install_memtable_apply_group(batch, plan, reservation),
            Err(error) => {
                self.fail_unstarted_memtable_apply(batch, reservation, error);
                return;
            }
        }
        self.progress_memtable_apply_group();
    }

    fn install_memtable_apply_group(
        &mut self,
        batch: CoalescedTransactionBatch,
        plan: MemtableApplyPlan,
        reservation: ResourceReservation,
    ) {
        let deadline = OperationDeadline::from_budget(self.routing.runtime_response_timeout);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancellation = Arc::clone(&cancelled);
        let fence = self.authority.fencing.clone();
        let authority: MemtableApplyAuthority = Arc::new(move || {
            if cancellation.load(Ordering::Acquire) || deadline.is_expired() {
                return Err(MidgeError::Fenced(
                    "admitted memtable apply was cancelled before its next effect".into(),
                ));
            }
            fence.check_health()
        });
        let (sender, completed) = crossbeam::channel::unbounded();
        self.pipeline.memtable_apply.pending = Some(ApplyingGroup {
            batch,
            plan,
            authority,
            completed,
            sender,
            in_flight: HashSet::new(),
            cancelled,
            wake_queued: Arc::new(AtomicBool::new(false)),
            deadline,
            failure: None,
            responses_failed: false,
            reservation,
        });
    }

    pub(super) fn progress_memtable_apply_group(&mut self) {
        let Some(mut group) = self.pipeline.memtable_apply.pending.take() else {
            return;
        };
        group.wake_queued.store(false, Ordering::Release);
        while let Ok(completion) = group.completed.try_recv() {
            if !group.in_flight.remove(&completion.ordinal) {
                group.failure.get_or_insert_with(|| {
                    MidgeError::Internal("duplicate memtable apply completion".into())
                });
                continue;
            }
            if let Err(error) = completion.result {
                group.failure.get_or_insert(error);
            }
        }
        if group.deadline.is_expired() {
            group.failure.get_or_insert_with(|| {
                MidgeError::OutcomeUnknown(
                    "admitted memtable apply deadline expired; effects remain owned until terminal"
                        .into(),
                )
            });
        }
        if group.failure.is_none() {
            self.submit_memtable_apply_jobs(&mut group);
        }
        if let Some(error) = &group.failure {
            group.cancelled.store(true, Ordering::Release);
            group.plan.jobs.clear();
            if !group.responses_failed {
                self.pipeline.memtable_apply.terminal_prefix = true;
                self.fence_wal_transition(error, None);
                self.fail_coalesced_transaction_responses(&group.batch, &MidgeError::OutcomeUnknown("a synced memtable group failed before ordered publication; restart recovers the complete WAL unit".into()));
                group.responses_failed = true;
            }
        }
        if !group.in_flight.is_empty() || !group.plan.jobs.is_empty() {
            self.pipeline.memtable_apply.pending = Some(group);
            return;
        }
        self.finish_memtable_apply_group(group);
    }

    fn submit_memtable_apply_jobs(&self, group: &mut ApplyingGroup) {
        let pool = self
            .pipeline
            .memtable_apply
            .pool
            .as_ref()
            .expect("owned apply pool");
        for _ in 0..64 {
            let Some(job) = group.plan.jobs.pop_front() else {
                break;
            };
            let ordinal = job.ordinal;
            let work = ApplyWork {
                job,
                authority: Arc::clone(&group.authority),
                completed: group.sender.clone(),
                cancelled: Arc::clone(&group.cancelled),
                wake_queued: Arc::clone(&group.wake_queued),
                wake: self.pipeline.worker_msg_tx.clone(),
            };
            match pool.try_submit(work) {
                Ok(()) => {
                    group.in_flight.insert(ordinal);
                }
                Err(TrySendError::Full(work)) => {
                    group.plan.jobs.push_front(work.job);
                    break;
                }
                Err(TrySendError::Disconnected(_)) => {
                    group.failure =
                        Some(MidgeError::Internal("memtable apply pool stopped".into()));
                    break;
                }
            }
        }
    }

    fn finish_memtable_apply_group(&mut self, group: ApplyingGroup) {
        let outcome = self.actors.wal_actor.finish_detached_memtable_apply(
            &mut self.state,
            group.plan,
            group.failure,
        );
        if let Err(error) = &outcome {
            self.pipeline.memtable_apply.terminal_prefix = true;
            self.fence_wal_transition(error, None);
            self.actors
                .wal_actor
                .release_fenced_writer_after_completion();
        }
        if group.responses_failed {
            return;
        }
        self.finish_coalesced_transaction_outcome(group.batch, outcome, Some(group.reservation));
    }

    fn fail_unstarted_memtable_apply(
        &mut self,
        batch: CoalescedTransactionBatch,
        reservation: ResourceReservation,
        error: MidgeError,
    ) {
        self.pipeline.memtable_apply.terminal_prefix = true;
        self.fence_wal_transition(&error, None);
        self.actors
            .wal_actor
            .release_fenced_writer_after_completion();
        self.finish_coalesced_transaction_outcome(
            batch,
            Err(MidgeError::OutcomeUnknown(format!(
                "synced memtable group could not start: {error}"
            ))),
            Some(reservation),
        );
    }
}
