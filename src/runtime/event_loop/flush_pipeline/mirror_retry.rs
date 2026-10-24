//! Accepted flush outputs retain their owner while capture admission waits.
use super::{EventLoop, FlushIdentity, FlushMirrorTask};
use crate::common::{DeadlineScope, MidgeError, MidgeResult, OperationDeadline};
use std::time::{Duration, Instant};

const CAPTURE_RETRY_DELAY: Duration = Duration::from_millis(10);
#[cfg(test)]
mod tests;

pub(in crate::runtime::event_loop) struct PendingFlushMirror {
    task: FlushMirrorTask,
    retry_at: Instant,
}

impl EventLoop {
    pub(in crate::runtime::event_loop) fn flush_mirror_deadline(
        &self,
        identity: FlushIdentity,
    ) -> OperationDeadline {
        let mut deadline = OperationDeadline::from_budget(self.routing.runtime_response_timeout);
        if let Some(waiters) = self.publication.flush_barrier_waiters.get(&identity.cf_id) {
            for waiter in waiters
                .iter()
                .filter(|waiter| waiter.frontier <= identity.sequence)
            {
                if let Some(caller) = self
                    .routing
                    .router
                    .request_deadline(waiter.request_id, self.routing.runtime_response_timeout)
                {
                    if caller.remaining() < deadline.remaining() {
                        deadline = caller;
                    }
                }
            }
        }
        deadline
    }

    pub(in crate::runtime::event_loop) fn admit_flush_mirror(
        &mut self,
        mut task: FlushMirrorTask,
    ) -> MidgeResult<()> {
        DeadlineScope::new(task.deadline).check("flush metadata capture admission")?;
        if !self.pipeline.inline_flush_worker
            && self
                .actors
                .cloud_coordinator
                .cloud_metadata_storage
                .is_some()
        {
            match self
                .publication
                .metadata_publication_lock
                .capture_control_cut(
                    &self.state.fs,
                    task.delta.identity.writer_epoch,
                    &task.deadline,
                ) {
                Ok(cut) => task.metadata_cut = Some(cut),
                Err(error)
                    if self
                        .publication
                        .metadata_publication_lock
                        .capture_admission_retryable(&error) =>
                {
                    let id = task.delta.identity.flush_id;
                    // A physical Publish completion is consumed once, so an
                    // owner can retain at most one unadmitted mirror task.
                    if !self
                        .actors
                        .flush_actor
                        .owns_pipeline(task.delta.identity.cf_id, id)
                        || self
                            .pipeline
                            .pending_flush_mirrors
                            .iter()
                            .any(|pending| pending.task.delta.identity.flush_id == id)
                    {
                        return Err(MidgeError::Fenced(
                            "flush capture retry has no unique admitted pipeline".into(),
                        ));
                    }
                    self.pipeline
                        .pending_flush_mirrors
                        .push_back(PendingFlushMirror {
                            task,
                            retry_at: Instant::now() + CAPTURE_RETRY_DELAY,
                        });
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        self.actors.flush_actor.submit_mirror(task)
    }

    pub(in crate::runtime::event_loop) fn flush_mirror_retry_timeout(&self) -> Option<Duration> {
        if self.publication.publication_gate.is_active() {
            return None;
        }
        self.pipeline
            .pending_flush_mirrors
            .iter()
            .map(|pending| {
                pending
                    .retry_at
                    .saturating_duration_since(Instant::now())
                    .min(pending.task.deadline.remaining())
            })
            .min()
    }

    pub(in crate::runtime::event_loop) fn retry_flush_mirrors_if_due(&mut self) {
        if self.publication.publication_gate.is_active() {
            return;
        }
        let Some(position) = self
            .pipeline
            .pending_flush_mirrors
            .iter()
            .position(|pending| {
                pending.retry_at <= Instant::now() || pending.task.deadline.is_expired()
            })
        else {
            return;
        };
        let pending = self
            .pipeline
            .pending_flush_mirrors
            .remove(position)
            .expect("selected flush mirror");
        let identity = pending.task.delta.identity;
        let reservation = pending.task.reservation;
        let owner = crate::runtime::event_loop::coordination::ManifestPublicationOwner::Flush {
            flush_id: identity.flush_id,
        };
        if !self.publication.publication_gate.try_acquire(owner.clone()) {
            self.pipeline.pending_flush_mirrors.push_back(pending);
            return;
        }
        let result = self
            .validate_flush_owner(identity)
            .and_then(|()| self.admit_flush_mirror(pending.task));
        self.publication.publication_gate.release(&owner);
        if let Err(error) = result {
            self.fail_flush_pipeline(identity.flush_id, reservation, &error);
        }
    }

    pub(in crate::runtime::event_loop) fn forget_flush_mirror_retry(&mut self, flush_id: u64) {
        self.pipeline
            .pending_flush_mirrors
            .retain(|pending| pending.task.delta.identity.flush_id != flush_id);
    }
}
