//! Retain an accepted publication while the shared metadata workspace is busy.
use super::{
    CompactionCoordinator, CompactionPublishPhase, EventLoop, MidgeError, MidgeResult,
    OperationDeadline,
};
use crate::runtime::CapturedMetadataCut;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(super) struct CaptureRetry {
    pub(super) deadline: OperationDeadline,
    ready_at: Instant,
}

pub(super) enum CaptureAdmission {
    Ready {
        cut: Option<Arc<CapturedMetadataCut>>,
        deadline: OperationDeadline,
    },
    Deferred,
}

impl CompactionCoordinator {
    pub(super) fn admit_publication_capture(
        event_loop: &mut EventLoop,
        phase: CompactionPublishPhase,
    ) -> MidgeResult<CaptureAdmission> {
        let pending = event_loop
            .publication
            .compaction_publication
            .get_mut()
            .ok_or_else(|| MidgeError::Internal("publication lost capture ownership".into()))?;
        let deadline = pending
            .capture_retry
            .filter(|_| pending.expected_phase == phase)
            .map(|retry| retry.deadline)
            .or(pending.manual_deadline)
            .unwrap_or_else(|| {
                OperationDeadline::from_budget(event_loop.routing.runtime_response_timeout)
            });
        if deadline.is_expired() {
            return Err(MidgeError::Timeout(
                "metadata capture admission exceeded publication deadline".into(),
            ));
        }
        pending.expected_phase = phase;
        let capture = if !event_loop.pipeline.inline_flush_worker
            && event_loop
                .actors
                .cloud_coordinator
                .cloud_metadata_storage
                .is_some()
        {
            event_loop
                .publication
                .metadata_publication_lock
                .capture_control_cut(&event_loop.state.fs, pending.token.writer_epoch, &deadline)
                .map(Some)
        } else {
            Ok(None)
        };
        match capture {
            Ok(cut) => {
                pending.capture_retry = None;
                Ok(CaptureAdmission::Ready { cut, deadline })
            }
            Err(error)
                if event_loop
                    .publication
                    .metadata_publication_lock
                    .capture_admission_retryable(&error) =>
            {
                pending.capture_retry = Some(CaptureRetry {
                    deadline,
                    ready_at: Instant::now()
                        + crate::runtime::event_loop::HYBRID_STORAGE_POLL_INTERVAL,
                });
                let owner = Self::publication_owner(&pending.token);
                event_loop.publication.publication_gate.release(&owner);
                Ok(CaptureAdmission::Deferred)
            }
            Err(error) => Err(error),
        }
    }

    pub(in crate::runtime::event_loop) fn capture_retry_timeout(
        event_loop: &EventLoop,
    ) -> Option<Duration> {
        if event_loop.publication.publication_gate.is_active() {
            return None;
        }
        let retry = event_loop
            .publication
            .compaction_publication
            .get()?
            .capture_retry?;
        Some(
            retry
                .deadline
                .remaining()
                .min(retry.ready_at.saturating_duration_since(Instant::now())),
        )
    }

    pub(in crate::runtime::event_loop) fn has_pending_capture(event_loop: &EventLoop) -> bool {
        event_loop
            .publication
            .compaction_publication
            .get()
            .is_some_and(|pending| pending.capture_retry.is_some())
    }

    pub(super) fn retry_publication_capture(event_loop: &mut EventLoop) {
        if Self::capture_retry_timeout(event_loop) != Some(Duration::ZERO)
            || event_loop.actors.compaction_publish_actor.is_inflight()
        {
            return;
        }
        let Some(pending) = event_loop.publication.compaction_publication.get() else {
            return;
        };
        let phase = pending.expected_phase;
        let request_id = pending.token.request_id;
        let owner = Self::publication_owner(&pending.token);
        if !Self::select_publication_compute_owner(event_loop, phase, request_id) {
            return;
        }
        if !event_loop.publication.publication_gate.try_acquire(owner) {
            return;
        }
        if let Err(error) = Self::submit_publication_phase(event_loop, phase) {
            Self::finish_failed_publication(event_loop, &error);
        }
    }

    pub(super) fn select_publication_compute_owner(
        event_loop: &mut EventLoop,
        phase: CompactionPublishPhase,
        request_id: u64,
    ) -> bool {
        // IntentCleared has already settled its core, which can now belong to
        // a sibling. All earlier continuations still require the exact owner.
        if phase == CompactionPublishPhase::IntentCleared
            || event_loop.actors.compaction_actor.active_count() == 0
            || event_loop.actors.compaction_actor.select_job(request_id)
        {
            return true;
        }
        event_loop.authority.compaction_fence.degrade();
        event_loop.state.mark_persistence_anomaly();
        tracing::error!(
            job_id = request_id,
            "retaining publication whose compute owner cannot be selected"
        );
        false
    }
}
