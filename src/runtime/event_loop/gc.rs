use super::{EventLoop, HandleOutcome};
#[cfg(test)]
use crate::runtime::RuntimeResponse;

pub(super) struct GcCoordinator;

impl GcCoordinator {
    pub(super) fn retry(event_loop: &mut EventLoop) -> HandleOutcome {
        let hybrid_storage = event_loop.hybrid_storage.clone();
        event_loop
            .gc_actor
            .retry_pending(&mut event_loop.state, hybrid_storage);
        HandleOutcome::Continue
    }

    #[cfg(test)]
    pub(super) fn check(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        let timed_out = event_loop.state.warn_timed_out_snapshots();
        if timed_out > 0 {
            tracing::warn!(
                timed_out,
                "Observed timed-out snapshots before GC check; retaining pins"
            );
        }

        crate::runtime::actors::GcActor::check(&event_loop.state);
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    #[cfg(test)]
    pub(super) fn delete_obsolete_ssts(
        event_loop: &mut EventLoop,
        request_id: u64,
        sst_names: &[String],
    ) -> HandleOutcome {
        let hybrid_storage = event_loop.hybrid_storage.clone();
        event_loop
            .gc_actor
            .delete_ssts(&mut event_loop.state, sst_names, hybrid_storage);
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }
}
