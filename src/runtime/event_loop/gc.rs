use super::{EventLoop, HandleOutcome};
use crate::runtime::RuntimeResponse;

pub(super) struct GcCoordinator;

impl GcCoordinator {
    pub(super) fn check(event_loop: &mut EventLoop, request_id: u64) -> HandleOutcome {
        let evicted = event_loop.state.evict_timed_out_snapshots();
        if evicted > 0 {
            tracing::warn!(evicted, "Evicted timed-out snapshots before GC check");
        }

        event_loop.gc_actor.check(&event_loop.state);
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    pub(super) fn delete_obsolete_ssts(
        event_loop: &mut EventLoop,
        request_id: u64,
        sst_names: Vec<String>,
    ) -> HandleOutcome {
        let hybrid_storage = event_loop.hybrid_storage.clone();
        let result =
            event_loop
                .gc_actor
                .delete_ssts(&mut event_loop.state, &sst_names, hybrid_storage);
        let resp = result.map_or_else(
            |error| RuntimeResponse::Error {
                request_id,
                error: crate::common::MidgeError::Internal(error.to_string()),
            },
            |()| RuntimeResponse::Ok { request_id },
        );
        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }
}
