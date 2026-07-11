use super::{EventLoop, HandleOutcome};
#[cfg(test)]
use crate::runtime::RuntimeResponse;

pub(super) struct GcCoordinator;

impl GcCoordinator {
    pub(super) fn retry(event_loop: &mut EventLoop) -> HandleOutcome {
        let hybrid_storage = event_loop.hybrid_storage.clone();
        match event_loop.state.reclaim_dropped_column_families() {
            Ok(names) => event_loop.gc_actor.queue_manifest_reclamation(names),
            Err(error) => {
                tracing::warn!(%error, "dropped column-family reclamation journal append failed; retaining files");
                return HandleOutcome::Continue;
            }
        }
        if event_loop.gc_actor.has_manifest_reclamation() {
            let published = crate::runtime::actors::ManifestActor::persist(&event_loop.state)
                .and_then(|()| {
                    event_loop
                        .mirror_metadata_after_local_commit("dropped column-family reclamation")
                });
            if let Err(error) = published {
                tracing::warn!(%error, "dropped column-family manifest publication failed; retaining files");
                return HandleOutcome::Continue;
            }
            let reclaimed = event_loop.gc_actor.take_manifest_reclamation();
            event_loop.gc_actor.delete_ssts(
                &mut event_loop.state,
                &reclaimed,
                hybrid_storage.clone(),
            );
            event_loop.publish_snapshot();
        }
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
