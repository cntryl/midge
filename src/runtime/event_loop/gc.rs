use super::{EventLoop, HandleOutcome};
#[cfg(test)]
use crate::runtime::RuntimeResponse;

pub(super) struct GcCoordinator;

impl GcCoordinator {
    pub(super) fn retry_within(
        event_loop: &mut EventLoop,
        deadline: &crate::common::OperationDeadline,
    ) -> HandleOutcome {
        if event_loop.local_ddl_requires_restart() {
            event_loop
                .actors
                .gc_actor
                .defer_manifest_reclamation_retry();
            return HandleOutcome::Continue;
        }
        if event_loop.publication.publication_gate.is_active() {
            event_loop
                .actors
                .gc_actor
                .defer_manifest_reclamation_retry();
            return HandleOutcome::Continue;
        }
        let hybrid_storage = event_loop.actors.cloud_coordinator.hybrid_storage.clone();
        event_loop
            .actors
            .gc_actor
            .begin_manifest_reclamation_attempt();
        match event_loop.state.reclaim_dropped_column_families() {
            Ok(names) => event_loop.actors.gc_actor.queue_manifest_reclamation(names),
            Err(error) => {
                event_loop
                    .actors
                    .gc_actor
                    .defer_manifest_reclamation_retry();
                tracing::warn!(%error, "dropped column-family reclamation journal append failed; retaining files");
                return HandleOutcome::Continue;
            }
        }
        if event_loop.actors.gc_actor.has_manifest_reclamation() {
            let published = crate::runtime::actors::ManifestActor::persist_for(
                &mut event_loop.state,
                crate::metadata::accounting::Origin::Administration,
            )
            .and_then(|()| {
                // Reclamation cannot use salvage-mode best effort. Until
                // the remote manifest mirror succeeds, its old snapshot
                // may still reference every queued SST.
                event_loop.start_metadata_mirror(
                    super::metadata_mirror::MirrorContinuation::Reclamation,
                    *deadline,
                )
            });
            if matches!(published, Ok(true)) {
                return HandleOutcome::Continue;
            }
            if published.is_err() {
                Self::finish_manifest_reclamation(event_loop, published.map(|_| ()));
                return HandleOutcome::Continue;
            }
            Self::finish_manifest_reclamation(event_loop, Ok(()));
        }
        event_loop
            .actors
            .gc_actor
            .retry_pending(&mut event_loop.state, hybrid_storage);
        HandleOutcome::Continue
    }

    pub(super) fn finish_manifest_reclamation(
        event_loop: &mut EventLoop,
        result: crate::common::MidgeResult<()>,
    ) {
        if let Err(error) = result {
            event_loop
                .actors
                .gc_actor
                .defer_manifest_reclamation_retry();
            event_loop.state.mark_persistence_anomaly();
            tracing::warn!(%error, "dropped column-family manifest publication failed; retaining files");
            return;
        }
        let reclaimed = event_loop.actors.gc_actor.take_manifest_reclamation();
        // The exact publication completion, not caller timeout, releases the
        // old manifest's references. Snapshot pins still protect old readers.
        event_loop.publish_snapshot();
        event_loop.actors.gc_actor.delete_ssts(
            &mut event_loop.state,
            &reclaimed,
            event_loop.actors.cloud_coordinator.hybrid_storage.clone(),
        );
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
        let hybrid_storage = event_loop.actors.cloud_coordinator.hybrid_storage.clone();
        event_loop
            .actors
            .gc_actor
            .delete_ssts(&mut event_loop.state, sst_names, hybrid_storage);
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }
}
