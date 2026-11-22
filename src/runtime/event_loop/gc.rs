use super::{EventLoop, HandleOutcome};
#[cfg(test)]
use crate::runtime::RuntimeResponse;

/// About one minute of deferrals at the production one-second retry interval.
const CONTENTION_WARN_DEFERRALS: u32 = 60;

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
            Self::defer_for_contention(event_loop);
            return HandleOutcome::Continue;
        }
        let hybrid_storage = event_loop.actors.cloud_coordinator.hybrid_storage.clone();
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
        // A running mirror holds no publication gate, so it is checked
        // explicitly. Discovery above still queues newly dropped names; the
        // queue is retained, and checkpointing and mirror admission wait for
        // the running mirror. Failures after this check are handled below.
        if event_loop.actors.gc_actor.has_manifest_reclamation()
            && event_loop
                .actors
                .cloud_coordinator
                .metadata_mirror
                .is_running()
        {
            Self::defer_for_contention(event_loop);
            return HandleOutcome::Continue;
        }
        event_loop
            .actors
            .gc_actor
            .begin_manifest_reclamation_attempt();
        if event_loop.actors.gc_actor.has_manifest_reclamation() {
            // Everything queued before the cut is captured is covered by this
            // publication; anything queued while it runs is not.
            let covered = event_loop.actors.gc_actor.manifest_reclamation_snapshot();
            let published = crate::runtime::actors::ManifestActor::persist_for(
                &mut event_loop.state,
                crate::metadata::accounting::Origin::Administration,
            )
            .and_then(|()| {
                // Reclamation cannot use salvage-mode best effort. Until
                // the remote manifest mirror succeeds, its old snapshot
                // may still reference every queued SST.
                event_loop.start_metadata_mirror(
                    super::metadata_mirror::MirrorContinuation::Reclamation {
                        covered: covered.clone(),
                    },
                    *deadline,
                )
            });
            if matches!(published, Ok(true)) {
                return HandleOutcome::Continue;
            }
            if published.is_err() {
                Self::finish_manifest_reclamation(event_loop, published.map(|_| ()), &covered);
                return HandleOutcome::Continue;
            }
            Self::finish_manifest_reclamation(event_loop, Ok(()), &covered);
        }
        event_loop
            .actors
            .gc_actor
            .retry_pending(&mut event_loop.state, hybrid_storage);
        HandleOutcome::Continue
    }

    fn defer_for_contention(event_loop: &mut EventLoop) {
        let deferrals = event_loop
            .actors
            .gc_actor
            .defer_manifest_reclamation_for_contention();
        if deferrals == CONTENTION_WARN_DEFERRALS {
            tracing::warn!(
                deferrals,
                "dropped column-family reclamation still deferred behind another metadata publication"
            );
        } else {
            tracing::debug!(
                deferrals,
                "reclamation retry deferred behind another publication"
            );
        }
    }

    pub(super) fn finish_manifest_reclamation(
        event_loop: &mut EventLoop,
        result: crate::common::MidgeResult<()>,
        covered: &[String],
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
        let reclaimed = event_loop
            .actors
            .gc_actor
            .take_covered_manifest_reclamation(covered);
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
