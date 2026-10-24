//! The loop consumes owned compute tickets; generic messages only wake it.
use super::{CompactionCompleteRequest, CompactionCoordinator, EventLoop};
use crate::runtime::RuntimeMsg;

impl EventLoop {
    pub(in crate::runtime::event_loop) fn reap_compaction_compute_results(&mut self) {
        if self.wal_group_in_flight()
            || self.publication.verification_barrier.is_active()
            || self.publication.publication_gate.is_active()
            || self.publication.compaction_publication.is_active()
        {
            return;
        }
        // Publication has one owner even when compute has several. Leave
        // sibling results in their native tickets until that slot is free.
        if let Some(RuntimeMsg::CompactionComplete {
            request_id,
            input_ssts,
            output_ssts,
            cf_id,
            target_level,
            succeeded,
        }) = self.actors.compaction_actor.poll_compute_completion()
        {
            CompactionCoordinator::complete(
                self,
                CompactionCompleteRequest {
                    request_id,
                    input_ssts,
                    output_ssts,
                    cf_id,
                    target_level,
                    succeeded,
                },
            );
        }
    }
}
