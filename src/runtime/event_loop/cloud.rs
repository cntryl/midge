use super::{EventLoop, HandleOutcome};
use crate::runtime::RuntimeResponse;

pub(super) struct CloudCoordinator;

impl CloudCoordinator {
    pub(super) fn upload_sst(
        event_loop: &mut EventLoop,
        request_id: u64,
        sst_name: &str,
    ) -> HandleOutcome {
        event_loop.cloud_actor.upload_sst(
            &mut event_loop.state,
            sst_name,
            event_loop.hybrid_storage.as_ref(),
        );
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    pub(super) fn upload_wal(
        event_loop: &mut EventLoop,
        request_id: u64,
        segment_id: u64,
    ) -> HandleOutcome {
        event_loop
            .cloud_actor
            .upload_wal(&mut event_loop.state, segment_id);
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }

    pub(super) fn upload_complete(
        event_loop: &mut EventLoop,
        request_id: u64,
        resource: &str,
    ) -> HandleOutcome {
        event_loop
            .cloud_actor
            .handle_upload_complete(&mut event_loop.state, resource);
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }
}
