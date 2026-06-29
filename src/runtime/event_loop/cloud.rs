use super::{EventLoop, HandleOutcome};
use crate::runtime::RuntimeResponse;

pub(super) struct CloudCoordinator;

impl CloudCoordinator {
    pub(super) fn upload_sst(
        event_loop: &mut EventLoop,
        request_id: u64,
        sst_name: String,
    ) -> HandleOutcome {
        let result = event_loop.cloud_actor.upload_sst(
            &mut event_loop.state,
            &sst_name,
            event_loop.hybrid_storage.as_ref(),
        );
        let resp = result
            .map(|_| RuntimeResponse::Ok { request_id })
            .unwrap_or_else(|error| RuntimeResponse::Error {
                request_id,
                error: crate::common::MidgeError::Internal(error.to_string()),
            });
        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    pub(super) fn upload_wal(
        event_loop: &mut EventLoop,
        request_id: u64,
        segment_id: u64,
    ) -> HandleOutcome {
        let result = event_loop
            .cloud_actor
            .upload_wal(&mut event_loop.state, segment_id);
        let resp = result
            .map(|_| RuntimeResponse::Ok { request_id })
            .unwrap_or_else(|error| RuntimeResponse::Error {
                request_id,
                error: crate::common::MidgeError::Internal(error.to_string()),
            });
        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    pub(super) fn upload_complete(
        event_loop: &mut EventLoop,
        request_id: u64,
        resource: String,
    ) -> HandleOutcome {
        event_loop
            .cloud_actor
            .handle_upload_complete(&mut event_loop.state, &resource);
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }
}
