use super::{EventLoop, HandleOutcome};
use crate::runtime::RuntimeResponse;

pub(super) struct FlushCoordinator;

impl FlushCoordinator {
    pub(super) fn flush_memtable(
        event_loop: &mut EventLoop,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    ) -> HandleOutcome {
        let resp = match event_loop.flush_actor.handle_flush(
            &mut event_loop.state,
            cf_id,
            event_loop.hybrid_storage.as_ref(),
        ) {
            Ok(flush_output) => {
                let sequence = event_loop.state.sequence;
                match event_loop.publish_flushed_sst_with_reservation(
                    cf_id,
                    &flush_output.sst_name,
                    sequence,
                    flush_output.file_meta,
                    flush_output.frozen_memtable.as_ref(),
                    flush_output.reservation,
                ) {
                    Ok(()) => {
                        event_loop.wake_write_stall_waiters();
                        RuntimeResponse::Ok { request_id }
                    }
                    Err(error) => RuntimeResponse::Error { request_id, error },
                }
            }
            Err(error) => RuntimeResponse::Error { request_id, error },
        };

        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }

    #[cfg(test)]
    pub(super) fn flush_complete(
        event_loop: &mut EventLoop,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        sst_name: &str,
        sequence: u64,
    ) -> HandleOutcome {
        let resp = match event_loop.publish_flushed_sst(cf_id, sst_name, sequence, None, None) {
            Ok(()) => {
                event_loop.wake_write_stall_waiters();
                RuntimeResponse::Ok { request_id }
            }
            Err(error) => RuntimeResponse::Error { request_id, error },
        };
        event_loop.respond(request_id, resp);
        HandleOutcome::Continue
    }
}
