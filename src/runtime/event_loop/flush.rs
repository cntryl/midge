use super::{EventLoop, HandleOutcome};
use crate::runtime::RuntimeResponse;

#[derive(Debug, Clone, Copy)]
pub(super) struct FlushBarrierWaiter {
    pub(super) request_id: u64,
    pub(super) frontier: u64,
}

pub(super) struct FlushCoordinator;

impl FlushCoordinator {
    pub(super) fn flush_memtable(
        event_loop: &mut EventLoop,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    ) -> HandleOutcome {
        if event_loop.fencing.ddl_authority_ambiguous {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Fenced(
                        "DDL authority is ambiguous; refusing flush until reconciliation".into(),
                    ),
                },
            );
            return HandleOutcome::Continue;
        }
        if let Err(error) = event_loop.check_lease_health() {
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            return HandleOutcome::Continue;
        }
        if event_loop.state.is_memory_mode() {
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
            return HandleOutcome::Continue;
        }
        if event_loop.state.get_cf(cf_id).is_none() {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::InvalidArgument(format!(
                        "column family {cf_id} does not exist"
                    )),
                },
            );
            return HandleOutcome::Continue;
        }

        let frontier = event_loop.state.sequence;
        if let Err(error) = event_loop.freeze_active_memtable(cf_id) {
            event_loop.respond(request_id, RuntimeResponse::Error { request_id, error });
            return HandleOutcome::Continue;
        }
        if event_loop.flush_frontier_satisfied(cf_id, frontier) {
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
            return HandleOutcome::Continue;
        }

        event_loop
            .flush_barrier_waiters
            .entry(cf_id)
            .or_default()
            .push(FlushBarrierWaiter {
                request_id,
                frontier,
            });
        event_loop.schedule_next_flush_worker();
        event_loop.drain_inline_flush_worker();
        HandleOutcome::Continue
    }

    #[cfg(test)]
    pub(super) fn flush_complete(
        event_loop: &mut EventLoop,
        request_id: u64,
        _cf_id: crate::types::ColumnFamilyId,
        _sst_name: &str,
        _sequence: u64,
    ) -> HandleOutcome {
        event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        HandleOutcome::Continue
    }
}

impl EventLoop {
    pub(super) fn wake_flush_waiters_for_terminal_fencing(&mut self) {
        if self.flush_barrier_waiters.is_empty() {
            return;
        }
        let Err(error) = self.check_lease_health() else {
            return;
        };
        // Only answer callers. Accepted immutable data, worker reservations
        // and the publication owner stay retained until their normal outcome.
        for waiters in std::mem::take(&mut self.flush_barrier_waiters).into_values() {
            for waiter in waiters {
                self.respond(
                    waiter.request_id,
                    RuntimeResponse::Error {
                        request_id: waiter.request_id,
                        error: error.replay(),
                    },
                );
            }
        }
    }
}
