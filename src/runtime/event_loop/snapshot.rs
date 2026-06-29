use super::{EventLoop, HandleOutcome};
use crate::runtime::RuntimeResponse;
use std::sync::Arc;

pub(super) struct SnapshotCoordinator;

impl SnapshotCoordinator {
    pub(super) fn capture(
        event_loop: &mut EventLoop,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        _sequence: u64,
    ) -> HandleOutcome {
        if let Some(snapshot) = event_loop.create_read_snapshot(cf_id) {
            event_loop.respond(
                request_id,
                RuntimeResponse::ReadSnapshot {
                    request_id,
                    snapshot: Arc::new(snapshot),
                },
            );
        } else {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::Internal(format!(
                        "Column family {} not found",
                        cf_id
                    )),
                },
            );
        }
        HandleOutcome::Continue
    }

    pub(super) fn begin_transaction(
        event_loop: &mut EventLoop,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
    ) -> HandleOutcome {
        let start_sequence = event_loop.state.sequence;
        let snapshot = event_loop.create_read_snapshot(cf_id).map(Arc::new);
        event_loop.respond(
            request_id,
            RuntimeResponse::BeginTransactionResult {
                request_id,
                start_sequence,
                snapshot,
            },
        );
        HandleOutcome::Continue
    }

    pub(super) fn register(
        event_loop: &mut EventLoop,
        request_id: u64,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    ) -> HandleOutcome {
        let inserted = event_loop
            .state
            .register_snapshot(snapshot_id, sequence, pinned_sst_names);
        if inserted {
            event_loop.respond(request_id, RuntimeResponse::Ok { request_id });
        } else {
            event_loop.respond(
                request_id,
                RuntimeResponse::Error {
                    request_id,
                    error: crate::common::MidgeError::InvalidArgument(format!(
                        "snapshot {} is already registered",
                        snapshot_id
                    )),
                },
            );
        }
        HandleOutcome::Continue
    }

    pub(super) fn unregister(event_loop: &mut EventLoop, snapshot_id: u64) -> HandleOutcome {
        event_loop.state.unregister_snapshot(snapshot_id);
        HandleOutcome::Continue
    }
}
