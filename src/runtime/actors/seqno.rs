//! Seqno Allocation Actor
//!
//! Manages centralized sequence number allocation.
//! All writes get their seqno from this actor, ensuring global ordering.
//! Also implements write stall detection based on memtable pressure.

use crate::common::MidgeResult;
use crate::runtime::{IntentLogEntry, RuntimeResponse, RuntimeState};

/// SeqnoAllocActor - allocates monotonic sequence numbers
pub struct SeqnoAllocActor;

impl Default for SeqnoAllocActor {
    fn default() -> Self {
        Self::new()
    }
}

impl SeqnoAllocActor {
    pub fn new() -> Self {
        Self
    }

    /// Allocate the next sequence number
    /// Checks for write stall conditions based on memtable pressure.
    /// Returns a new unique seqno and logs it in the intent log.
    pub fn alloc_seqno(
        state: &mut RuntimeState,
        cf_id: crate::engine::ColumnFamilyId,
    ) -> MidgeResult<(u64, RuntimeResponse)> {
        if state.should_hard_stall_writes(cf_id) {
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_write_stall_memory();
            }
            return Err(crate::common::MidgeError::WriteStall(
                "write stalled: memtable pressure or external backpressure".to_string(),
            ));
        }

        // Allocate new seqno
        state.sequence += 1;
        let seqno = state.sequence;

        // Log intent and persist
        state.append_intent(IntentLogEntry::SeqnoAllocated { seqno, cf_id })?;

        Ok((
            seqno,
            RuntimeResponse::SeqnoAllocated {
                seqno,
                request_id: 0,
            },
        ))
    }
}
