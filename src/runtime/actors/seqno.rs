//! Seqno Allocation Actor
//!
//! Manages centralized sequence number allocation.
//! All writes get their seqno from this actor, ensuring global ordering.
//! Also implements write stall detection based on memtable pressure.

use crate::common::MidgeResult;
use crate::runtime::{IntentLogEntry, RuntimeResponse, RuntimeState};
use crate::sst::Memtable;

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
        // Check if write is stalled due to explicit flag
        if state.write_stalled {
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_write_stall_memory();
            }
            return Err(crate::common::MidgeError::WriteStall(
                "write stalled: memtable full or compaction lagging".to_string(),
            ));
        }

        // Check memtable size pressure for this column family
        if let Some(cf_state) = state.get_cf(cf_id) {
            let memtable_size = cf_state.memtable.size_bytes();
            if memtable_size > state.memtable_flush_threshold {
                // Memtable is too large - signal write stall
                state.write_stalled = true;
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_write_stall_memory();
                }
                return Err(crate::common::MidgeError::WriteStall(format!(
                    "memtable full: {}MB > threshold {}MB",
                    memtable_size / (1024 * 1024),
                    state.memtable_flush_threshold / (1024 * 1024)
                )));
            }
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
