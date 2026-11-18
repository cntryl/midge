//! Opt-in amortized WAL sync floor (#904).
//!
//! With [`crate::config::AmortizedSyncFloor`] configured, a strict group's
//! completed fsync may be acknowledged before its ledger floor edit. The
//! deferred end is remembered here, keyed by the segment it belongs to, and
//! recorded exactly once a bound is reached, at the idle timer, or by any
//! later exact edit.

use super::{FloorEdit, WalActor};
use crate::common::MidgeResult;
use crate::config::AmortizedSyncFloor;
use crate::runtime::state::RuntimeState;
use crate::wal::format::HEADER_LEN;
use crate::wal::ledger::SegmentId;
use std::time::Instant;

/// The end of a completed, acknowledged fsync whose ledger edit was deferred.
///
/// `end_offset` is the receipt's end (captured before the fsync was issued),
/// never the writer's current position, so it never covers bytes an fsync has
/// not completed. It belongs to `segment_id` only: a value found on any other
/// open segment is stale and is discarded, never recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct UnrecordedFloor {
    pub(super) segment_id: SegmentId,
    pub(super) end_offset: u64,
    /// The oldest acknowledgement still waiting for a floor edit.
    pub(super) since: Instant,
}

impl WalActor {
    /// Record `end_offset` as the exact floor and forget any deferred end.
    ///
    /// Every exact edit is at least as new as a deferred end of the same
    /// segment (receipts only grow within a segment), and a deferred end of
    /// another segment is stale, so clearing is correct in both cases. A
    /// failed edit fences the WAL, which clears the same state.
    pub(super) fn record_exact_floor(
        &mut self,
        state: &mut RuntimeState,
        group: Option<&crate::lease::WriterEffectGroup>,
        end_offset: Option<u64>,
    ) -> MidgeResult<()> {
        let recorded = self.record_exact_sync_boundary(state, group, end_offset);
        self.unrecorded_floor = None;
        recorded
    }

    /// A strict group's completed fsync under the opt-in amortized floor.
    ///
    /// `end_offset` is the receipt's end captured before the fsync, so it is
    /// never beyond a completed fsync (C1). The edit is skipped while fewer
    /// than `max_unrecorded_bytes` follow the recorded floor and the oldest
    /// deferred acknowledgement is younger than `max_unrecorded_delay`.
    pub(super) fn persist_amortized_sync_boundary(
        &mut self,
        state: &mut RuntimeState,
        group: Option<&crate::lease::WriterEffectGroup>,
        end_offset: Option<u64>,
        policy: AmortizedSyncFloor,
    ) -> MidgeResult<FloorEdit> {
        let Some(end) = end_offset else {
            // Memory mode records nothing, as with an exact floor.
            return self
                .record_exact_floor(state, group, None)
                .map(|()| FloorEdit::Recorded);
        };
        let segment = state.wal.open_segment();
        let recorded = state
            .wal
            .history
            .ledger()
            .open_member()
            .durable_bytes
            .unwrap_or(HEADER_LEN as u64);
        // A deferred end of another segment is stale: drop it, keep nothing.
        let deferred = self
            .unrecorded_floor
            .take()
            .filter(|floor| floor.segment_id == segment);
        if end <= recorded {
            return Ok(FloorEdit::Deferred);
        }
        let since = deferred.map_or_else(Instant::now, |floor| floor.since);
        if end - recorded >= policy.max_unrecorded_bytes
            || since.elapsed() >= policy.max_unrecorded_delay
        {
            return self
                .record_exact_floor(state, group, Some(end))
                .map(|()| FloorEdit::Recorded);
        }
        self.unrecorded_floor = Some(UnrecordedFloor {
            segment_id: segment,
            end_offset: end,
            since,
        });
        Ok(FloorEdit::Deferred)
    }

    /// The deferred floor as `(segment, end offset)`, for tests.
    #[cfg(test)]
    pub(crate) fn unrecorded_floor_for_test(&self) -> Option<(SegmentId, u64)> {
        self.unrecorded_floor
            .map(|floor| (floor.segment_id, floor.end_offset))
    }

    /// Plant a deferred end, as if a segment change had failed to clear it.
    #[cfg(test)]
    pub(crate) fn plant_unrecorded_floor_for_test(&mut self, segment_id: SegmentId, end: u64) {
        self.unrecorded_floor = Some(UnrecordedFloor {
            segment_id,
            end_offset: end,
            since: Instant::now(),
        });
    }
}
