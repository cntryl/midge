//! Opt-in amortized WAL sync floor (#904).
//!
//! With [`crate::config::AmortizedSyncFloor`] configured, a strict group's
//! completed fsync may be acknowledged before its ledger floor edit. The
//! deferred end is remembered here, keyed by the segment it belongs to, and
//! recorded exactly once a bound is reached, at the idle timer, or by any
//! later exact edit.

use super::{FloorEdit, WalActor};
use cntryl_midge_core::common::MidgeResult;
use crate::config::AmortizedSyncFloor;
use crate::runtime::state::RuntimeState;
use cntryl_midge_durability::wal::format::HEADER_LEN;
use cntryl_midge_durability::wal::ledger::SegmentId;
use std::time::{Duration, Instant};

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
        group: Option<&cntryl_midge_durability::lease::WriterEffectGroup>,
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
        group: Option<&cntryl_midge_durability::lease::WriterEffectGroup>,
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

    /// How long until the oldest deferred acknowledgement reaches its delay
    /// bound; `None` when nothing is deferred or the WAL cannot take an edit.
    pub(crate) fn floor_deadline_timeout(&self) -> Option<Duration> {
        let policy = self.sync_floor?;
        let deferred = self.unrecorded_floor?;
        self.is_open().then(|| {
            policy
                .max_unrecorded_delay
                .saturating_sub(deferred.since.elapsed())
        })
    }

    pub(crate) fn has_unrecorded_floor(&self) -> bool {
        self.unrecorded_floor.is_some()
    }

    /// Record the deferred floor outside a strict group: when its delay bound
    /// has passed, or at once with `force`.
    ///
    /// The edit is the deferred receipt's end, never the writer's position.
    /// The caller must hold the loop-owned WAL still: no strict group in
    /// flight (its writer root is parked) and no sync or seal ticket, which is
    /// what makes a standalone fenced edit as safe as the sequence
    /// reservation's. A deferred end of another segment is dropped unrecorded.
    ///
    /// # Errors
    ///
    /// The lease or ledger error; the WAL is then fenced, as for any failed
    /// floor edit.
    pub(crate) fn record_deferred_floor(
        &mut self,
        state: &mut RuntimeState,
        force: bool,
    ) -> MidgeResult<()> {
        let (Some(policy), Some(deferred)) = (self.sync_floor, self.unrecorded_floor) else {
            return Ok(());
        };
        if !self.is_open() {
            return Ok(());
        }
        if deferred.segment_id != state.wal.open_segment() {
            self.unrecorded_floor = None;
            return Ok(());
        }
        if !force && deferred.since.elapsed() < policy.max_unrecorded_delay {
            return Ok(());
        }
        self.ensure_lease_validity()?;
        self.record_exact_floor(state, None, Some(deferred.end_offset))
    }

    /// The deferred floor as `(segment, end offset)`, for tests.
    #[cfg(all(test, feature = "failpoints"))]
    pub(crate) fn unrecorded_floor_for_test(&self) -> Option<(SegmentId, u64)> {
        self.unrecorded_floor
            .map(|floor| (floor.segment_id, floor.end_offset))
    }

    /// Plant a deferred end, as if a segment change had failed to clear it.
    #[cfg(all(test, feature = "failpoints"))]
    pub(crate) fn plant_unrecorded_floor_for_test(&mut self, segment_id: SegmentId, end: u64) {
        self.unrecorded_floor = Some(UnrecordedFloor {
            segment_id,
            end_offset: end,
            since: Instant::now(),
        });
    }
}
