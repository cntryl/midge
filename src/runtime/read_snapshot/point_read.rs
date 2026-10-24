//! Point reads walk sources newest-first and stop at the first group that
//! decides the key (#903).
//!
//! Level order (SST-10) guarantees that every source older than a deciding one
//! holds only older versions of the key, including range tombstones, so the
//! walk may stop as soon as a group yields a point state or a covering range
//! tombstone. A group is one memtable, one L0 file whose sequence range is
//! strictly ordered against its neighbours, or one whole deeper level: files
//! that cannot be ordered against each other are visited together so the
//! equal-sequence conflict check still sees both.

use super::{Arc, FileMeta, KeyState, MidgeResult, ReadSnapshot, SkipListMemtable, SstStateReader};
use crate::sst::read_path_metrics::SstReadObserver as _;

/// Whether a walk records read heat and read-path diagnostics. Conflict checks
/// walk the same sources without counting as reads.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Observe {
    Reads,
    Nothing,
}

/// Whether a walk stops at the first deciding group or visits every source.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Extent {
    FirstHit,
    /// Only the audit walks every source.
    #[cfg(any(test, feature = "internal-testing"))]
    Full,
}

/// Value-free point state used by detached commit validation. Expiration is
/// interpreted at final admission, so a TTL crossing during I/O stays exact.
#[derive(Clone, Copy)]
pub(crate) struct ValidationPointState {
    pub(crate) sequence: Option<u64>,
    pub(crate) value_expiration: Option<Option<u64>>,
}

impl ValidationPointState {
    pub(crate) fn exists_at(self, read_time_millis: u64) -> bool {
        self.value_expiration.is_some_and(|expiration| {
            !crate::common::time::is_expired_at(expiration, read_time_millis)
        })
    }
}

/// Point-read state accumulated over the sources visited so far.
pub(super) struct PointReadWalk {
    snapshot_seq: u64,
    observe: Observe,
    best_state: Option<KeyState>,
    /// Only the highest covering tombstone matters, so none is copied.
    covering_tombstone_seq: Option<u64>,
    pub(super) ssts_touched: u64,
    pub(super) l0_ssts_touched: u64,
    pub(super) blocks_read: u64,
}

impl PointReadWalk {
    fn new(snapshot_seq: u64, observe: Observe) -> Self {
        Self {
            snapshot_seq,
            observe,
            best_state: None,
            covering_tombstone_seq: None,
            ssts_touched: 0,
            l0_ssts_touched: 0,
            blocks_read: 0,
        }
    }

    /// A point state or a covering range tombstone decides the key: every
    /// older source holds only older versions.
    fn decided(&self) -> bool {
        self.best_state.is_some() || self.covering_tombstone_seq.is_some()
    }

    /// The newest sequence touching the key among the visited sources.
    pub(super) fn decided_sequence(&self) -> Option<u64> {
        let state_seq = self
            .best_state
            .as_ref()
            .and_then(ReadSnapshot::state_sequence);
        state_seq.max(self.covering_tombstone_seq)
    }

    fn visit_memtable(&mut self, memtable: &SkipListMemtable, key: &[u8]) -> MidgeResult<()> {
        let state = memtable.get_raw_key_state_at(key, self.snapshot_seq);
        ReadSnapshot::merge_best_state(&mut self.best_state, state)?;
        self.covering_tombstone_seq = self
            .covering_tombstone_seq
            .max(memtable.max_covering_tombstone_seq(key, self.snapshot_seq));
        Ok(())
    }

    fn visit_sst(
        &mut self,
        snapshot: &ReadSnapshot,
        file_meta: &FileMeta,
        key: &[u8],
    ) -> MidgeResult<()> {
        if self.observe == Observe::Reads {
            file_meta.record_read();
            snapshot
                .diagnostics
                .sst_metrics()
                .record_candidate_sst_file_checked();
        }
        let reader = snapshot.sst_reader(file_meta)?;
        let (state, read_stats) = reader.get_raw_state_at_with_stats(key, self.snapshot_seq)?;
        if read_stats.sst_touched {
            self.ssts_touched = self.ssts_touched.saturating_add(1);
            if file_meta.level == 0 {
                self.l0_ssts_touched = self.l0_ssts_touched.saturating_add(1);
            }
        }
        self.blocks_read = self.blocks_read.saturating_add(read_stats.blocks_read);
        ReadSnapshot::merge_best_state(&mut self.best_state, state)?;

        if self.observe == Observe::Reads {
            snapshot
                .diagnostics
                .sst_metrics()
                .record_range_tombstone_scan();
        }
        self.covering_tombstone_seq = self
            .covering_tombstone_seq
            .max(reader.max_covering_tombstone_seq(key, self.snapshot_seq));
        Ok(())
    }

    /// The visible value, or `None` when the newest version is a tombstone,
    /// is covered by a range tombstone, or has expired at `read_time_millis`.
    pub(super) fn resolve(self, read_time_millis: u64) -> Option<bytes::Bytes> {
        let state = self.best_state?;
        let state_seq = ReadSnapshot::state_sequence(&state).unwrap_or(0);
        if self
            .covering_tombstone_seq
            .is_some_and(|tombstone_seq| tombstone_seq >= state_seq)
        {
            return None;
        }
        match state {
            KeyState::Value(value, _, exp, _)
                if !crate::common::time::is_expired_at(exp, read_time_millis) =>
            {
                Some(value)
            }
            _ => None,
        }
    }
}

/// End (exclusive) of the group that starts at `candidates[start]`.
///
/// `point_candidates` yields L0 newest-first, then each deeper level with its
/// fallback bucket. Deeper levels are one group each: their files are
/// key-disjoint when indexed, and a fallback file may overlap any of them. An
/// L0 group closes only when every L0 file after it is strictly older than
/// everything in it, judged by recorded sequence ranges rather than by the
/// view's sort order; if any L0 file lacks a sequence range, all of L0 is one
/// group, so unordered files are always judged together.
pub(super) fn point_group_end(candidates: &[Arc<FileMeta>], start: usize) -> usize {
    let first = &candidates[start];
    let level_end = candidates[start..]
        .iter()
        .position(|file| file.level != first.level)
        .map_or(candidates.len(), |offset| start + offset);
    if first.level != 0 {
        return level_end;
    }
    let l0 = &candidates[start..level_end];
    if l0
        .iter()
        .any(|file| file.smallest_seq.is_none() || file.largest_seq.is_none())
    {
        return level_end;
    }
    let mut group_oldest = u64::MAX;
    for (offset, file) in l0.iter().enumerate() {
        group_oldest = group_oldest.min(file.smallest_seq.expect("checked above"));
        let rest_is_older = l0[offset + 1..]
            .iter()
            .all(|later| later.largest_seq.expect("checked above") < group_oldest);
        if rest_is_older {
            return start + offset + 1;
        }
    }
    level_end
}

impl ReadSnapshot {
    pub(crate) fn validation_point_state(
        &self,
        key: &[u8],
        sequence: u64,
    ) -> MidgeResult<ValidationPointState> {
        let walk = self.walk_point(key, sequence, Observe::Nothing, Extent::FirstHit)?;
        #[cfg(any(test, feature = "internal-testing"))]
        self.audit_point_walk(key, sequence, &walk)?;
        let latest = walk.decided_sequence().filter(|sequence| *sequence > 0);
        let value_expiration = match &walk.best_state {
            Some(KeyState::Value(_, sequence, expiration, _))
                if walk
                    .covering_tombstone_seq
                    .is_none_or(|cover| cover < *sequence) =>
            {
                Some(*expiration)
            }
            _ => None,
        };
        Ok(ValidationPointState {
            sequence: latest,
            value_expiration,
        })
    }

    /// Visit sources newest-first: the active memtable, immutables newest
    /// first, then the SST candidates group by group.
    pub(super) fn walk_point(
        &self,
        key: &[u8],
        snapshot_seq: u64,
        observe: Observe,
        extent: Extent,
    ) -> MidgeResult<PointReadWalk> {
        let mut walk = PointReadWalk::new(snapshot_seq, observe);
        let stop = |walk: &PointReadWalk| extent == Extent::FirstHit && walk.decided();

        walk.visit_memtable(&self.memtable, key)?;
        for imm in self.immutable_memtables.iter().rev() {
            if stop(&walk) {
                return Ok(walk);
            }
            walk.visit_memtable(imm, key)?;
        }
        if self.memory_mode || stop(&walk) {
            return Ok(walk);
        }

        // Complete manifest bounds drive indexed selection. Legacy bounds stay
        // in the view's fallback bucket and are still opened conservatively.
        let candidates = self.sst_view.point_candidates(key);
        let mut start = 0;
        while start < candidates.len() && !stop(&walk) {
            let end = point_group_end(&candidates, start);
            for file_meta in &candidates[start..end] {
                walk.visit_sst(self, file_meta, key)?;
            }
            start = end;
        }
        Ok(walk)
    }

    /// Under test, re-walk every source and check that nothing older than the
    /// deciding group holds a newer version: the early return relies on level
    /// order (SST-10), and a violation anywhere would return stale data. A
    /// conflicting equal-sequence version in an older group surfaces here as
    /// the merge's `Corruption` error; production detects that only within a
    /// group.
    #[cfg(any(test, feature = "internal-testing"))]
    pub(super) fn audit_point_walk(
        &self,
        key: &[u8],
        snapshot_seq: u64,
        early: &PointReadWalk,
    ) -> MidgeResult<()> {
        if !audit::enabled() {
            return Ok(());
        }
        let full = self.walk_point(key, snapshot_seq, Observe::Nothing, Extent::Full)?;
        assert_eq!(
            full.decided_sequence(),
            early.decided_sequence(),
            "an older source holds a newer version of {key:?}: level order (SST-10) is violated"
        );
        Ok(())
    }
}

#[cfg(any(test, feature = "internal-testing"))]
pub(crate) mod audit {
    use std::sync::atomic::{AtomicBool, Ordering};

    /// On for the lib suite; integration tests and benches opt in.
    static ENABLED: AtomicBool = AtomicBool::new(cfg!(test));

    pub(super) fn enabled() -> bool {
        ENABLED.load(Ordering::Relaxed)
    }

    /// Make every point read in this process re-walk all sources and assert
    /// level order. Benchmarks leave it off so the early return is measured.
    pub fn set_point_read_level_order_audit(enabled: bool) {
        ENABLED.store(enabled, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::point_group_end;
    use crate::metadata::FileMeta;
    use std::sync::Arc;

    fn file(level: u32, seqs: Option<(u64, u64)>) -> Arc<FileMeta> {
        Arc::new(FileMeta {
            name: format!("{level}-{seqs:?}.sst"),
            level,
            smallest_seq: seqs.map(|(first, _)| first),
            largest_seq: seqs.map(|(_, last)| last),
            ..FileMeta::default()
        })
    }

    #[test]
    fn should_make_each_l0_file_its_own_group_given_disjoint_sequence_ranges_when_grouped() {
        // Arrange: three flushes newest-first, then two L1 files.
        let candidates = vec![
            file(0, Some((300, 399))),
            file(0, Some((200, 299))),
            file(0, Some((100, 199))),
            file(1, Some((1, 99))),
            file(1, Some((1, 99))),
        ];

        // Act
        let ends: Vec<usize> = [0, 1, 2, 3]
            .into_iter()
            .map(|start| point_group_end(&candidates, start))
            .collect();

        // Assert: one group per L0 file; the whole level for L1.
        assert_eq!(ends, vec![1, 2, 3, 5]);
    }

    #[test]
    fn should_keep_unordered_l0_files_together_given_overlapping_or_unknown_sequences_when_grouped()
    {
        // Arrange: two files share sequence 5; a file without sequences
        // follows and absorbs the rest of L0.
        let candidates = vec![
            file(0, Some((5, 5))),
            file(0, Some((5, 5))),
            file(0, Some((1, 4))),
            file(0, None),
            file(0, Some((0, 0))),
            file(2, Some((0, 0))),
        ];

        // Act
        let first = point_group_end(&candidates, 0);

        // Assert: one file without a sequence range makes all of L0 one group.
        assert_eq!(first, 5);
    }

    #[test]
    fn should_keep_equal_sequence_files_together_given_shared_sequence_when_grouped() {
        // Arrange: two files hold sequence 5; an ordered older file follows.
        let candidates = vec![
            file(0, Some((5, 5))),
            file(0, Some((5, 5))),
            file(0, Some((1, 4))),
        ];

        // Act
        let first = point_group_end(&candidates, 0);
        let second = point_group_end(&candidates, first);

        // Assert
        assert_eq!(first, 2);
        assert_eq!(second, 3);
    }

    #[test]
    fn should_not_close_a_group_given_unknown_sequence_file_sorted_last_when_grouped() {
        // Arrange: the view sorts a file without a sequence range after every
        // ordered file; it may hold anything, so the newest file must not form
        // a group of its own.
        let candidates = vec![
            file(0, Some((300, 399))),
            file(0, Some((200, 299))),
            file(0, None),
            file(1, Some((1, 99))),
        ];

        // Act
        let end = point_group_end(&candidates, 0);

        // Assert
        assert_eq!(end, 3);
    }
}
