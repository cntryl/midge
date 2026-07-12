//! Immutable flush lifecycle, retries, and write-pressure selection.

use super::{
    Arc, Duration, FlushCandidate, FlushReason, HashSet, ImmutableFlush, ImmutableFlushPhase,
    Instant, Memtable, RuntimeState, SkipListMemtable, INITIAL_FLUSH_RETRY_BACKOFF,
    MAX_FLUSH_RETRY_BACKOFF,
};

impl RuntimeState {
    pub(crate) fn track_new_immutable_flush(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        memtable: Arc<SkipListMemtable>,
        sst_name: String,
        sst_seq: u64,
        sequence: u64,
    ) -> Option<ImmutableFlush> {
        let cf_state = self.column_families.get_mut(&cf_id)?;
        cf_state.immutable_memtables.push(Arc::clone(&memtable));
        let flush = ImmutableFlush {
            memtable,
            sst_name,
            sst_seq,
            sequence,
            phase: ImmutableFlushPhase::Flushing,
            failures: 0,
            retry_at: Instant::now(),
        };
        cf_state.immutable_flushes.push(flush.clone());
        Some(flush)
    }

    pub(crate) fn begin_pending_immutable_flush(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        retry_due_only: bool,
    ) -> Option<ImmutableFlush> {
        let now = Instant::now();
        let flush = self
            .column_families
            .get_mut(&cf_id)?
            .immutable_flushes
            .iter_mut()
            .find(|flush| {
                flush.phase == ImmutableFlushPhase::Pending
                    && (!retry_due_only || flush.retry_at <= now)
            })?;
        flush.phase = ImmutableFlushPhase::Flushing;
        Some(flush.clone())
    }

    pub(crate) fn mark_immutable_flush_failed(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        memtable: &Arc<SkipListMemtable>,
    ) -> Option<Duration> {
        let flush = self
            .column_families
            .get_mut(&cf_id)?
            .immutable_flushes
            .iter_mut()
            .find(|flush| Arc::ptr_eq(&flush.memtable, memtable))?;

        if flush.phase == ImmutableFlushPhase::Pending {
            return Some(flush.retry_at.saturating_duration_since(Instant::now()));
        }

        flush.failures = flush.failures.saturating_add(1);
        let multiplier = 1_u64 << flush.failures.saturating_sub(1).min(7);
        let delay = INITIAL_FLUSH_RETRY_BACKOFF
            .saturating_mul(u32::try_from(multiplier).unwrap_or(u32::MAX))
            .min(MAX_FLUSH_RETRY_BACKOFF);
        flush.phase = ImmutableFlushPhase::Pending;
        flush.retry_at = Instant::now() + delay;
        Some(delay)
    }

    pub(crate) fn complete_immutable_flush(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        memtable: &Arc<SkipListMemtable>,
    ) -> Option<usize> {
        let cf_state = self.column_families.get_mut(&cf_id)?;
        let flush_index = cf_state
            .immutable_flushes
            .iter()
            .position(|flush| Arc::ptr_eq(&flush.memtable, memtable))?;
        let memtable_index = cf_state
            .immutable_memtables
            .iter()
            .position(|candidate| Arc::ptr_eq(candidate, memtable))?;
        cf_state.immutable_flushes.remove(flush_index);
        Some(
            cf_state
                .immutable_memtables
                .remove(memtable_index)
                .size_bytes(),
        )
    }

    pub(crate) fn has_due_immutable_flush(&self) -> bool {
        let now = Instant::now();
        self.column_families.values().any(|cf_state| {
            cf_state
                .immutable_flushes
                .iter()
                .any(|flush| flush.phase == ImmutableFlushPhase::Pending && flush.retry_at <= now)
        })
    }

    pub(crate) fn flush_retry_deadline_timeout(&self) -> Option<Duration> {
        let now = Instant::now();
        self.column_families
            .values()
            .flat_map(|cf_state| cf_state.immutable_flushes.iter())
            .filter(|flush| flush.phase == ImmutableFlushPhase::Pending)
            .map(|flush| flush.retry_at.saturating_duration_since(now))
            .min()
    }

    #[cfg(test)]
    pub(crate) fn make_immutable_flush_retry_due(&mut self, cf_id: crate::types::ColumnFamilyId) {
        if let Some(cf_state) = self.column_families.get_mut(&cf_id) {
            for flush in &mut cf_state.immutable_flushes {
                if flush.phase == ImmutableFlushPhase::Pending {
                    flush.retry_at = Instant::now();
                }
            }
        }
    }

    pub fn memtable_flush_trigger_bytes(&self) -> usize {
        self.memtable_size_limit
            .min(self.memtable_flush_threshold)
            .max(1)
    }

    pub fn is_immutable_memtable_queue_full(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        self.column_families.get(&cf_id).is_some_and(|cf_state| {
            cf_state.immutable_memtables.len() >= self.max_immutable_memtables
        })
    }

    pub fn is_total_memtable_hard_limit_exceeded(&self) -> bool {
        self.total_memtable_bytes >= self.memtable_flush_threshold.saturating_mul(2)
    }

    pub(crate) fn recompute_total_memtable_bytes(&mut self) {
        self.total_memtable_bytes = self
            .column_families
            .values()
            .map(|cf_state| {
                cf_state.memtable.size_bytes()
                    + cf_state
                        .immutable_memtables
                        .iter()
                        .map(|memtable| memtable.size_bytes())
                        .sum::<usize>()
            })
            .sum();
    }

    /// Check whether writes must be rejected until runtime pressure clears.
    ///
    /// This intentionally excludes active memtable size: active memtable pressure
    /// should trigger flush, not block the flush that would relieve it.
    pub fn should_hard_stall_writes(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        if self.write_pressure.stalled {
            return true;
        }
        if self.is_immutable_memtable_queue_full(cf_id) {
            return true;
        }
        self.is_total_memtable_hard_limit_exceeded()
    }

    pub fn has_any_hard_write_stall(&self) -> bool {
        if self.write_pressure.stalled || self.is_total_memtable_hard_limit_exceeded() {
            return true;
        }
        self.column_families
            .keys()
            .any(|cf_id| self.is_immutable_memtable_queue_full(*cf_id))
    }

    /// Compatibility wrapper for write-admission callsites.
    pub fn should_stall_writes(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        self.should_hard_stall_writes(cf_id)
    }

    #[cfg(test)]
    pub fn is_read_only(&self) -> bool {
        self.mode.read_only
    }

    #[cfg(test)]
    pub fn set_read_only(&mut self, read_only: bool) {
        self.mode.read_only = read_only;
    }

    pub fn is_memory_mode(&self) -> bool {
        self.mode.memory_mode
    }

    pub fn recovery_policy(&self) -> crate::config::RecoveryPolicy {
        self.recovery.policy
    }

    pub fn opened_in_salvage_mode(&self) -> bool {
        self.recovery.opened_in_salvage_mode
    }

    pub fn mark_opened_in_salvage_mode(&mut self) {
        self.recovery.opened_in_salvage_mode = true;
    }

    pub fn persistence_anomaly_detected(&self) -> bool {
        self.recovery.persistence_anomaly_detected
    }

    pub fn mark_persistence_anomaly(&mut self) {
        self.recovery.persistence_anomaly_detected = true;
    }

    pub fn compaction_enabled(&self) -> bool {
        self.compaction_config.enabled
    }

    pub fn set_compaction_enabled(&mut self, enabled: bool) {
        self.compaction_config.enabled = enabled;
    }

    pub fn write_stalled(&self) -> bool {
        self.write_pressure.stalled
    }

    pub fn set_write_stalled(&mut self, stalled: bool) {
        self.write_pressure.stalled = stalled;
    }

    pub(crate) fn active_memtable_wal_segment_gap(
        &self,
        cf_id: crate::types::ColumnFamilyId,
    ) -> u64 {
        self.column_families.get(&cf_id).map_or(0, |cf_state| {
            if cf_state.memtable.size_bytes() == 0 {
                0
            } else {
                self.wal
                    .current_segment_id
                    .saturating_sub(cf_state.active_memtable_started_in_segment)
            }
        })
    }

    pub(crate) fn max_memtable_wal_segment_gap(&self) -> u64 {
        self.column_families
            .keys()
            .map(|cf_id| self.active_memtable_wal_segment_gap(*cf_id))
            .max()
            .unwrap_or(0)
    }

    pub(crate) fn cloud_wal_recovery_floor_segment(&self) -> Option<u64> {
        if self
            .column_families
            .values()
            .any(|cf_state| !cf_state.immutable_memtables.is_empty())
        {
            return None;
        }

        let oldest_active_segment = self
            .column_families
            .values()
            .filter(|cf_state| cf_state.memtable.size_bytes() > 0)
            .map(|cf_state| cf_state.active_memtable_started_in_segment)
            .min();

        Some(oldest_active_segment.unwrap_or(self.wal.current_segment_id))
    }

    #[cfg(test)]
    pub(crate) fn next_flush_candidate(
        &self,
        cloud_segment_gap_enabled: bool,
    ) -> Option<FlushCandidate> {
        self.next_flush_candidate_skipping(cloud_segment_gap_enabled, &HashSet::new())
    }

    pub(crate) fn next_flush_candidate_skipping(
        &self,
        cloud_segment_gap_enabled: bool,
        attempted_cfs: &HashSet<crate::types::ColumnFamilyId>,
    ) -> Option<FlushCandidate> {
        let now = Instant::now();
        let retry_candidate = self
            .column_families
            .iter()
            .filter(|(cf_id, _)| !attempted_cfs.contains(cf_id))
            .filter(|(_, cf_state)| {
                cf_state.immutable_flushes.iter().any(|flush| {
                    flush.phase == ImmutableFlushPhase::Pending && flush.retry_at <= now
                })
            })
            .map(|(cf_id, _)| *cf_id)
            .min();

        if let Some(cf_id) = retry_candidate {
            return Some(FlushCandidate {
                cf_id,
                reason: FlushReason::PendingImmutable,
            });
        }

        let flush_threshold = self.memtable_flush_trigger_bytes();

        let size_candidate = self
            .column_families
            .iter()
            .filter(|(cf_id, cf_state)| {
                !attempted_cfs.contains(cf_id) && cf_state.immutable_flushes.is_empty()
            })
            .filter_map(|(cf_id, cf_state)| {
                let size = cf_state.memtable.size_bytes();
                (size >= flush_threshold).then_some((*cf_id, size))
            })
            .max_by_key(|(cf_id, size)| (*size, std::cmp::Reverse(*cf_id)));

        if let Some((cf_id, _)) = size_candidate {
            return Some(FlushCandidate {
                cf_id,
                reason: FlushReason::SizeThreshold,
            });
        }

        if !cloud_segment_gap_enabled {
            return None;
        }

        self.column_families
            .iter()
            .filter(|(cf_id, cf_state)| {
                !attempted_cfs.contains(cf_id) && cf_state.immutable_flushes.is_empty()
            })
            .filter_map(|(cf_id, cf_state)| {
                if cf_state.memtable.size_bytes() == 0 {
                    return None;
                }

                let gap = self
                    .wal
                    .current_segment_id
                    .saturating_sub(cf_state.active_memtable_started_in_segment);
                (gap >= self.cloud_eventual_flush_segment_gap).then_some((*cf_id, gap))
            })
            .max_by_key(|(cf_id, gap)| (*gap, std::cmp::Reverse(*cf_id)))
            .map(|(cf_id, _)| FlushCandidate {
                cf_id,
                reason: FlushReason::CloudSegmentGap,
            })
    }

    pub(crate) fn reinitialize_active_memtable_segment_tracking(&mut self) {
        let current_segment_id = self.wal.current_segment_id;
        for cf_state in self.column_families.values_mut() {
            if cf_state.memtable.size_bytes() > 0 {
                cf_state.active_memtable_started_in_segment = current_segment_id;
            }
        }
    }
}
