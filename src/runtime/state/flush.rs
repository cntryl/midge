//! Immutable flush lifecycle, retries, and write-pressure selection.

use super::{
    Arc, Duration, EventualFlush, FlushCandidate, FlushReason, HashSet, ImmutableFlush,
    ImmutableFlushPhase, RuntimeState, SkipListMemtable, INITIAL_FLUSH_RETRY_BACKOFF,
    LOCAL_EVENTUAL_FLUSH_TRIGGER_MULTIPLE, MAX_FLUSH_RETRY_BACKOFF,
};

impl RuntimeState {
    #[cfg(test)]
    pub(crate) fn track_new_immutable_flush(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        memtable: Arc<SkipListMemtable>,
        sequence: u64,
    ) -> Option<ImmutableFlush> {
        self.track_new_immutable_flush_for(
            cf_id,
            memtable,
            sequence,
            crate::metadata::accounting::Origin::Unclassified,
        )
    }

    pub(crate) fn track_new_immutable_flush_for(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        memtable: Arc<SkipListMemtable>,
        sequence: u64,
        origin: crate::metadata::accounting::Origin,
    ) -> Option<ImmutableFlush> {
        let flush_id = self.next_flush_id;
        self.next_flush_id = self.next_flush_id.checked_add(1)?;
        let cf_state = self.column_families.get_mut(&cf_id)?;
        cf_state.immutable_memtables.push(Arc::clone(&memtable));
        let flush = ImmutableFlush {
            flush_id,
            started_at: std::time::Instant::now(),
            accounting: super::FlushPublicationAccounting::new(origin),
            writer_epoch: self.writer_epoch,
            memtable,
            sst_name: None,
            sst_seq: None,
            sequence,
            phase: ImmutableFlushPhase::Queued,
            built: None,
            failures: 0,
            retry: crate::runtime::retry_schedule::RetrySchedule::new(INITIAL_FLUSH_RETRY_BACKOFF),
        };
        cf_state.immutable_flushes.push(flush.clone());
        self.flush_metrics.enqueued_total = self.flush_metrics.enqueued_total.saturating_add(1);
        Some(flush)
    }

    pub(crate) fn begin_next_immutable_flush(&mut self) -> Option<ImmutableFlush> {
        let (cf_id, flush_id) = self
            .column_families
            .iter()
            .flat_map(|(cf_id, cf)| {
                cf.immutable_flushes.iter().filter_map(move |flush| {
                    matches!(
                        flush.phase,
                        ImmutableFlushPhase::Queued | ImmutableFlushPhase::RetryPending
                    )
                    .then_some((*cf_id, flush.flush_id, flush.sequence))
                })
            })
            .min_by_key(|(cf_id, flush_id, sequence)| (*sequence, *flush_id, *cf_id))
            .map(|(cf_id, flush_id, _)| (cf_id, flush_id))?;
        let flush = self
            .column_families
            .get_mut(&cf_id)?
            .immutable_flushes
            .iter_mut()
            .find(|flush| flush.flush_id == flush_id)?;
        if flush.phase == ImmutableFlushPhase::RetryPending && !flush.retry.is_ready() {
            return None;
        }
        if flush.failures > 0 {
            self.flush_metrics.retries_total = self.flush_metrics.retries_total.saturating_add(1);
        }
        flush.phase = if flush.built.is_some() {
            ImmutableFlushPhase::Publishing
        } else {
            ImmutableFlushPhase::Building
        };
        Some(flush.clone())
    }

    pub(crate) fn immutable_flush_by_id(&self, flush_id: u64) -> Option<(u32, &ImmutableFlush)> {
        self.column_families.iter().find_map(|(cf_id, cf)| {
            cf.immutable_flushes
                .iter()
                .find(|flush| flush.flush_id == flush_id)
                .map(|flush| (*cf_id, flush))
        })
    }

    pub(crate) fn immutable_flush_by_id_mut(
        &mut self,
        flush_id: u64,
    ) -> Option<(u32, &mut ImmutableFlush)> {
        self.column_families.iter_mut().find_map(|(cf_id, cf)| {
            cf.immutable_flushes
                .iter_mut()
                .find(|flush| flush.flush_id == flush_id)
                .map(|flush| (*cf_id, flush))
        })
    }

    pub(crate) fn mark_immutable_flush_failed(&mut self, flush_id: u64) -> Option<Duration> {
        let (_, flush) = self.immutable_flush_by_id_mut(flush_id)?;

        if flush.phase == ImmutableFlushPhase::RetryPending {
            return flush.retry.remaining();
        }

        flush.failures = flush.failures.saturating_add(1);
        let multiplier = 1_u64 << flush.failures.saturating_sub(1).min(7);
        let delay = INITIAL_FLUSH_RETRY_BACKOFF
            .saturating_mul(u32::try_from(multiplier).unwrap_or(u32::MAX))
            .min(MAX_FLUSH_RETRY_BACKOFF);
        flush.phase = ImmutableFlushPhase::RetryPending;
        flush.retry.defer_for(delay);
        self.flush_metrics.failures_total = self.flush_metrics.failures_total.saturating_add(1);
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
        self.column_families
            .values()
            .flat_map(|cf_state| cf_state.immutable_flushes.iter())
            .filter(|flush| {
                matches!(
                    flush.phase,
                    ImmutableFlushPhase::Queued | ImmutableFlushPhase::RetryPending
                )
            })
            .min_by_key(|flush| (flush.sequence, flush.flush_id))
            .is_some_and(|flush| {
                flush.phase == ImmutableFlushPhase::Queued || flush.retry.is_ready()
            })
    }

    pub(crate) fn flush_retry_deadline_timeout(&self) -> Option<Duration> {
        self.column_families
            .values()
            .flat_map(|cf_state| cf_state.immutable_flushes.iter())
            .filter(|flush| flush.phase == ImmutableFlushPhase::RetryPending)
            .filter_map(|flush| flush.retry.remaining())
            .min()
    }

    #[cfg(test)]
    pub(crate) fn make_immutable_flush_retry_due(&mut self, cf_id: crate::types::ColumnFamilyId) {
        if let Some(cf_state) = self.column_families.get_mut(&cf_id) {
            for flush in &mut cf_state.immutable_flushes {
                if flush.phase == ImmutableFlushPhase::RetryPending {
                    flush.retry.mark_due();
                }
            }
        }
    }

    pub fn memtable_flush_trigger_bytes(&self) -> usize {
        self.limits
            .memtable_size_limit
            .min(self.limits.memtable_flush_threshold)
            .max(1)
    }

    pub fn is_immutable_memtable_queue_full(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        self.column_families.get(&cf_id).is_some_and(|cf_state| {
            cf_state.immutable_memtables.len() >= self.limits.max_immutable_memtables
        })
    }

    /// Whether the memtable bytes a flush can relieve reach the global hard
    /// limit. The active memtable of a family holding a partly applied unit
    /// is left out: that family rejects every new write, so the memtable no
    /// longer grows, and it cannot freeze until restart, so counting it would
    /// stall every other family for bytes no flush can release.
    pub fn is_total_memtable_hard_limit_exceeded(&self) -> bool {
        let held = self
            .column_families
            .values()
            .filter(|cf| cf.partially_applied_commit().is_some())
            .map(|cf| cf.memtable.size_bytes())
            .fold(0_usize, usize::saturating_add);
        self.total_memtable_bytes.saturating_sub(held)
            >= self.limits.memtable_flush_threshold.saturating_mul(2)
    }

    /// Maximum number of published or reserved L0 generations for one column
    /// family. The extra slot is the active memtable generation.
    pub(crate) fn l0_hard_ceiling(&self) -> usize {
        self.limits
            .l0_compaction_trigger
            .saturating_add(self.limits.max_immutable_memtables)
            .saturating_add(1)
    }

    /// Published L0 files plus every in-memory generation that can still
    /// publish one L0 file. `max` is conservative if test or recovery state is
    /// temporarily between the paired immutable indexes.
    pub(crate) fn l0_slot_usage(&self, cf_id: crate::types::ColumnFamilyId) -> usize {
        let published = self
            .manifest
            .files
            .iter()
            .filter(|file| file.cf_id == cf_id && file.level == 0)
            .count();
        let Some(cf) = self.column_families.get(&cf_id) else {
            return published;
        };
        let immutable = cf.immutable_memtables.len().max(cf.immutable_flushes.len());
        published
            .saturating_add(immutable)
            .saturating_add(usize::from(cf.memtable.size_bytes() > 0))
    }

    pub(crate) fn has_critical_l0_debt(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        self.l0_slot_usage(cf_id) >= self.l0_hard_ceiling()
    }

    pub(crate) fn has_any_critical_l0_debt(&self) -> bool {
        self.column_families
            .keys()
            .any(|cf_id| self.has_critical_l0_debt(*cf_id))
    }

    /// Return true when admitting another transaction would require an L0 slot
    /// that does not exist. A below-threshold active memtable may continue to
    /// consume its already-reserved slot; the transaction that crosses the
    /// flush threshold is accepted, then the next transaction stalls.
    pub(crate) fn l0_write_slot_unavailable(&self, cf_id: crate::types::ColumnFamilyId) -> bool {
        let usage = self.l0_slot_usage(cf_id);
        let ceiling = self.l0_hard_ceiling();
        if usage < ceiling {
            return false;
        }
        if usage > ceiling {
            return true;
        }
        self.column_families.get(&cf_id).is_none_or(|cf| {
            cf.memtable.size_bytes() == 0
                || cf.memtable.size_bytes() >= self.memtable_flush_trigger_bytes()
        })
    }

    #[cfg(test)]
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
        if self.l0_write_slot_unavailable(cf_id) {
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
        self.column_families.keys().any(|cf_id| {
            self.l0_write_slot_unavailable(*cf_id) || self.is_immutable_memtable_queue_full(*cf_id)
        })
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
        self.mode.persistence.is_memory()
    }

    pub fn recovery_policy(&self) -> crate::config::RecoveryPolicy {
        self.recovery.policy
    }

    #[cfg(test)]
    pub(crate) fn set_recovery_policy_for_test(
        &mut self,
        recovery_policy: crate::config::RecoveryPolicy,
    ) {
        self.recovery.policy = recovery_policy;
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

    /// Record that a remote DDL authority switch could not be resolved.
    pub(crate) fn mark_ddl_authority_ambiguous(&mut self) {
        self.recovery.ddl_authority_ambiguous = true;
    }

    /// Take the DDL ambiguity recorded since the last call.
    pub(crate) fn take_ddl_authority_ambiguous(&mut self) -> bool {
        std::mem::take(&mut self.recovery.ddl_authority_ambiguous)
    }

    pub fn compaction_enabled(&self) -> bool {
        self.limits.compaction_config.enabled
    }

    pub fn set_compaction_enabled(&mut self, enabled: bool) {
        self.limits.compaction_config.enabled = enabled;
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
                    .open_segment()
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

    #[cfg(test)]
    pub(crate) fn next_flush_candidate(&self, rule: EventualFlush) -> Option<FlushCandidate> {
        self.next_flush_candidate_skipping(rule, &HashSet::new(), None)
    }

    pub(crate) fn next_flush_candidate_skipping(
        &self,
        rule: EventualFlush,
        attempted_cfs: &HashSet<crate::types::ColumnFamilyId>,
        oldest_shared: Option<&crate::wal::format::SealedFacts>,
    ) -> Option<FlushCandidate> {
        let retry_candidate = self
            .column_families
            .iter()
            .filter(|(cf_id, _)| !attempted_cfs.contains(cf_id))
            .filter(|(_, cf_state)| {
                cf_state.immutable_flushes.iter().any(|flush| {
                    flush.phase == ImmutableFlushPhase::RetryPending && flush.retry.is_ready()
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
            .filter(|(cf_id, _cf_state)| !attempted_cfs.contains(cf_id))
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

        // A family's gap: WAL segments (cloud) or WAL bytes (local) since its
        // active memtable started.
        let (reason, limit, gap_of): (_, _, fn(&Self, &super::ColumnFamilyState) -> u64) =
            match rule {
                #[cfg(test)]
                EventualFlush::Disabled => return None,
                EventualFlush::SegmentGap => (
                    FlushReason::WalSegmentGap,
                    self.limits.eventual_flush_segment_gap,
                    |state, cf| {
                        state
                            .wal
                            .open_segment()
                            .saturating_sub(cf.active_memtable_started_in_segment)
                    },
                ),
                EventualFlush::WalBytes => (
                    FlushReason::WalBytesGap,
                    u64::try_from(flush_threshold)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(LOCAL_EVENTUAL_FLUSH_TRIGGER_MULTIPLE),
                    |state, cf| {
                        state
                            .wal
                            .appended_bytes
                            .saturating_sub(cf.active_memtable_started_at_wal_bytes)
                    },
                ),
            };
        // Force the family preventing the oldest sealed member from retiring.
        // A newer family's larger gap cannot help advance this prefix.
        let pressured = self
            .column_families
            .values()
            .any(|cf| cf.memtable.size_bytes() > 0 && gap_of(self, cf) >= limit);
        if pressured {
            if let Some(cf_id) = self.oldest_wal_blocking_family(oldest_shared) {
                return self
                    .column_families
                    .get(&cf_id)
                    .filter(|cf| cf.memtable.size_bytes() > 0 && !attempted_cfs.contains(&cf_id))
                    .map(|_| FlushCandidate { cf_id, reason });
            }
        }
        self.column_families
            .iter()
            .filter(|(cf_id, _cf_state)| !attempted_cfs.contains(cf_id))
            .filter_map(|(cf_id, cf_state)| {
                if cf_state.memtable.size_bytes() == 0 {
                    return None;
                }
                let gap = gap_of(self, cf_state);
                (gap >= limit).then_some((*cf_id, gap))
            })
            .max_by_key(|(cf_id, gap)| (*gap, std::cmp::Reverse(*cf_id)))
            .map(|(cf_id, _)| FlushCandidate { cf_id, reason })
    }

    /// A family the local manifest has not flushed through the oldest WAL
    /// segment: the oldest published one when given, else the oldest sealed
    /// local member.
    pub(crate) fn oldest_wal_blocking_family(
        &self,
        oldest_shared: Option<&crate::wal::format::SealedFacts>,
    ) -> Option<crate::types::ColumnFamilyId> {
        let facts = oldest_shared.or_else(|| {
            self.wal
                .history
                .ledger()
                .member_facts()
                .next()
                .and_then(|member| member.sealed.as_ref())
        })?;
        facts.max_seq_per_cf.iter().find_map(|&(cf, sequence)| {
            let needs_flush = match self.manifest.flush_frontiers().get(cf) {
                Ok(crate::metadata::FamilyFrontier::Live { through_sequence }) => {
                    sequence > through_sequence
                }
                Ok(crate::metadata::FamilyFrontier::Dropped) => false,
                Err(_) => true,
            };
            needs_flush.then_some(cf)
        })
    }

    pub(crate) fn reinitialize_active_memtable_segment_tracking(&mut self) {
        for cf_state in self.column_families.values_mut() {
            if cf_state.memtable.size_bytes() > 0 {
                cf_state.active_memtable_started_at_wal_bytes = 0;
                // Replay reconstructs table contents without exact per-generation
                // source provenance. The next writable segment cannot describe
                // older recovered records, so retain every earlier segment until
                // this generation is durably published.
                cf_state.active_memtable_started_in_segment = 1;
            }
        }
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;

    #[test]
    fn should_not_stall_for_active_memtable_held_by_partly_applied_family() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let mut state = RuntimeState::new(directory.path().to_path_buf(), true);
        state.limits.memtable_flush_threshold = 1024;
        let other = state.create_cf("other".into()).unwrap();
        let cf = state.get_cf_mut(0).unwrap();
        cf.memtable
            .put_with_seq(b"partial".to_vec(), vec![1; 4096], 1, None)
            .unwrap();
        cf.mark_partially_applied(2);
        state.recompute_total_memtable_bytes();

        // Act
        let stalled = state.should_hard_stall_writes(other);

        // Assert
        assert!(state.total_memtable_bytes >= 2048);
        assert!(!stalled);
    }

    #[test]
    fn should_stall_when_flushable_bytes_reach_hard_limit_beside_partly_applied_family() {
        // Arrange: the exemption covers only the marked family's bytes; an
        // unmarked family at twice the flush threshold still stalls.
        let directory = tempfile::tempdir().unwrap();
        let mut state = RuntimeState::new(directory.path().to_path_buf(), true);
        state.limits.memtable_flush_threshold = 1024;
        let other = state.create_cf("other".into()).unwrap();
        let cf = state.get_cf_mut(0).unwrap();
        cf.memtable
            .put_with_seq(b"partial".to_vec(), vec![1; 4096], 1, None)
            .unwrap();
        cf.mark_partially_applied(2);
        state
            .get_cf_mut(other)
            .unwrap()
            .memtable
            .put_with_seq(b"flushable".to_vec(), vec![2; 2048], 3, None)
            .unwrap();
        state.recompute_total_memtable_bytes();

        // Act
        let stalled = state.should_hard_stall_writes(other);

        // Assert
        assert!(stalled);
    }

    #[test]
    fn should_force_the_family_blocking_the_oldest_segment_before_a_larger_gap() {
        // Arrange
        let directory = tempfile::tempdir().unwrap();
        let mut state = RuntimeState::new(directory.path().to_path_buf(), true);
        state.limits.memtable_flush_threshold = 1024 * 1024;
        state.limits.eventual_flush_segment_gap = 1;
        let blocker = state.create_cf("blocker".into()).unwrap();
        state.wal.history = crate::runtime::wal_history::WalHistory::in_memory(0);
        let facts = crate::wal::format::SealedFacts {
            link: crate::wal::format::ChainLink {
                id: 1,
                last_seq: 3,
                digest: [1; 16],
            },
            record_count: 1,
            data_len: 160,
            max_seq_per_cf: vec![(blocker, 3)],
        };
        let rotation = state
            .wal
            .history
            .prepare_rotation(facts.clone(), 4)
            .unwrap();
        state.wal.history.record_rotation(rotation).unwrap();
        crate::runtime::wal_history::fixture::rotate_to(&mut state.wal.history, 10).unwrap();
        for id in [0, blocker] {
            state
                .get_cf(id)
                .unwrap()
                .memtable
                .put_with_seq(b"key".to_vec(), b"value".to_vec(), 3, None)
                .unwrap();
        }
        state
            .get_cf_mut(0)
            .unwrap()
            .active_memtable_started_in_segment = 1;
        state
            .get_cf_mut(blocker)
            .unwrap()
            .active_memtable_started_in_segment = 9;

        // Act
        let candidate = state
            .next_flush_candidate(EventualFlush::SegmentGap)
            .unwrap();

        // Assert
        assert_eq!(candidate.cf_id, blocker);
        assert_eq!(candidate.reason, FlushReason::WalSegmentGap);
        // Act: cloud acknowledgement has already retired the local cache.
        state.wal.history.retire_below(10).unwrap();
        let cloud_candidate = state
            .next_flush_candidate_skipping(EventualFlush::SegmentGap, &HashSet::new(), Some(&facts))
            .unwrap();

        // Assert: the persisted remote summary still targets the same blocker.
        assert_eq!(cloud_candidate.cf_id, blocker);
    }
}
