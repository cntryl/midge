//! Snapshot pins, range-delete retention, and dropped-CF reclamation.

use super::{HashSet, MidgeResult, RecentDeleteRange, RuntimeState, MAX_RECENT_DELETE_RANGES};

impl RuntimeState {
    /// Register a new snapshot to prevent SST garbage collection
    /// while the snapshot is active and performing range scans.
    ///
    /// Returns true if snapshot was registered successfully.
    /// Returns false if `snapshot_id` already exists (duplicate registration).
    #[cfg(test)]
    pub fn register_snapshot(
        &mut self,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    ) -> bool {
        if !self
            .snapshot_pins
            .register(snapshot_id, sequence, pinned_sst_names)
        {
            tracing::warn!(snapshot_id, "Attempted to register duplicate snapshot ID");
            return false;
        }

        self.prune_recent_delete_ranges_by_snapshot_horizon();

        tracing::trace!(snapshot_id, sequence, "Snapshot registered for SST pinning");

        true
    }

    /// Unregister a snapshot, allowing SSTs referenced by it to be garbage collected.
    #[cfg(test)]
    pub fn unregister_snapshot(&mut self, snapshot_id: u64) {
        if self.snapshot_pins.unregister(snapshot_id) {
            self.prune_recent_delete_ranges_by_snapshot_horizon();
            tracing::trace!(snapshot_id, "Snapshot unregistered; SSTs eligible for GC");
        }
    }

    /// Get list of SSTs referenced by active snapshots.
    /// These SSTs must NOT be deleted during garbage collection.
    pub fn get_pinned_sst_names(&self) -> HashSet<String> {
        self.snapshot_pins
            .pinned_sst_names(self.snapshots.max_snapshot_lifetime)
    }

    /// Warn about snapshots that exceeded max lifetime and return the number observed.
    ///
    /// Timed-out snapshots remain pinned until the owning transaction unregisters.
    /// Retaining data is safer than allowing compaction or GC to invalidate a live
    /// transaction snapshot.
    pub fn warn_timed_out_snapshots(&self) -> usize {
        self.snapshot_pins
            .warn_timed_out(self.snapshots.max_snapshot_lifetime)
    }

    /// Return the oldest active snapshot sequence, if any snapshots are pinned.
    pub fn oldest_active_snapshot_sequence(&self) -> Option<u64> {
        self.snapshot_pins.oldest_sequence()
    }

    /// Durably remove SSTs belonging to dropped column families once no
    /// active snapshot can observe the pre-drop frontier.  The returned names
    /// are safe for the GC actor to delete only after the caller publishes the
    /// updated manifest to any remote authority.
    pub fn reclaim_dropped_column_families(&mut self) -> MidgeResult<Vec<String>> {
        let candidates = self
            .manifest
            .reclaimable_column_family_files(self.oldest_active_snapshot_sequence());
        let edits = candidates
            .iter()
            .map(
                |(cf_id, names)| crate::metadata::ManifestEdit::ReclaimColumnFamily {
                    id: *cf_id,
                    names: names.clone(),
                },
            )
            .collect::<Vec<_>>();
        if !self.is_memory_mode() && !edits.is_empty() {
            crate::metadata::append_edit_batch(&self.db_path, &edits)?;
        }
        let mut reclaimed_names = Vec::new();
        for ((_, names), edit) in candidates.into_iter().zip(edits) {
            self.manifest.apply_edit(&edit);
            reclaimed_names.extend(names);
        }
        Ok(reclaimed_names)
    }

    pub fn record_delete_range(
        &mut self,
        cf_id: crate::types::ColumnFamilyId,
        start_key: &[u8],
        end_key: &[u8],
        sequence: u64,
    ) {
        self.recent_delete_ranges.push(RecentDeleteRange {
            cf_id,
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
            sequence,
        });

        let overflow = self
            .recent_delete_ranges
            .len()
            .saturating_sub(MAX_RECENT_DELETE_RANGES);
        if overflow > 0 {
            self.recent_delete_ranges.drain(0..overflow);
        }

        self.prune_recent_delete_ranges_by_snapshot_horizon();
    }

    pub(super) fn prune_recent_delete_ranges_by_snapshot_horizon(&mut self) {
        let Some(oldest_snapshot_seq) = self.oldest_active_snapshot_sequence() else {
            self.recent_delete_ranges.clear();
            return;
        };

        self.recent_delete_ranges
            .retain(|entry| entry.sequence > oldest_snapshot_seq);
    }

    pub fn latest_covering_delete_range_sequence(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
    ) -> Option<u64> {
        self.recent_delete_ranges
            .iter()
            .rev()
            .find(|entry| {
                entry.cf_id == cf_id
                    && key >= entry.start_key.as_slice()
                    && key < entry.end_key.as_slice()
            })
            .map(|entry| entry.sequence)
    }

    pub fn latest_overlapping_delete_range_sequence(
        &self,
        cf_id: crate::types::ColumnFamilyId,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Option<u64> {
        self.recent_delete_ranges
            .iter()
            .rev()
            .find(|entry| {
                entry.cf_id == cf_id
                    && entry.start_key.as_slice() < end_key
                    && entry.end_key.as_slice() > start_key
            })
            .map(|entry| entry.sequence)
    }
}
