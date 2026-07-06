//! Concurrent snapshot pin registry shared by API threads and the runtime.

use crate::types::SnapshotPinSnapshot;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::collections::HashSet;
use std::time::{Duration, Instant};

struct SnapshotPin {
    sequence: u64,
    created_at: Instant,
    ref_count: usize,
    pinned_ssts: HashSet<String>,
}

#[derive(Default)]
pub(crate) struct SnapshotPinRegistry {
    active: DashMap<u64, SnapshotPin>,
}

impl SnapshotPinRegistry {
    pub(crate) fn register(
        &self,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    ) -> bool {
        let pinned_ssts = pinned_sst_names.into_iter().collect::<HashSet<_>>();
        match self.active.entry(snapshot_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(SnapshotPin {
                    sequence,
                    created_at: Instant::now(),
                    ref_count: 1,
                    pinned_ssts,
                });
                true
            }
        }
    }

    pub(crate) fn unregister(&self, snapshot_id: u64) -> bool {
        self.active.remove(&snapshot_id).is_some()
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active.len()
    }

    pub(crate) fn pinned_sst_names(&self, max_lifetime: Duration) -> HashSet<String> {
        let now = Instant::now();
        let mut pinned = HashSet::new();

        for entry in &self.active {
            let snapshot_id = *entry.key();
            let snapshot = entry.value();
            let age = now.duration_since(snapshot.created_at);
            if age > max_lifetime {
                tracing::warn!(
                    snapshot_id,
                    age_secs = age.as_secs(),
                    max_secs = max_lifetime.as_secs(),
                    "Long-lived snapshot exceeds max lifetime (should be auto-closed)"
                );
            }
            pinned.extend(snapshot.pinned_ssts.iter().cloned());
        }

        pinned
    }

    pub(crate) fn oldest_sequence(&self) -> Option<u64> {
        self.active.iter().map(|entry| entry.value().sequence).min()
    }

    pub(crate) fn oldest_age_seconds(&self, now: Instant) -> Option<u64> {
        self.active
            .iter()
            .map(|entry| now.duration_since(entry.value().created_at).as_secs())
            .max()
    }

    pub(crate) fn count_timed_out(&self, max_lifetime: Duration) -> usize {
        let now = Instant::now();
        self.active
            .iter()
            .filter(|entry| now.duration_since(entry.value().created_at) > max_lifetime)
            .count()
    }

    pub(crate) fn evict_timed_out(&self, max_lifetime: Duration) -> usize {
        let now = Instant::now();
        let timed_out_ids = self
            .active
            .iter()
            .filter_map(|entry| {
                let snapshot = entry.value();
                (now.duration_since(snapshot.created_at) > max_lifetime).then_some(*entry.key())
            })
            .collect::<Vec<_>>();

        let mut evicted = 0usize;
        for snapshot_id in timed_out_ids {
            if self.active.remove(&snapshot_id).is_some() {
                evicted += 1;
                tracing::warn!(
                    snapshot_id,
                    max_secs = max_lifetime.as_secs(),
                    "Evicted timed-out snapshot"
                );
            }
        }
        evicted
    }

    pub(crate) fn snapshots(&self, now: Instant) -> Vec<SnapshotPinSnapshot> {
        let mut snapshots = self
            .active
            .iter()
            .map(|entry| {
                let snapshot = entry.value();
                SnapshotPinSnapshot {
                    snapshot_id: *entry.key(),
                    sequence: snapshot.sequence,
                    age_seconds: now.duration_since(snapshot.created_at).as_secs(),
                    ref_count: snapshot.ref_count,
                }
            })
            .collect::<Vec<_>>();
        snapshots.sort_by_key(|snapshot| snapshot.snapshot_id);
        snapshots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_track_pinned_sst_names_when_snapshot_pin_registered() {
        // Arrange
        let registry = SnapshotPinRegistry::default();

        // Act
        let registered = registry.register(7, 42, vec!["a.sst".to_string(), "b.sst".to_string()]);

        // Assert
        assert!(registered);
        assert_eq!(registry.active_count(), 1);
        assert_eq!(registry.oldest_sequence(), Some(42));
        let pinned = registry.pinned_sst_names(Duration::from_mins(1));
        assert!(pinned.contains("a.sst"));
        assert!(pinned.contains("b.sst"));
    }

    #[test]
    fn should_reject_duplicate_snapshot_pin_ids() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        assert!(registry.register(7, 42, Vec::new()));

        // Act
        let registered = registry.register(7, 43, Vec::new());

        // Assert
        assert!(!registered);
        assert_eq!(registry.oldest_sequence(), Some(42));
    }

    #[test]
    fn should_unregister_snapshot_pin() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        assert!(registry.register(7, 42, vec!["a.sst".to_string()]));

        // Act
        let removed = registry.unregister(7);

        // Assert
        assert!(removed);
        assert_eq!(registry.active_count(), 0);
        assert!(registry.pinned_sst_names(Duration::from_mins(1)).is_empty());
    }
}
