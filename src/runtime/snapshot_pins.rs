//! Concurrent snapshot pin registry shared by API threads and the runtime.

use crate::types::SnapshotPinSnapshot;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
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
    /// Serializes snapshot capture/pin registration with GC's pin sampling.
    acquisition: RwLock<()>,
}

pub(crate) struct SnapshotAcquisitionGuard<'a> {
    _guard: RwLockReadGuard<'a, ()>,
}

impl SnapshotPinRegistry {
    #[cfg(test)]
    pub(crate) fn register(
        &self,
        snapshot_id: u64,
        sequence: u64,
        pinned_sst_names: Vec<String>,
    ) -> bool {
        let _guard = self.acquisition.write();
        self.register_while_acquired(snapshot_id, sequence, pinned_sst_names)
    }

    pub(crate) fn register_while_acquired(
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

    #[cfg(test)]
    pub(crate) fn unregister(&self, snapshot_id: u64) -> bool {
        self.unregister_with_gc_hint(snapshot_id).0
    }

    pub(crate) fn unregister_with_gc_hint(&self, snapshot_id: u64) -> (bool, bool) {
        // A transaction drops this pin only after it has stopped using the
        // captured snapshot. Sharing the acquisition guard keeps normal read
        // transactions concurrent while a GC pass obtains the exclusive guard
        // before sampling pins and deleting obsolete files.
        let _guard = self.acquisition.read();
        self.active
            .remove(&snapshot_id)
            .map_or((false, false), |(_, pin)| {
                let released_last_pin = pin.pinned_ssts.iter().any(|sst_name| {
                    self.active
                        .iter()
                        .all(|active| !active.pinned_ssts.contains(sst_name))
                });
                (true, released_last_pin)
            })
    }

    pub(crate) fn begin_acquisition(&self) -> SnapshotAcquisitionGuard<'_> {
        SnapshotAcquisitionGuard {
            _guard: self.acquisition.read(),
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active.len()
    }

    pub(crate) fn pinned_sst_names(&self, max_lifetime: Duration) -> HashSet<String> {
        // GC must exclude the capture-to-registration window. Snapshot
        // acquisition takes a shared guard, so the exclusive guard here waits
        // until every in-flight capture has published its pin.
        let _guard: RwLockWriteGuard<'_, ()> = self.acquisition.write();
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
                    "Long-lived snapshot exceeds max lifetime; retaining pin until transaction closes"
                );
            }
            pinned.extend(snapshot.pinned_ssts.iter().cloned());
        }

        pinned
    }

    pub(crate) fn oldest_sequence(&self) -> Option<u64> {
        let _guard = self.acquisition.read();
        self.active.iter().map(|entry| entry.value().sequence).min()
    }

    pub(crate) fn oldest_age_seconds(&self, now: Instant) -> Option<u64> {
        self.active
            .iter()
            .map(|entry| now.duration_since(entry.value().created_at).as_secs())
            .max()
    }

    pub(crate) fn warn_timed_out(&self, max_lifetime: Duration) -> usize {
        let now = Instant::now();
        let mut timed_out = 0usize;
        for entry in &self.active {
            let snapshot_id = *entry.key();
            let snapshot = entry.value();
            let age = now.duration_since(snapshot.created_at);
            if age > max_lifetime {
                timed_out += 1;
                tracing::warn!(
                    snapshot_id,
                    age_secs = age.as_secs(),
                    max_secs = max_lifetime.as_secs(),
                    "Long-lived snapshot exceeds max lifetime; retaining pin until transaction closes"
                );
            }
        }
        timed_out
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

    #[test]
    fn should_only_request_gc_retry_when_released_snapshot_removes_last_sst_pin() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        assert!(registry.register(7, 42, Vec::new()));
        assert!(registry.register(8, 42, vec!["a.sst".to_string()]));
        assert!(registry.register(9, 43, vec!["a.sst".to_string()]));

        // Act
        let empty_release = registry.unregister_with_gc_hint(7);
        let shared_sst_release = registry.unregister_with_gc_hint(8);
        let last_sst_release = registry.unregister_with_gc_hint(9);

        // Assert
        assert_eq!(empty_release, (true, false));
        assert_eq!(shared_sst_release, (true, false));
        assert_eq!(last_sst_release, (true, true));
    }

    #[test]
    fn should_retain_timed_out_snapshot_pin_until_unregister() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        assert!(registry.register(7, 42, vec!["a.sst".to_string()]));
        std::thread::sleep(Duration::from_millis(1));

        // Act
        let timed_out = registry.warn_timed_out(Duration::from_millis(0));

        // Assert
        assert_eq!(timed_out, 1);
        assert_eq!(registry.active_count(), 1);
        assert_eq!(registry.oldest_sequence(), Some(42));
        let pinned = registry.pinned_sst_names(Duration::from_mins(1));
        assert!(pinned.contains("a.sst"));
    }
}
