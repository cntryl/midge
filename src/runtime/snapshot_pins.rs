//! Concurrent snapshot pin registry shared by API threads and the runtime.

use crate::types::SnapshotPinSnapshot;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct SnapshotPin {
    sequence: u64,
    created_at: Instant,
    ref_count: usize,
    pinned_ssts: Arc<HashSet<String>>,
}

#[derive(Default)]
pub(crate) struct SnapshotPinRegistry {
    active: DashMap<u64, SnapshotPin>,
    /// Serializes snapshot capture/pin registration with GC's pin sampling.
    acquisition: RwLock<()>,
    /// Lower bounds of snapshots being captured but not yet registered, keyed
    /// by acquisition token. The compaction horizon must not pass them.
    inflight_floors: DashMap<u64, u64>,
    next_acquisition: std::sync::atomic::AtomicU64,
    /// Set while GC is deferred behind an in-flight acquisition, so the
    /// acquisition's owner can request a retry once it has finished.
    gc_deferred: AtomicBool,
}

pub(crate) struct SnapshotAcquisitionGuard<'a> {
    registry: &'a SnapshotPinRegistry,
    token: u64,
    _guard: RwLockReadGuard<'a, ()>,
}

impl Drop for SnapshotAcquisitionGuard<'_> {
    fn drop(&mut self) {
        self.registry.inflight_floors.remove(&self.token);
    }
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
        self.register_while_acquired(
            snapshot_id,
            sequence,
            Arc::new(pinned_sst_names.into_iter().collect()),
        )
    }

    pub(crate) fn register_while_acquired(
        &self,
        snapshot_id: u64,
        sequence: u64,
        pinned_ssts: Arc<HashSet<String>>,
    ) -> bool {
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

    /// Start capturing a snapshot whose sequence will be at least
    /// `sequence_floor` (the committed sequence read before capture). Until
    /// the guard drops, `oldest_sequence` treats that floor as a live reader,
    /// closing the capture-to-registration window for the compaction horizon.
    pub(crate) fn begin_acquisition(&self, sequence_floor: u64) -> SnapshotAcquisitionGuard<'_> {
        let token = self
            .next_acquisition
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inflight_floors.insert(token, sequence_floor);
        SnapshotAcquisitionGuard {
            registry: self,
            token,
            _guard: self.acquisition.read(),
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.active.len()
    }

    #[cfg(test)]
    pub(crate) fn pinned_sst_names(&self, max_lifetime: Duration) -> HashSet<String> {
        // GC must exclude the capture-to-registration window. Snapshot
        // acquisition takes a shared guard, so the exclusive guard here waits
        // until every in-flight capture has published its pin.
        let guard: RwLockWriteGuard<'_, ()> = self.acquisition.write();
        self.sample_pinned_sst_names(&guard, max_lifetime)
    }

    /// Like [`Self::pinned_sst_names`], but returns `None` instead of waiting
    /// while a snapshot acquisition is in progress. The event loop must use
    /// this: an acquiring API thread can itself be waiting on the event loop.
    ///
    /// A `None` result leaves [`Self::take_gc_deferred`] set. The flag is
    /// raised before the try-lock, so an acquisition that ends concurrently
    /// still observes it after releasing its guard.
    pub(crate) fn try_pinned_sst_names(&self, max_lifetime: Duration) -> Option<HashSet<String>> {
        self.gc_deferred.store(true, Ordering::SeqCst);
        let guard = self.acquisition.try_write()?;
        self.gc_deferred.store(false, Ordering::SeqCst);
        Some(self.sample_pinned_sst_names(&guard, max_lifetime))
    }

    /// Return whether GC deferred behind an acquisition, clearing the flag.
    /// Call it only after releasing the acquisition guard.
    pub(crate) fn take_gc_deferred(&self) -> bool {
        self.gc_deferred.swap(false, Ordering::SeqCst)
    }

    /// Pin sample for metrics. It skips the acquisition exclusion because a
    /// gauge can tolerate the capture-to-registration window, and the event
    /// loop must not wait on an acquiring API thread.
    pub(crate) fn observed_pinned_sst_count(&self) -> usize {
        let mut pinned = HashSet::new();
        for entry in &self.active {
            pinned.extend(entry.value().pinned_ssts.iter().cloned());
        }
        pinned.len()
    }

    fn sample_pinned_sst_names(
        &self,
        _exclusive: &RwLockWriteGuard<'_, ()>,
        max_lifetime: Duration,
    ) -> HashSet<String> {
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
        let pinned = self.active.iter().map(|entry| entry.value().sequence);
        let inflight = self.inflight_floors.iter().map(|entry| *entry.value());
        pinned.chain(inflight).min()
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
    fn should_hold_horizon_at_inflight_acquisition_floor_until_it_ends() {
        // Arrange: a transaction has captured (or is capturing) a snapshot at
        // or above sequence 5 but has not registered its pin yet.
        let registry = SnapshotPinRegistry::default();
        assert!(registry.register(1, 42, Vec::new()));

        // Act
        let acquisition = registry.begin_acquisition(5);
        let during = registry.oldest_sequence();
        drop(acquisition);
        let after = registry.oldest_sequence();

        // Assert
        assert_eq!(
            during,
            Some(5),
            "compaction must not drop versions an in-flight snapshot can still read"
        );
        assert_eq!(after, Some(42));
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

    #[test]
    fn should_clear_gc_deferral_when_pin_sample_succeeds() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        let acquisition = registry.begin_acquisition(0);
        assert!(registry
            .try_pinned_sst_names(Duration::from_mins(1))
            .is_none());
        drop(acquisition);

        // Act
        let sampled = registry.try_pinned_sst_names(Duration::from_mins(1));

        // Assert
        assert!(sampled.is_some());
        assert!(
            !registry.take_gc_deferred(),
            "a successful sample must not leave a stale retry request"
        );
    }
}
