//! Concurrent snapshot pin registry shared by API threads and the runtime.

use crate::types::SnapshotPinSnapshot;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Number of owners of a registered pin. A pin is registered by exactly one
/// transaction and unregistered when that transaction drops it, so the count
/// reported to observability is always one; the registry keeps no per-pin
/// counter to increment.
const PIN_REF_COUNT: usize = 1;

struct SnapshotPin {
    sequence: u64,
    created_at: Instant,
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
    /// Test-only count of per-SST-name membership probes performed while
    /// releasing pins, so regression tests can assert that releasing a pin
    /// whose generation is still held stays independent of the pinned count.
    #[cfg(test)]
    membership_probes: std::sync::atomic::AtomicUsize,
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
        let Some((_, pin)) = self.active.remove(&snapshot_id) else {
            return (false, false);
        };
        (true, self.released_last_pin(&pin.pinned_ssts))
    }

    /// Whether dropping `released` left any of its SSTs unpinned.
    ///
    /// Snapshots taken against the same SST generation share one
    /// `Arc<HashSet<String>>`, so the common concurrent case is decided by a
    /// pointer comparison: if another live pin holds the very same generation,
    /// it pins exactly the same names and nothing was released. Only when
    /// every survivor holds a different generation does this fall back to
    /// per-name probes, and then it probes each distinct surviving generation
    /// once rather than re-walking the registry for every name.
    fn released_last_pin(&self, released: &Arc<HashSet<String>>) -> bool {
        if released.is_empty() {
            return false;
        }

        let mut survivors: Vec<Arc<HashSet<String>>> = Vec::new();
        for active in &self.active {
            let generation = &active.value().pinned_ssts;
            if Arc::ptr_eq(generation, released) {
                return false;
            }
            if generation.is_empty() || Self::contains_generation(&survivors, generation) {
                continue;
            }
            survivors.push(Arc::clone(generation));
        }

        if survivors.is_empty() {
            self.record_membership_probes(0);
            return true;
        }

        let mut probes = 0usize;
        let released_last_pin = released.iter().any(|sst_name| {
            probes += 1;
            survivors
                .iter()
                .all(|generation| !generation.contains(sst_name))
        });
        self.record_membership_probes(probes);
        released_last_pin
    }

    fn contains_generation(
        generations: &[Arc<HashSet<String>>],
        candidate: &Arc<HashSet<String>>,
    ) -> bool {
        generations
            .iter()
            .any(|generation| Arc::ptr_eq(generation, candidate))
    }

    #[cfg_attr(not(test), allow(unused_variables, clippy::unused_self))]
    fn record_membership_probes(&self, probes: usize) {
        #[cfg(test)]
        self.membership_probes.fetch_add(probes, Ordering::Relaxed);
    }

    #[cfg(test)]
    fn membership_probes(&self) -> usize {
        self.membership_probes.load(Ordering::Relaxed)
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
        let generations = self.sample_pinned_generations(&guard, max_lifetime);
        drop(guard);
        Self::union_generations(&generations)
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
        // Only the generation Arcs are collected under the exclusive guard;
        // materializing the names (which blocks every begin_tx and every pin
        // release while held) happens after it is dropped.
        let generations = self.sample_pinned_generations(&guard, max_lifetime);
        drop(guard);
        Some(Self::union_generations(&generations))
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
        let mut generations: Vec<Arc<HashSet<String>>> = Vec::new();
        for entry in &self.active {
            let generation = &entry.value().pinned_ssts;
            if Self::contains_generation(&generations, generation) {
                continue;
            }
            generations.push(Arc::clone(generation));
        }
        match generations.as_slice() {
            [] => 0,
            [only] => only.len(),
            _ => Self::union_generations(&generations).len(),
        }
    }

    /// Collect the distinct SST generations held by live pins, warning about
    /// pins older than `max_lifetime`. Deduplicating by `Arc` pointer keeps the
    /// work proportional to the number of distinct generations rather than to
    /// active pins times pinned SSTs, and no names are cloned here, so the
    /// caller's exclusive guard is held for as short a time as possible.
    fn sample_pinned_generations(
        &self,
        _exclusive: &RwLockWriteGuard<'_, ()>,
        max_lifetime: Duration,
    ) -> Vec<Arc<HashSet<String>>> {
        let now = Instant::now();
        let mut generations: Vec<Arc<HashSet<String>>> = Vec::new();

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
            if Self::contains_generation(&generations, &snapshot.pinned_ssts) {
                continue;
            }
            generations.push(Arc::clone(&snapshot.pinned_ssts));
        }

        generations
    }

    /// Flatten distinct generations into the set of pinned SST names. GC must
    /// see every name any live pin holds, so this is a union: a name missing
    /// here would let GC delete a file a reader still needs.
    fn union_generations(generations: &[Arc<HashSet<String>>]) -> HashSet<String> {
        match generations {
            [] => HashSet::new(),
            [only] => (**only).clone(),
            _ => {
                let mut pinned = HashSet::new();
                for generation in generations {
                    pinned.extend(generation.iter().cloned());
                }
                pinned
            }
        }
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
                    ref_count: PIN_REF_COUNT,
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

    fn generation(names: &[&str]) -> Arc<HashSet<String>> {
        Arc::new(names.iter().map(|name| (*name).to_string()).collect())
    }

    #[test]
    fn should_release_pin_in_constant_time_when_other_pin_shares_generation() {
        // Arrange: two snapshots captured against the same SST generation hold
        // the same Arc, the normal concurrent-reader case.
        let registry = SnapshotPinRegistry::default();
        let shared: Arc<HashSet<String>> = Arc::new(
            (0..100_000u32)
                .map(|index| format!("{index:06}.sst"))
                .collect(),
        );
        {
            let _guard = registry.acquisition.write();
            assert!(registry.register_while_acquired(7, 42, Arc::clone(&shared)));
            assert!(registry.register_while_acquired(8, 43, Arc::clone(&shared)));
        }

        // Act
        let release = registry.unregister_with_gc_hint(7);

        // Assert
        assert_eq!(
            release,
            (true, false),
            "a generation another pin still holds releases nothing"
        );
        assert_eq!(
            registry.membership_probes(),
            0,
            "a shared generation must be decided by pointer identity, not by probing every name"
        );
    }

    #[test]
    fn should_report_released_ssts_when_surviving_generations_differ() {
        // Arrange: distinct generations, so pointer identity cannot decide it.
        let registry = SnapshotPinRegistry::default();
        {
            let _guard = registry.acquisition.write();
            assert!(registry.register_while_acquired(7, 42, generation(&["a.sst", "b.sst"])));
            assert!(registry.register_while_acquired(8, 43, generation(&["b.sst"])));
        }

        // Act
        let release = registry.unregister_with_gc_hint(7);

        // Assert
        assert_eq!(
            release,
            (true, true),
            "a.sst is no longer pinned by any live snapshot"
        );
        assert!(
            registry.membership_probes() > 0,
            "distinct generations must still fall back to per-name probes"
        );
    }

    #[test]
    fn should_not_probe_names_when_released_pin_holds_no_ssts() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        assert!(registry.register(7, 42, Vec::new()));
        assert!(registry.register(8, 43, vec!["a.sst".to_string()]));

        // Act
        let release = registry.unregister_with_gc_hint(7);

        // Assert
        assert_eq!(release, (true, false));
        assert_eq!(registry.membership_probes(), 0);
    }

    #[test]
    fn should_sample_union_of_names_when_pins_hold_distinct_generations() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        {
            let _guard = registry.acquisition.write();
            assert!(registry.register_while_acquired(7, 42, generation(&["a.sst", "b.sst"])));
            assert!(registry.register_while_acquired(8, 43, generation(&["b.sst", "c.sst"])));
            let shared = generation(&["d.sst"]);
            assert!(registry.register_while_acquired(9, 44, Arc::clone(&shared)));
            assert!(registry.register_while_acquired(10, 45, shared));
        }

        // Act
        let pinned = registry.pinned_sst_names(Duration::from_mins(1));

        // Assert: deduplicating generations must never drop a pinned name.
        assert_eq!(
            pinned,
            ["a.sst", "b.sst", "c.sst", "d.sst"]
                .iter()
                .map(|name| (*name).to_string())
                .collect::<HashSet<String>>()
        );
        assert_eq!(registry.observed_pinned_sst_count(), 4);
    }

    #[test]
    fn should_count_shared_generation_once_when_many_pins_hold_it() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        let shared = generation(&["a.sst", "b.sst"]);
        {
            let _guard = registry.acquisition.write();
            for snapshot_id in 0..8 {
                assert!(registry.register_while_acquired(snapshot_id, 42, Arc::clone(&shared)));
            }
        }

        // Act
        let observed = registry.observed_pinned_sst_count();

        // Assert
        assert_eq!(observed, 2);
        assert_eq!(
            registry.pinned_sst_names(Duration::from_mins(1)),
            (*shared).clone()
        );
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
