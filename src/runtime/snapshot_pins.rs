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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum PinId {
    Transaction(u64),
    Validation(u64),
}

impl PinId {
    fn number(self) -> u64 {
        match self {
            Self::Transaction(id) | Self::Validation(id) => id,
        }
    }
}

/// An internal read job owns its source pin through its final storage read.
pub(crate) struct OwnedSnapshotPin {
    registry: Arc<SnapshotPinRegistry>,
    id: u64,
}

impl Drop for OwnedSnapshotPin {
    fn drop(&mut self) {
        let _guard = self.registry.acquisition.read();
        if let Some((_, pin)) = self.registry.active.remove(&PinId::Validation(self.id)) {
            self.registry.release_generation(&pin.pinned_ssts);
        }
        self.registry.gc_deferred.store(true, Ordering::SeqCst);
    }
}

struct SnapshotPin {
    sequence: u64,
    created_at: Instant,
    pinned_ssts: Arc<HashSet<String>>,
}

struct GenerationPins {
    names: Arc<HashSet<String>>,
    pins: usize,
}

#[derive(Default)]
pub(crate) struct SnapshotPinRegistry {
    active: DashMap<PinId, SnapshotPin>,
    /// A generation owns its Arc until its last registration is removed, so
    /// its pointer identity cannot be reused while this entry exists.
    generations: DashMap<usize, GenerationPins>,
    /// Counts distinct live generations, not snapshots, protecting each name.
    file_generations: DashMap<String, usize>,
    next_validation: std::sync::atomic::AtomicU64,
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
        match self.active.entry(PinId::Transaction(snapshot_id)) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                self.retain_generation(&pinned_ssts);
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
        let Some((_, pin)) = self.active.remove(&PinId::Transaction(snapshot_id)) else {
            return (false, false);
        };
        (true, self.release_generation(&pin.pinned_ssts))
    }

    fn retain_generation(&self, names: &Arc<HashSet<String>>) {
        let identity = Arc::as_ptr(names) as usize;
        match self.generations.entry(identity) {
            Entry::Occupied(mut entry) => entry.get_mut().pins += 1,
            Entry::Vacant(entry) => {
                for name in names.iter() {
                    *self.file_generations.entry(name.clone()).or_insert(0) += 1;
                }
                entry.insert(GenerationPins {
                    names: Arc::clone(names),
                    pins: 1,
                });
            }
        }
    }

    /// Normal unpin touches one generation counter, independent of the number
    /// of active snapshots. Only its last pin visits that generation's names.
    /// A zero per-name count proves all generations released that name; GC's
    /// actual protection sample still comes from the authoritative active map.
    fn release_generation(&self, names: &Arc<HashSet<String>>) -> bool {
        let identity = Arc::as_ptr(names) as usize;
        let Entry::Occupied(mut entry) = self.generations.entry(identity) else {
            return !names.is_empty(); // conservative retry hint, never deletion authority
        };
        if entry.get().pins > 1 {
            entry.get_mut().pins -= 1;
            return false;
        }
        let generation = entry.remove();
        let mut released_any = false;
        let other_generations = !self.generations.is_empty();
        for name in generation.names.iter() {
            if let Entry::Occupied(mut count) = self.file_generations.entry(name.clone()) {
                if *count.get() > 1 {
                    *count.get_mut() -= 1;
                } else {
                    count.remove();
                    released_any = true;
                }
            }
        }
        self.record_membership_probes(if other_generations {
            generation.names.len()
        } else {
            0
        });
        released_any
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

    /// Register an internally captured source view in its own ID domain.
    /// The caller must hold capture-to-registration exclusion for this registry.
    pub(crate) fn pin_sources_while_acquiring(
        self: &Arc<Self>,
        sequence: u64,
        pinned_ssts: Arc<HashSet<String>>,
        acquisition: &SnapshotAcquisitionGuard<'_>,
    ) -> crate::common::MidgeResult<OwnedSnapshotPin> {
        if !std::ptr::eq(self.as_ref(), acquisition.registry) {
            return Err(crate::common::MidgeError::Internal(
                "validation pin belongs to a different snapshot registry".into(),
            ));
        }
        let id = self
            .next_validation
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                crate::common::MidgeError::ResourceLimit(
                    "validation pin identity space exhausted".into(),
                )
            })?;
        self.retain_generation(&pinned_ssts);
        self.active.insert(
            PinId::Validation(id),
            SnapshotPin {
                sequence,
                created_at: Instant::now(),
                pinned_ssts,
            },
        );
        Ok(OwnedSnapshotPin {
            registry: Arc::clone(self),
            id,
        })
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
            let snapshot_id = entry.key().number();
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
            let snapshot_id = entry.key().number();
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
                    snapshot_id: entry.key().number(),
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
    fn should_defer_gc_during_capture_then_observe_the_registered_pin() {
        // Arrange: hold the real acquisition guard across a scheduled GC turn.
        for sequence in 1..=256 {
            let registry = SnapshotPinRegistry::default();
            let (captured_tx, captured_rx) = std::sync::mpsc::channel();
            let (register_tx, register_rx) = std::sync::mpsc::channel();

            // Act
            std::thread::scope(|scope| {
                let reader_registry = &registry;
                let reader = scope.spawn(move || {
                    let acquisition = reader_registry.begin_acquisition(sequence);
                    captured_tx.send(()).unwrap();
                    register_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    assert!(reader_registry.register_while_acquired(
                        7,
                        sequence,
                        Arc::new(HashSet::from(["held.sst".to_string()])),
                    ));
                    drop(acquisition);
                });
                captured_rx.recv_timeout(Duration::from_secs(5)).unwrap();

                // Assert: GC cannot sample an empty pin set in the capture gap.
                assert!(registry
                    .try_pinned_sst_names(Duration::from_mins(1))
                    .is_none());
                assert_eq!(registry.oldest_sequence(), Some(sequence));
                register_tx.send(()).unwrap();
                reader.join().unwrap();
            });
            let pinned = registry
                .try_pinned_sst_names(Duration::from_mins(1))
                .unwrap();
            assert!(pinned.contains("held.sst"));
            assert!(registry.unregister(7));
            assert!(registry
                .try_pinned_sst_names(Duration::from_mins(1))
                .unwrap()
                .is_empty());
        }
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
    fn should_retain_shared_files_across_generations_when_ten_thousand_snapshots_unpin() {
        // Arrange
        let registry = SnapshotPinRegistry::default();
        let first = generation(&["shared.sst", "left.sst"]);
        let second = generation(&["shared.sst", "right.sst"]);
        {
            let _capture = registry.acquisition.write();
            for id in 0..10_000 {
                let names = if id < 5_000 { &first } else { &second };
                assert!(registry.register_while_acquired(id, 42, Arc::clone(names)));
            }
        }

        // Act
        for id in 0..4_999 {
            assert_eq!(registry.unregister_with_gc_hint(id), (true, false));
        }

        // Assert
        assert_eq!(registry.membership_probes(), 0);
        assert_eq!(registry.unregister_with_gc_hint(4_999), (true, true));
        assert_eq!(
            registry.pinned_sst_names(Duration::from_secs(60)),
            (*second).clone()
        );
        for id in 5_000..9_999 {
            assert_eq!(registry.unregister_with_gc_hint(id), (true, false));
        }
        assert_eq!(registry.unregister_with_gc_hint(9_999), (true, true));
        assert!(registry
            .pinned_sst_names(Duration::from_secs(60))
            .is_empty());
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

#[cfg(test)]
mod validation_pin_tests {
    use super::*;

    #[test]
    fn should_preserve_independent_pin_ownership_when_numeric_ids_match() {
        // Arrange
        let registry = Arc::new(SnapshotPinRegistry::default());
        assert!(registry.register(0, 7, vec!["transaction.sst".into()]));
        let acquisition = registry.begin_acquisition(3);

        // Act
        let internal = registry
            .pin_sources_while_acquiring(
                3,
                Arc::new(HashSet::from(["validation.sst".into()])),
                &acquisition,
            )
            .expect("register internal pin");
        drop(acquisition);

        // Assert
        assert_eq!(registry.oldest_sequence(), Some(3));
        assert_eq!(
            registry.pinned_sst_names(Duration::from_secs(30)),
            HashSet::from(["transaction.sst".into(), "validation.sst".into()])
        );
        assert!(registry.unregister(0));
        assert_eq!(registry.oldest_sequence(), Some(3));
        assert_eq!(
            registry.pinned_sst_names(Duration::from_secs(30)),
            HashSet::from(["validation.sst".into()])
        );
        drop(internal);
        assert_eq!(registry.oldest_sequence(), None);
        assert!(registry
            .pinned_sst_names(Duration::from_secs(30))
            .is_empty());
    }
}
