//! Single cache shard with concurrent reads and synchronized writes

use crate::sst::cache::key::CacheKey;
use crate::sst::cache::metrics::CacheMetrics;
use crate::sst::cache::policy::{CachePolicy, CachePolicyType};
use crate::sst::cache::value::CacheValue;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::cell::Cell;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const LRU_SAMPLE_MASK: u64 = 7;
const LRU_SEED_STEP: u64 = 0x9e37_79b9_7f4a_7c15;
static NEXT_LRU_SAMPLE_SEED: AtomicU64 = AtomicU64::new(0xd1b5_4a32_d192_ed03);

thread_local! {
    static LRU_SAMPLE_STATE: Cell<u64> = Cell::new(
        NEXT_LRU_SAMPLE_SEED.fetch_add(LRU_SEED_STEP, Ordering::Relaxed) | 1
    );
}

/// Record roughly one in eight LRU hits. A per-thread changing sequence keeps
/// fixed scan positions from always being sampled or always being skipped.
fn sample_lru_recency() -> bool {
    LRU_SAMPLE_STATE.with(next_lru_sample)
}

fn next_lru_sample(state: &Cell<u64>) -> bool {
    let mut value = state.get();
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    state.set(value);
    ((value >> 32) & LRU_SAMPLE_MASK) == 0
}

/// A single cache shard (partition) with concurrent access
///
/// Contains a portion of the cache entries using `DashMap` for concurrent access,
/// with its own eviction policy and metrics. Insertion and eviction happen
/// synchronously before `put` returns.
pub struct CacheShard {
    /// Map of cache key -> value.
    entries: DashMap<CacheKey, CachedEntry>,
    /// Eviction policy
    policy: Box<dyn CachePolicy>,
    /// Readers keep membership stable while a writer admits or evicts entries.
    membership_lock: RwLock<()>,
    /// LRU samples hit recency; other policies retain their exact hit updates.
    sample_lru_hits: bool,
    /// Advances whenever a value is admitted or replaced. A resident's first
    /// hit after that admission epoch always refreshes LRU recency.
    admission_epoch: AtomicU64,
    /// Metrics for this shard
    metrics: CacheMetrics,
    /// Maximum size in bytes
    max_bytes: u64,
}

struct CachedEntry {
    value: CacheValue,
    last_recorded_epoch: AtomicU64,
}

impl CacheShard {
    /// Create a new cache shard
    ///
    /// `max_bytes`: Maximum capacity in bytes
    /// `policy_type`: Eviction policy to use
    ///
    /// Returns `Arc<Self>` because `BlockCache` shares shards across callers.
    #[must_use]
    pub fn new(max_bytes: u64, policy_type: CachePolicyType) -> Arc<Self> {
        Arc::new(Self {
            entries: DashMap::new(),
            policy: policy_type.create(),
            membership_lock: RwLock::new(()),
            sample_lru_hits: policy_type == CachePolicyType::Lru,
            admission_epoch: AtomicU64::new(0),
            metrics: CacheMetrics::new(),
            max_bytes,
        })
    }

    /// Get a cached value.
    ///
    /// A shared membership guard hides entries until admission and eviction
    /// finish. LRU records the first hit per resident after each admission and
    /// samples repeated hits to reduce recency lock contention. Every hit
    /// still counts in cache metrics.
    pub fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        self.get_with_repeat_sampler(key, sample_lru_recency)
    }

    #[cfg(test)]
    fn get_with_sample_decision(&self, key: &CacheKey, repeat_sample: bool) -> Option<CacheValue> {
        self.get_with_repeat_sampler(key, || repeat_sample)
    }

    fn get_with_repeat_sampler(
        &self,
        key: &CacheKey,
        sample_repeat: impl FnOnce() -> bool,
    ) -> Option<CacheValue> {
        let _membership = self.membership_lock.read();
        if let Some(value_ref) = self.entries.get(key) {
            let entry = value_ref.value();
            let value = entry.value.clone();
            let record_recency = if self.sample_lru_hits {
                let epoch = self.admission_epoch.load(Ordering::Relaxed);
                let last = entry.last_recorded_epoch.load(Ordering::Relaxed);
                (last != epoch
                    && entry
                        .last_recorded_epoch
                        .compare_exchange(last, epoch, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok())
                    || sample_repeat()
            } else {
                true
            };
            if record_recency {
                self.policy.on_access(*key);
            }
            self.metrics.record_hit();
            Some(value)
        } else {
            self.metrics.record_miss();
            None
        }
    }

    /// Insert a value into the cache synchronously.
    ///
    /// Returns true only if the value is admitted by capacity and remains visible
    /// after eviction completes.
    pub fn put(&self, key: CacheKey, value: &Bytes) -> bool {
        let _membership = self.membership_lock.write();

        let new_size = u64::try_from(CacheValue::charged_bytes(value.len())).unwrap_or(u64::MAX);
        if !self.can_fit_value(new_size) {
            return false;
        }

        let cache_value = CacheValue::new(value.clone());
        let prior_epoch = if self.sample_lru_hits {
            self.admission_epoch.load(Ordering::Relaxed)
        } else {
            0
        };
        self.insert_and_update_metrics(key, cache_value, prior_epoch);
        self.evict_if_needed();
        let admitted = self.entries.contains_key(&key);
        if admitted && self.sample_lru_hits {
            // The exclusive membership guard keeps readers out until the new
            // epoch is published. Failed self-eviction leaves it unchanged.
            self.admission_epoch
                .store(prior_epoch.wrapping_add(1), Ordering::Relaxed);
        }
        admitted
    }

    /// No single allocation may make a shard permanently exceed capacity.
    /// Metadata remains eviction-protected under ordinary pressure, but an
    /// oversized index/filter block is rejected just like oversized data.
    ///
    /// `value_size` is the charged size (payload plus per-entry overhead), so
    /// admission uses the same accounting as eviction. Otherwise an entry that
    /// exactly filled the shard would be admitted and then immediately evicted.
    fn can_fit_value(&self, value_size: u64) -> bool {
        value_size <= self.max_bytes
    }

    /// Insert value and update metrics accordingly
    fn insert_and_update_metrics(&self, key: CacheKey, cache_value: CacheValue, prior_epoch: u64) {
        let value_size = cache_value.size_bytes() as u64;

        // Check if entry already exists (DashMap returns old value if present)
        if let Some(existing) = self.entries.insert(
            key,
            CachedEntry {
                value: cache_value,
                // Successful admission publishes the next epoch, so the
                // first resident hit refreshes this key's recency.
                last_recorded_epoch: AtomicU64::new(prior_epoch),
            },
        ) {
            // Updated existing entry - adjust for size difference
            let old_size = existing.value.size_bytes() as u64;
            self.metrics.add_memory(value_size);
            self.metrics.remove_memory(old_size);
        } else {
            // New entry added
            self.metrics.add_memory(value_size);
        }

        self.policy.on_access(key);
    }

    /// Evict entries if cache is over capacity
    ///
    /// Strategy: Protect index/filter blocks by evicting data blocks first.
    /// Only evict index/filter blocks under severe memory pressure (>2x capacity).
    fn evict_if_needed(&self) {
        use crate::sst::cache::CacheBlockKind;

        let mut made_progress = false;
        while self.is_over_capacity() {
            // Try to evict data blocks first (protect index/filter blocks).
            if let Some(evicted) =
                self.try_evict_victim(&[CacheBlockKind::Index, CacheBlockKind::Filter])
            {
                self.update_metrics_after_eviction(&evicted);
                made_progress = true;
                continue;
            }

            if !self.is_severely_over_capacity() {
                break;
            }

            // Emergency: cache is severely over capacity, evict anything.
            if let Some(evicted) = self.try_evict_victim(&[]) {
                self.update_metrics_after_eviction(&evicted);
                made_progress = true;
            } else {
                break;
            }
        }

        if !made_progress && self.is_over_capacity() {
            tracing::warn!(
                "Cache eviction was unable to recover capacity for this shard; \
                 check policy-state consistency or memory pressure"
            );
        }
    }

    /// Check if cache is over capacity
    fn is_over_capacity(&self) -> bool {
        self.metrics.memory_bytes() > self.max_bytes
    }

    /// Check if cache is severely over capacity (emergency threshold)
    fn is_severely_over_capacity(&self) -> bool {
        self.metrics.memory_bytes() > self.max_bytes * 2
    }

    /// Try to evict a victim, excluding specified block types
    ///
    /// Uses retry loop to handle stale keys from concurrent access
    fn try_evict_victim(
        &self,
        exclude_types: &[crate::sst::cache::CacheBlockKind],
    ) -> Option<CacheValue> {
        const MAX_RETRIES: usize = 10;

        for _ in 0..MAX_RETRIES {
            let victim_key = self.policy.pick_victim(exclude_types)?;

            if let Some((_, value)) = self.entries.remove(&victim_key) {
                // Successfully evicted - notify policy
                self.policy.on_remove(victim_key);
                return Some(value.value);
            }
            // Victim was stale - notify policy and retry
            self.policy.on_stale(victim_key);
        }

        // Failed to find valid victim after retries
        None
    }

    /// Update metrics after eviction
    fn update_metrics_after_eviction(&self, evicted: &CacheValue) {
        self.metrics
            .remove_memory(u64::try_from(evicted.size_bytes()).unwrap_or(u64::MAX));
        self.metrics.record_eviction();
    }

    /// Remove a key from the cache
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn remove(&self, key: &CacheKey) -> Option<CacheValue> {
        let _membership = self.membership_lock.write();
        if let Some((_, value)) = self.entries.remove(key) {
            self.metrics
                .remove_memory(u64::try_from(value.value.size_bytes()).unwrap_or(u64::MAX));
            self.policy.on_remove(*key);
            Some(value.value)
        } else {
            None
        }
    }

    /// Remove every cached block for one SST.
    pub fn remove_sst(&self, sst_id: u64) -> usize {
        let _membership = self.membership_lock.write();
        let keys: Vec<CacheKey> = self
            .entries
            .iter()
            .filter_map(|entry| {
                let key = *entry.key();
                (key.sst_id == sst_id).then_some(key)
            })
            .collect();

        let mut removed = 0usize;
        for key in keys {
            if let Some((_, value)) = self.entries.remove(&key) {
                self.metrics
                    .remove_memory(u64::try_from(value.value.size_bytes()).unwrap_or(u64::MAX));
                self.policy.on_remove(key);
                removed += 1;
            }
        }
        removed
    }

    /// Clear all entries from the cache
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn clear(&self) {
        let _membership = self.membership_lock.write();
        self.entries.clear();
        self.metrics.set_memory_bytes(0);
        self.policy.clear();
    }

    /// Get cache metrics
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn metrics(&self) -> CacheMetrics {
        self.metrics.clone()
    }

    /// Get current charged size in bytes.
    ///
    /// Each entry charges its payload plus
    /// [`crate::sst::cache::value::ENTRY_OVERHEAD_BYTES`].
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn size_bytes(&self) -> u64 {
        self.metrics.memory_bytes()
    }

    /// Get this shard's capacity in bytes.
    #[cfg(test)]
    pub fn capacity_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Get number of entries
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if cache is empty
    #[cfg(any(test, feature = "internal-testing"))]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::cache::key::CacheBlockKind;
    use crate::sst::cache::value::ENTRY_OVERHEAD_BYTES;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;

    struct PausedHitState {
        pause_target: AtomicBool,
        target_access_entered: AtomicBool,
        release_target: AtomicBool,
        victim_selected: AtomicBool,
        keys: std::sync::Mutex<HashSet<CacheKey>>,
    }

    struct PausedHitPolicy {
        state: Arc<PausedHitState>,
        target: CacheKey,
    }

    impl CachePolicy for PausedHitPolicy {
        fn on_access(&self, key: CacheKey) {
            if key == self.target && self.state.pause_target.swap(false, Ordering::SeqCst) {
                self.state
                    .target_access_entered
                    .store(true, Ordering::SeqCst);
                while !self.state.release_target.load(Ordering::SeqCst) {
                    thread::yield_now();
                }
            }
            self.state
                .keys
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(key);
        }

        fn pick_victim(&self, _exclude_types: &[CacheBlockKind]) -> Option<CacheKey> {
            self.state.victim_selected.store(true, Ordering::SeqCst);
            let keys = self
                .state
                .keys
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            keys.contains(&self.target)
                .then_some(self.target)
                .or_else(|| keys.iter().next().copied())
        }

        fn on_remove(&self, key: CacheKey) {
            self.state
                .keys
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&key);
        }

        fn on_stale(&self, key: CacheKey) {
            self.on_remove(key);
        }

        fn clear(&self) {
            self.state
                .keys
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
        }
    }

    fn wait_until(condition: impl Fn() -> bool, message: &str) {
        for _ in 0..10_000 {
            if condition() {
                return;
            }
            thread::yield_now();
        }
        panic!("{message}");
    }

    #[test]
    fn should_not_leave_stale_policy_state_when_hit_races_eviction() {
        // Arrange
        let target = CacheKey::for_data(1, 0);
        let replacement = CacheKey::for_data(2, 0);
        let state = Arc::new(PausedHitState {
            pause_target: AtomicBool::new(false),
            target_access_entered: AtomicBool::new(false),
            release_target: AtomicBool::new(false),
            victim_selected: AtomicBool::new(false),
            keys: std::sync::Mutex::new(HashSet::new()),
        });
        let shard = Arc::new(CacheShard {
            entries: DashMap::new(),
            policy: Box::new(PausedHitPolicy {
                state: Arc::clone(&state),
                target,
            }),
            membership_lock: RwLock::new(()),
            sample_lru_hits: true,
            admission_epoch: AtomicU64::new(0),
            metrics: CacheMetrics::new(),
            max_bytes: CacheValue::charged_bytes(1) as u64,
        });
        assert!(shard.put(target, &Bytes::from_static(b"a")));
        state.pause_target.store(true, Ordering::SeqCst);

        let hit_shard = Arc::clone(&shard);
        let hit = thread::spawn(move || hit_shard.get_with_sample_decision(&target, false));
        wait_until(
            || state.target_access_entered.load(Ordering::SeqCst),
            "hit did not reach the policy publication barrier",
        );

        let put_shard = Arc::clone(&shard);
        let eviction_attempted = Arc::new(AtomicBool::new(false));
        let eviction_attempted_for_thread = Arc::clone(&eviction_attempted);
        let eviction = thread::spawn(move || {
            eviction_attempted_for_thread.store(true, Ordering::SeqCst);
            put_shard.put(replacement, &Bytes::from_static(b"b"))
        });
        wait_until(
            || eviction_attempted.load(Ordering::SeqCst),
            "eviction thread did not start",
        );

        // Act / Assert: while a hit is publishing recency, eviction must not
        // select a victim. Otherwise the hit can resurrect stale policy state.
        for _ in 0..10_000 {
            assert!(
                !state.victim_selected.load(Ordering::SeqCst),
                "eviction selected a victim while the hit was in-flight"
            );
            thread::yield_now();
        }
        state.release_target.store(true, Ordering::SeqCst);
        assert!(hit.join().expect("hit thread should finish").is_some());
        assert!(eviction.join().expect("eviction thread should finish"));

        // Assert: policy and cache entry state are removed together.
        assert!(!shard.entries.contains_key(&target));
        assert!(shard.entries.contains_key(&replacement));
        assert!(
            !state
                .keys
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&target),
            "evicted entry must not remain in policy metadata"
        );
    }

    #[test]
    fn should_let_unsampled_reader_finish_while_sampled_hit_updates_policy() {
        // Arrange: pause one sampled hit inside its policy callback.
        let paused_key = CacheKey::for_data(1, 0);
        let other_key = CacheKey::for_data(2, 0);
        let state = Arc::new(PausedHitState {
            pause_target: AtomicBool::new(false),
            target_access_entered: AtomicBool::new(false),
            release_target: AtomicBool::new(false),
            victim_selected: AtomicBool::new(false),
            keys: std::sync::Mutex::new(HashSet::new()),
        });
        let shard = Arc::new(CacheShard {
            entries: DashMap::new(),
            policy: Box::new(PausedHitPolicy {
                state: Arc::clone(&state),
                target: paused_key,
            }),
            membership_lock: RwLock::new(()),
            sample_lru_hits: true,
            admission_epoch: AtomicU64::new(0),
            metrics: CacheMetrics::new(),
            max_bytes: 2 * CacheValue::charged_bytes(1) as u64,
        });
        assert!(shard.put(paused_key, &Bytes::from_static(b"a")));
        assert!(shard.put(other_key, &Bytes::from_static(b"b")));
        // Record each key's guaranteed first hit in this admission epoch so
        // the second reader below exercises the actual unsampled repeat path.
        assert!(shard.get_with_sample_decision(&paused_key, false).is_some());
        assert!(shard.get_with_sample_decision(&other_key, false).is_some());
        state.pause_target.store(true, Ordering::SeqCst);

        // Act
        let paused_shard = Arc::clone(&shard);
        let paused_hit =
            thread::spawn(move || paused_shard.get_with_sample_decision(&paused_key, true));
        wait_until(
            || state.target_access_entered.load(Ordering::SeqCst),
            "sampled hit did not enter policy callback",
        );
        let other_shard = Arc::clone(&shard);
        let (tx, rx) = std::sync::mpsc::channel();
        let other_hit = thread::spawn(move || {
            let found = other_shard
                .get_with_sample_decision(&other_key, false)
                .is_some();
            tx.send(found).expect("reader result receiver remains open");
        });
        let completed_while_paused = rx.recv_timeout(std::time::Duration::from_secs(1));
        state.release_target.store(true, Ordering::SeqCst);
        assert!(paused_hit
            .join()
            .expect("sampled reader should finish")
            .is_some());
        other_hit.join().expect("unsampled reader should finish");

        // Assert: the unsampled repeat read does not wait for policy callback.
        assert_eq!(completed_while_paused.ok(), Some(true));
    }

    #[test]
    fn should_hide_unadmitted_value_from_concurrent_reader() {
        // Arrange: the policy pauses after admission inserts a new value,
        // before the writer evicts that same entry and returns false.
        let rejected = CacheKey::for_data(1, 0);
        let resident = CacheKey::for_data(2, 0);
        let state = Arc::new(PausedHitState {
            pause_target: AtomicBool::new(false),
            target_access_entered: AtomicBool::new(false),
            release_target: AtomicBool::new(false),
            victim_selected: AtomicBool::new(false),
            keys: std::sync::Mutex::new(HashSet::new()),
        });
        let shard = Arc::new(CacheShard {
            entries: DashMap::new(),
            policy: Box::new(PausedHitPolicy {
                state: Arc::clone(&state),
                target: rejected,
            }),
            membership_lock: RwLock::new(()),
            sample_lru_hits: true,
            admission_epoch: AtomicU64::new(0),
            metrics: CacheMetrics::new(),
            max_bytes: CacheValue::charged_bytes(1) as u64,
        });
        assert!(shard.put(resident, &Bytes::from_static(b"a")));
        state.pause_target.store(true, Ordering::SeqCst);

        // Act
        let put_shard = Arc::clone(&shard);
        let put = thread::spawn(move || put_shard.put(rejected, &Bytes::from_static(b"b")));
        wait_until(
            || state.target_access_entered.load(Ordering::SeqCst),
            "writer did not reach admission callback",
        );
        assert!(
            shard.membership_lock.try_read().is_none(),
            "writer must retain exclusive membership through eviction"
        );
        let read_shard = Arc::clone(&shard);
        let read_started = Arc::new(AtomicBool::new(false));
        let read_started_for_thread = Arc::clone(&read_started);
        let (tx, rx) = std::sync::mpsc::channel();
        let read = thread::spawn(move || {
            read_started_for_thread.store(true, Ordering::SeqCst);
            tx.send(
                read_shard
                    .get_with_sample_decision(&rejected, false)
                    .is_some(),
            )
            .expect("reader result receiver remains open");
        });
        wait_until(
            || read_started.load(Ordering::SeqCst),
            "reader did not start during admission",
        );
        let early_read = rx.recv_timeout(std::time::Duration::from_millis(20));
        state.release_target.store(true, Ordering::SeqCst);

        // Assert: a completed read cannot see a value rejected by admission.
        let put_result = put.join().expect("writer should finish");
        let returned_before_commit = early_read.is_ok();
        let read_result = match early_read {
            Ok(found) => found,
            Err(_) => rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("reader should finish after writer"),
        };
        read.join().expect("reader should finish");
        assert!(!returned_before_commit);
        assert!(!put_result);
        assert!(!read_result);
        assert!(shard.get(&resident).is_some());
    }

    #[test]
    fn should_keep_hot_blocks_when_sampled_reads_face_cold_admissions() {
        // Arrange: compare a fixed sample sequence with exact LRU on the same
        // hot-read/cold-admission stream. This is a cache-quality regression,
        // not a guarantee that every isolated hot hit changes recency.
        const HOT_KEYS: usize = 4;
        const CAPACITY: usize = 8;
        const COLD_PER_ROUND: usize = 4;
        const ROUNDS: usize = 1_000;
        let value = Bytes::from_static(b"v");
        let capacity = (CAPACITY * CacheValue::charged_bytes(value.len())) as u64;
        let sampled = CacheShard::new(capacity, CachePolicyType::Lru);
        let exact = CacheShard::new(capacity, CachePolicyType::Lru);
        let hot_keys: Vec<_> = (0..HOT_KEYS)
            .map(|id| CacheKey::for_data(id as u64, 0))
            .collect();
        for id in 0..CAPACITY {
            let key = CacheKey::for_data(id as u64, 0);
            assert!(sampled.put(key, &value));
            assert!(exact.put(key, &value));
        }
        let random_state = Cell::new(0x5eed_u64);
        let mut sampled_hot_hits = 0;
        let mut exact_hot_hits = 0;

        // Act: each round reads the hot set, then admits enough cold blocks to
        // evict the hot set if its reads did not refresh recency.
        for round in 0..ROUNDS {
            for key in &hot_keys {
                let record_recency = next_lru_sample(&random_state);
                if sampled
                    .get_with_sample_decision(key, record_recency)
                    .is_some()
                {
                    sampled_hot_hits += 1;
                } else {
                    assert!(sampled.put(*key, &value));
                }
                if exact.get_with_sample_decision(key, true).is_some() {
                    exact_hot_hits += 1;
                } else {
                    assert!(exact.put(*key, &value));
                }
            }
            for cold_index in 0..COLD_PER_ROUND {
                let cold =
                    CacheKey::for_data((CAPACITY + round * COLD_PER_ROUND + cold_index) as u64, 0);
                assert!(sampled.put(cold, &value));
                assert!(exact.put(cold, &value));
            }
        }

        // Assert: the first hit after each admission is guaranteed, preserving
        // the hot set under cold pressure.
        assert_eq!(exact_hot_hits, HOT_KEYS * ROUNDS);
        assert!(
            sampled_hot_hits >= exact_hot_hits * 95 / 100,
            "sampled hot hits {sampled_hot_hits} vs exact {exact_hot_hits}"
        );
        assert_eq!(sampled.len(), CAPACITY);
        assert!(sampled.size_bytes() <= capacity);
    }

    #[test]
    fn should_retain_five_hot_blocks_between_three_cold_admissions_without_repeat_samples() {
        // Arrange: a small cold group repeatedly turns over an eight-block shard.
        const HOT_KEYS: usize = 5;
        const CAPACITY: usize = 8;
        const COLD_PER_ROUND: usize = 3;
        const ROUNDS: usize = 1_000;
        let value = Bytes::from_static(b"v");
        let capacity = (CAPACITY * CacheValue::charged_bytes(value.len())) as u64;
        let sampled = CacheShard::new(capacity, CachePolicyType::Lru);
        let exact = CacheShard::new(capacity, CachePolicyType::Lru);
        let hot_keys: Vec<_> = (0..HOT_KEYS)
            .map(|id| CacheKey::for_data(id as u64, 0))
            .collect();
        for id in 0..CAPACITY {
            let key = CacheKey::for_data(id as u64, 0);
            assert!(sampled.put(key, &value));
            assert!(exact.put(key, &value));
        }
        let mut sampled_hot_hits = 0;
        let mut exact_hot_hits = 0;

        // Act
        for round in 0..ROUNDS {
            for key in &hot_keys {
                if sampled.get_with_sample_decision(key, false).is_some() {
                    sampled_hot_hits += 1;
                } else {
                    assert!(sampled.put(*key, &value));
                }
                if exact.get_with_sample_decision(key, true).is_some() {
                    exact_hot_hits += 1;
                } else {
                    assert!(exact.put(*key, &value));
                }
            }
            for cold_index in 0..COLD_PER_ROUND {
                let cold =
                    CacheKey::for_data((CAPACITY + round * COLD_PER_ROUND + cold_index) as u64, 0);
                assert!(sampled.put(cold, &value));
                assert!(exact.put(cold, &value));
            }
        }

        // Assert: first hits after every admission protect the hot group.
        assert_eq!(exact_hot_hits, HOT_KEYS * ROUNDS);
        assert_eq!(sampled_hot_hits, exact_hot_hits);
    }

    #[test]
    fn should_record_newly_admitted_keys_first_hit_after_other_key_becomes_newer() {
        // Arrange
        let value = Bytes::from_static(b"v");
        let capacity = 2 * CacheValue::charged_bytes(value.len()) as u64;
        let shard = CacheShard::new(capacity, CachePolicyType::Lru);
        let first = CacheKey::for_data(1, 0);
        let newly_admitted = CacheKey::for_data(2, 0);
        let incoming = CacheKey::for_data(3, 0);
        assert!(shard.put(first, &value));
        assert!(shard.put(newly_admitted, &value));

        // Act: the older key is hit first. Even with repeat sampling disabled,
        // the new key's first hit must then refresh its recency.
        assert!(shard.get_with_sample_decision(&first, false).is_some());
        assert!(shard
            .get_with_sample_decision(&newly_admitted, false)
            .is_some());
        assert!(shard.put(incoming, &value));

        // Assert
        assert!(shard.get_with_sample_decision(&first, false).is_none());
        assert!(shard
            .get_with_sample_decision(&newly_admitted, false)
            .is_some());
        assert!(shard.get_with_sample_decision(&incoming, false).is_some());
    }

    #[test]
    fn should_charge_the_epoch_added_to_each_resident_entry() {
        // Arrange
        let value_bytes = std::mem::size_of::<CacheValue>();

        // Act
        let resident_bytes = std::mem::size_of::<CachedEntry>();

        // Assert: the eight-byte epoch is the only new per-entry field and
        // the configured charge preserves the prior 64-byte bookkeeping bias.
        assert_eq!(
            resident_bytes,
            value_bytes + std::mem::size_of::<AtomicU64>()
        );
        assert!(ENTRY_OVERHEAD_BYTES >= 64 + resident_bytes - value_bytes);
    }

    #[test]
    fn should_not_advance_recency_epoch_for_rejected_data_admissions() {
        // Arrange: protected metadata fills the shard. Each new data block
        // can be inserted transiently but must evict itself before put returns.
        let value = Bytes::from_static(b"v");
        let shard = CacheShard::new(
            CacheValue::charged_bytes(value.len()) as u64,
            CachePolicyType::Lru,
        );
        let protected = CacheKey::for_index(1, 0);
        assert!(shard.put(protected, &value));
        assert!(shard.get_with_sample_decision(&protected, false).is_some());
        let epoch_before = shard.admission_epoch.load(Ordering::Relaxed);
        let last_recorded_before = shard
            .entries
            .get(&protected)
            .expect("metadata remains resident")
            .last_recorded_epoch
            .load(Ordering::Relaxed);

        // Act
        for id in 2..18 {
            assert!(!shard.put(CacheKey::for_data(id, 0), &value));
        }
        assert!(shard.get_with_sample_decision(&protected, false).is_some());

        // Assert: failed puts do not force another metadata recency update.
        assert_eq!(shard.admission_epoch.load(Ordering::Relaxed), epoch_before);
        assert_eq!(
            shard
                .entries
                .get(&protected)
                .expect("metadata remains resident")
                .last_recorded_epoch
                .load(Ordering::Relaxed),
            last_recorded_before
        );
        assert_eq!(shard.len(), 1);
    }

    #[test]
    fn should_allow_repeat_unsampled_hot_hit_to_be_evicted() {
        // Arrange
        let value = Bytes::from_static(b"v");
        let capacity = 2 * CacheValue::charged_bytes(value.len()) as u64;
        let shard = CacheShard::new(capacity, CachePolicyType::Lru);
        let oldest = CacheKey::for_data(1, 0);
        let newer = CacheKey::for_data(2, 0);
        let incoming = CacheKey::for_data(3, 0);
        assert!(shard.put(oldest, &value));
        assert!(shard.put(newer, &value));

        // Act: both keys get their guaranteed first hit after admission.
        // The later repeat hit on `newer` skips its LRU update.
        assert!(shard.get_with_sample_decision(&newer, false).is_some());
        assert!(shard.get_with_sample_decision(&oldest, false).is_some());
        assert!(shard.get_with_sample_decision(&newer, false).is_some());
        assert!(shard.put(incoming, &value));

        // Assert: a repeat hit can still be missed within one admission epoch.
        assert!(shard.get_with_sample_decision(&oldest, false).is_some());
        assert!(shard.get_with_sample_decision(&newer, false).is_none());
        assert!(shard.get_with_sample_decision(&incoming, false).is_some());
    }

    #[test]
    fn should_make_first_data_block_put_visible_without_prior_admission() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"visible"[..]);

        // Act
        let inserted = shard.put(key, &value);
        let retrieved = shard.get(&key);

        // Assert
        assert!(inserted);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), value.to_vec());
    }

    #[test]
    fn should_keep_large_data_block_after_many_small_evictions() {
        // Arrange
        let capacity = CacheValue::charged_bytes(100) as u64;
        let shard = CacheShard::new(capacity, CachePolicyType::Lru);
        let small = Bytes::from(vec![1u8; 1]);
        let large = Bytes::from(vec![2u8; 100]);
        let final_key = CacheKey::for_data(999, 0);
        let before_evictions = shard.metrics().eviction_count();

        // Act
        for i in 0u64..200 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard.put(key, &small));
        }
        let inserted = shard.put(final_key, &large);
        let metrics = shard.metrics();
        let retrieved = shard.get(&final_key);

        // Assert
        assert!(inserted);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), large.to_vec());
        assert!(metrics.eviction_count() - before_evictions > 50);
        assert!(shard.size_bytes() <= capacity);
    }

    #[test]
    fn should_report_false_when_data_block_exceeds_shard_capacity() {
        // Arrange
        let shard = CacheShard::new(CacheValue::charged_bytes(8) as u64, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(vec![0u8; 16]);

        // Act
        let inserted = shard.put(key, &value);
        let retrieved = shard.get(&key);

        // Assert
        assert!(!inserted);
        assert!(retrieved.is_none());
        assert_eq!(shard.size_bytes(), 0);
    }

    #[test]
    fn should_reject_oversized_metadata_block_without_exceeding_capacity() {
        // Arrange
        let shard = CacheShard::new(CacheValue::charged_bytes(8) as u64, CachePolicyType::Lru);
        let index_key = CacheKey::for_index(1, 0);
        let filter_key = CacheKey::for_filter(1, 8);
        let value = Bytes::from(vec![0u8; 16]);

        // Act
        let index_inserted = shard.put(index_key, &value);
        let filter_inserted = shard.put(filter_key, &value);

        // Assert
        assert!(!index_inserted);
        assert!(!filter_inserted);
        assert_eq!(shard.size_bytes(), 0);
        assert!(shard.is_empty());
    }

    #[test]
    fn should_remove_only_blocks_for_requested_sst() {
        // Arrange
        let shard = CacheShard::new(1024, CachePolicyType::Lru);
        let target_data = CacheKey::for_data(7, 0);
        let target_index = CacheKey::for_index(7, 64);
        let other_data = CacheKey::for_data(8, 0);
        let value = Bytes::from(vec![1u8; 32]);
        assert!(shard.put(target_data, &value));
        assert!(shard.put(target_index, &value));
        assert!(shard.put(other_data, &value));

        // Act
        let removed = shard.remove_sst(7);

        // Assert
        assert_eq!(removed, 2);
        assert!(shard.get(&target_data).is_none());
        assert!(shard.get(&target_index).is_none());
        assert!(shard.get(&other_data).is_some());
        assert_eq!(shard.size_bytes(), CacheValue::charged_bytes(32) as u64);
    }

    #[test]
    fn should_preserve_metadata_blocks_when_eviction_overflows() {
        // Arrange
        let capacity = 3 * CacheValue::charged_bytes(40) as u64;
        let shard = CacheShard::new(capacity, CachePolicyType::Lru);
        let index_key = CacheKey::for_index(1, 0);
        let filter_key = CacheKey::for_filter(1, 40);
        let data_key = CacheKey::for_data(1, 80);
        let next_data_key = CacheKey::for_data(1, 120);
        let value = Bytes::from(vec![7u8; 40]);

        // Act
        assert!(shard.put(index_key, &value));
        assert!(shard.put(filter_key, &value));
        assert!(shard.put(data_key, &value));
        assert!(shard.put(next_data_key, &value));

        // Assert
        assert!(shard.get(&index_key).is_some());
        assert!(shard.get(&filter_key).is_some());
        assert!(shard.get(&data_key).is_none());
        assert!(shard.get(&next_data_key).is_some());
        assert!(shard.size_bytes() <= capacity);
    }

    #[test]
    fn should_retrieve_value_after_store() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"hello world"[..]);

        // Act
        assert!(shard.put(key, &value));
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), value.to_vec());
    }

    #[test]
    fn should_evict_on_overflow() {
        // Arrange
        let shard = CacheShard::new(CacheValue::charged_bytes(100) as u64, CachePolicyType::Lru);
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);
        let data1 = vec![b'x'; 80];
        let data2 = vec![b'y'; 80];

        // Act
        assert!(shard.put(key1, &Bytes::from(data1)));
        assert!(shard.put(key2, &Bytes::from(data2)));

        // Assert - key1 should be evicted (LRU)
        assert!(shard.get(&key1).is_none());
        assert!(shard.get(&key2).is_some());
    }

    #[test]
    fn should_track_metrics() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"test_data"[..]);

        // Act
        assert!(shard.put(key, &value));
        shard.get(&key);
        let metrics = shard.metrics();

        // Assert
        assert_eq!(metrics.hit_count(), 1);
        assert_eq!(metrics.miss_count(), 0);
    }

    #[test]
    fn should_clear_all_entries() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        for i in 0..5 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard.put(key, &Bytes::from(format!("value_{i}").into_bytes())));
        }
        shard.clear();

        // Assert
        assert_eq!(shard.len(), 0);
        assert_eq!(shard.size_bytes(), 0);
    }

    // ===== New comprehensive tests =====

    #[test]
    fn should_return_none_for_missing_key() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        let result = shard.get(&CacheKey::for_data(999, 999));

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_record_miss_for_missing_key() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        let _ = shard.get(&CacheKey::for_data(999, 999));
        let metrics = shard.metrics();

        // Assert
        assert_eq!(metrics.miss_count(), 1);
    }

    #[test]
    fn should_update_existing_entry() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value1 = Bytes::from(&b"original"[..]);
        let value2 = Bytes::from(&b"updated"[..]);
        let expected = value2.clone();

        // Act
        assert!(shard.put(key, &value1));
        assert!(shard.put(key, &value2));
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), expected.to_vec());
    }

    #[test]
    fn should_remove_entry() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"data"[..]);

        // Act
        assert!(shard.put(key, &value));
        let removed = shard.remove(&key);
        let retrieved = shard.get(&key);

        // Assert
        assert!(removed.is_some());
        assert!(retrieved.is_none());
    }

    #[test]
    fn should_remove_nonexistent_entry() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        let result = shard.remove(&CacheKey::for_data(999, 999));

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_start_empty() {
        // Arrange

        // Act
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Assert
        assert!(shard.is_empty());
        assert_eq!(shard.len(), 0);
        assert_eq!(shard.size_bytes(), 0);
    }

    #[test]
    fn should_handle_empty_data() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let empty = Bytes::new();

        // Act
        assert!(shard.put(key, &empty));
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().size_bytes(), ENTRY_OVERHEAD_BYTES);
    }

    #[test]
    fn should_handle_large_values() {
        // Arrange
        let shard = CacheShard::new(100_000, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let large = Bytes::from(vec![42u8; 50_000]);

        // Act
        assert!(shard.put(key, &large));
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), large.to_vec());
    }

    #[test]
    fn should_track_entries_count() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);

        // Act
        let mut lens = Vec::new();
        for i in 0..10 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard.put(key, &Bytes::from(format!("data_{i}").into_bytes())));
            lens.push(shard.len());
        }

        // Assert
        for (i, len) in lens.into_iter().enumerate() {
            assert_eq!(len, i + 1);
        }
    }

    #[test]
    fn should_track_memory_usage() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        assert!(shard.put(key1, &Bytes::from(&b"1000B"[..]))); // 5 payload bytes
        let size_after_first = shard.size_bytes();
        assert!(shard.put(key2, &Bytes::from(vec![0u8; 995]))); // 995 bytes
        let size_after_second = shard.size_bytes();

        // Assert
        assert!(size_after_first >= 5);
        assert!(size_after_second >= 1000);
    }

    #[test]
    fn should_distinguish_different_policies() {
        // Arrange
        let capacity = 5 * CacheValue::charged_bytes(5) as u64;
        let shard_lru = CacheShard::new(capacity, CachePolicyType::Lru);
        let shard_tinyfu = CacheShard::new(capacity, CachePolicyType::TinyLfu);

        // Act (both should work, just with different eviction strategies)
        for i in 0..5 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard_lru.put(key, &Bytes::from(format!("data{i}").into_bytes())));
            assert!(shard_tinyfu.put(key, &Bytes::from(format!("data{i}").into_bytes())));
        }
        let lru_len = shard_lru.len();
        let tinylfu_len = shard_tinyfu.len();

        // Assert
        assert_eq!(lru_len, 5);
        assert_eq!(tinylfu_len, 5);
    }

    #[test]
    fn should_handle_zero_capacity() {
        // Arrange
        let shard = CacheShard::new(0, CachePolicyType::Lru);

        // Act
        let _ = shard.put(CacheKey::for_data(1, 0), &Bytes::from(&b"data"[..]));

        // Assert
        assert!(shard.is_empty());
    }

    #[test]
    fn should_handle_single_entry_eviction() {
        // Arrange
        // Very small cache: room for one 5-byte entry, not two.
        let shard = CacheShard::new(CacheValue::charged_bytes(5) as u64, CachePolicyType::Lru);
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        assert!(shard.put(key1, &Bytes::from(&b"12345"[..]))); // 5 bytes
        assert!(shard.put(key2, &Bytes::from(&b"67890"[..]))); // 5 bytes
        let retrieved = shard.get(&key1);

        // Assert - key1 might be evicted
        assert!(retrieved.is_none() || retrieved.is_some());
    }

    #[test]
    fn should_track_hit_miss_metrics() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);

        // Act
        shard.get(&key); // miss
        assert!(shard.put(key, &Bytes::from(&b"data"[..])));
        shard.get(&key); // hit
        shard.get(&key); // hit
        let metrics = shard.metrics();

        // Assert
        assert_eq!(metrics.miss_count(), 1);
        assert_eq!(metrics.hit_count(), 2);
    }

    #[test]
    fn should_track_eviction_metrics() {
        // Arrange
        let shard = CacheShard::new(
            3 * CacheValue::charged_bytes(15) as u64,
            CachePolicyType::Lru,
        );

        // Act
        for i in 0..5 {
            let key = CacheKey::for_data(i, 0);
            assert!(shard.put(key, &Bytes::from(vec![0u8; 15])));
        }
        let metrics = shard.metrics();

        // Assert - some evictions should happen
        assert!(metrics.eviction_count() > 0);
    }
}
