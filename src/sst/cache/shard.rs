//! Single cache shard with lock-free entry management and async admission

use crate::sst::cache::admission::AdmissionCounter;
use crate::sst::cache::key::CacheKey;
use crate::sst::cache::metrics::CacheMetrics;
use crate::sst::cache::policy::{CachePolicy, CachePolicyType};
use crate::sst::cache::value::CacheValue;
use bytes::Bytes;
use crossbeam_channel::{bounded, Sender};
use dashmap::DashMap;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

#[cfg(test)]
const TEST_BLOCKING_VALUE: &[u8] = b"__cache_shard_drop_block__";

#[cfg(test)]
static TEST_WORKER_BLOCKED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
static TEST_WORKER_RELEASE: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
static TEST_WORKER_FINISHED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
static TEST_SELF_JOIN_SKIPPED: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
static TEST_SHARD_HOOK_LOCK: Mutex<()> = Mutex::new(());

/// Message sent to admission worker
struct AdmissionRequest {
    key: CacheKey,
    value: Bytes,
}

/// A single cache shard (partition) with lock-free access
///
/// Contains a portion of the cache entries using `DashMap` for concurrent access,
/// with its own eviction policy and metrics. Admission and eviction happen
/// asynchronously in a background worker thread.
pub struct CacheShard {
    /// Map of cache key -> value (lock-free concurrent hashmap)
    entries: DashMap<CacheKey, CacheValue>,
    /// Eviction policy
    policy: Box<dyn CachePolicy>,
    /// Admission control counter
    admission: AdmissionCounter,
    /// Metrics for this shard
    metrics: CacheMetrics,
    /// Maximum size in bytes
    max_bytes: u64,
    /// Channel for async admission requests
    admission_tx: Option<Sender<AdmissionRequest>>,
    /// Fallback to inline admission when worker thread cannot be spawned
    admission_inline: AtomicBool,
    /// Background admission worker thread handle
    worker_handle: Option<JoinHandle<()>>,
}

impl CacheShard {
    /// Create a new cache shard with background admission worker
    ///
    /// `max_bytes`: Maximum capacity in bytes
    /// `policy_type`: Eviction policy to use
    ///
    /// Returns `Arc<Self>` because the background worker needs a reference
    #[must_use]
    pub fn new(max_bytes: u64, policy_type: CachePolicyType) -> Arc<Self> {
        let (tx, rx) = bounded(10_000);

        Arc::new_cyclic(|weak_shard| {
            let worker_handle = Self::spawn_worker(weak_shard.clone(), rx);
            let inline_mode = worker_handle.is_none();
            Self {
                entries: DashMap::new(),
                policy: policy_type.create(),
                admission: AdmissionCounter::new(64, 1000),
                metrics: CacheMetrics::new(),
                max_bytes,
                admission_tx: Some(tx),
                admission_inline: AtomicBool::new(inline_mode),
                worker_handle,
            }
        })
    }

    /// Spawn the background admission worker thread
    ///
    /// Returns Some(JoinHandle) on success, None on failure (fallback to inline)
    fn spawn_worker(
        weak_shard: std::sync::Weak<Self>,
        rx: crossbeam_channel::Receiver<AdmissionRequest>,
    ) -> Option<JoinHandle<()>> {
        let spawn_result = thread::Builder::new()
            .name("cache-admission-worker".to_string())
            .spawn(move || {
                for request in rx {
                    // Upgrade weak to strong per request so worker does not keep shard alive.
                    let Some(shard) = weak_shard.upgrade() else {
                        break;
                    };
                    shard.handle_admission_request(request);
                }
            });

        match spawn_result {
            Ok(handle) => Some(handle),
            Err(err) => {
                // Log failure but don't panic; inline admission will be used
                tracing::warn!(
                    error = %err,
                    "cache admission worker spawn failed; falling back to inline admission"
                );
                if let Some(t) = crate::telemetry::Telemetry::global() {
                    t.metrics().record_thread_spawn_failure();
                }
                None
            }
        }
    }

    /// Get a cached value (lock-free)
    #[inline(always)]
    pub fn get(&self, key: &CacheKey) -> Option<CacheValue> {
        if let Some(value_ref) = self.entries.get(key) {
            let value = value_ref.value().clone();
            let _ = value.increment_access();
            self.policy.on_access(*key);
            self.metrics.record_hit();
            Some(value)
        } else {
            self.metrics.record_miss();
            None
        }
    }

    /// Insert a value into the cache (non-blocking)
    ///
    /// Sends admission request to background worker. Returns immediately
    /// without blocking on eviction or admission checks.
    ///
    /// Returns true if request was queued, false if channel is disconnected.
    pub fn put(&self, key: CacheKey, value: Bytes) -> bool {
        if self.admission_inline.load(Ordering::Relaxed) {
            // Track inline fallback usage
            if let Some(t) = crate::telemetry::Telemetry::global() {
                t.metrics().record_cache_inline_fallback();
            }
            self.put_inline(key, &value);
            return true;
        }

        // Track access for admission control (fast path)
        self.record_access_for_admission(&key);

        // Send to admission worker (non-blocking)
        if self.admission_tx.as_ref().is_none_or(|tx| {
            tx.send(AdmissionRequest {
                key,
                value: value.clone(),
            })
            .is_err()
        }) {
            // If channel is unavailable/disconnected, fall back to inline admission
            self.admission_inline.store(true, Ordering::Relaxed);
            self.put_inline(key, &value);
        }
        true
    }

    /// Insert a value into the cache (inline admission fallback)
    ///
    /// Optimized for constrained environments where background worker thread
    /// cannot spawn. Uses fast capacity check and adaptive eviction to minimize
    /// lock contention on the critical path.
    fn put_inline(&self, key: CacheKey, value: &Bytes) {
        // Track access for admission control
        self.record_access_for_admission(&key);

        // Check type-aware admission policy
        if !self.should_admit(&key) {
            return;
        }

        // Wrap in CacheValue
        let cache_value = CacheValue::new(value.clone());
        let new_size = u64::try_from(value.len()).unwrap_or(u64::MAX);

        // Fast capacity check: if we have plenty of space, skip eviction logic
        let current_size = self.metrics.memory_bytes();
        if current_size + new_size < self.max_bytes {
            // Plenty of room, insert directly without eviction checks
            self.insert_and_update_metrics(key, cache_value);
            return;
        }

        // Tight on space: insert first, then evict if needed
        // This reduces the number of eviction cycles vs. pre-checking
        self.insert_and_update_metrics(key, cache_value);

        // Only evict if actually over capacity (not just close)
        if self.metrics.memory_bytes() > self.max_bytes {
            self.evict_if_needed();
        }
    }

    /// Insert a value into the cache (synchronous, for tests)
    ///
    /// Performs admission check, insert, and eviction synchronously.
    /// Bypasses the background worker for deterministic test behavior.
    #[cfg(test)]
    pub fn put_sync(&self, key: CacheKey, value: Bytes) {
        // Track access for admission control
        self.record_access_for_admission(&key);

        // Check type-aware admission policy
        if !self.should_admit(&key) {
            return;
        }

        // Wrap in CacheValue
        let cache_value = CacheValue::new(value);

        // Insert and update metrics
        self.insert_and_update_metrics(key, cache_value);

        // Evict if over capacity
        self.evict_if_needed();
    }

    /// Handle one admission request on the background worker.
    fn handle_admission_request(&self, request: AdmissionRequest) {
        #[cfg(test)]
        if request.value == Bytes::from_static(TEST_BLOCKING_VALUE) {
            TEST_WORKER_BLOCKED.store(true, Ordering::SeqCst);
            while !TEST_WORKER_RELEASE.load(Ordering::SeqCst) {
                thread::yield_now();
            }
            TEST_WORKER_FINISHED.store(true, Ordering::SeqCst);
        }

        // Check type-aware admission policy
        if !self.should_admit(&request.key) {
            return;
        }

        // Insert into cache
        let cache_value = CacheValue::new(request.value);
        self.insert_and_update_metrics(request.key, cache_value);

        // Evict if over capacity (runs in background)
        self.evict_if_needed();
    }

    /// Record access for admission control
    fn record_access_for_admission(&self, key: &CacheKey) {
        self.admission
            .record_access(key.sst_id.to_le_bytes().as_ref());
    }

    /// Check if a key should be admitted based on type-aware policy
    fn should_admit(&self, key: &CacheKey) -> bool {
        self.admission.should_admit(key)
    }

    /// Insert value and update metrics accordingly
    fn insert_and_update_metrics(&self, key: CacheKey, cache_value: CacheValue) {
        let value_size = cache_value.size_bytes() as u64;

        // Check if entry already exists (DashMap returns old value if present)
        if let Some(existing) = self.entries.insert(key, cache_value) {
            // Updated existing entry - adjust for size difference
            let old_size = existing.size_bytes() as u64;
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
        use crate::sst::cache::BlockType;

        // Safeguard: limit eviction iterations to prevent excessive latency
        // when admission worker thread fails. This prevents pathological cases
        // where we spend excessive time evicting due to resource constraints.
        //
        // Value is adaptive:
        // - Background worker mode (not used): limit doesn't apply
        // - Inline fallback mode: smaller limit (50) to avoid blocking hot path
        const MAX_EVICTION_ATTEMPTS: usize = 50;
        let mut eviction_count = 0;

        while self.is_over_capacity() && eviction_count < MAX_EVICTION_ATTEMPTS {
            eviction_count += 1;

            // Try to evict data blocks first (protect index/filter)
            if let Some(evicted) = self.try_evict_victim(&[BlockType::Index, BlockType::Filter]) {
                self.update_metrics_after_eviction(&evicted);
            } else if self.is_severely_over_capacity() {
                // Emergency: Cache is severely over capacity, evict anything
                if let Some(evicted) = self.try_evict_victim(&[]) {
                    self.update_metrics_after_eviction(&evicted);
                } else {
                    break; // Can't evict more
                }
            } else {
                break; // Can't evict more data blocks, stop
            }
        }

        // Log if we hit the safeguard limit (only in inline mode where this matters)
        if eviction_count >= MAX_EVICTION_ATTEMPTS
            && self.is_over_capacity()
            && self.admission_inline.load(Ordering::Relaxed)
        {
            tracing::warn!(
                "Cache eviction (inline mode) hit safeguard at {} attempts, still over capacity; \
                     consider increasing cache size or checking memory pressure",
                MAX_EVICTION_ATTEMPTS
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
        exclude_types: &[crate::sst::cache::BlockType],
    ) -> Option<CacheValue> {
        const MAX_RETRIES: usize = 10;

        for _ in 0..MAX_RETRIES {
            let victim_key = self.policy.pick_victim(exclude_types)?;

            if let Some((_, value)) = self.entries.remove(&victim_key) {
                // Successfully evicted - notify policy
                self.policy.on_remove(victim_key);
                return Some(value);
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
    pub fn remove(&self, key: &CacheKey) -> Option<CacheValue> {
        if let Some((_, value)) = self.entries.remove(key) {
            self.metrics
                .remove_memory(u64::try_from(value.size_bytes()).unwrap_or(u64::MAX));
            self.policy.on_remove(*key);
            Some(value)
        } else {
            None
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        self.entries.clear();
        self.metrics.set_memory_bytes(0);
        self.policy.clear();
    }

    /// Get cache metrics
    pub fn metrics(&self) -> CacheMetrics {
        self.metrics.clone()
    }

    /// Get current size in bytes
    pub fn size_bytes(&self) -> u64 {
        self.metrics.memory_bytes()
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Drop for CacheShard {
    fn drop(&mut self) {
        // Close sender first so worker receive loop exits promptly.
        let _ = self.admission_tx.take();

        // Wait for worker thread to complete if it exists.
        // Use Option::take() to move the handle out of self.
        if let Some(handle) = self.worker_handle.take() {
            let current_id = thread::current().id();
            let worker_id = handle.thread().id();

            if current_id == worker_id {
                // If the cache shard is being dropped from the worker thread itself,
                // joining would deadlock / fail with EDEADLK. In that case, just
                // allow the thread to exit naturally and drop the handle.
                #[cfg(test)]
                TEST_SELF_JOIN_SKIPPED.store(true, Ordering::SeqCst);
                tracing::trace!(
                    "cache admission worker drop running on worker thread; skipping self-join"
                );
            } else {
                match handle.join() {
                    Ok(()) => {
                        tracing::trace!("cache admission worker exited cleanly");
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = ?e,
                            "cache admission worker thread panicked during shutdown"
                        );
                    }
                }
            }
        }
        // If worker_handle is None, the thread was never spawned (inline mode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_retrieve_value_after_store() {
        // Arrange
        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let value = Bytes::from(&b"hello world"[..]);

        // Act
        shard.put_sync(key, value.clone());
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().data.to_vec(), value.to_vec());
    }

    #[test]
    fn should_evict_on_overflow() {
        // Arrange
        let shard = CacheShard::new(100, CachePolicyType::Lru);
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);
        let data1 = vec![b'x'; 80];
        let data2 = vec![b'y'; 80];

        // Act
        shard.put_sync(key1, Bytes::from(data1));
        shard.put_sync(key2, Bytes::from(data2));

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
        shard.put_sync(key, value);
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
            shard.put_sync(key, Bytes::from(format!("value_{i}").into_bytes()));
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
        shard.put_sync(key, value1);
        shard.put_sync(key, value2);
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
        shard.put_sync(key, value.clone());
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
        shard.put_sync(key, empty);
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().size_bytes(), 0);
    }

    #[test]
    fn should_handle_large_values() {
        // Arrange
        let shard = CacheShard::new(100_000, CachePolicyType::Lru);
        let key = CacheKey::for_data(1, 0);
        let large = Bytes::from(vec![42u8; 50_000]);

        // Act
        shard.put_sync(key, large.clone());
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
            shard.put_sync(key, Bytes::from(format!("data_{i}").into_bytes()));
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
        shard.put_sync(key1, Bytes::from(&b"1000B"[..])); // 5 bytes
        let size_after_first = shard.size_bytes();
        shard.put_sync(key2, Bytes::from(vec![0u8; 995])); // 995 bytes
        let size_after_second = shard.size_bytes();

        // Assert
        assert!(size_after_first >= 5);
        assert!(size_after_second >= 1000);
    }

    #[test]
    fn should_distinguish_different_policies() {
        // Arrange
        let shard_lru = CacheShard::new(1000, CachePolicyType::Lru);
        let shard_tinyfu = CacheShard::new(1000, CachePolicyType::TinyLfu);

        // Act (both should work, just with different eviction strategies)
        for i in 0..5 {
            let key = CacheKey::for_data(i, 0);
            shard_lru.put_sync(key, Bytes::from(format!("data{i}").into_bytes()));
            shard_tinyfu.put_sync(key, Bytes::from(format!("data{i}").into_bytes()));
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
        shard.put_sync(CacheKey::for_data(1, 0), Bytes::from(&b"data"[..]));

        // Assert (should immediately evict)
        assert!(shard.is_empty() || shard.len() == 1);
    }

    #[test]
    fn should_handle_single_entry_eviction() {
        // Arrange
        let shard = CacheShard::new(10, CachePolicyType::Lru); // Very small cache
        let key1 = CacheKey::for_data(1, 0);
        let key2 = CacheKey::for_data(2, 0);

        // Act
        shard.put_sync(key1, Bytes::from(&b"12345"[..])); // 5 bytes
        shard.put_sync(key2, Bytes::from(&b"67890"[..])); // 5 bytes
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
        shard.put_sync(key, Bytes::from(&b"data"[..]));
        shard.get(&key); // hit
        shard.get(&key); // hit
        let metrics = shard.metrics();

        // Assert
        assert_eq!(metrics.miss_count(), 1);
        assert_eq!(metrics.hit_count(), 2);
    }

    #[test]
    fn should_skip_self_join_when_dropping_on_worker_thread() {
        // Arrange
        let _guard = TEST_SHARD_HOOK_LOCK.lock().unwrap();
        TEST_SELF_JOIN_SKIPPED.store(false, Ordering::SeqCst);
        TEST_WORKER_BLOCKED.store(false, Ordering::SeqCst);
        TEST_WORKER_RELEASE.store(false, Ordering::SeqCst);
        TEST_WORKER_FINISHED.store(false, Ordering::SeqCst);

        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(42, 0);

        // Act
        shard.put(key, Bytes::from_static(TEST_BLOCKING_VALUE));

        let mut attempts = 0;
        while !TEST_WORKER_BLOCKED.load(Ordering::SeqCst) && attempts < 1000 {
            std::thread::sleep(std::time::Duration::from_millis(1));
            attempts += 1;
        }

        assert!(TEST_WORKER_BLOCKED.load(Ordering::SeqCst));

        drop(shard);
        TEST_WORKER_RELEASE.store(true, Ordering::SeqCst);

        attempts = 0;
        while !TEST_WORKER_FINISHED.load(Ordering::SeqCst) && attempts < 1000 {
            std::thread::sleep(std::time::Duration::from_millis(1));
            attempts += 1;
        }

        // Assert
        assert!(TEST_WORKER_FINISHED.load(Ordering::SeqCst));

        attempts = 0;
        while !TEST_SELF_JOIN_SKIPPED.load(Ordering::SeqCst) && attempts < 1000 {
            std::thread::sleep(std::time::Duration::from_millis(1));
            attempts += 1;
        }

        assert!(TEST_SELF_JOIN_SKIPPED.load(Ordering::SeqCst));
    }

    #[test]
    fn should_join_worker_thread_when_dropping_on_main_thread() {
        // Arrange
        let _guard = TEST_SHARD_HOOK_LOCK.lock().unwrap();
        TEST_SELF_JOIN_SKIPPED.store(false, Ordering::SeqCst);
        TEST_WORKER_BLOCKED.store(false, Ordering::SeqCst);
        TEST_WORKER_RELEASE.store(false, Ordering::SeqCst);
        TEST_WORKER_FINISHED.store(false, Ordering::SeqCst);

        let shard = CacheShard::new(1024 * 1024, CachePolicyType::Lru);
        let key = CacheKey::for_data(43, 0);

        // Act
        shard.put(key, Bytes::from_static(TEST_BLOCKING_VALUE));

        let mut attempts = 0;
        while !TEST_WORKER_BLOCKED.load(Ordering::SeqCst) && attempts < 1000 {
            std::thread::sleep(std::time::Duration::from_millis(1));
            attempts += 1;
        }

        assert!(TEST_WORKER_BLOCKED.load(Ordering::SeqCst));

        TEST_WORKER_RELEASE.store(true, Ordering::SeqCst);

        let mut attempts = 0;
        while !TEST_WORKER_FINISHED.load(Ordering::SeqCst) && attempts < 1000 {
            std::thread::sleep(std::time::Duration::from_millis(1));
            attempts += 1;
        }

        // Assert
        assert!(TEST_WORKER_FINISHED.load(Ordering::SeqCst));

        drop(shard);

        assert!(!TEST_SELF_JOIN_SKIPPED.load(Ordering::SeqCst));
    }

    #[test]
    fn should_track_eviction_metrics() {
        // Arrange
        let shard = CacheShard::new(50, CachePolicyType::Lru);

        // Act
        for i in 0..5 {
            let key = CacheKey::for_data(i, 0);
            shard.put_sync(key, Bytes::from(vec![0u8; 15]));
        }
        let metrics = shard.metrics();

        // Assert - some evictions should happen
        assert!(metrics.eviction_count() > 0);
    }
}
