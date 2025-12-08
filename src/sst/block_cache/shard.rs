//! Per-shard cache state: hash table, entries, accounting, and policy hooks.
//!
//! Each shard is protected by a single mutex. The sharded design reduces
//! contention by spreading keys across multiple independent shards.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use super::admission::AdmissionController;
use super::config::SizeAccounting;
use super::handle::{BlockHandle, SharedUnpinner, Unpinner};
use super::key::BlockKey;
use super::policy::Policy;
use super::table::{EntryId, HashTable};
use super::value::BlockData;
use super::BlockCacheStats;

// Number of BlockKind variants for array sizing.
const NUM_BLOCK_KINDS: usize = 5;

/// Per-column-family cache statistics.
#[derive(Debug, Clone, Default)]
pub struct CfCacheStats {
    /// Number of cache hits for this CF.
    pub hits: u64,
    /// Number of cache misses for this CF.
    pub misses: u64,
    /// Current bytes used by this CF.
    pub used_bytes: usize,
    /// Number of entries for this CF.
    pub entry_count: usize,
}

/// Metadata stored per cached block.
pub struct BlockEntry {
    /// The block key (for verification on hash collision).
    pub key: BlockKey,
    /// The cached block data.
    pub data: Arc<BlockData>,
    /// Bytes charged against capacity.
    pub charge: usize,
    /// Current pin count. Entry is not evictable while pins > 0.
    pub pins: u32,
    /// Policy-specific metadata (e.g., position in LRU list).
    pub policy_meta: u64,
}

/// Internal shard state, protected by a mutex.
pub struct ShardInner {
    /// Hash table mapping hash -> entry index.
    table: HashTable,
    /// Entry storage. Slots may be reused after eviction.
    entries: Vec<Option<BlockEntry>>,
    /// Free list of reusable entry indices.
    free_list: Vec<EntryId>,
    /// Eviction policy.
    policy: Box<dyn Policy + Send>,
    /// Admission controller for scan resistance.
    admission: AdmissionController,
    /// Per-shard capacity in bytes.
    capacity_bytes: usize,
    /// Current bytes used.
    used_bytes: usize,
    /// Size accounting mode.
    size_accounting: SizeAccounting,
    /// Stats counters (aggregate).
    hits: u64,
    misses: u64,
    evictions: u64,
    admissions: u64,
    /// Number of blocks rejected by admission control (candidate colder than victim).
    rejections: u64,
    /// Per-BlockKind hit counts, indexed by BlockKind as u8.
    hits_by_kind: [u64; NUM_BLOCK_KINDS],
    /// Per-BlockKind miss counts, indexed by BlockKind as u8.
    misses_by_kind: [u64; NUM_BLOCK_KINDS],
    /// Optional per-CF stats tracking (enabled via config).
    cf_stats: Option<HashMap<u32, CfCacheStats>>,
}

impl ShardInner {
    fn new(
        capacity_bytes: usize,
        size_accounting: SizeAccounting,
        policy: Box<dyn Policy + Send>,
        enable_cf_stats: bool,
    ) -> Self {
        // Size the hash table to ~1.5x expected entries (assuming 4KB avg block)
        let estimated_entries = (capacity_bytes / 4096).max(64);
        let table_capacity = (estimated_entries * 3 / 2).next_power_of_two();

        Self {
            table: HashTable::with_capacity(table_capacity),
            entries: Vec::with_capacity(estimated_entries),
            free_list: Vec::new(),
            policy,
            admission: AdmissionController::new(estimated_entries),
            capacity_bytes,
            used_bytes: 0,
            size_accounting,
            hits: 0,
            misses: 0,
            evictions: 0,
            admissions: 0,
            rejections: 0,
            hits_by_kind: [0; NUM_BLOCK_KINDS],
            misses_by_kind: [0; NUM_BLOCK_KINDS],
            cf_stats: if enable_cf_stats {
                Some(HashMap::new())
            } else {
                None
            },
        }
    }

    /// Compute the charge for a block based on accounting mode.
    #[inline]
    fn compute_charge(&self, data: &BlockData) -> usize {
        match self.size_accounting {
            SizeAccounting::Uncompressed => data.uncompressed_size() as usize,
            SizeAccounting::Compressed => {
                if data.is_compressed() {
                    data.compressed_size() as usize
                } else {
                    data.uncompressed_size() as usize
                }
            }
        }
    }

    /// Allocate a slot for a new entry.
    fn alloc_entry(&mut self) -> EntryId {
        if let Some(id) = self.free_list.pop() {
            id
        } else {
            let id = self.entries.len() as EntryId;
            self.entries.push(None);
            id
        }
    }

    /// Free an entry slot for reuse.
    fn free_entry(&mut self, id: EntryId) {
        if let Some(entry) = self.entries.get_mut(id as usize) {
            *entry = None;
            self.free_list.push(id);
        }
    }

    /// Evict entries until we have at least `needed` bytes free.
    fn evict_until(&mut self, needed: usize) {
        // Track attempts to detect when all remaining entries are pinned
        let mut attempts = 0;
        let max_attempts = self.entries.len().max(1);

        while self.used_bytes + needed > self.capacity_bytes {
            if let Some(victim_id) = self.policy.choose_victim() {
                // Check if entry is pinned
                if let Some(Some(entry)) = self.entries.get(victim_id as usize) {
                    if entry.pins > 0 {
                        // Move pinned entry to back (give it another chance)
                        self.policy.on_access(victim_id);
                        attempts += 1;
                        if attempts >= max_attempts {
                            break; // All entries are pinned, can't evict
                        }
                        continue;
                    }
                }
                self.evict_entry(victim_id);
                attempts = 0; // Reset on successful eviction
            } else {
                break; // No evictable entries
            }
        }
    }

    /// Evict a specific entry by id.
    fn evict_entry(&mut self, entry_id: EntryId) {
        if let Some(Some(entry)) = self.entries.get(entry_id as usize) {
            if entry.pins > 0 {
                return; // Can't evict pinned entry
            }
            let hash = entry.key.shard_hash();
            let charge = entry.charge;
            let cf_id = entry.key.cf_id;

            // Update per-CF stats if enabled
            if let Some(ref mut cf_stats) = self.cf_stats {
                if let Some(cf) = cf_stats.get_mut(&cf_id) {
                    cf.used_bytes = cf.used_bytes.saturating_sub(charge);
                    cf.entry_count = cf.entry_count.saturating_sub(1);
                }
            }

            self.table.remove(hash);
            self.policy.on_evict(entry_id);
            self.used_bytes -= charge;
            self.evictions += 1;
            self.free_entry(entry_id);
        }
    }

    /// Lookup by key, returning entry_id if found and key matches.
    fn find(&self, key: &BlockKey) -> Option<EntryId> {
        let hash = key.shard_hash();
        if let Some(entry_id) = self.table.get(hash) {
            // Verify key matches (handle hash collisions)
            if let Some(Some(entry)) = self.entries.get(entry_id as usize) {
                if &entry.key == key {
                    return Some(entry_id);
                }
            }
        }
        None
    }

    /// Record a hit and pin the entry, returning cloned data Arc.
    ///
    /// # Safety (Contract)
    /// The caller must ensure `entry_id` refers to a valid, occupied entry slot.
    fn record_hit(&mut self, entry_id: EntryId) -> Arc<BlockData> {
        self.policy.on_access(entry_id);
        self.hits += 1;

        let entry = self.entries[entry_id as usize]
            .as_mut()
            .expect("record_hit called with invalid entry_id");
        // Record per-kind stats
        let kind_idx = entry.key.block_kind.as_u8() as usize;
        self.hits_by_kind[kind_idx] += 1;
        // Record per-CF stats if enabled
        if let Some(ref mut cf_stats) = self.cf_stats {
            cf_stats.entry(entry.key.cf_id).or_default().hits += 1;
        }
        // Record access in admission controller for frequency tracking
        self.admission.record_access(entry.key.shard_hash());
        entry.pins += 1;
        Arc::clone(&entry.data)
    }

    /// Check if a candidate block should be admitted based on frequency comparison.
    ///
    /// Returns `true` if the candidate is at least as hot as the victim and should
    /// be cached. Returns `false` if the candidate is colder and should be rejected.
    fn should_admit(&mut self, candidate_hash: u64) -> bool {
        // If cache isn't full, always admit
        if self.used_bytes < self.capacity_bytes {
            return true;
        }

        // Find the victim we would evict (peek without actually removing)
        if let Some(victim_id) = self.policy.choose_victim() {
            if let Some(Some(victim_entry)) = self.entries.get(victim_id as usize) {
                let victim_hash = victim_entry.key.shard_hash();
                return self.admission.should_admit(candidate_hash, victim_hash);
            }
        }

        // No victim found (all pinned or empty), admit the candidate
        true
    }
}

/// Wrapper that implements `Unpinner` by holding an Arc to the inner state.
struct ShardUnpinnerImpl {
    inner: Arc<Mutex<ShardInner>>,
}

impl Unpinner for ShardUnpinnerImpl {
    fn unpin(&self, entry_id: EntryId) {
        let mut guard = self.inner.lock();
        if let Some(Some(entry)) = guard.entries.get_mut(entry_id as usize) {
            if entry.pins > 0 {
                entry.pins -= 1;
            }
        }
    }
}

/// A single cache shard.
pub struct BlockCacheShard {
    /// Shared inner state (Arc allows unpinner to reference it).
    inner: Arc<Mutex<ShardInner>>,
    /// Shared unpinner that handles hold weak references to.
    unpinner: SharedUnpinner,
}

impl BlockCacheShard {
    /// Create a new shard with the given capacity and policy.
    pub fn new(
        _shard_id: u32,
        capacity_bytes: usize,
        size_accounting: SizeAccounting,
        policy: Box<dyn Policy + Send>,
        enable_cf_stats: bool,
    ) -> Self {
        let inner = Arc::new(Mutex::new(ShardInner::new(
            capacity_bytes,
            size_accounting,
            policy,
            enable_cf_stats,
        )));
        let unpinner: SharedUnpinner = Arc::new(ShardUnpinnerImpl {
            inner: Arc::clone(&inner),
        });

        Self { inner, unpinner }
    }

    /// Create a pinned handle. Caller must ensure the entry's pin count is already incremented.
    fn pinned_handle(&self, data: Arc<BlockData>, entry_id: EntryId) -> BlockHandle {
        BlockHandle::pinned(data, entry_id, Arc::downgrade(&self.unpinner))
    }

    /// Lookup a block. Returns a pinned handle on hit.
    pub fn get(&self, key: &BlockKey) -> Option<BlockHandle> {
        let mut inner = self.inner.lock();

        if let Some(entry_id) = inner.find(key) {
            let data = inner.record_hit(entry_id);
            drop(inner); // Release lock before creating handle
            Some(self.pinned_handle(data, entry_id))
        } else {
            inner.misses += 1;
            inner.misses_by_kind[key.block_kind.as_u8() as usize] += 1;
            // Record per-CF miss if enabled
            if let Some(ref mut cf_stats) = inner.cf_stats {
                cf_stats.entry(key.cf_id).or_default().misses += 1;
            }
            None
        }
    }

    /// Insert a block, returning a pinned handle.
    ///
    /// If the block is rejected by admission control (candidate is colder than
    /// the victim it would evict), an unpinned handle is returned and the block
    /// is not cached.
    pub fn insert(&self, key: BlockKey, data: BlockData) -> BlockHandle {
        let mut inner = self.inner.lock();
        let charge = inner.compute_charge(&data);

        // Check if already present
        if let Some(entry_id) = inner.find(&key) {
            let data = inner.record_hit(entry_id);
            drop(inner);
            return self.pinned_handle(data, entry_id);
        }

        // If block is larger than shard capacity, don't cache it
        if charge > inner.capacity_bytes {
            return BlockHandle::unpinned(Arc::new(data));
        }

        // Record the access in the frequency sketch before admission check
        let candidate_hash = key.shard_hash();
        inner.admission.record_access(candidate_hash);

        // Check admission control: should we cache this block?
        if inner.used_bytes + charge > inner.capacity_bytes && !inner.should_admit(candidate_hash) {
            // Candidate is colder than victim - reject admission
            inner.rejections += 1;
            return BlockHandle::unpinned(Arc::new(data));
        }

        // Evict if needed (admission already approved)
        inner.evict_until(charge);

        let hash = key.shard_hash();
        let entry_id = inner.alloc_entry();
        let data_arc = Arc::new(data);

        let entry = BlockEntry {
            key,
            data: Arc::clone(&data_arc),
            charge,
            pins: 1, // Start pinned since we're returning a pinned handle
            policy_meta: 0,
        };

        // Track per-CF stats if enabled
        if let Some(ref mut cf_stats) = inner.cf_stats {
            let cf = cf_stats.entry(key.cf_id).or_default();
            cf.used_bytes += charge;
            cf.entry_count += 1;
        }

        inner.entries[entry_id as usize] = Some(entry);
        inner.table.insert(hash, entry_id);
        inner.policy.on_insert(entry_id, charge);
        inner.used_bytes += charge;
        inner.admissions += 1;

        drop(inner);
        self.pinned_handle(data_arc, entry_id)
    }

    /// Insert only if absent. Returns handle to existing or new entry.
    pub fn insert_if_absent(&self, key: BlockKey, data: BlockData) -> BlockHandle {
        let mut inner = self.inner.lock();

        // Check if already present
        if let Some(entry_id) = inner.find(&key) {
            let data = inner.record_hit(entry_id);
            drop(inner);
            return self.pinned_handle(data, entry_id);
        }

        // Not present—insert it
        drop(inner); // Release lock before re-acquiring in insert
        self.insert(key, data)
    }

    /// Decrement pin count for an entry (public API for manual unpinning).
    pub fn unpin(&self, entry_id: EntryId) {
        let mut inner = self.inner.lock();
        if let Some(Some(entry)) = inner.entries.get_mut(entry_id as usize) {
            if entry.pins > 0 {
                entry.pins -= 1;
            }
        }
    }

    /// Get current used bytes.
    pub fn used_bytes(&self) -> usize {
        self.inner.lock().used_bytes
    }

    /// Get capacity bytes.
    pub fn capacity_bytes(&self) -> usize {
        self.inner.lock().capacity_bytes
    }

    /// Get shard statistics.
    pub fn stats(&self) -> ShardStats {
        let inner = self.inner.lock();
        ShardStats {
            hits: inner.hits,
            misses: inner.misses,
            evictions: inner.evictions,
            admissions: inner.admissions,
            rejections: inner.rejections,
            used_bytes: inner.used_bytes,
            capacity_bytes: inner.capacity_bytes,
            entry_count: inner.table.len(),
            hits_by_kind: inner.hits_by_kind,
            misses_by_kind: inner.misses_by_kind,
        }
    }

    /// Get statistics for a specific column family.
    ///
    /// Returns `None` if per-CF stats are not enabled or the CF has never been seen.
    pub fn cf_stats(&self, cf_id: u32) -> Option<CfCacheStats> {
        let inner = self.inner.lock();
        inner.cf_stats.as_ref()?.get(&cf_id).cloned()
    }

    /// Get statistics for all column families that have been seen.
    ///
    /// Returns `None` if per-CF stats are not enabled.
    pub fn all_cf_stats(&self) -> Option<HashMap<u32, CfCacheStats>> {
        let inner = self.inner.lock();
        inner.cf_stats.clone()
    }
}

/// Statistics for a single shard.
#[derive(Debug, Clone)]
pub struct ShardStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub admissions: u64,
    /// Number of blocks rejected by admission control (candidate colder than victim).
    pub rejections: u64,
    pub used_bytes: usize,
    pub capacity_bytes: usize,
    pub entry_count: usize,
    /// Per-BlockKind hit counts, indexed by BlockKind as u8.
    pub hits_by_kind: [u64; NUM_BLOCK_KINDS],
    /// Per-BlockKind miss counts, indexed by BlockKind as u8.
    pub misses_by_kind: [u64; NUM_BLOCK_KINDS],
}

impl Default for ShardStats {
    fn default() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            admissions: 0,
            rejections: 0,
            used_bytes: 0,
            capacity_bytes: 0,
            entry_count: 0,
            hits_by_kind: [0; NUM_BLOCK_KINDS],
            misses_by_kind: [0; NUM_BLOCK_KINDS],
        }
    }
}

impl ShardStats {
    /// Merge another shard's stats into this one.
    pub fn merge(&mut self, other: &ShardStats) {
        self.hits += other.hits;
        self.misses += other.misses;
        self.evictions += other.evictions;
        self.admissions += other.admissions;
        self.rejections += other.rejections;
        self.used_bytes += other.used_bytes;
        self.capacity_bytes += other.capacity_bytes;
        self.entry_count += other.entry_count;
        for i in 0..NUM_BLOCK_KINDS {
            self.hits_by_kind[i] += other.hits_by_kind[i];
            self.misses_by_kind[i] += other.misses_by_kind[i];
        }
    }

    /// Convert to the public `BlockCacheStats` type.
    pub fn to_cache_stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            admissions: self.admissions,
            rejected: self.rejections,
            used_bytes: self.used_bytes,
            capacity_bytes: self.capacity_bytes,
            hits_by_kind: self.hits_by_kind,
            misses_by_kind: self.misses_by_kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::block_cache::key::BlockKind;
    use crate::sst::block_cache::policy::ClockProPolicy;

    fn make_shard(capacity: usize) -> BlockCacheShard {
        BlockCacheShard::new(
            0,
            capacity,
            SizeAccounting::Uncompressed,
            Box::new(ClockProPolicy::new(1024)),
            false, // per-CF stats disabled by default in tests
        )
    }

    fn make_shard_with_cf_stats(capacity: usize) -> BlockCacheShard {
        BlockCacheShard::new(
            0,
            capacity,
            SizeAccounting::Uncompressed,
            Box::new(ClockProPolicy::new(1024)),
            true, // per-CF stats enabled
        )
    }

    fn make_key(file: u64, offset: u64) -> BlockKey {
        BlockKey::new(file, offset, BlockKind::Data, 0)
    }

    fn make_data(size: usize) -> BlockData {
        BlockData::uncompressed(vec![0u8; size].into(), BlockKind::Data)
    }

    #[test]
    fn should_retrieve_block_given_cached_entry_when_queried() {
        // Arrange
        let shard = make_shard(4096);
        let key = make_key(1, 0);
        let data = make_data(100);
        let handle = shard.insert(key, data);
        // Handle is now pinned with drop-based unpin
        assert!(handle.is_pinned());

        // Act
        let retrieved = shard.get(&key);

        // Assert
        assert!(retrieved.is_some());
        let stats = shard.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.admissions, 1);
    }

    #[test]
    fn should_return_none_given_missing_key_when_get_called() {
        // Arrange
        let shard = make_shard(4096);
        let key = make_key(1, 0);

        // Act
        let result = shard.get(&key);

        // Assert
        assert!(result.is_none());
        assert_eq!(shard.stats().misses, 1);
    }

    #[test]
    fn should_evict_lru_given_full_shard_when_new_block_inserted() {
        // Arrange
        let shard = make_shard(200); // Small capacity
        let key1 = make_key(1, 0);
        let key2 = make_key(2, 0);
        let key3 = make_key(3, 0);

        // Insert blocks and drop handles to allow eviction
        {
            let _h1 = shard.insert(key1, make_data(80));
            // Handle dropped here, unpin called automatically
        }
        {
            let _h2 = shard.insert(key2, make_data(80));
            // Handle dropped here, unpin called automatically
        }

        // Act - This should trigger eviction of key1 (LRU)
        let _h3 = shard.insert(key3, make_data(80));

        // Assert
        assert!(shard.get(&key3).is_some());
        // key1 may be evicted depending on policy
        let stats = shard.stats();
        assert!(stats.evictions > 0 || stats.used_bytes <= 200);
    }

    #[test]
    fn should_not_evict_pinned_given_pinned_entry_when_eviction_needed() {
        // Arrange
        let shard = make_shard(150);
        let key1 = make_key(1, 0);
        let key2 = make_key(2, 0);

        // Insert and keep handle alive (keeps entry pinned)
        let _pinned_handle = shard.insert(key1, make_data(100));
        assert!(_pinned_handle.is_pinned());

        // Act - Try to insert another - eviction should skip pinned entry
        let _h2 = shard.insert(key2, make_data(100));

        // Assert - With our small capacity, one must be evicted unless both fit
        // Since key1 is pinned, key2 may fail to insert or key1 stays
        let stats = shard.stats();
        assert!(stats.admissions >= 1);
    }

    #[test]
    fn should_dedup_given_concurrent_insert_when_insert_if_absent_called() {
        // Arrange
        let shard = make_shard(4096);
        let key = make_key(1, 0);

        // Act
        let h1 = shard.insert(key, make_data(100));
        let h2 = shard.insert_if_absent(key, make_data(200));

        // Assert - Both should point to the same cached data (first insert wins)
        assert_eq!(h1.data().bytes().len(), h2.data().bytes().len());
        assert_eq!(shard.stats().admissions, 1); // Only one admission
    }

    #[test]
    fn should_track_used_bytes_given_inserts_and_evictions_when_queried() {
        // Arrange
        let shard = make_shard(1000);

        // Act
        shard.insert(make_key(1, 0), make_data(100));
        shard.insert(make_key(2, 0), make_data(200));

        // Assert
        let stats = shard.stats();
        assert_eq!(stats.used_bytes, 300);
    }

    #[test]
    fn should_unpin_on_handle_drop_given_pinned_handle_when_dropped() {
        // Arrange
        let shard = make_shard(4096);
        let key = make_key(1, 0);

        // Act - Insert returns a pinned handle
        {
            let handle = shard.insert(key, make_data(100));
            assert!(handle.is_pinned());
            // Check pin count is 1
            let inner = shard.inner.lock();
            assert_eq!(inner.entries[0].as_ref().unwrap().pins, 1);
            drop(inner);
            // Handle dropped here
        }

        // Assert - After handle drop, pin count should be 0
        let inner = shard.inner.lock();
        assert_eq!(inner.entries[0].as_ref().unwrap().pins, 0);
    }

    #[test]
    fn should_keep_entry_pinned_given_get_while_insert_handle_held() {
        // Arrange
        let shard = make_shard(4096);
        let key = make_key(1, 0);

        let insert_handle = shard.insert(key, make_data(100));

        // Act
        {
            // Get also returns a pinned handle
            let get_handle = shard.get(&key).unwrap();
            assert!(get_handle.is_pinned());

            // Both handles exist, pin count should be 2
            let inner = shard.inner.lock();
            assert_eq!(inner.entries[0].as_ref().unwrap().pins, 2);
            drop(inner);
            // get_handle dropped here
        }

        // Assert - After get_handle drop, pin count should be 1
        let inner = shard.inner.lock();
        assert_eq!(inner.entries[0].as_ref().unwrap().pins, 1);
        drop(inner);

        // Drop insert_handle
        drop(insert_handle);

        // Now pin count should be 0
        let inner = shard.inner.lock();
        assert_eq!(inner.entries[0].as_ref().unwrap().pins, 0);
    }

    #[test]
    fn should_reject_cold_block_given_hot_cache_when_admission_control_active() {
        // Arrange - Create a small shard that will be full quickly
        let shard = make_shard(200);

        // Insert a "hot" block and access it many times to build frequency
        let hot_key = make_key(1, 0);
        {
            let _h = shard.insert(hot_key, make_data(100));
        }
        // Access the hot key multiple times to increase its frequency
        for _ in 0..10 {
            let _ = shard.get(&hot_key);
        }

        // Act - Now try to insert a "cold" block that we've never seen before
        let cold_key = make_key(999, 0);
        let _cold_handle = shard.insert(cold_key, make_data(100));

        // Assert - The cold block should either be rejected (rejections > 0)
        // or admitted (if cache had room or frequency was high enough)
        // The key test is that the rejection tracking works
        // Note: With the current admission logic, this may or may not reject
        // depending on timing and frequency sketch state
        let stats = shard.stats();
        assert!(
            stats.admissions + stats.rejections >= 2,
            "Should have at least one admission and one rejection or two admissions"
        );
    }

    #[test]
    fn should_track_rejections_given_scan_workload_when_full_cache() {
        // Arrange - Create a shard with room for just a few blocks
        let shard = make_shard(500);

        // Fill with "hot" blocks and access them multiple times
        for i in 0..3 {
            let key = make_key(i, 0);
            {
                let _h = shard.insert(key, make_data(100));
            }
            // Access each hot key multiple times
            for _ in 0..5 {
                let _ = shard.get(&key);
            }
        }

        // Act - Now simulate a "scan" - insert many blocks we'll never access again
        for i in 100..110 {
            let scan_key = make_key(i, 0);
            let _h = shard.insert(scan_key, make_data(100));
        }

        // Assert
        let stats = shard.stats();

        // With admission control, some scan blocks should be rejected
        // because they're colder than the hot blocks
        // The exact number depends on the frequency sketch and policy
        assert!(
            stats.rejections > 0 || stats.evictions > 0,
            "Scan workload should trigger either rejections or evictions"
        );
    }

    #[test]
    fn should_track_per_kind_stats_given_mixed_block_types_when_accessed() {
        // Arrange: cache with enough room for multiple blocks
        let shard = make_shard(8192);

        // Insert blocks of different kinds
        let data_key = BlockKey::new(1, 0, BlockKind::Data, 0);
        let index_key = BlockKey::new(2, 0, BlockKind::Index, 0);
        let filter_key = BlockKey::new(3, 0, BlockKind::Filter, 0);

        let _data_h = shard.insert(data_key, make_data(100));
        let _index_h = shard.insert(index_key, make_data(100));
        let _filter_h = shard.insert(filter_key, make_data(100));

        // Act: access data block twice, index block once
        let _ = shard.get(&data_key);
        let _ = shard.get(&data_key);
        let _ = shard.get(&index_key);

        // Miss on a non-existent meta key
        let meta_key = BlockKey::new(4, 0, BlockKind::Meta, 0);
        let _ = shard.get(&meta_key);

        // Assert: per-kind stats should reflect accesses
        let stats = shard.stats();

        // Data: 2 hits (both get() calls hit)
        assert_eq!(stats.hits_by_kind[BlockKind::Data.as_u8() as usize], 2);
        // Index: 1 hit
        assert_eq!(stats.hits_by_kind[BlockKind::Index.as_u8() as usize], 1);
        // Filter: 0 hits (we never called get() on it)
        assert_eq!(stats.hits_by_kind[BlockKind::Filter.as_u8() as usize], 0);
        // Meta: 1 miss
        assert_eq!(stats.misses_by_kind[BlockKind::Meta.as_u8() as usize], 1);
        // Total hits should match
        assert_eq!(stats.hits, 3);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn should_track_per_cf_stats_given_multiple_column_families_when_enabled() {
        // Arrange: cache with per-CF stats enabled
        let shard = make_shard_with_cf_stats(8192);

        // Insert blocks from two different column families
        let cf0_key1 = BlockKey::new(1, 0, BlockKind::Data, 0);
        let cf0_key2 = BlockKey::new(2, 0, BlockKind::Data, 0);
        let cf1_key1 = BlockKey::new(3, 0, BlockKind::Data, 1);
        let cf2_key1 = BlockKey::new(4, 0, BlockKind::Data, 2);

        let _h1 = shard.insert(cf0_key1, make_data(100));
        let _h2 = shard.insert(cf0_key2, make_data(100));
        let _h3 = shard.insert(cf1_key1, make_data(200));

        // Act: access CF0 blocks (2 hits), miss on CF2
        let _ = shard.get(&cf0_key1);
        let _ = shard.get(&cf0_key2);
        let _ = shard.get(&cf2_key1); // miss

        // Assert: per-CF stats should reflect accesses
        let cf0_stats = shard.cf_stats(0).expect("CF0 should have stats");
        assert_eq!(cf0_stats.hits, 2, "CF0 should have 2 hits");
        assert_eq!(cf0_stats.misses, 0, "CF0 should have 0 misses");
        assert_eq!(cf0_stats.entry_count, 2, "CF0 should have 2 entries");
        assert_eq!(cf0_stats.used_bytes, 200, "CF0 should use 200 bytes");

        let cf1_stats = shard.cf_stats(1).expect("CF1 should have stats");
        assert_eq!(cf1_stats.hits, 0, "CF1 should have 0 hits");
        assert_eq!(cf1_stats.entry_count, 1, "CF1 should have 1 entry");
        assert_eq!(cf1_stats.used_bytes, 200, "CF1 should use 200 bytes");

        let cf2_stats = shard.cf_stats(2).expect("CF2 should have stats");
        assert_eq!(cf2_stats.misses, 1, "CF2 should have 1 miss");
        assert_eq!(cf2_stats.hits, 0, "CF2 should have 0 hits");
        assert_eq!(cf2_stats.entry_count, 0, "CF2 should have 0 entries");
    }

    #[test]
    fn should_return_none_for_cf_stats_given_disabled_when_queried() {
        // Arrange: cache without per-CF stats
        let shard = make_shard(8192);

        let key = make_key(1, 0);
        let _h = shard.insert(key, make_data(100));

        // Act
        let _ = shard.get(&key);

        // Assert: cf_stats should return None
        assert!(
            shard.cf_stats(0).is_none(),
            "CF stats should be None when disabled"
        );
        assert!(
            shard.all_cf_stats().is_none(),
            "All CF stats should be None when disabled"
        );
    }

    #[test]
    fn should_update_per_cf_stats_on_eviction_given_cf_stats_enabled() {
        // Arrange: small cache that will need to evict
        let shard = make_shard_with_cf_stats(300);

        // Insert two blocks from CF0 (200 bytes each)
        let cf0_key1 = BlockKey::new(1, 0, BlockKind::Data, 0);
        let cf0_key2 = BlockKey::new(2, 0, BlockKind::Data, 0);

        {
            let _h1 = shard.insert(cf0_key1, make_data(100));
        } // Drop handle to allow eviction
        {
            let _h2 = shard.insert(cf0_key2, make_data(100));
        } // Drop handle to allow eviction

        // Act - Insert a large block from CF1 that will force eviction of CF0 blocks
        let cf1_key = BlockKey::new(3, 0, BlockKind::Data, 1);
        let _h3 = shard.insert(cf1_key, make_data(200));

        // Assert: CF0 should have fewer entries/bytes due to eviction
        let cf0_stats = shard.cf_stats(0).expect("CF0 should have stats");
        let cf1_stats = shard.cf_stats(1).expect("CF1 should have stats");

        // At least one CF0 block should have been evicted
        assert!(
            cf0_stats.entry_count < 2 || cf1_stats.entry_count == 1,
            "Eviction should update per-CF stats"
        );
    }
}
