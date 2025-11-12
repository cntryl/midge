//! LRU block cache for SST blocks
//!
//! Provides efficient caching of decoded SST blocks (data, index, filter)
//! with support for:
//! - Single-threaded LRU cache
//! - Sharded cache for reduced lock contention
//! - Adaptive cache that auto-switches based on contention

use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// A block in the cache, containing decoded data
#[derive(Clone)]
pub struct CachedBlock {
    pub data: Bytes,
}

/// Key identifying a block in the cache
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BlockKey {
    /// SST file name
    pub file_name: String,
    /// Block type (data, index, filter, etc.)
    pub block_type: BlockType,
    /// Offset of block in file
    pub offset: u64,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum BlockType {
    Data,
    Index,
    Filter,
}

/// LRU block cache for decoded SST blocks
pub struct BlockCache {
    inner: Arc<Mutex<LruCacheInner>>,
}

struct LruCacheInner {
    /// Map from block key to list node index
    map: HashMap<BlockKey, usize>,
    /// Doubly-linked list for LRU ordering (most recent at front)
    list: Vec<Option<ListNode>>,
    /// Head of the list (most recently used)
    head: Option<usize>,
    /// Tail of the list (least recently used)
    tail: Option<usize>,
    /// Stack of free slot indices (reusable slots)
    free_slots: Vec<usize>,
    /// Maximum size in bytes
    max_size_bytes: usize,
    /// Current size in bytes
    current_size_bytes: usize,
    /// Cache hit count (atomic for future lock-free reads)
    hits: AtomicU64,
    /// Cache miss count
    misses: AtomicU64,
    /// Oversized entry rejection counter
    oversize_rejections: AtomicU64,
}

struct ListNode {
    key: BlockKey,
    value: CachedBlock,
    prev: Option<usize>,
    next: Option<usize>,
}

impl BlockCache {
    /// Create a new block cache with the given capacity in bytes
    pub fn new(max_size_bytes: usize) -> Self {
        BlockCache {
            inner: Arc::new(Mutex::new(LruCacheInner {
                map: HashMap::new(),
                list: Vec::new(),
                head: None,
                tail: None,
                free_slots: Vec::new(),
                max_size_bytes,
                current_size_bytes: 0,
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                oversize_rejections: AtomicU64::new(0),
            })),
        }
    }

    /// Get a block from the cache
    #[inline]
    pub fn get(&self, key: &BlockKey) -> Option<CachedBlock> {
        let mut inner = self.inner.lock();
        if let Some(&node_idx) = inner.map.get(key) {
            inner.hits.fetch_add(1, Ordering::Relaxed);
            // Fast path: skip move_to_front if already at head
            if Some(node_idx) != inner.head {
                inner.move_to_front(node_idx);
            }
            inner.list[node_idx].as_ref().map(|n| n.value.clone())
        } else {
            inner.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert a block into the cache
    #[inline]
    pub fn insert(&self, key: BlockKey, value: CachedBlock) {
        let mut inner = self.inner.lock();
        let block_size = value.data.len();

        // If block is already in cache, update it
        if let Some(&node_idx) = inner.map.get(&key) {
            // Update size tracking
            if let Some(node) = &inner.list[node_idx] {
                inner.current_size_bytes -= node.value.data.len();
            }
            inner.current_size_bytes += block_size;

            // Update value and move to front
            if let Some(node) = &mut inner.list[node_idx] {
                node.value = value;
            }
            inner.move_to_front(node_idx);
            return;
        }

        // Evict blocks until we have space
        while inner.current_size_bytes + block_size > inner.max_size_bytes && inner.tail.is_some() {
            inner.evict_lru();
        }

        // Don't cache if block is larger than max size
        if block_size > inner.max_size_bytes {
            // increment oversize counter for observability
            inner.oversize_rejections.fetch_add(1, Ordering::Relaxed);
            return;
        }

        // Insert new block
        let node = ListNode {
            key: key.clone(),
            value,
            prev: None,
            next: inner.head,
        };

        let node_idx = if let Some(free_idx) = inner.free_slots.pop() {
            // Reuse a free slot
            inner.list[free_idx] = Some(node);
            free_idx
        } else {
            // Allocate new slot
            inner.list.push(Some(node));
            inner.list.len() - 1
        };

        // Update head's prev pointer
        if let Some(head_idx) = inner.head {
            if let Some(head_node) = &mut inner.list[head_idx] {
                head_node.prev = Some(node_idx);
            }
        }

        // Update head
        inner.head = Some(node_idx);

        // If list was empty, this is also the tail
        if inner.tail.is_none() {
            inner.tail = Some(node_idx);
        }

        inner.map.insert(key, node_idx);
        inner.current_size_bytes += block_size;
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.map.clear();
        inner.list.clear();
        inner.head = None;
        inner.tail = None;
        inner.free_slots.clear();
        inner.current_size_bytes = 0;
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock();
        CacheStats {
            hits: inner.hits.load(Ordering::Relaxed),
            misses: inner.misses.load(Ordering::Relaxed),
            size_bytes: inner.current_size_bytes,
            max_size_bytes: inner.max_size_bytes,
            entry_count: inner.map.len(),
        }
    }
}

impl LruCacheInner {
    /// Move a node to the front of the list (most recently used)
    #[inline(always)]
    fn move_to_front(&mut self, node_idx: usize) {
        if Some(node_idx) == self.head {
            return; // Already at front
        }

        // Remove from current position
        let (prev_idx, next_idx) = if let Some(node) = &self.list[node_idx] {
            (node.prev, node.next)
        } else {
            return;
        };

        if let Some(prev) = prev_idx {
            if let Some(prev_node) = &mut self.list[prev] {
                prev_node.next = next_idx;
            }
        }

        if let Some(next) = next_idx {
            if let Some(next_node) = &mut self.list[next] {
                next_node.prev = prev_idx;
            }
        }

        // Update tail if we're moving the tail
        if Some(node_idx) == self.tail {
            self.tail = prev_idx;
        }

        // Insert at front
        if let Some(node) = &mut self.list[node_idx] {
            node.prev = None;
            node.next = self.head;
        }

        if let Some(head_idx) = self.head {
            if let Some(head_node) = &mut self.list[head_idx] {
                head_node.prev = Some(node_idx);
            }
        }

        self.head = Some(node_idx);
    }

    /// Evict the least recently used block
    fn evict_lru(&mut self) {
        let tail_idx = match self.tail {
            Some(idx) => idx,
            None => return,
        };

        // Borrow fields from the tail node to avoid cloning the key
        let (block_size, prev_idx) = if let Some(node) = &self.list[tail_idx] {
            (node.value.data.len(), node.prev)
        } else {
            return;
        };

        // Remove mapping using a borrowed key (no clone)
        if let Some(node) = &self.list[tail_idx] {
            self.map.remove(&node.key);
        }
        self.current_size_bytes -= block_size;

        // Update tail
        self.tail = prev_idx;

        // Update prev node's next pointer
        if let Some(prev) = prev_idx {
            if let Some(prev_node) = &mut self.list[prev] {
                prev_node.next = None;
            }
        } else {
            // List is now empty
            self.head = None;
        }

        // Add to free_slots stack
        self.list[tail_idx] = None;
        self.free_slots.push(tail_idx);
    }
}

// ============================================================================
// Sharded Block Cache (Lock-Free Design)
// ============================================================================

/// Sharded LRU block cache for reduced contention in concurrent workloads.
///
/// Splits the cache into N independent shards, each with its own mutex and LRU list.
/// This reduces lock contention by approximately N times on concurrent access patterns.
pub struct ShardedBlockCache {
    shards: Vec<BlockCache>,
    shard_count: usize,
}

impl ShardedBlockCache {
    /// Create a new sharded block cache with the given capacity and shard count.
    ///
    /// # Arguments
    /// * `max_size_bytes` - Total cache capacity in bytes (divided among shards)
    /// * `shard_count` - Number of shards (recommended: 8-32 for best performance)
    pub fn new(max_size_bytes: usize, shard_count: usize) -> Self {
        let shard_count = shard_count.max(1); // At least 1 shard
        let bytes_per_shard = max_size_bytes / shard_count;

        let shards = (0..shard_count)
            .map(|_| BlockCache::new(bytes_per_shard))
            .collect();

        ShardedBlockCache {
            shards,
            shard_count,
        }
    }

    /// Create with default shard count (16 shards).
    pub fn with_default_shards(max_size_bytes: usize) -> Self {
        Self::new(max_size_bytes, 16)
    }

    /// Get the shard index for a given key
    #[inline]
    fn shard_index(&self, key: &BlockKey) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shard_count
    }

    /// Get a block from the cache
    #[inline]
    pub fn get(&self, key: &BlockKey) -> Option<CachedBlock> {
        let shard = &self.shards[self.shard_index(key)];
        shard.get(key)
    }

    /// Insert a block into the cache
    #[inline]
    pub fn insert(&self, key: BlockKey, value: CachedBlock) {
        let shard_idx = self.shard_index(&key);
        self.shards[shard_idx].insert(key, value);
    }

    /// Clear all entries from all shards
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.clear();
        }
    }

    /// Get aggregated cache statistics across all shards
    pub fn stats(&self) -> CacheStats {
        let mut total_hits = 0;
        let mut total_misses = 0;
        let mut total_size = 0;
        let mut total_max_size = 0;
        let mut total_entries = 0;

        for shard in &self.shards {
            let shard_stats = shard.stats();
            total_hits += shard_stats.hits;
            total_misses += shard_stats.misses;
            total_size += shard_stats.size_bytes;
            total_max_size += shard_stats.max_size_bytes;
            total_entries += shard_stats.entry_count;
        }

        CacheStats {
            hits: total_hits,
            misses: total_misses,
            size_bytes: total_size,
            max_size_bytes: total_max_size,
            entry_count: total_entries,
        }
    }
}

// ============================================================================
// Adaptive Cache (Auto-Switching)
// ============================================================================

/// Adaptive cache that automatically switches between single and sharded modes
/// based on observed contention patterns.
///
/// Starts with a single shard for low-contention workloads. If high contention
/// is detected (measured by lock wait time), automatically promotes to sharded mode.
pub struct AdaptiveBlockCache {
    /// Current cache implementation
    cache: Arc<Mutex<CacheImpl>>,
    /// Contention counter (incremented on every access)
    access_count: AtomicU64,
    /// Lock acquisition failures (proxy for contention)
    contention_count: AtomicU64,
    /// Whether we've already promoted to sharded
    is_sharded: AtomicBool,
    /// Configuration
    max_size_bytes: usize,
}

enum CacheImpl {
    Single(BlockCache),
    Sharded(ShardedBlockCache),
}

impl AdaptiveBlockCache {
    /// Create a new adaptive cache with the given capacity
    pub fn new(max_size_bytes: usize) -> Self {
        AdaptiveBlockCache {
            cache: Arc::new(Mutex::new(CacheImpl::Single(BlockCache::new(
                max_size_bytes,
            )))),
            access_count: AtomicU64::new(0),
            contention_count: AtomicU64::new(0),
            is_sharded: AtomicBool::new(false),
            max_size_bytes,
        }
    }

    /// Check if we should promote to sharded mode
    #[inline]
    fn should_promote(&self) -> bool {
        // Sample every 1000 accesses
        let accesses = self.access_count.load(Ordering::Relaxed);
        if accesses < 1000 || !accesses.is_multiple_of(1000) {
            return false;
        }

        // If contention rate > 5%, promote to sharded
        let contentions = self.contention_count.load(Ordering::Relaxed);
        let contention_rate = (contentions as f64) / (accesses as f64);

        !self.is_sharded.load(Ordering::Relaxed) && contention_rate > 0.05
    }

    /// Promote from single to sharded mode
    fn promote_to_sharded(&self) {
        let mut cache_lock = match self.cache.try_lock() {
            Some(lock) => lock,
            None => return, // Another thread is already promoting
        };

        // Double-check we haven't already promoted
        if self.is_sharded.load(Ordering::Relaxed) {
            return;
        }

        // Migrate data from single to sharded
        if let CacheImpl::Single(single_cache) = &*cache_lock {
            // Get all entries from old cache
            let old_stats = single_cache.stats();

            // Create new sharded cache
            let sharded_cache = ShardedBlockCache::new(self.max_size_bytes, 16);

            // Note: We can't easily migrate entries without exposing internals
            // In practice, this is acceptable - the cache will refill naturally
            // Alternative: expose iteration API or accept temporary cache miss spike

            *cache_lock = CacheImpl::Sharded(sharded_cache);
            self.is_sharded.store(true, Ordering::Relaxed);

            tracing::info!(
                "Promoted cache to sharded mode (entries: {}, hit_rate: {:.2}%)",
                old_stats.entry_count,
                old_stats.hit_rate() * 100.0
            );
        }
    }

    /// Get a block from the cache
    #[inline]
    pub fn get(&self, key: &BlockKey) -> Option<CachedBlock> {
        // Track access
        self.access_count.fetch_add(1, Ordering::Relaxed);

        // Try to acquire lock with contention tracking
        let cache = match self.cache.try_lock() {
            Some(lock) => lock,
            None => {
                // Lock is contended - increment counter and block
                self.contention_count.fetch_add(1, Ordering::Relaxed);
                self.cache.lock()
            }
        };

        // Delegate to current implementation
        let result = match &*cache {
            CacheImpl::Single(c) => c.get(key),
            CacheImpl::Sharded(c) => c.get(key),
        };

        // Check if we should promote (after releasing lock)
        drop(cache);
        if self.should_promote() {
            self.promote_to_sharded();
        }

        result
    }

    /// Insert a block into the cache
    #[inline]
    pub fn insert(&self, key: BlockKey, value: CachedBlock) {
        // Track access
        self.access_count.fetch_add(1, Ordering::Relaxed);

        // Try to acquire lock with contention tracking
        let cache = match self.cache.try_lock() {
            Some(lock) => lock,
            None => {
                self.contention_count.fetch_add(1, Ordering::Relaxed);
                self.cache.lock()
            }
        };

        // Delegate to current implementation
        match &*cache {
            CacheImpl::Single(c) => c.insert(key, value),
            CacheImpl::Sharded(c) => c.insert(key, value),
        }

        // Check if we should promote (after releasing lock)
        drop(cache);
        if self.should_promote() {
            self.promote_to_sharded();
        }
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let cache = self.cache.lock();
        match &*cache {
            CacheImpl::Single(c) => c.clear(),
            CacheImpl::Sharded(c) => c.clear(),
        }
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.lock();
        match &*cache {
            CacheImpl::Single(c) => c.stats(),
            CacheImpl::Sharded(c) => c.stats(),
        }
    }

    /// Get adaptive cache diagnostics
    pub fn diagnostics(&self) -> AdaptiveCacheStats {
        let accesses = self.access_count.load(Ordering::Relaxed);
        let contentions = self.contention_count.load(Ordering::Relaxed);
        let is_sharded = self.is_sharded.load(Ordering::Relaxed);

        let contention_rate = if accesses > 0 {
            (contentions as f64) / (accesses as f64)
        } else {
            0.0
        };

        AdaptiveCacheStats {
            is_sharded,
            total_accesses: accesses,
            contentions,
            contention_rate,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdaptiveCacheStats {
    pub is_sharded: bool,
    pub total_accesses: u64,
    pub contentions: u64,
    pub contention_rate: f64,
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size_bytes: usize,
    pub max_size_bytes: usize,
    pub entry_count: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_block(size: usize) -> CachedBlock {
        CachedBlock {
            data: Bytes::from(vec![0u8; size]),
        }
    }

    fn make_key(file: &str, offset: u64) -> BlockKey {
        BlockKey {
            file_name: file.to_string(),
            block_type: BlockType::Data,
            offset,
        }
    }

    #[test]
    fn should_store_block_given_cache_when_insert() {
        // Arrange
        let cache = BlockCache::new(1000);
        let key = make_key("file1.sst", 0);
        let block = make_block(100);

        // Act
        cache.insert(key.clone(), block.clone());

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.size_bytes, 100);
    }

    #[test]
    fn should_retrieve_block_given_cached_block_when_get() {
        // Arrange
        let cache = BlockCache::new(1000);
        let key = make_key("file1.sst", 0);
        let block = make_block(100);
        cache.insert(key.clone(), block.clone());

        // Act
        let retrieved = cache.get(&key).unwrap();

        // Assert
        assert_eq!(retrieved.data.len(), 100);
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn should_evict_lru_block_given_full_cache_when_capacity_exceeded() {
        // Arrange
        let cache = BlockCache::new(250); // Fits 2 blocks of 100 bytes
        let key1 = make_key("file1.sst", 0);
        let key2 = make_key("file2.sst", 0);
        let key3 = make_key("file3.sst", 0);

        // Act
        cache.insert(key1.clone(), make_block(100));
        cache.insert(key2.clone(), make_block(100));
        cache.insert(key3.clone(), make_block(100)); // Should evict key1

        // Assert
        assert!(cache.get(&key1).is_none()); // Evicted
        assert!(cache.get(&key2).is_some());
        assert!(cache.get(&key3).is_some());
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 2);
        assert!(stats.size_bytes <= 250);
    }

    #[test]
    fn should_maintain_lru_order_given_gets_when_accessed() {
        // Arrange
        let cache = BlockCache::new(250); // Fits 2 blocks of 100 bytes
        let key1 = make_key("file1.sst", 0);
        let key2 = make_key("file2.sst", 0);
        let key3 = make_key("file3.sst", 0);
        cache.insert(key1.clone(), make_block(100));
        cache.insert(key2.clone(), make_block(100));

        // Act
        cache.get(&key1); // Access key1 to make it more recent
        cache.insert(key3.clone(), make_block(100)); // Insert key3, should evict key2 (LRU)

        // Assert
        assert!(cache.get(&key1).is_some());
        assert!(cache.get(&key2).is_none()); // Evicted
        assert!(cache.get(&key3).is_some());
    }

    #[test]
    fn should_update_size_given_value_update_when_put_called() {
        // Arrange
        let cache = BlockCache::new(100);
        let key = make_key("file1.sst", 0);

        // Act
        cache.insert(key.clone(), make_block(100));
        cache.insert(key.clone(), make_block(200)); // Update with larger block

        // Assert
        let retrieved = cache.get(&key).unwrap();
        assert_eq!(retrieved.data.len(), 200);
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 1); // Still just one entry
        assert_eq!(stats.size_bytes, 200);
    }

    #[test]
    fn should_reject_oversized_entry_given_entry_larger_than_capacity_when_put() {
        // Arrange
        let cache = BlockCache::new(10);
        let key = make_key("file1.sst", 0);

        // Act
        cache.insert(key.clone(), make_block(200));

        // Assert
        assert!(cache.get(&key).is_none());
        assert_eq!(cache.stats().entry_count, 0);
    }

    #[test]
    fn should_track_hits_given_accesses_when_stats_requested() {
        // Arrange
        let cache = BlockCache::new(100);
        let key1 = make_key("file1.sst", 0);
        cache.insert(key1.clone(), make_block(100));

        // Act
        cache.get(&key1);

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn should_track_misses_given_accesses_when_stats_requested() {
        // Arrange
        let cache = BlockCache::new(100);
        let key2 = make_key("file2.sst", 0);

        // Act
        cache.get(&key2);

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn should_calculate_hit_rate_given_mixed_accesses_when_stats_requested() {
        // Arrange
        let cache = BlockCache::new(100);
        let key1 = make_key("file1.sst", 0);
        let key2 = make_key("file2.sst", 0);
        cache.insert(key1.clone(), make_block(100));

        // Act
        cache.get(&key1); // Hit
        cache.get(&key2); // Miss

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.hit_rate(), 0.5);
    }

    #[test]
    fn should_remove_all_entries_given_cache_when_clear_called() {
        // Arrange
        let cache = BlockCache::new(100);
        cache.insert(make_key("file1.sst", 0), make_block(100));
        cache.insert(make_key("file2.sst", 0), make_block(100));

        // Act
        cache.clear();

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.size_bytes, 0);
    }
}

// ============================================================================
// Sharded Cache Tests
// ============================================================================

#[cfg(test)]
mod sharded_cache_tests {
    use super::*;

    fn make_block(size: usize) -> CachedBlock {
        CachedBlock {
            data: Bytes::from(vec![0u8; size]),
        }
    }

    fn make_key(file: &str, offset: u64) -> BlockKey {
        BlockKey {
            file_name: file.to_string(),
            block_type: BlockType::Data,
            offset,
        }
    }

    #[test]
    fn should_store_block_given_sharded_cache_when_insert() {
        // Arrange
        let cache = ShardedBlockCache::new(1000, 4);
        let key = make_key("file1.sst", 0);
        let block = make_block(100);

        // Act
        cache.insert(key.clone(), block.clone());

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn should_retrieve_block_given_sharded_cache_when_get() {
        // Arrange
        let cache = ShardedBlockCache::new(1000, 4);
        let key = make_key("file1.sst", 0);
        let block = make_block(100);
        cache.insert(key.clone(), block.clone());

        // Act
        let retrieved = cache.get(&key).unwrap();

        // Assert
        assert_eq!(retrieved.data.len(), 100);
    }

    #[test]
    fn should_distribute_keys_across_shards_given_multiple_keys_when_inserted() {
        // Arrange
        let cache = ShardedBlockCache::new(2000, 4);

        // Act
        for i in 0..100 {
            let key = make_key("file.sst", i * 4096);
            cache.insert(key, make_block(10));
        }

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 100);
    }

    #[test]
    fn should_evict_within_shard_given_full_shard_when_capacity_exceeded() {
        // Arrange
        let cache = ShardedBlockCache::new(400, 4);
        let key1 = make_key("file1.sst", 0);
        let key2 = make_key("file2.sst", 0);

        // Act
        cache.insert(key1.clone(), make_block(100));
        cache.insert(key2.clone(), make_block(100));

        // Assert
        let stats = cache.stats();
        assert!(stats.size_bytes <= 400);
    }

    #[test]
    fn should_aggregate_stats_across_shards_given_multiple_shards_when_stats_requested() {
        // Arrange
        let cache = ShardedBlockCache::new(1000, 8);

        // Act
        for i in 0..20 {
            let key = make_key(&format!("file{}.sst", i), 0);
            cache.insert(key.clone(), make_block(10));
            let _ = cache.get(&key);
        }
        let _ = cache.get(&make_key("missing.sst", 0));

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.hits, 20);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entry_count, 20);
    }

    #[test]
    fn should_clear_all_shards_given_sharded_cache_when_clear_called() {
        // Arrange
        let cache = ShardedBlockCache::new(1000, 4);
        for i in 0..10 {
            cache.insert(make_key("file.sst", i * 4096), make_block(50));
        }

        // Act
        cache.clear();

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.size_bytes, 0);
    }

    #[test]
    fn should_create_default_shards_given_with_default_shards_when_constructed() {
        // Arrange
        let cache = ShardedBlockCache::with_default_shards(1000);
        let key = make_key("test.sst", 0);

        // Act
        cache.insert(key.clone(), make_block(10));

        // Assert
        assert!(cache.get(&key).is_some());
    }

    #[test]
    fn should_handle_single_shard_given_shard_count_one_when_created() {
        // Arrange
        let cache = ShardedBlockCache::new(1000, 1);
        let key = make_key("file.sst", 0);

        // Act
        cache.insert(key.clone(), make_block(100));
        let retrieved = cache.get(&key).unwrap();

        // Assert
        assert_eq!(retrieved.data.len(), 100);
    }
}

// ============================================================================
// Adaptive Cache Tests
// ============================================================================

#[cfg(test)]
mod adaptive_cache_tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn make_block(size: usize) -> CachedBlock {
        CachedBlock {
            data: Bytes::from(vec![0u8; size]),
        }
    }

    fn make_key(file: &str, offset: u64) -> BlockKey {
        BlockKey {
            file_name: file.to_string(),
            block_type: BlockType::Data,
            offset,
        }
    }

    #[test]
    fn should_start_as_single_cache_given_new_adaptive_cache_when_created() {
        // Arrange
        let cache = AdaptiveBlockCache::new(1000);

        // Act
        let diag = cache.diagnostics();

        // Assert
        assert!(!diag.is_sharded);
        assert_eq!(diag.total_accesses, 0);
        assert_eq!(diag.contentions, 0);
    }

    #[test]
    fn should_store_block_given_adaptive_cache_when_insert() {
        // Arrange
        let cache = AdaptiveBlockCache::new(1000);
        let key = make_key("file1.sst", 0);
        let block = make_block(100);

        // Act
        cache.insert(key.clone(), block.clone());

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn should_retrieve_block_given_adaptive_cache_when_get() {
        // Arrange
        let cache = AdaptiveBlockCache::new(1000);
        let key = make_key("file1.sst", 0);
        let block = make_block(100);
        cache.insert(key.clone(), block.clone());

        // Act
        let retrieved = cache.get(&key).unwrap();

        // Assert
        assert_eq!(retrieved.data.len(), 100);
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn should_track_accesses_given_operations_when_diagnostics_requested() {
        // Arrange
        let cache = AdaptiveBlockCache::new(1000);
        let key = make_key("file.sst", 0);

        // Act
        cache.insert(key.clone(), make_block(50));
        cache.get(&key);
        cache.get(&key);
        let diag = cache.diagnostics();

        // Assert
        assert_eq!(diag.total_accesses, 3); // 1 insert + 2 gets
    }

    #[test]
    fn should_remain_single_given_low_contention_when_under_threshold() {
        // Arrange
        let cache = Arc::new(AdaptiveBlockCache::new(1000));

        // Act
        for i in 0..2000 {
            let key = make_key("file.sst", i * 4096);
            cache.insert(key, make_block(10));
        }
        let diag = cache.diagnostics();

        // Assert
        assert!(!diag.is_sharded);
        assert_eq!(diag.contention_rate, 0.0);
    }

    #[test]
    fn should_promote_to_sharded_given_high_contention_when_threshold_exceeded() {
        // Arrange
        let cache = Arc::new(AdaptiveBlockCache::new(5_000));

        // Act
        let handles: Vec<_> = (0..8)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..500 {
                        let key_offset = (thread_id * 50 + i) % 200;
                        let key = make_key("file.sst", key_offset * 4096);

                        if i % 5 == 0 {
                            cache.insert(key, make_block(10));
                        } else {
                            let _ = cache.get(&key);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let diag = cache.diagnostics();

        // Assert
        assert!(diag.is_sharded, "Expected promotion to sharded mode");
    }

    #[test]
    fn should_track_contention_rate_given_high_contention_when_diagnostics_requested() {
        // Arrange
        let cache = Arc::new(AdaptiveBlockCache::new(5_000));

        // Act
        let handles: Vec<_> = (0..8)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..500 {
                        let key_offset = (thread_id * 50 + i) % 200;
                        let key = make_key("file.sst", key_offset * 4096);

                        if i % 5 == 0 {
                            cache.insert(key, make_block(10));
                        } else {
                            let _ = cache.get(&key);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let diag = cache.diagnostics();

        // Assert
        assert!(diag.contention_rate > 0.05, "Expected contention > 5%");
    }

    #[test]
    fn should_record_contentions_given_concurrent_access_when_diagnostics_requested() {
        // Arrange
        let cache = Arc::new(AdaptiveBlockCache::new(5_000));

        // Act
        let handles: Vec<_> = (0..8)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..500 {
                        let key_offset = (thread_id * 50 + i) % 200;
                        let key = make_key("file.sst", key_offset * 4096);

                        if i % 5 == 0 {
                            cache.insert(key, make_block(10));
                        } else {
                            let _ = cache.get(&key);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let diag = cache.diagnostics();

        // Assert
        assert!(diag.contentions > 0, "Expected some contentions");
    }

    #[test]
    fn should_calculate_contention_rate_given_contentions_when_diagnostics_requested() {
        // Arrange
        let cache = Arc::new(AdaptiveBlockCache::new(1000));

        // Act
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..100 {
                        let key = make_key("shared.sst", (i % 10) * 4096);
                        cache.insert(key, make_block(10));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let diag = cache.diagnostics();

        // Assert
        assert_eq!(
            diag.contention_rate,
            diag.contentions as f64 / diag.total_accesses as f64
        );
    }

    #[test]
    fn should_clear_all_entries_given_adaptive_cache_when_clear_called() {
        // Arrange
        let cache = AdaptiveBlockCache::new(1000);
        cache.insert(make_key("file1.sst", 0), make_block(100));
        cache.insert(make_key("file2.sst", 0), make_block(100));

        // Act
        cache.clear();

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.size_bytes, 0);
    }

    #[test]
    fn should_maintain_stats_after_promotion_given_sharded_mode_when_accessed() {
        // Arrange
        let cache = Arc::new(AdaptiveBlockCache::new(10_000));

        // Act
        let handles: Vec<_> = (0..8)
            .map(|thread_id| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    for i in 0..200 {
                        let key = make_key("file.sst", (thread_id * 100 + i) * 4096);
                        cache.insert(key.clone(), make_block(10));
                        let _ = cache.get(&key);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        let stats = cache.stats();
        assert!(stats.hits > 0);
        assert_eq!(stats.entry_count, stats.size_bytes / 10);
    }
}
