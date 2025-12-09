//! LRU cache for open SST table files
//!
//! Provides efficient caching of SST file handles to minimize
//! disk I/O and file opening overhead.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// A cached SST table with its metadata
#[derive(Clone)]
pub struct CachedTable {
    /// Path to the SST file
    pub path: PathBuf,
    /// Approximate size of the table file in bytes
    pub size_bytes: u64,
}

/// LRU cache for open SST tables
pub struct TableCache {
    inner: Arc<Mutex<TableCacheInner>>,
}

struct TableCacheInner {
    /// Map from file path to list node index
    map: HashMap<String, usize>,
    /// Doubly-linked list for LRU ordering (most recent at front)
    list: Vec<Option<TableNode>>,
    /// Head of the list (most recently used)
    head: Option<usize>,
    /// Tail of the list (least recently used)
    tail: Option<usize>,
    /// Next free slot in the list array
    free_head: Option<usize>,
    /// Maximum number of open tables
    max_tables: usize,
    /// Cache hit count
    hits: u64,
    /// Cache miss count
    misses: u64,
}

struct TableNode {
    key: String,
    value: CachedTable,
    prev: Option<usize>,
    next: Option<usize>,
}

impl TableCache {
    /// Create a new table cache with the given capacity
    pub fn new(max_tables: usize) -> Self {
        TableCache {
            inner: Arc::new(Mutex::new(TableCacheInner {
                map: HashMap::new(),
                list: Vec::new(),
                head: None,
                tail: None,
                free_head: None,
                max_tables,
                hits: 0,
                misses: 0,
            })),
        }
    }

    /// Get a table from the cache
    pub fn get(&self, path: &str) -> Option<CachedTable> {
        let mut inner = self.inner.lock();
        if let Some(&node_idx) = inner.map.get(path) {
            inner.hits += 1;
            // Move to front (most recently used)
            inner.move_to_front(node_idx);
            inner.list[node_idx].as_ref().map(|n| n.value.clone())
        } else {
            inner.misses += 1;
            None
        }
    }

    /// Insert a table into the cache
    pub fn insert(&self, path: String, table: CachedTable) {
        let mut inner = self.inner.lock();

        // If table is already in cache, update it
        if let Some(&node_idx) = inner.map.get(&path) {
            // Update value and move to front
            if let Some(node) = &mut inner.list[node_idx] {
                node.value = table;
            }
            inner.move_to_front(node_idx);
            return;
        }

        // Evict tables until we have space
        while inner.map.len() >= inner.max_tables && inner.tail.is_some() {
            inner.evict_lru();
        }

        // Insert new table
        let node = TableNode {
            key: path.clone(),
            value: table,
            prev: None,
            next: inner.head,
        };

        let node_idx = if let Some(free_idx) = inner.free_head {
            // Reuse a free slot
            if let Some(free_node) = &inner.list[free_idx] {
                inner.free_head = free_node.next;
            }
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

        inner.map.insert(path, node_idx);
    }

    /// Remove a table from the cache
    pub fn remove(&self, path: &str) -> Option<CachedTable> {
        let mut inner = self.inner.lock();
        let node_idx = *inner.map.get(path)?;

        let (key, value, prev_idx, next_idx) = if let Some(node) = &inner.list[node_idx] {
            (node.key.clone(), node.value.clone(), node.prev, node.next)
        } else {
            return None;
        };

        // Remove from map
        inner.map.remove(&key);

        // Update linked list pointers
        if let Some(prev) = prev_idx {
            if let Some(prev_node) = &mut inner.list[prev] {
                prev_node.next = next_idx;
            }
        }

        if let Some(next) = next_idx {
            if let Some(next_node) = &mut inner.list[next] {
                next_node.prev = prev_idx;
            }
        }

        // Update head/tail if needed
        if Some(node_idx) == inner.head {
            inner.head = next_idx;
        }
        if Some(node_idx) == inner.tail {
            inner.tail = prev_idx;
        }

        // Add to free list
        let free_head = inner.free_head;
        if let Some(node) = &mut inner.list[node_idx] {
            node.next = free_head;
        }
        inner.free_head = Some(node_idx);
        inner.list[node_idx] = None;

        Some(value)
    }

    /// Clear all entries from the cache
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.map.clear();
        inner.list.clear();
        inner.head = None;
        inner.tail = None;
        inner.free_head = None;
    }

    /// Get cache statistics
    pub fn stats(&self) -> TableCacheStats {
        let inner = self.inner.lock();
        TableCacheStats {
            hits: inner.hits,
            misses: inner.misses,
            table_count: inner.map.len(),
            max_tables: inner.max_tables,
        }
    }
}

impl TableCacheInner {
    /// Move a node to the front of the list (most recently used)
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

    /// Evict the least recently used table
    fn evict_lru(&mut self) {
        let tail_idx = match self.tail {
            Some(idx) => idx,
            None => return,
        };

        let (key, prev_idx) = if let Some(node) = &self.list[tail_idx] {
            (node.key.clone(), node.prev)
        } else {
            return;
        };

        // Remove from map
        self.map.remove(&key);

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

        // Add to free list
        let free_head = self.free_head;
        if let Some(node) = &mut self.list[tail_idx] {
            node.next = free_head;
        }
        self.free_head = Some(tail_idx);
        self.list[tail_idx] = None;
    }
}

#[derive(Debug, Clone)]
pub struct TableCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub table_count: usize,
    pub max_tables: usize,
}

impl TableCacheStats {
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
mod table_cache_tests {
    use super::*;

    fn make_table(path: &str, size: u64) -> CachedTable {
        CachedTable {
            path: PathBuf::from(path),
            size_bytes: size,
        }
    }

    #[test]
    fn should_store_table_given_cache_when_insert() {
        // Arrange
        let cache = TableCache::new(10);
        let path = "file1.sst";
        let table = make_table(path, 1024);

        // Act
        cache.insert(path.to_string(), table.clone());

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.table_count, 1);
    }

    #[test]
    fn should_retrieve_table_given_cache_when_get() {
        // Arrange
        let cache = TableCache::new(10);
        let path = "file1.sst";
        let table = make_table(path, 1024);
        cache.insert(path.to_string(), table.clone());

        // Act
        let retrieved = cache.get(path).unwrap();

        // Assert
        assert_eq!(retrieved.path, PathBuf::from(path));
        assert_eq!(retrieved.size_bytes, 1024);
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn should_evict_lru_table_given_full_cache_when_capacity_exceeded() {
        // Arrange
        let cache = TableCache::new(2); // Hold 2 tables

        // Act
        cache.insert("file1.sst".to_string(), make_table("file1.sst", 100));
        cache.insert("file2.sst".to_string(), make_table("file2.sst", 200));
        cache.insert("file3.sst".to_string(), make_table("file3.sst", 300)); // Should evict file1

        // Assert
        assert!(cache.get("file1.sst").is_none()); // Evicted
        assert!(cache.get("file2.sst").is_some());
        assert!(cache.get("file3.sst").is_some());
        let stats = cache.stats();
        assert_eq!(stats.table_count, 2);
    }

    #[test]
    fn should_maintain_lru_order_given_gets_when_tables_accessed() {
        // Arrange
        let cache = TableCache::new(2); // Hold 2 tables
        cache.insert("file1.sst".to_string(), make_table("file1.sst", 100));
        cache.insert("file2.sst".to_string(), make_table("file2.sst", 200));

        // Act
        cache.get("file1.sst"); // Access file1 to make it more recent
        cache.insert("file3.sst".to_string(), make_table("file3.sst", 300)); // Should evict file2 (LRU)

        // Assert
        assert!(cache.get("file1.sst").is_some());
        assert!(cache.get("file2.sst").is_none()); // Evicted
        assert!(cache.get("file3.sst").is_some());
    }

    #[test]
    fn should_update_table_given_new_value_when_put_called() {
        // Arrange
        let cache = TableCache::new(10);
        let path = "file1.sst";

        // Act
        cache.insert(path.to_string(), make_table(path, 100));
        cache.insert(path.to_string(), make_table(path, 200)); // Update

        // Assert
        let retrieved = cache.get(path).unwrap();
        assert_eq!(retrieved.size_bytes, 200);
        let stats = cache.stats();
        assert_eq!(stats.table_count, 1); // Still just one entry
    }

    #[test]
    fn should_remove_table_given_path_when_remove_called() {
        // Arrange
        let cache = TableCache::new(10);
        cache.insert("file1.sst".to_string(), make_table("file1.sst", 100));
        cache.insert("file2.sst".to_string(), make_table("file2.sst", 200));

        // Act
        let removed = cache.remove("file1.sst").unwrap();

        // Assert
        assert_eq!(removed.size_bytes, 100);
        assert!(cache.get("file1.sst").is_none());
        assert!(cache.get("file2.sst").is_some());
        let stats = cache.stats();
        assert_eq!(stats.table_count, 1);
    }

    #[test]
    fn should_remove_all_tables_given_cache_when_clear_called() {
        // Arrange
        let cache = TableCache::new(10);
        cache.insert("file1.sst".to_string(), make_table("file1.sst", 100));
        cache.insert("file2.sst".to_string(), make_table("file2.sst", 200));

        // Act
        cache.clear();

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.table_count, 0);
    }

    #[test]
    fn should_track_hits_given_table_accesses_when_stats_requested() {
        // Arrange
        let cache = TableCache::new(10);
        cache.insert("file1.sst".to_string(), make_table("file1.sst", 100));

        // Act
        cache.get("file1.sst");

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
    }

    #[test]
    fn should_track_misses_given_table_accesses_when_stats_requested() {
        // Arrange
        let cache = TableCache::new(10);

        // Act
        cache.get("file2.sst");

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn should_calculate_hit_rate_given_mixed_table_accesses_when_stats_requested() {
        // Arrange
        let cache = TableCache::new(10);
        cache.insert("file1.sst".to_string(), make_table("file1.sst", 100));

        // Act
        cache.get("file1.sst");
        cache.get("file2.sst");

        // Assert
        let stats = cache.stats();
        assert_eq!(stats.hit_rate(), 0.5);
    }
}
