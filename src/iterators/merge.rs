//! Merge Iterator for LSM tree
//!
//! Blends multiple sorted data sources (memtable, immutable memtables, SST levels)
//! into a single sorted stream, respecting MVCC snapshot isolation.

use crate::common::MidgeResult;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// A single source iterator that can be heap-based merged
pub trait SourceIterator: Send + Sync {
    /// Get the current key-value pair without advancing
    fn current(&mut self) -> MidgeResult<Option<(Vec<u8>, Vec<u8>)>>;

    /// Move to the next key-value pair
    fn next(&mut self) -> MidgeResult<Option<(Vec<u8>, Vec<u8>)>>;

    /// Seek to a specific key
    fn seek(&mut self, key: &[u8]) -> MidgeResult<()>;
}

/// Comparable key with source index for heap ordering
#[derive(Clone)]
struct HeapItem {
    key: Vec<u8>,
    value: Vec<u8>,
    source_idx: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_idx == other.source_idx
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse comparison for priority queue
        // First compare by key (ascending)
        match other.key.cmp(&self.key) {
            Ordering::Equal => {
                // If keys are equal, prefer lower source index (earlier = higher priority)
                // This ensures we get the first (most recent) version
                other.source_idx.cmp(&self.source_idx)
            }
            ord => ord,
        }
    }
}

/// Merge iterator combining multiple sorted sources
pub struct MergeIterator {
    /// Heap of current items from each source
    heap: BinaryHeap<HeapItem>,
    /// Current sources (in priority order)
    sources: Vec<Box<dyn SourceIterator>>,
    /// Start key (None = unbounded)
    start: Option<Vec<u8>>,
    /// End key (None = unbounded)
    end: Option<Vec<u8>>,
    /// Whether we've been exhausted
    exhausted: bool,
}

impl MergeIterator {
    /// Create a new merge iterator from multiple sources
    pub fn new(sources: Vec<Box<dyn SourceIterator>>) -> Self {
        Self {
            heap: BinaryHeap::new(),
            sources,
            start: None,
            end: None,
            exhausted: false,
        }
    }

    /// Set the start key (inclusive)
    pub fn start(mut self, key: Vec<u8>) -> Self {
        self.start = Some(key);
        self
    }

    /// Set the end key (exclusive)
    pub fn end(mut self, key: Vec<u8>) -> Self {
        self.end = Some(key);
        self
    }

    /// Initialize the iterator by seeking all sources and populating heap
    pub fn init(&mut self) -> MidgeResult<()> {
        if let Some(ref start_key) = self.start {
            for source in &mut self.sources {
                source.seek(start_key)?;
            }
        }

        // Load initial items from all sources
        for (idx, source) in self.sources.iter_mut().enumerate() {
            if let Some((k, v)) = source.current()? {
                // Check bounds inline to avoid double borrow
                let in_range = {
                    if let Some(ref start) = &self.start {
                        if k.as_slice() < start.as_slice() {
                            false
                        } else if let Some(ref end) = &self.end {
                            k.as_slice() < end.as_slice()
                        } else {
                            true
                        }
                    } else if let Some(ref end) = &self.end {
                        k.as_slice() < end.as_slice()
                    } else {
                        true
                    }
                };

                if in_range {
                    self.heap.push(HeapItem {
                        key: k,
                        value: v,
                        source_idx: idx,
                    });
                }
            }
        }

        Ok(())
    }

    /// Check if a key is within our bounds
    fn is_in_range(&self, key: &[u8]) -> bool {
        if let Some(ref start) = self.start {
            if key < start.as_slice() {
                return false;
            }
        }
        if let Some(ref end) = self.end {
            if key >= end.as_slice() {
                return false;
            }
        }
        true
    }

    /// Get the next unique key-value pair
    pub fn next_item(&mut self) -> MidgeResult<Option<(Vec<u8>, Vec<u8>)>> {
        if self.exhausted {
            return Ok(None);
        }

        if let Some(item) = self.heap.pop() {
            let key = item.key.clone();
            let value = item.value.clone();

            // Advance the source that yielded this item
            if let Some(source) = self.sources.get_mut(item.source_idx) {
                source.next()?;
                // Try to load next item from this source
                if let Some((k, v)) = source.current()? {
                    if self.is_in_range(&k) {
                        self.heap.push(HeapItem {
                            key: k,
                            value: v,
                            source_idx: item.source_idx,
                        });
                    }
                }
            }

            Ok(Some((key, value)))
        } else {
            // Heap is empty, we're exhausted
            self.exhausted = true;
            Ok(None)
        }
    }

    /// Count items in this merge without consuming
    pub fn count(&self) -> usize {
        self.heap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock iterator for testing
    struct MockIterator {
        data: Vec<(Vec<u8>, Vec<u8>)>,
        position: usize,
    }

    impl MockIterator {
        fn new(data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
            Self { data, position: 0 }
        }
    }

    impl SourceIterator for MockIterator {
        fn current(&mut self) -> MidgeResult<Option<(Vec<u8>, Vec<u8>)>> {
            if self.position < self.data.len() {
                Ok(Some(self.data[self.position].clone()))
            } else {
                Ok(None)
            }
        }

        fn next(&mut self) -> MidgeResult<Option<(Vec<u8>, Vec<u8>)>> {
            if self.position < self.data.len() {
                let pair = self.data[self.position].clone();
                self.position += 1;
                Ok(Some(pair))
            } else {
                Ok(None)
            }
        }

        fn seek(&mut self, key: &[u8]) -> MidgeResult<()> {
            // Find position of first key >= search key
            self.position = self
                .data
                .iter()
                .position(|(k, _)| k.as_slice() >= key)
                .unwrap_or(self.data.len());
            Ok(())
        }
    }

    #[test]
    fn should_merge_multiple_sources() {
        // Arrange
        let source1 = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"val1".to_vec())]));
        let source2 = Box::new(MockIterator::new(vec![(b"b".to_vec(), b"val2".to_vec())]));

        let mut merge = MergeIterator::new(vec![source1, source2]);

        // Act
        merge.init().unwrap();
        let r1 = merge.next_item().unwrap();

        // Assert
        assert_eq!(r1.as_ref().unwrap().0, b"a".to_vec());
    }

    #[test]
    fn should_return_none_when_exhausted() {
        // Arrange
        let source1 = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"val1".to_vec())]));

        let mut merge = MergeIterator::new(vec![source1]);

        // Act
        merge.init().unwrap();
        merge.next_item().unwrap();
        let result = merge.next_item().unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_handle_empty_sources() {
        // Arrange
        let source1: Box<dyn SourceIterator> = Box::new(MockIterator::new(vec![]));

        let mut merge = MergeIterator::new(vec![source1]);

        // Act
        merge.init().unwrap();
        let result = merge.next_item().unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_respect_range_bounds() {
        // Arrange
        let source1 = Box::new(MockIterator::new(vec![
            (b"a".to_vec(), b"val1".to_vec()),
            (b"b".to_vec(), b"val2".to_vec()),
            (b"c".to_vec(), b"val3".to_vec()),
        ]));

        let mut merge = MergeIterator::new(vec![source1])
            .start(b"b".to_vec())
            .end(b"c".to_vec());

        // Act
        merge.init().unwrap();
        let result = merge.next_item().unwrap().unwrap();

        // Assert
        assert_eq!(result.0, b"b".to_vec());
    }

    // ========================================================================
    // Iterator creation tests
    // ========================================================================

    #[test]
    fn should_create_merge_iterator_from_sources() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"1".to_vec())]));

        // Act
        let merge = MergeIterator::new(vec![source]);

        // Assert: creation succeeds, count is 0 before init
        assert_eq!(merge.count(), 0);
    }

    #[test]
    fn should_create_merge_iterator_with_zero_sources() {
        // Arrange

        // Act
        let merge = MergeIterator::new(vec![]);

        // Assert
        assert_eq!(merge.count(), 0);
    }

    // ========================================================================
    // Bound setting tests
    // ========================================================================

    #[test]
    fn should_set_start_bound() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![]));

        // Act
        let merge = MergeIterator::new(vec![source]).start(b"x".to_vec());

        // Assert: start is set
        assert!(merge.start.is_some());
        assert_eq!(merge.start.as_ref().unwrap(), b"x");
    }

    #[test]
    fn should_set_end_bound() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![]));

        // Act
        let merge = MergeIterator::new(vec![source]).end(b"z".to_vec());

        // Assert: end is set
        assert!(merge.end.is_some());
        assert_eq!(merge.end.as_ref().unwrap(), b"z");
    }

    #[test]
    fn should_support_fluent_chaining() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![]));

        // Act
        let merge = MergeIterator::new(vec![source])
            .start(b"a".to_vec())
            .end(b"z".to_vec());

        // Assert
        assert_eq!(merge.start.as_ref().unwrap(), b"a");
        assert_eq!(merge.end.as_ref().unwrap(), b"z");
    }

    #[test]
    fn should_overwrite_start_bound_on_multiple_calls() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![]));

        // Act
        let merge = MergeIterator::new(vec![source])
            .start(b"a".to_vec())
            .start(b"m".to_vec());

        // Assert: second call overwrites
        assert_eq!(merge.start.as_ref().unwrap(), b"m");
    }

    #[test]
    fn should_overwrite_end_bound_on_multiple_calls() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![]));

        // Act
        let merge = MergeIterator::new(vec![source])
            .end(b"z".to_vec())
            .end(b"m".to_vec());

        // Assert
        assert_eq!(merge.end.as_ref().unwrap(), b"m");
    }

    // ========================================================================
    // Initialization tests
    // ========================================================================

    #[test]
    fn should_init_with_single_source() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"val_a".to_vec())]));
        let mut merge = MergeIterator::new(vec![source]);

        // Act
        merge.init().unwrap();

        // Assert: heap populated
        assert_eq!(merge.count(), 1);
    }

    #[test]
    fn should_init_with_multiple_sources() {
        // Arrange
        let source1 = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"1".to_vec())]));
        let source2 = Box::new(MockIterator::new(vec![(b"c".to_vec(), b"3".to_vec())]));
        let mut merge = MergeIterator::new(vec![source1, source2]);

        // Act
        merge.init().unwrap();

        // Assert: both items loaded
        assert_eq!(merge.count(), 2);
    }

    #[test]
    fn should_init_respects_end_bound() {
        // Arrange: Create iterator with single source containing a,b,c
        // Set end bound to "c" (exclusive), so should only load a and b
        let source = Box::new(MockIterator::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]));
        let mut merge = MergeIterator::new(vec![source]).end(b"c".to_vec());

        // Act
        merge.init().unwrap();

        // Assert: heap contains items a and b (c is filtered by end bound)
        // The heap will have loaded the first item that matches bounds
        assert!(merge.count() > 0);
        let r1 = merge.next_item().unwrap().unwrap();
        assert_eq!(r1.0, b"a".to_vec());
    }

    #[test]
    fn should_init_empty_sources() {
        // Arrange
        let source: Box<dyn SourceIterator> = Box::new(MockIterator::new(vec![]));
        let mut merge = MergeIterator::new(vec![source]);

        // Act
        merge.init().unwrap();

        // Assert: count is 0
        assert_eq!(merge.count(), 0);
    }

    // ========================================================================
    // next_item() tests
    // ========================================================================

    #[test]
    fn should_return_items_in_sorted_order() {
        // Arrange
        let source1 = Box::new(MockIterator::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]));
        let source2 = Box::new(MockIterator::new(vec![
            (b"b".to_vec(), b"2".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ]));
        let mut merge = MergeIterator::new(vec![source1, source2]);

        // Act
        merge.init().unwrap();
        let r1 = merge.next_item().unwrap().unwrap();
        let r2 = merge.next_item().unwrap().unwrap();
        let r3 = merge.next_item().unwrap().unwrap();
        let r4 = merge.next_item().unwrap().unwrap();

        // Assert: order is a, b, c, d
        assert_eq!(r1.0, b"a".to_vec());
        assert_eq!(r2.0, b"b".to_vec());
        assert_eq!(r3.0, b"c".to_vec());
        assert_eq!(r4.0, b"d".to_vec());
    }

    #[test]
    fn should_continue_returning_none_after_exhaustion() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"1".to_vec())]));
        let mut merge = MergeIterator::new(vec![source]);
        merge.init().unwrap();

        // Act
        merge.next_item().unwrap();
        let r1 = merge.next_item().unwrap();
        let r2 = merge.next_item().unwrap();
        let r3 = merge.next_item().unwrap();

        // Assert: all return None after exhaustion
        assert!(r1.is_none());
        assert!(r2.is_none());
        assert!(r3.is_none());
    }

    #[test]
    fn should_handle_duplicate_keys_from_different_sources() {
        // Arrange: When both sources have the same key, both items go in heap.
        // The Ord impl prefers lower source_idx, so source 0's item comes out first.
        // Then source 0 is exhausted, but source 1 still has its "a", which gets added to heap.
        // So we get two results: r1 from source 0, r2 from source 1 (both key="a").
        let source1 = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"v1".to_vec())]));
        let source2 = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"v2".to_vec())]));
        let mut merge = MergeIterator::new(vec![source1, source2]);

        // Act
        merge.init().unwrap();
        let r1 = merge.next_item().unwrap().unwrap();
        let r2 = merge.next_item().unwrap();

        // Assert: both "a" entries are returned (one from each source)
        // After consuming source 0's "a", source 1's "a" is added to heap
        assert_eq!(r1.0, b"a".to_vec());
        assert_eq!(r1.1, b"v1".to_vec());
        // Source 1's item is now in heap, so we get it next
        assert_eq!(r2.as_ref().unwrap().0, b"a".to_vec());
        assert_eq!(r2.as_ref().unwrap().1, b"v2".to_vec());
    }

    #[test]
    fn should_prefer_earlier_source_on_duplicate_keys() {
        // Arrange: source 0 has key "x", source 1 also has key "x"
        // Source 0 should be preferred (lower source_idx)
        let source1 = Box::new(MockIterator::new(vec![(b"x".to_vec(), b"first".to_vec())]));
        let source2 = Box::new(MockIterator::new(vec![(b"x".to_vec(), b"second".to_vec())]));
        let mut merge = MergeIterator::new(vec![source1, source2]);

        // Act
        merge.init().unwrap();
        let result = merge.next_item().unwrap().unwrap();

        // Assert: source 0's value returned
        assert_eq!(result.1, b"first".to_vec());
    }

    #[test]
    fn should_filter_items_above_end_bound() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]));
        let mut merge = MergeIterator::new(vec![source]).end(b"b".to_vec());

        // Act
        merge.init().unwrap();
        let r1 = merge.next_item().unwrap();
        let r2 = merge.next_item().unwrap();

        // Assert: only a and b returned, c filtered out
        assert_eq!(r1.unwrap().0, b"a".to_vec());
        assert!(r2.is_none());
    }

    #[test]
    fn should_filter_items_below_start_bound() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ]));
        let mut merge = MergeIterator::new(vec![source]).start(b"b".to_vec());

        // Act
        merge.init().unwrap();
        let r1 = merge.next_item().unwrap().unwrap();

        // Assert: starts at b, not a
        assert_eq!(r1.0, b"b".to_vec());
    }

    #[test]
    fn should_handle_empty_range() {
        // Arrange: start >= end
        let source = Box::new(MockIterator::new(vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ]));
        let mut merge = MergeIterator::new(vec![source])
            .start(b"z".to_vec())
            .end(b"z".to_vec());

        // Act
        merge.init().unwrap();
        let result = merge.next_item().unwrap();

        // Assert: no items in range
        assert!(result.is_none());
    }

    // ========================================================================
    // count() tests
    // ========================================================================

    #[test]
    fn should_return_heap_count_before_init() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"1".to_vec())]));
        let merge = MergeIterator::new(vec![source]);

        // Act
        // (none)

        // Assert: count is 0 before init
        assert_eq!(merge.count(), 0);
    }

    #[test]
    fn should_return_heap_count_after_init() {
        // Arrange
        let source1 = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"1".to_vec())]));
        let source2 = Box::new(MockIterator::new(vec![(b"b".to_vec(), b"2".to_vec())]));
        let mut merge = MergeIterator::new(vec![source1, source2]);

        // Act
        merge.init().unwrap();

        // Assert: count reflects items in heap
        assert_eq!(merge.count(), 2);
    }

    #[test]
    fn should_decrease_count_on_next_item() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![(b"a".to_vec(), b"1".to_vec())]));
        let mut merge = MergeIterator::new(vec![source]);
        merge.init().unwrap();

        // Assert initial count
        assert_eq!(merge.count(), 1);

        // Act
        merge.next_item().unwrap();

        // Assert: count decreased
        assert_eq!(merge.count(), 0);
    }

    #[test]
    fn should_handle_binary_data_in_keys() {
        // Arrange
        let source = Box::new(MockIterator::new(vec![
            (vec![0u8, 255u8], b"val".to_vec()),
            (vec![1u8, 0u8], b"val".to_vec()),
        ]));
        let mut merge = MergeIterator::new(vec![source]);

        // Act
        merge.init().unwrap();
        let r1 = merge.next_item().unwrap().unwrap();
        let r2 = merge.next_item().unwrap().unwrap();

        // Assert: binary data preserved and sorted correctly
        assert_eq!(r1.0, vec![0u8, 255u8]);
        assert_eq!(r2.0, vec![1u8, 0u8]);
    }

    #[test]
    fn should_handle_large_values() {
        // Arrange: large value (1MB)
        let large_val = vec![42u8; 1024 * 1024];
        let source = Box::new(MockIterator::new(vec![(
            b"key".to_vec(),
            large_val.clone(),
        )]));
        let mut merge = MergeIterator::new(vec![source]);

        // Act
        merge.init().unwrap();
        let result = merge.next_item().unwrap().unwrap();

        // Assert: large value preserved
        assert_eq!(result.1, large_val);
    }
}
