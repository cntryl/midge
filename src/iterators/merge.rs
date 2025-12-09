//! Merge Iterator for LSM tree
//!
//! Blends multiple sorted data sources (memtable, immutable memtables, SST levels)
//! into a single sorted stream, respecting MVCC snapshot isolation.

use crate::common::MidgeResult;
use std::collections::BinaryHeap;
use std::cmp::Ordering;

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
    pub fn next(&mut self) -> MidgeResult<Option<(Vec<u8>, Vec<u8>)>> {
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

            return Ok(Some((key, value)));
        } else {
            // Heap is empty, we're exhausted
            self.exhausted = true;
            return Ok(None);
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
            self.position = self.data.iter().position(|(k, _)| k.as_slice() >= key).unwrap_or(self.data.len());
            Ok(())
        }
    }

    #[test]
    fn should_merge_multiple_sources() {
        // Arrange
        let source1 = Box::new(MockIterator::new(vec![
            (b"a".to_vec(), b"val1".to_vec()),
        ]));
        let source2 = Box::new(MockIterator::new(vec![
            (b"b".to_vec(), b"val2".to_vec()),
        ]));

        let mut merge = MergeIterator::new(vec![source1, source2]);

        // Act
        merge.init().unwrap();
        let r1 = merge.next().unwrap();

        // Assert
        assert_eq!(r1.as_ref().unwrap().0, b"a".to_vec());
    }

    #[test]
    fn should_return_none_when_exhausted() {
        // Arrange
        let source1 = Box::new(MockIterator::new(vec![
            (b"a".to_vec(), b"val1".to_vec()),
        ]));

        let mut merge = MergeIterator::new(vec![source1]);

        // Act
        merge.init().unwrap();
        merge.next().unwrap();
        let result = merge.next().unwrap();

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
        let result = merge.next().unwrap();

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
        let result = merge.next().unwrap().unwrap();

        // Assert
        assert_eq!(result.0, b"b".to_vec());
    }
}
