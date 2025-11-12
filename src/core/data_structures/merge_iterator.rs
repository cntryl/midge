//! Streaming merging iterator for efficient range scans across multiple sources.
//!
//! Merges results from memtable, immutable memtables, and SSTs in newest-to-oldest order,
//! applying tombstone masking and deduplication without materializing the entire result set.

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};

/// Entry from a source with priority for min-heap ordering.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct HeapEntry {
    key: Bytes,
    value: Option<Bytes>, // None = tombstone
    seq: u64,
    source_id: usize, // Unique ID per source, lower = newer
    reverse: bool,
}

impl Eq for HeapEntry {}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_id == other.source_id
    }
}

impl Ord for HeapEntry {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // We want the smallest (or largest for reverse) key at the top of BinaryHeap.
        // BinaryHeap is a max-heap, so we invert ordering where appropriate.
        let ord = if self.reverse {
            self.key.cmp(&other.key) // reverse → largest key first
        } else {
            other.key.cmp(&self.key) // forward → smallest key first
        };

        match ord {
            Ordering::Equal => other.source_id.cmp(&self.source_id),
            o => o,
        }
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Iterator source trait - abstracts over memtable, immutable memtables, and SSTs.
pub trait IteratorSource {
    /// Get the next key-value pair from this source. Returns None when exhausted.
    fn next(&mut self) -> Option<(Bytes, Option<Bytes>, u64)>;
}

/// In-memory source backed by a Vec (sorted key-value pairs).
pub struct VecSource {
    items: Vec<Option<(Bytes, Option<Bytes>, u64)>>,
    pos: usize,
    #[allow(dead_code)]
    reverse: bool,
}

impl VecSource {
    pub fn new(mut items: Vec<(Bytes, Option<Bytes>, u64)>) -> Self {
        items.sort_by(|a, b| a.0.cmp(&b.0));
        Self {
            items: items.into_iter().map(Some).collect(),
            pos: 0,
            reverse: false,
        }
    }

    pub fn new_reverse(mut items: Vec<(Bytes, Option<Bytes>, u64)>) -> Self {
        items.sort_by(|a, b| b.0.cmp(&a.0));
        Self {
            items: items.into_iter().map(Some).collect(),
            pos: 0,
            reverse: true,
        }
    }
}

impl IteratorSource for VecSource {
    #[inline]
    fn next(&mut self) -> Option<(Bytes, Option<Bytes>, u64)> {
        if self.pos >= self.items.len() {
            return None;
        }
        // Move the item out of the vector without cloning
        let item_opt = self.items[self.pos].take();
        self.pos += 1;
        item_opt
    }
}

/// Merging iterator that streams results from multiple sources.
pub struct MergingIterator {
    heap: BinaryHeap<HeapEntry>,
    sources: Vec<Box<dyn IteratorSource>>,
    seen_keys: HashSet<Bytes>,
    limit: Option<usize>,
    emitted: usize,
    reverse: bool,
}

impl MergingIterator {
    /// Create a new merging iterator (forward).
    pub fn new(sources: Vec<Box<dyn IteratorSource>>, limit: Option<usize>) -> Self {
        Self::with_reverse(sources, limit, false)
    }

    /// Create a new merging iterator with reverse iteration support.
    pub fn with_reverse(
        mut sources: Vec<Box<dyn IteratorSource>>,
        limit: Option<usize>,
        reverse: bool,
    ) -> Self {
        let mut heap = BinaryHeap::with_capacity(sources.len());
        for (id, src) in sources.iter_mut().enumerate() {
            if let Some((key, val, seq)) = src.next() {
                heap.push(HeapEntry {
                    key,
                    value: val,
                    seq,
                    source_id: id,
                    reverse,
                });
            }
        }

        Self {
            heap,
            sources,
            seen_keys: HashSet::with_capacity(256),
            limit,
            emitted: 0,
            reverse,
        }
    }
}

impl Iterator for MergingIterator {
    type Item = (Bytes, Bytes);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.heap.pop() {
            // Stop if limit reached
            if let Some(limit) = self.limit {
                if self.emitted >= limit {
                    return None;
                }
            }

            // Fetch next from this source
            if let Some((key, val, seq)) = self.sources[entry.source_id].next() {
                self.heap.push(HeapEntry {
                    key,
                    value: val,
                    seq,
                    source_id: entry.source_id,
                    reverse: self.reverse,
                });
            }

            // Deduplication: skip if key already emitted (newest wins)
            if !self.seen_keys.insert(entry.key.clone()) {
                // Key was already in set, skip
                continue;
            }

            // Skip tombstones
            let Some(value) = entry.value else { continue };

            // Emit live key/value
            self.emitted += 1;
            return Some((entry.key, value));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn kv(key: &str, val: &str, seq: u64) -> (Bytes, Option<Bytes>, u64) {
        (
            Bytes::copy_from_slice(key.as_bytes()),
            Some(Bytes::copy_from_slice(val.as_bytes())),
            seq,
        )
    }

    #[test]
    fn should_merge_two_sources_with_deduplication() {
        // Arrange
        let src0 = VecSource::new(vec![kv("k1", "v1_new", 2)]);
        let src1 = VecSource::new(vec![kv("k1", "v1_old", 1), kv("k2", "v2", 1)]);
        let sources: Vec<Box<dyn IteratorSource>> = vec![Box::new(src0), Box::new(src1)];

        // Act
        let iter = MergingIterator::new(sources, None);
        let results: Vec<_> = iter.collect();

        // Assert
        assert_eq!(
            results,
            vec![
                (Bytes::from("k1"), Bytes::from("v1_new")),
                (Bytes::from("k2"), Bytes::from("v2"))
            ]
        );
    }

    #[test]
    fn should_mask_tombstones() {
        // Arrange
        let s0 = VecSource::new(vec![(Bytes::from("k1"), None, 2)]);
        let s1 = VecSource::new(vec![kv("k1", "v1", 1)]);
        let sources: Vec<Box<dyn IteratorSource>> = vec![Box::new(s0), Box::new(s1)];

        // Act
        let results: Vec<_> = MergingIterator::new(sources, None).collect();

        // Assert
        assert!(results.is_empty());
    }

    #[test]
    fn should_respect_limit() {
        // Arrange
        let src = VecSource::new(vec![
            kv("k1", "v1", 1),
            kv("k2", "v2", 1),
            kv("k3", "v3", 1),
            kv("k4", "v4", 1),
            kv("k5", "v5", 1),
        ]);
        let sources: Vec<Box<dyn IteratorSource>> = vec![Box::new(src)];

        // Act
        let results: Vec<_> = MergingIterator::new(sources, Some(3)).collect();

        // Assert
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn should_merge_sorted_across_sources() {
        // Arrange
        let s0 = VecSource::new(vec![kv("a", "a0", 3), kv("c", "c0", 3)]);
        let s1 = VecSource::new(vec![kv("b", "b1", 2), kv("d", "d1", 2)]);
        let sources: Vec<Box<dyn IteratorSource>> = vec![Box::new(s0), Box::new(s1)];

        // Act
        let res: Vec<_> = MergingIterator::new(sources, None).collect();
        let keys: Vec<_> = res.iter().map(|(k, _)| k.clone()).collect();

        // Assert
        assert_eq!(
            keys,
            vec!["a", "b", "c", "d"]
                .into_iter()
                .map(Bytes::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn should_iterate_in_reverse() {
        // Arrange
        let src = VecSource::new_reverse(vec![
            kv("k1", "v1", 1),
            kv("k2", "v2", 1),
            kv("k3", "v3", 1),
            kv("k4", "v4", 1),
        ]);
        let sources: Vec<Box<dyn IteratorSource>> = vec![Box::new(src)];

        // Act
        let results: Vec<_> = MergingIterator::with_reverse(sources, None, true).collect();
        let keys: Vec<_> = results.iter().map(|(k, _)| k.clone()).collect();

        // Assert
        assert_eq!(
            keys,
            vec!["k4", "k3", "k2", "k1"]
                .into_iter()
                .map(Bytes::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn should_reverse_with_deduplication() {
        // Arrange
        let s0 = VecSource::new_reverse(vec![(Bytes::from("k3"), Some(Bytes::from("v3_new")), 2)]);
        let s1 = VecSource::new_reverse(vec![
            (Bytes::from("k3"), Some(Bytes::from("v3_old")), 1),
            (Bytes::from("k2"), Some(Bytes::from("v2")), 1),
            (Bytes::from("k1"), Some(Bytes::from("v1")), 1),
        ]);
        let sources: Vec<Box<dyn IteratorSource>> = vec![Box::new(s0), Box::new(s1)];

        // Act
        let results: Vec<_> = MergingIterator::with_reverse(sources, None, true).collect();

        // Assert
        assert_eq!(results[0], (Bytes::from("k3"), Bytes::from("v3_new")));
        assert_eq!(results[1].0, Bytes::from("k2"));
        assert_eq!(results[2].0, Bytes::from("k1"));
    }

    #[test]
    fn should_return_empty_given_empty_sources() {
        // Arrange
        let s0 = VecSource::new(vec![]);
        let s1 = VecSource::new(vec![]);
        let sources: Vec<Box<dyn IteratorSource>> = vec![Box::new(s0), Box::new(s1)];

        // Act
        let results = MergingIterator::new(sources, None).collect::<Vec<_>>();

        // Assert
        assert!(results.is_empty());
    }

    #[test]
    fn should_handle_limit_with_tombstones() {
        // Arrange
        let src = VecSource::new(vec![
            kv("k1", "v1", 1),
            (Bytes::from("k2"), None, 2),
            kv("k3", "v3", 3),
            (Bytes::from("k4"), None, 4),
            kv("k5", "v5", 5),
        ]);
        let sources: Vec<Box<dyn IteratorSource>> = vec![Box::new(src)];

        // Act
        let res: Vec<_> = MergingIterator::new(sources, Some(2)).collect();

        // Assert
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].0, Bytes::from("k1"));
        assert_eq!(res[1].0, Bytes::from("k3"));
    }
}
