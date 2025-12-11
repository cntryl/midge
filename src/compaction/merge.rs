//! Multi-way merge iterator for compaction
//!
//! Merges multiple sorted sequences of entries while maintaining a global
//! ordering across all inputs.
//!
//! Ordering semantics:
//!   - Primary: key ascending (lexicographically by raw bytes)
//!   - Secondary: sequence descending (newest first for a given key)
//!
//! This makes it easy for compaction to:
//!   - See all versions of a key in order (newest → oldest).
//!   - Deduplicate by key by consuming only the first version per key.
//!
//! Typical usage in compaction:
//!   - Build per-SST iterators that yield `MergeEntry` in key-ascending
//!     and seq-descending order.
//!   - Wrap them with `MergeIterator::from_iterators`.
//!   - Drive compaction by pulling from the combined iterator.

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Entry from a single SST during merge.
#[derive(Debug, Clone)]
pub struct MergeEntry {
    pub key: Bytes,
    pub value: Bytes,
    /// Sequence number (higher = newer).
    pub seq: u64,
}

/// Internal wrapper for each input iterator.
struct MergeInput {
    iter: Box<dyn Iterator<Item = MergeEntry>>,
    /// The current head item from this iterator, if any.
    current: Option<MergeEntry>,
}

/// Item stored in the heap; represents "the next candidate" from a given input.
#[derive(Debug, Clone)]
struct HeapItem {
    key: Bytes,
    seq: u64,
    input_idx: usize,
}

impl HeapItem {
    fn new(entry: &MergeEntry, input_idx: usize) -> Self {
        Self {
            key: entry.key.clone(),
            seq: entry.seq,
            input_idx,
        }
    }
}

// `BinaryHeap` in Rust is a max-heap. We want:
//   - smallest key first
//   - for the same key, largest seq first
// So we invert comparisons appropriately.
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq && self.input_idx == other.input_idx
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
        match self.key.cmp(&other.key) {
            Ordering::Less => Ordering::Greater, // smaller key = "higher" priority
            Ordering::Greater => Ordering::Less,
            Ordering::Equal => match self.seq.cmp(&other.seq) {
                Ordering::Less => Ordering::Less, // for same key, larger seq wins
                Ordering::Greater => Ordering::Greater,
                Ordering::Equal => self.input_idx.cmp(&other.input_idx).reverse(),
            },
        }
    }
}

/// Multi-way merge iterator combining sorted inputs.
pub struct MergeIterator {
    inputs: Vec<MergeInput>,
    heap: BinaryHeap<HeapItem>,
}

impl MergeIterator {
    /// Create an empty merge iterator (no inputs).
    pub fn new() -> Self {
        Self {
            inputs: Vec::new(),
            heap: BinaryHeap::new(),
        }
    }

    /// Construct a merge iterator from a set of input iterators.
    ///
    /// Requirements:
    ///   - Each input iterator must yield `MergeEntry` in:
    ///       * key ascending
    ///       * seq descending (for entries with the same key)
    ///   - The iterators do not need to be balanced in length.
    pub fn from_iterators<I>(iters: Vec<I>) -> Self
    where
        I: Iterator<Item = MergeEntry> + 'static,
    {
        let mut inputs: Vec<MergeInput> = Vec::with_capacity(iters.len());
        let mut heap = BinaryHeap::new();

        for (idx, it) in iters.into_iter().enumerate() {
            let mut input = MergeInput {
                iter: Box::new(it),
                current: None,
            };

            // Prime each iterator by pulling its first element, if any.
            if let Some(entry) = input.iter.next() {
                input.current = Some(entry.clone());
                heap.push(HeapItem::new(&entry, idx));
            }

            inputs.push(input);
        }

        Self { inputs, heap }
    }
}

impl Default for MergeIterator {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for MergeIterator {
    type Item = MergeEntry;

    fn next(&mut self) -> Option<Self::Item> {
        // Take the next candidate from the heap.
        let head = self.heap.pop()?;
        let input_idx = head.input_idx;

        // There must be a current entry for this input; if not, we skip it.
        let input = self.inputs.get_mut(input_idx)?;
        let current_entry = input.current.take()?;

        // Advance that input iterator and reinsert its new head into the heap.
        if let Some(next_entry) = input.iter.next() {
            input.current = Some(next_entry.clone());
            self.heap.push(HeapItem::new(&next_entry, input_idx));
        }

        Some(current_entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(k: &str, v: &str, seq: u64) -> MergeEntry {
        MergeEntry {
            key: Bytes::from(k.as_bytes().to_vec()),
            value: Bytes::from(v.as_bytes().to_vec()),
            seq,
        }
    }

    #[test]
    fn should_create_merge_iterator_when_new() {
        // Arrange / Act
        let iter = MergeIterator::new();

        // Assert
        assert_eq!(iter.count(), 0);
    }

    #[test]
    fn should_merge_two_sorted_streams_by_key_then_seq() {
        // Input 0: a(3), a(1), c(2)
        let s0 = vec![
            entry("a", "a3", 3),
            entry("a", "a1", 1),
            entry("c", "c2", 2),
        ];

        // Input 1: a(2), b(5)
        let s1 = vec![entry("a", "a2", 2), entry("b", "b5", 5)];

        let it0 = s0.into_iter();
        let it1 = s1.into_iter();

        let merged: Vec<MergeEntry> = MergeIterator::from_iterators(vec![it0, it1]).collect();

        let keys: Vec<Vec<u8>> = merged.iter().map(|e| e.key.to_vec()).collect();
        let seqs: Vec<u64> = merged.iter().map(|e| e.seq).collect();

        // Keys in ascending order: a, a, a, b, c
        assert_eq!(
            keys,
            vec![
                b"a".to_vec(),
                b"a".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec()
            ]
        );

        // Within "a", sequences should be: 3, 2, 1
        assert_eq!(seqs, vec![3, 2, 1, 5, 2]);
    }

    #[test]
    fn should_handle_empty_inputs_gracefully() {
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators::<std::vec::IntoIter<MergeEntry>>(vec![]).collect();
        assert!(merged.is_empty());
    }

    #[test]
    fn should_merge_when_some_streams_are_empty() {
        let s0: Vec<MergeEntry> = vec![];
        let s1 = vec![entry("k", "v", 10)];

        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter()]).collect();

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].key, Bytes::from("k".as_bytes().to_vec()));
        assert_eq!(merged[0].seq, 10);
    }
}
