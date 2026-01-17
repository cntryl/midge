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

    // ============================================================================
    // Tests for constructor and initialization invariants
    // ============================================================================

    #[test]
    fn should_create_empty_iterator_when_new() {
        // Arrange
        // (no setup required)

        // Act
        let iter = MergeIterator::new();

        // Assert
        assert_eq!(iter.count(), 0);
    }

    #[test]
    fn should_create_default_iterator_when_default() {
        // Arrange
        // (no setup required)

        // Act
        let iter = MergeIterator::default();

        // Assert
        assert_eq!(iter.count(), 0);
    }

    #[test]
    fn should_handle_no_input_iterators_when_from_empty_list() {
        // Arrange
        // (empty input list)

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators::<std::vec::IntoIter<MergeEntry>>(vec![]).collect();

        // Assert
        assert!(merged.is_empty());
    }

    #[test]
    fn should_handle_single_empty_iterator_when_from_empty_input() {
        // Arrange
        let s0: Vec<MergeEntry> = vec![];

        // Act
        let merged: Vec<MergeEntry> = MergeIterator::from_iterators(vec![s0.into_iter()]).collect();

        // Assert
        assert!(merged.is_empty());
    }

    // ============================================================================
    // Tests for key ordering invariant: keys must be in ascending order
    // ============================================================================

    #[test]
    fn should_yield_keys_in_ascending_order_when_single_sorted_input() {
        // Arrange
        let s0 = vec![
            entry("a", "a_val", 10),
            entry("c", "c_val", 10),
            entry("e", "e_val", 10),
        ];

        // Act
        let merged: Vec<MergeEntry> = MergeIterator::from_iterators(vec![s0.into_iter()]).collect();
        let keys: Vec<Vec<u8>> = merged.iter().map(|e| e.key.to_vec()).collect();

        // Assert
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec(), b"e".to_vec()]);
    }

    #[test]
    fn should_merge_into_global_ascending_key_order_when_interleaved_inputs() {
        // Arrange: inputs with alternating keys
        let s0 = vec![
            entry("a", "a0", 1),
            entry("c", "c0", 1),
            entry("e", "e0", 1),
        ];
        let s1 = vec![
            entry("b", "b1", 1),
            entry("d", "d1", 1),
            entry("f", "f1", 1),
        ];

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter()]).collect();
        let keys: Vec<Vec<u8>> = merged.iter().map(|e| e.key.to_vec()).collect();

        // Assert: keys should be globally sorted: a, b, c, d, e, f
        assert_eq!(
            keys,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec()
            ]
        );
    }

    #[test]
    fn should_maintain_key_order_with_multiple_inputs() {
        // Arrange: 3 inputs with different key ranges
        let s0 = vec![entry("a", "a0", 1), entry("d", "d0", 1)];
        let s1 = vec![entry("b", "b1", 1), entry("e", "e1", 1)];
        let s2 = vec![entry("c", "c2", 1), entry("f", "f2", 1)];

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter(), s2.into_iter()])
                .collect();
        let keys: Vec<Vec<u8>> = merged.iter().map(|e| e.key.to_vec()).collect();

        // Assert
        assert_eq!(
            keys,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"f".to_vec()
            ]
        );
    }

    // ============================================================================
    // Tests for sequence ordering invariant: same key must have seq descending
    // ============================================================================

    #[test]
    fn should_yield_multiple_same_keys_in_descending_sequence_order() {
        // Arrange: single input with same key but different sequences
        let s0 = vec![
            entry("x", "x_seq5", 5),
            entry("x", "x_seq3", 3),
            entry("x", "x_seq1", 1),
        ];

        // Act
        let merged: Vec<MergeEntry> = MergeIterator::from_iterators(vec![s0.into_iter()]).collect();
        let seqs: Vec<u64> = merged.iter().map(|e| e.seq).collect();

        // Assert: sequences for "x" should be 5, 3, 1 (descending)
        assert_eq!(seqs, vec![5, 3, 1]);
    }

    #[test]
    fn should_interleave_sequence_descending_when_same_key_across_multiple_inputs() {
        // Arrange: multiple inputs with same key but different sequences
        let s0 = vec![entry("k", "k_seq5", 5), entry("k", "k_seq1", 1)];
        let s1 = vec![entry("k", "k_seq4", 4), entry("k", "k_seq2", 2)];

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter()]).collect();
        let seqs: Vec<u64> = merged.iter().map(|e| e.seq).collect();

        // Assert: sequences should be 5, 4, 2, 1 (strictly descending)
        assert_eq!(seqs, vec![5, 4, 2, 1]);
    }

    #[test]
    fn should_yield_newest_sequence_first_when_same_key_same_seq() {
        // Arrange: different inputs with identical key and sequence
        let s0 = vec![entry("k", "first", 10)];
        let s1 = vec![entry("k", "second", 10)];

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter()]).collect();

        // Assert: both entries should be present, order determined by input_idx
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].seq, 10);
        assert_eq!(merged[1].seq, 10);
    }

    // ============================================================================
    // Tests for comprehensive merge ordering: key primary, seq secondary
    // ============================================================================

    #[test]
    fn should_sort_by_key_with_sequence_as_secondary() {
        // Arrange: complex case with overlapping keys and sequences
        let s0 = vec![
            entry("a", "a3", 3),
            entry("a", "a1", 1),
            entry("c", "c2", 2),
        ];
        let s1 = vec![entry("a", "a2", 2), entry("b", "b5", 5)];

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter()]).collect();
        let keys: Vec<Vec<u8>> = merged.iter().map(|e| e.key.to_vec()).collect();
        let seqs: Vec<u64> = merged.iter().map(|e| e.seq).collect();

        // Assert: keys [a, a, a, b, c], seqs [3, 2, 1, 5, 2]
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
        assert_eq!(seqs, vec![3, 2, 1, 5, 2]);
    }

    // ============================================================================
    // Tests for exhaustion invariant: all entries yielded exactly once
    // ============================================================================

    #[test]
    fn should_yield_all_entries_from_all_inputs() {
        // Arrange: 3 inputs with known entry counts
        let s0 = vec![entry("a", "a0", 1), entry("c", "c0", 1)]; // 2 entries
        let s1 = vec![entry("b", "b1", 1)]; // 1 entry
        let s2 = vec![
            entry("d", "d2", 1),
            entry("e", "e2", 1),
            entry("f", "f2", 1),
        ]; // 3 entries

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter(), s2.into_iter()])
                .collect();

        // Assert: total 6 entries, all present
        assert_eq!(merged.len(), 6);
    }

    #[test]
    fn should_yield_each_entry_exactly_once_when_no_duplicates() {
        // Arrange: distinct keys/seqs across inputs
        let s0 = vec![entry("a", "v", 1)];
        let s1 = vec![entry("b", "v", 1)];
        let s2 = vec![entry("c", "v", 1)];

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter(), s2.into_iter()])
                .collect();

        // Assert: all entries present, none duplicated
        assert_eq!(merged.len(), 3);
        let keys: Vec<Vec<u8>> = merged.iter().map(|e| e.key.to_vec()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    // ============================================================================
    // Tests for input advancement invariant: heap management during iteration
    // ============================================================================

    #[test]
    fn should_advance_iterator_then_reinserve_into_heap() {
        // Arrange: input with multiple entries from same source (must be pre-sorted)
        let s0 = vec![
            entry("a", "a3", 3),
            entry("a", "a2", 2),
            entry("a", "a1", 1),
        ];

        // Act
        let merged: Vec<MergeEntry> = MergeIterator::from_iterators(vec![s0.into_iter()]).collect();

        // Assert: all 3 entries present in descending sequence order
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].seq, 3);
        assert_eq!(merged[1].seq, 2);
        assert_eq!(merged[2].seq, 1);
    }

    #[test]
    fn should_handle_iterator_exhaustion_correctly() {
        // Arrange: input with 2 entries
        let s0 = vec![entry("a", "a1", 1), entry("a", "a2", 2)];

        // Act: convert to iterator and verify both entries are yielded
        let mut iter = MergeIterator::from_iterators(vec![s0.into_iter()]);
        let first = iter.next();
        let second = iter.next();
        let third = iter.next();

        // Assert
        assert!(first.is_some());
        assert!(second.is_some());
        assert!(third.is_none()); // Iterator exhausted
    }

    // ============================================================================
    // Tests for edge cases and robustness
    // ============================================================================

    #[test]
    fn should_handle_very_large_sequence_numbers() {
        // Arrange: entries with max/large sequence numbers
        let s0 = vec![entry("k", "v1", u64::MAX - 1)];
        let s1 = vec![entry("k", "v2", u64::MAX)];

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter()]).collect();

        // Assert: max sequence should come first
        assert_eq!(merged[0].seq, u64::MAX);
        assert_eq!(merged[1].seq, u64::MAX - 1);
    }

    #[test]
    fn should_handle_sequence_zero_correctly() {
        // Arrange: entry with sequence 0
        let s0 = vec![entry("k", "v", 0)];

        // Act
        let merged: Vec<MergeEntry> = MergeIterator::from_iterators(vec![s0.into_iter()]).collect();

        // Assert
        assert_eq!(merged[0].seq, 0);
    }

    #[test]
    fn should_handle_empty_input_among_non_empty_inputs() {
        // Arrange: mix of empty and non-empty inputs
        let s0: Vec<MergeEntry> = vec![];
        let s1 = vec![entry("b", "b", 1)];
        let s2: Vec<MergeEntry> = vec![];
        let s3 = vec![entry("a", "a", 1)];

        // Act
        let merged: Vec<MergeEntry> = MergeIterator::from_iterators(vec![
            s0.into_iter(),
            s1.into_iter(),
            s2.into_iter(),
            s3.into_iter(),
        ])
        .collect();

        // Assert: only non-empty entries present, correctly ordered
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].key, Bytes::from("a"));
        assert_eq!(merged[1].key, Bytes::from("b"));
    }

    #[test]
    fn should_preserve_entry_values_during_merge() {
        // Arrange: entries with distinct values
        let s0 = vec![entry("k", "value0", 1)];
        let s1 = vec![entry("k", "value1", 2)];

        // Act
        let merged: Vec<MergeEntry> =
            MergeIterator::from_iterators(vec![s0.into_iter(), s1.into_iter()]).collect();

        // Assert: values preserved correctly
        assert_eq!(merged[0].value, Bytes::from("value1"));
        assert_eq!(merged[1].value, Bytes::from("value0"));
    }

    // ============================================================================
    // Tests for heap ordering correctness
    // ============================================================================

    #[test]
    fn should_correctly_order_heap_items_by_key_ascending() {
        // Arrange: HeapItems with different keys
        let item_a = HeapItem::new(&entry("aaa", "v", 1), 0);
        let item_b = HeapItem::new(&entry("bbb", "v", 1), 0);

        // Act: compare ordering

        // Assert: smaller key should have Greater ordering (max-heap inversion)
        assert_eq!(item_a.cmp(&item_b), Ordering::Greater);
    }

    #[test]
    fn should_correctly_order_heap_items_by_sequence_descending() {
        // Arrange: HeapItems with same key, different sequences
        let item_high = HeapItem::new(&entry("k", "v", 10), 0);
        let item_low = HeapItem::new(&entry("k", "v", 5), 0);

        // Act: compare ordering

        // Assert: higher sequence should win
        assert_eq!(item_high.cmp(&item_low), Ordering::Greater);
    }

    #[test]
    fn should_use_input_idx_as_tiebreaker() {
        // Arrange: HeapItems with same key and sequence, different input_idx
        let item_idx0 = HeapItem::new(&entry("k", "v", 5), 0);
        let item_idx1 = HeapItem::new(&entry("k", "v", 5), 1);

        // Act: compare ordering

        // Assert: lower index should have Greater ordering
        assert_eq!(item_idx0.cmp(&item_idx1), Ordering::Greater);
    }

    #[test]
    fn should_implement_equality_correctly() {
        // Arrange
        let item1 = HeapItem::new(&entry("k", "v", 5), 0);
        let item2 = HeapItem::new(&entry("k", "v", 5), 0);
        let item3 = HeapItem::new(&entry("k", "v", 3), 0);

        // Act: equality checks

        // Assert
        assert_eq!(item1, item2);
        assert_ne!(item1, item3);
    }

    // ============================================================================
    // Tests for iterator trait implementation
    // ============================================================================

    #[test]
    fn should_implement_iterator_trait_correctly() {
        // Arrange: iterator with 2 entries
        let s0 = vec![entry("a", "a", 1), entry("b", "b", 1)];
        let mut iter = MergeIterator::from_iterators(vec![s0.into_iter()]);

        // Act: use iterator trait methods
        assert!(iter.next().is_some());
        assert!(iter.next().is_some());
        let exhausted = iter.next().is_none();

        // Assert
        assert!(exhausted);
    }

    #[test]
    fn should_support_collect_on_iterator() {
        // Arrange
        let s0 = vec![entry("a", "a", 1)];

        // Act: use collect which depends on Iterator trait
        let result: Vec<_> = MergeIterator::from_iterators(vec![s0.into_iter()]).collect();

        // Assert
        assert_eq!(result.len(), 1);
    }
}
