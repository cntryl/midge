//! Range tombstone support for efficient deletion of key ranges.
//!
//! Range tombstones allow deleting a range [start, end) with a single operation
//! rather than creating individual tombstones for each key. This is critical
//! for correctness in LSM trees - without persisting range tombstones in SSTs,
//! compaction can resurrect deleted keys.

use bytes::Bytes;
use std::ops::Range;

/// A range tombstone deletes all keys in [start, end) at a given sequence number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTombstone {
    pub start: Bytes,
    pub end: Bytes,
    pub seq: u64,
}

impl RangeTombstone {
    pub fn new(start: Bytes, end: Bytes, seq: u64) -> Self {
        Self { start, end, seq }
    }

    /// Check if this range tombstone covers the given key.
    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_ref() && key < self.end.as_ref()
    }

    /// Check if this range tombstone overlaps with the given range.
    #[inline]
    pub fn overlaps(&self, start: &[u8], end: &[u8]) -> bool {
        self.start.as_ref() < end && self.end.as_ref() > start
    }

    /// Get the range as a standard Range.
    #[inline]
    pub fn as_range(&self) -> Range<&[u8]> {
        self.start.as_ref()..self.end.as_ref()
    }
}

/// Check if two range tombstones can be merged.
///
/// Two tombstones can be merged if they have the same sequence number AND
/// are either overlapping or adjacent (one's end equals the other's start).
fn can_merge(a: &RangeTombstone, b: &RangeTombstone) -> bool {
    a.seq == b.seq
        && (a.overlaps(b.start.as_ref(), b.end.as_ref()) || a.end == b.start || b.end == a.start)
}

/// Merge two range tombstones into a single tombstone.
///
/// Returns a new tombstone that covers the union of both input ranges.
/// Assumes the tombstones have the same sequence number.
fn merge_ranges(a: &RangeTombstone, b: &RangeTombstone) -> RangeTombstone {
    RangeTombstone {
        start: std::cmp::min(&a.start, &b.start).clone(),
        end: std::cmp::max(&a.end, &b.end).clone(),
        seq: a.seq,
    }
}

/// Collection of range tombstones sorted by start key.
#[derive(Debug, Clone, Default)]
pub struct RangeTombstoneList {
    tombstones: Vec<RangeTombstone>,
}

impl RangeTombstoneList {
    pub fn new() -> Self {
        Self {
            tombstones: Vec::new(),
        }
    }

    /// Add a range tombstone to the list.
    pub fn add(&mut self, tombstone: RangeTombstone) {
        self.tombstones.push(tombstone);
        // Keep sorted by start key, then by sequence (descending)
        self.tombstones.sort_by(|a, b| {
            match a.start.cmp(&b.start) {
                std::cmp::Ordering::Equal => b.seq.cmp(&a.seq), // Newer first
                ord => ord,
            }
        });
    }

    /// Add a range tombstone, merging with adjacent/overlapping tombstones.
    ///
    /// If the new tombstone has the same sequence number as existing tombstones
    /// and is adjacent or overlapping, they will be merged into a single larger
    /// tombstone. This reduces memory overhead and speeds up lookup operations.
    ///
    /// # Examples
    ///
    /// ```
    /// # use cntryl_midge::common::range_tombstone::{RangeTombstone, RangeTombstoneList};
    /// # use bytes::Bytes;
    /// let mut list = RangeTombstoneList::new();
    /// list.add_with_coalesce(RangeTombstone::new(Bytes::from("a"), Bytes::from("b"), 1));
    /// list.add_with_coalesce(RangeTombstone::new(Bytes::from("b"), Bytes::from("c"), 1));
    /// // Two adjacent tombstones merged into [a,c)
    /// assert_eq!(list.len(), 1);
    /// ```
    pub fn add_with_coalesce(&mut self, tombstone: RangeTombstone) {
        // Check for merge opportunities with existing tombstones
        let mut merged = tombstone;
        let mut to_remove = vec![];

        for (idx, existing) in self.tombstones.iter().enumerate() {
            // Same sequence number + (adjacent OR overlapping) = merge
            if existing.seq == merged.seq && can_merge(existing, &merged) {
                merged = merge_ranges(existing, &merged);
                to_remove.push(idx);
            }
        }

        // Remove merged tombstones (in reverse order to maintain indices)
        for idx in to_remove.iter().rev() {
            self.tombstones.remove(*idx);
        }

        // Add the coalesced tombstone
        self.tombstones.push(merged);

        // Re-sort the list
        self.tombstones.sort_by(|a, b| match a.start.cmp(&b.start) {
            std::cmp::Ordering::Equal => b.seq.cmp(&a.seq),
            ord => ord,
        });
    }

    /// Check if a key is covered by any range tombstone.
    /// Returns the sequence number of the covering tombstone, if any.
    /// If multiple tombstones cover the key, returns the newest (highest seq).
    ///
    /// Uses binary search for O(log n + k) complexity where k is the number
    /// of overlapping tombstones.
    #[inline]
    pub fn covers(&self, key: &[u8]) -> Option<u64> {
        if self.tombstones.is_empty() {
            return None;
        }

        // Binary search to find the rightmost tombstone where start <= key
        let idx = match self.tombstones.binary_search_by(|tomb| {
            if tomb.start.as_ref() > key {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(i) => i,
            Err(i) => {
                if i == 0 {
                    return None; // All tombstones start after key
                }
                i - 1
            }
        };

        // Check backwards from idx (tombstones sorted by start)
        let mut max_seq: Option<u64> = None;
        for tomb in self.tombstones[..=idx].iter().rev() {
            if tomb.end.as_ref() <= key {
                break; // No more tombstones can cover (end <= key)
            }
            if tomb.contains(key) {
                max_seq = Some(max_seq.map(|s| s.max(tomb.seq)).unwrap_or(tomb.seq));
            }
        }

        max_seq
    }

    /// Get all tombstones that overlap with the given range.
    #[inline]
    pub fn overlapping(&self, start: &[u8], end: &[u8]) -> Vec<&RangeTombstone> {
        self.tombstones
            .iter()
            .filter(|t| t.overlaps(start, end))
            .collect()
    }

    /// Get all tombstones.
    pub fn all(&self) -> &[RangeTombstone] {
        &self.tombstones
    }

    /// Check if the list is empty.
    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty()
    }

    /// Get the number of tombstones.
    pub fn len(&self) -> usize {
        self.tombstones.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_store_start_key() {
        // Arrange
        let start = Bytes::from("a");
        let end = Bytes::from("z");
        let seq = 42;

        // Act
        let rt = RangeTombstone::new(start.clone(), end, seq);

        // Assert
        assert_eq!(rt.start, start);
    }

    #[test]
    fn should_store_end_key() {
        // Arrange
        let start = Bytes::from("a");
        let end = Bytes::from("z");
        let seq = 42;

        // Act
        let rt = RangeTombstone::new(start, end.clone(), seq);

        // Assert
        assert_eq!(rt.end, end);
    }

    #[test]
    fn should_store_sequence_number() {
        // Arrange
        let start = Bytes::from("a");
        let end = Bytes::from("z");
        let seq = 42;

        // Act
        let rt = RangeTombstone::new(start, end, seq);

        // Assert
        assert_eq!(rt.seq, seq);
    }

    #[test]
    fn should_contain_key_within_range() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 1);

        // Act
        let contains = rt.contains(b"m");

        // Assert
        assert!(contains);
    }

    #[test]
    fn should_contain_start_key_inclusively() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 1);

        // Act
        let contains = rt.contains(b"a");

        // Assert
        assert!(contains);
    }

    #[test]
    fn should_not_contain_end_key_exclusively() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 1);

        // Act
        let contains = rt.contains(b"z");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_not_contain_key_before_start() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1);

        // Act
        let contains = rt.contains(b"a");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_not_contain_key_after_end() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1);

        // Act
        let contains = rt.contains(b"z");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_detect_overlap_with_partial_left_intersection() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("d"), Bytes::from("g"), 1);

        // Act
        let overlaps = rt.overlaps(b"a", b"e");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_detect_overlap_with_partial_right_intersection() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("d"), Bytes::from("g"), 1);

        // Act
        let overlaps = rt.overlaps(b"e", b"h");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_detect_overlap_with_exact_match() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("d"), Bytes::from("g"), 1);

        // Act
        let overlaps = rt.overlaps(b"d", b"g");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_detect_overlap_when_fully_contained() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("d"), Bytes::from("g"), 1);

        // Act
        let overlaps = rt.overlaps(b"a", b"z");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_detect_overlap_when_range_within() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("d"), Bytes::from("g"), 1);

        // Act
        let overlaps = rt.overlaps(b"e", b"f");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_not_detect_overlap_with_range_before() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1);

        // Act
        let overlaps = rt.overlaps(b"a", b"j");

        // Assert
        assert!(!overlaps);
    }

    #[test]
    fn should_not_detect_overlap_with_range_after() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1);

        // Act
        let overlaps = rt.overlaps(b"q", b"z");

        // Assert
        assert!(!overlaps);
    }

    #[test]
    fn should_not_detect_overlap_with_adjacent_range_before() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1);

        // Act
        let overlaps = rt.overlaps(b"a", b"m");

        // Assert
        assert!(!overlaps);
    }

    #[test]
    fn should_not_detect_overlap_with_adjacent_range_after() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1);

        // Act
        let overlaps = rt.overlaps(b"p", b"z");

        // Assert
        assert!(!overlaps);
    }

    #[test]
    fn should_return_start_as_borrowed_range() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 1);

        // Act
        let range = rt.as_range();

        // Assert
        assert_eq!(range.start, b"a".as_ref());
    }

    #[test]
    fn should_return_end_as_borrowed_range() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 1);

        // Act
        let range = rt.as_range();

        // Assert
        assert_eq!(range.end, b"z".as_ref());
    }

    #[test]
    fn should_be_empty_when_created() {
        // Arrange
        // Act
        let list = RangeTombstoneList::new();

        // Assert
        assert!(list.is_empty());
    }

    #[test]
    fn should_have_zero_tombstones_when_created() {
        // Arrange
        // Act
        let list = RangeTombstoneList::new();

        // Assert
        assert_eq!(list.all().len(), 0);
    }

    #[test]
    fn should_contain_tombstone_after_add() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 1);

        // Act
        list.add(rt.clone());

        // Assert
        assert!(!list.is_empty());
        assert_eq!(list.all().len(), 1);
        assert_eq!(list.all()[0], rt);
    }

    #[test]
    fn should_maintain_sorted_order_by_start_key_ascending() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        let rt1 = RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1);
        let rt2 = RangeTombstone::new(Bytes::from("a"), Bytes::from("d"), 2);
        let rt3 = RangeTombstone::new(Bytes::from("x"), Bytes::from("z"), 3);

        // Act
        list.add(rt1);
        list.add(rt2);
        list.add(rt3);

        // Assert
        let all = list.all();
        assert_eq!(all[0].start, Bytes::from("a"));
        assert_eq!(all[1].start, Bytes::from("m"));
        assert_eq!(all[2].start, Bytes::from("x"));
    }

    #[test]
    fn should_sort_by_sequence_descending_given_same_start_key() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        let rt1 = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 10);
        let rt2 = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 20);
        let rt3 = RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 5);

        // Act
        list.add(rt1);
        list.add(rt2);
        list.add(rt3);

        // Assert
        let all = list.all();
        assert_eq!(all[0].seq, 20);
        assert_eq!(all[1].seq, 10);
        assert_eq!(all[2].seq, 5);
    }

    #[test]
    fn should_return_sequence_for_key_in_first_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("m"), 10));
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("z"), 20));

        // Act
        let seq = list.covers(b"e");

        // Assert
        assert_eq!(seq, Some(10));
    }

    #[test]
    fn should_return_sequence_for_key_in_second_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("m"), 10));
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("z"), 20));

        // Act
        let seq = list.covers(b"p");

        // Assert
        assert_eq!(seq, Some(20));
    }

    #[test]
    fn should_return_none_for_key_not_covered() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("m"), 10));
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("z"), 20));

        // Act
        let seq = list.covers(b"zz");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_return_highest_sequence_given_multiple_covering_tombstones() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 10));
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 20));
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 5));

        // Act
        let covered_seq = list.covers(b"m");

        // Assert
        assert_eq!(covered_seq, Some(20));
    }

    #[test]
    fn should_return_all_overlapping_tombstones_for_range_query() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("d"), 1));
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 2));
        list.add(RangeTombstone::new(Bytes::from("x"), Bytes::from("z"), 3));

        // Act
        let overlapping = list.overlapping(b"b", b"n");

        // Assert
        assert_eq!(overlapping.len(), 2);
        assert_eq!(overlapping[0].start, Bytes::from("a"));
        assert_eq!(overlapping[1].start, Bytes::from("m"));
    }

    #[test]
    fn should_return_empty_list_given_no_overlapping_tombstones() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("d"), 1));
        list.add(RangeTombstone::new(Bytes::from("x"), Bytes::from("z"), 2));

        // Act
        let overlapping = list.overlapping(b"m", b"p");

        // Assert
        assert_eq!(overlapping.len(), 0);
    }

    #[test]
    fn should_not_contain_start_key_when_empty_range() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("a"), 1);

        // Act
        let contains = rt.contains(b"a");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_not_contain_empty_key_when_empty_range() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("a"), 1);

        // Act
        let contains = rt.contains(b"");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_not_cover_key_before_all_ranges_in_sorted_list() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        for i in 0..100 {
            let start = format!("k{:03}", i);
            let end = format!("k{:03}", i + 1);
            list.add(RangeTombstone::new(
                Bytes::from(start),
                Bytes::from(end),
                i as u64,
            ));
        }

        // Act
        let seq = list.covers(b"a");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_not_cover_key_after_all_ranges_in_sorted_list() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        for i in 0..100 {
            let start = format!("k{:03}", i);
            let end = format!("k{:03}", i + 1);
            list.add(RangeTombstone::new(
                Bytes::from(start),
                Bytes::from(end),
                i as u64,
            ));
        }

        // Act
        let seq = list.covers(b"z");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_contain_start_key_in_single_byte_range() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("b"), 1);

        // Act
        let contains = rt.contains(b"a");

        // Assert
        assert!(contains);
    }

    #[test]
    fn should_not_contain_end_key_in_single_byte_range() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("b"), 1);

        // Act
        let contains = rt.contains(b"b");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_contain_key_between_start_and_end_in_single_byte_range() {
        // Arrange
        let rt = RangeTombstone::new(Bytes::from("a"), Bytes::from("b"), 1);

        // Act
        let contains = rt.contains(b"aa");

        // Assert
        assert!(contains);
    }

    #[test]
    fn should_cover_start_key_inclusively() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_a"),
            Bytes::from_static(b"key_z"),
            100,
        );

        // Act
        let contains = tomb.contains(b"key_a");

        // Assert
        assert!(contains);
    }

    #[test]
    fn should_cover_middle_key() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_a"),
            Bytes::from_static(b"key_z"),
            100,
        );

        // Act
        let contains = tomb.contains(b"key_m");

        // Assert
        assert!(contains);
    }

    #[test]
    fn should_not_cover_end_key_exclusively() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_a"),
            Bytes::from_static(b"key_z"),
            100,
        );

        // Act
        let contains = tomb.contains(b"key_z");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_not_cover_key_before_range() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_a"),
            Bytes::from_static(b"key_z"),
            100,
        );

        // Act
        let contains = tomb.contains(b"key_A");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_not_cover_key_after_range() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_a"),
            Bytes::from_static(b"key_z"),
            100,
        );

        // Act
        let contains = tomb.contains(b"zzz");

        // Assert
        assert!(!contains);
    }

    #[test]
    fn should_overlap_when_query_starts_before_and_ends_in_range() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_d"),
            Bytes::from_static(b"key_w"),
            100,
        );

        // Act
        let overlaps = tomb.overlaps(b"key_a", b"key_f");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_overlap_when_query_starts_in_range_and_ends_after() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_d"),
            Bytes::from_static(b"key_w"),
            100,
        );

        // Act
        let overlaps = tomb.overlaps(b"key_m", b"key_z");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_overlap_when_query_fully_contained_in_range() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_d"),
            Bytes::from_static(b"key_w"),
            100,
        );

        // Act
        let overlaps = tomb.overlaps(b"key_e", b"key_p");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_overlap_when_query_fully_contains_range() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_d"),
            Bytes::from_static(b"key_w"),
            100,
        );

        // Act
        let overlaps = tomb.overlaps(b"key_a", b"key_z");

        // Assert
        assert!(overlaps);
    }

    #[test]
    fn should_not_overlap_when_query_before_range() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_d"),
            Bytes::from_static(b"key_w"),
            100,
        );

        // Act
        let overlaps = tomb.overlaps(b"key_a", b"key_d");

        // Assert
        assert!(!overlaps);
    }

    #[test]
    fn should_not_overlap_when_query_after_range() {
        // Arrange
        let tomb = RangeTombstone::new(
            Bytes::from_static(b"key_d"),
            Bytes::from_static(b"key_w"),
            100,
        );

        // Act
        let overlaps = tomb.overlaps(b"key_w", b"key_z");

        // Assert
        assert!(!overlaps);
    }

    #[test]
    fn should_find_covering_tombstone_in_first_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"m"),
            100,
        ));
        list.add(RangeTombstone::new(
            Bytes::from_static(b"m"),
            Bytes::from_static(b"z"),
            200,
        ));

        // Act
        let seq = list.covers(b"b");

        // Assert
        assert_eq!(seq, Some(100));
    }

    #[test]
    fn should_find_covering_tombstone_in_second_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"m"),
            100,
        ));
        list.add(RangeTombstone::new(
            Bytes::from_static(b"m"),
            Bytes::from_static(b"z"),
            200,
        ));

        // Act
        let seq = list.covers(b"n");

        // Assert
        assert_eq!(seq, Some(200));
    }

    #[test]
    fn should_not_find_covering_tombstone_before_all_ranges() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"m"),
            100,
        ));
        list.add(RangeTombstone::new(
            Bytes::from_static(b"m"),
            Bytes::from_static(b"z"),
            200,
        ));

        // Act
        let seq = list.covers(b"A");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_not_find_covering_tombstone_after_all_ranges() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"m"),
            100,
        ));
        list.add(RangeTombstone::new(
            Bytes::from_static(b"m"),
            Bytes::from_static(b"z"),
            200,
        ));

        // Act
        let seq = list.covers(b"zzz");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_return_newer_sequence_when_multiple_tombstones_cover_key() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"z"),
            100,
        ));
        list.add(RangeTombstone::new(
            Bytes::from_static(b"h"),
            Bytes::from_static(b"p"),
            200,
        ));

        // Act
        let seq = list.covers(b"k");

        // Assert
        assert_eq!(seq, Some(200));
    }

    #[test]
    fn should_return_sequence_when_only_older_tombstone_covers_key() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(
            Bytes::from_static(b"a"),
            Bytes::from_static(b"z"),
            100,
        ));
        list.add(RangeTombstone::new(
            Bytes::from_static(b"h"),
            Bytes::from_static(b"p"),
            200,
        ));

        // Act
        let seq = list.covers(b"b");

        // Assert
        assert_eq!(seq, Some(100));
    }

    // Tests for coalescing functionality

    #[test]
    fn should_coalesce_adjacent_range_tombstones() {
        // Arrange
        let mut list = RangeTombstoneList::new();

        // Act
        list.add_with_coalesce(RangeTombstone::new(Bytes::from("a"), Bytes::from("b"), 1));
        list.add_with_coalesce(RangeTombstone::new(Bytes::from("b"), Bytes::from("c"), 1));

        // Assert
        assert_eq!(list.len(), 1);
        let tomb = &list.all()[0];
        assert_eq!(tomb.start, Bytes::from("a"));
        assert_eq!(tomb.end, Bytes::from("c"));
        assert_eq!(tomb.seq, 1);
    }

    #[test]
    fn should_coalesce_overlapping_range_tombstones() {
        // Arrange
        let mut list = RangeTombstoneList::new();

        // Act
        list.add_with_coalesce(RangeTombstone::new(Bytes::from("a"), Bytes::from("d"), 1));
        list.add_with_coalesce(RangeTombstone::new(Bytes::from("c"), Bytes::from("f"), 1));

        // Assert
        assert_eq!(list.len(), 1);
        let tomb = &list.all()[0];
        assert_eq!(tomb.start, Bytes::from("a"));
        assert_eq!(tomb.end, Bytes::from("f"));
    }

    #[test]
    fn should_not_coalesce_different_sequence_numbers() {
        // Arrange
        let mut list = RangeTombstoneList::new();

        // Act
        list.add_with_coalesce(RangeTombstone::new(Bytes::from("a"), Bytes::from("b"), 1));
        list.add_with_coalesce(RangeTombstone::new(Bytes::from("b"), Bytes::from("c"), 2));

        // Assert
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn should_coalesce_multiple_tombstones() {
        // Arrange
        let mut list = RangeTombstoneList::new();

        // Act
        for i in 0..10 {
            let start = format!("key{:03}", i);
            let end = format!("key{:03}", i + 1);
            list.add_with_coalesce(RangeTombstone::new(Bytes::from(start), Bytes::from(end), 1));
        }

        // Assert
        assert_eq!(list.len(), 1);
        let tomb = &list.all()[0];
        assert_eq!(tomb.start, Bytes::from("key000"));
        assert_eq!(tomb.end, Bytes::from("key010"));
    }

    #[test]
    fn should_not_coalesce_non_adjacent_tombstones() {
        // Arrange
        let mut list = RangeTombstoneList::new();

        // Act
        list.add_with_coalesce(RangeTombstone::new(Bytes::from("a"), Bytes::from("c"), 1));
        list.add_with_coalesce(RangeTombstone::new(Bytes::from("e"), Bytes::from("g"), 1));

        // Assert
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn should_binary_search_find_coverage_in_first_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("c"), 1));
        list.add(RangeTombstone::new(Bytes::from("e"), Bytes::from("g"), 2));
        list.add(RangeTombstone::new(Bytes::from("k"), Bytes::from("m"), 3));

        // Act
        let seq = list.covers(b"b");

        // Assert
        assert_eq!(seq, Some(1));
    }

    #[test]
    fn should_binary_search_find_coverage_in_second_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("c"), 1));
        list.add(RangeTombstone::new(Bytes::from("e"), Bytes::from("g"), 2));
        list.add(RangeTombstone::new(Bytes::from("k"), Bytes::from("m"), 3));

        // Act
        let seq = list.covers(b"f");

        // Assert
        assert_eq!(seq, Some(2));
    }

    #[test]
    fn should_binary_search_find_coverage_in_third_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("c"), 1));
        list.add(RangeTombstone::new(Bytes::from("e"), Bytes::from("g"), 2));
        list.add(RangeTombstone::new(Bytes::from("k"), Bytes::from("m"), 3));

        // Act
        let seq = list.covers(b"l");

        // Assert
        assert_eq!(seq, Some(3));
    }

    #[test]
    fn should_binary_search_return_none_for_gap_after_first_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("c"), 1));
        list.add(RangeTombstone::new(Bytes::from("e"), Bytes::from("g"), 2));
        list.add(RangeTombstone::new(Bytes::from("k"), Bytes::from("m"), 3));

        // Act
        let seq = list.covers(b"d");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_binary_search_return_none_for_gap_after_second_range() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("c"), 1));
        list.add(RangeTombstone::new(Bytes::from("e"), Bytes::from("g"), 2));
        list.add(RangeTombstone::new(Bytes::from("k"), Bytes::from("m"), 3));

        // Act
        let seq = list.covers(b"h");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_binary_search_return_none_for_key_before_all_tombstones() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1));

        // Act
        let seq = list.covers(b"a");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_binary_search_return_none_for_key_after_all_tombstones() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1));

        // Act
        let seq = list.covers(b"z");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_binary_search_cover_start_key_inclusively() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1));

        // Act
        let seq = list.covers(b"m");

        // Assert
        assert_eq!(seq, Some(1));
    }

    #[test]
    fn should_binary_search_not_cover_end_key_exclusively() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 1));

        // Act
        let seq = list.covers(b"p");

        // Assert
        assert_eq!(seq, None);
    }

    #[test]
    fn should_handle_multiple_overlapping_tombstones_with_binary_search() {
        // Arrange
        let mut list = RangeTombstoneList::new();
        list.add(RangeTombstone::new(Bytes::from("a"), Bytes::from("z"), 1));
        list.add(RangeTombstone::new(Bytes::from("m"), Bytes::from("p"), 5));
        list.add(RangeTombstone::new(Bytes::from("k"), Bytes::from("r"), 3));

        // Act
        let seq = list.covers(b"n");

        // Assert
        assert_eq!(seq, Some(5));
    }
}
