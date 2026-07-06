//! Iterator API - Range scanning and sequential reads
//!
//! Iterators provide efficient sequential access to key-value pairs,
//! including full range scans, prefix scans, and reverse iteration.

/// Iteration direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Forward iteration (ascending keys)
    Forward,
    /// Reverse iteration (descending keys)
    Reverse,
}

/// A range iterator over key-value pairs
///
/// Iterators provide efficient sequential access to the database.
/// They can be created with various options (direction, bounds, etc).
///
/// Supports two modes:
/// Results are buffered upfront by transaction scans and snapshot-based reads.
pub struct Iterator {
    /// Current position within the current batch
    position: usize,
    /// Current batch of results
    results: Vec<(Vec<u8>, Vec<u8>)>,
    /// Iteration direction
    direction: Direction,
    /// Whether iteration has completed
    exhausted: bool,
}

impl Iterator {
    /// Create a new iterator with the given results (eager mode)
    pub(crate) fn new(results: Vec<(Vec<u8>, Vec<u8>)>, direction: Direction) -> Self {
        Self {
            position: 0,
            results,
            direction,
            exhausted: false,
        }
    }

    /// Create a forward iterator (eager mode)
    pub(crate) fn forward(results: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self::new(results, Direction::Forward)
    }

    /// Create a reverse iterator (eager mode)
    pub(crate) fn reverse(mut results: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        results.reverse();
        Self::new(results, Direction::Reverse)
    }

    /// Get the current key-value pair without advancing
    #[must_use]
    pub fn current(&self) -> Option<(&[u8], &[u8])> {
        if self.exhausted || self.position >= self.results.len() {
            return None;
        }
        let (k, v) = &self.results[self.position];
        Some((k.as_slice(), v.as_slice()))
    }

    /// Check if iteration is complete
    #[must_use]
    pub fn exhausted(&self) -> bool {
        self.exhausted || self.position >= self.results.len()
    }

    /// Get the direction of this iterator
    #[must_use]
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// Get the count of items remaining.
    #[must_use]
    pub fn remaining(&self) -> usize {
        if self.exhausted {
            0
        } else {
            self.results.len().saturating_sub(self.position)
        }
    }

    /// Collect all remaining pairs into a vector
    pub fn collect_all(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.by_ref().collect()
    }
}

impl std::iter::Iterator for Iterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        if self.position >= self.results.len() {
            self.exhausted = true;
            return None;
        }

        let pair = self.results[self.position].clone();
        self.position += 1;
        Some(pair)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iterator_with_bounds(
        results: Vec<(Vec<u8>, Vec<u8>)>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        include_end: bool,
        direction: Direction,
    ) -> Iterator {
        let filtered = results
            .into_iter()
            .filter(|(key, _)| {
                if let Some(start_key) = start {
                    if key.as_slice() < start_key {
                        return false;
                    }
                }

                if let Some(end_key) = end {
                    if include_end {
                        if key.as_slice() > end_key {
                            return false;
                        }
                    } else if key.as_slice() >= end_key {
                        return false;
                    }
                }

                true
            })
            .collect::<Vec<_>>();

        match direction {
            Direction::Forward => Iterator::forward(filtered),
            Direction::Reverse => Iterator::reverse(filtered),
        }
    }

    fn forward_with_bounds(
        results: Vec<(Vec<u8>, Vec<u8>)>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        include_end: bool,
    ) -> Iterator {
        iterator_with_bounds(results, start, end, include_end, Direction::Forward)
    }

    fn reverse_with_bounds(
        results: Vec<(Vec<u8>, Vec<u8>)>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        include_end: bool,
    ) -> Iterator {
        iterator_with_bounds(results, start, end, include_end, Direction::Reverse)
    }

    // ========== Direction Enum Tests ==========
    // Tests for Direction invariants: equality, copy semantics, debug representation

    #[test]
    fn should_have_forward_variant() {
        // Arrange

        // Act
        let forward = Direction::Forward;

        // Assert
        assert_eq!(forward, Direction::Forward);
    }

    #[test]
    fn should_have_reverse_variant() {
        // Arrange

        // Act
        let reverse = Direction::Reverse;

        // Assert
        assert_eq!(reverse, Direction::Reverse);
    }

    #[test]
    fn should_distinguish_forward_from_reverse() {
        // Arrange
        let forward = Direction::Forward;
        let reverse = Direction::Reverse;

        // Act
        // (compare values)

        // Assert
        assert_ne!(forward, reverse);
    }

    #[test]
    fn should_be_copyable_when_passing_direction() {
        // Arrange
        let dir1 = Direction::Forward;

        // Act
        let dir2 = dir1; // Copy
        let dir3 = dir1;

        // Assert
        assert_eq!(dir2, Direction::Forward);
        assert_eq!(dir3, Direction::Forward);
    }

    #[test]
    fn should_maintain_equality_across_copies_when_direction_copied() {
        // Arrange
        let original = Direction::Reverse;

        // Act
        let copy1 = original;
        let copy2 = original;

        // Assert
        assert_eq!(copy1, copy2);
        assert_eq!(copy1, original);
    }

    // ========== Iterator Initialization Tests ==========
    // Tests for Iterator creation invariants: position starts at 0, direction preserved, exhausted starts false

    #[test]
    fn should_initialize_position_at_zero_when_creating_iterator() {
        // Arrange
        // Act
        let results = vec![(vec![1], vec![10])];
        let iter = Iterator::forward(results);

        // Assert - position is 0, so current() should return first element
        assert_eq!(iter.current(), Some((&[1][..], &[10][..])));
    }

    #[test]
    fn should_set_exhausted_false_when_creating_new_iterator() {
        // Arrange
        // Act
        let results = vec![(vec![1], vec![10])];
        let iter = Iterator::forward(results);

        // Assert
        assert!(!iter.exhausted());
    }

    #[test]
    fn should_preserve_direction_when_creating_iterator_with_direction() {
        // Arrange
        let results = vec![(vec![1], vec![10])];

        // Act
        let forward_iter = Iterator::forward(results.clone());
        let reverse_iter = Iterator::reverse(results);

        // Assert
        assert_eq!(forward_iter.direction(), Direction::Forward);
        assert_eq!(reverse_iter.direction(), Direction::Reverse);
    }

    #[test]
    fn should_create_forward_iterator_when_initialized() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];

        // Act
        let iter = Iterator::forward(results);

        // Assert
        assert_eq!(iter.direction(), Direction::Forward);
        assert!(!iter.exhausted());
        assert_eq!(iter.remaining(), 2);
    }

    #[test]
    fn should_set_correct_length_when_creating_iterator() {
        // Arrange
        // Act
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];
        let iter = Iterator::forward(results);

        // Assert
        assert_eq!(iter.remaining(), 3);
    }

    // ========== Iterator Next() Behavior Tests ==========
    // Tests for Iterator::next() invariants: advances position, returns cloned pair, sets exhausted when done

    #[test]
    fn should_iterate_forward_when_calling_next() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];
        let mut iter = Iterator::forward(results);

        // Act
        let pair1 = iter.next();
        let pair2 = iter.next();
        let pair3 = iter.next();
        let pair4 = iter.next();

        // Assert
        assert_eq!(pair1, Some((vec![1], vec![10])));
        assert_eq!(pair2, Some((vec![2], vec![20])));
        assert_eq!(pair3, Some((vec![3], vec![30])));
        assert_eq!(pair4, None);
        assert!(iter.exhausted());
    }

    #[test]
    fn should_advance_position_when_calling_next() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];
        let mut iter = Iterator::forward(results);

        // Act
        assert_eq!(iter.remaining(), 2);
        iter.next();

        // Assert
        assert_eq!(iter.remaining(), 1);
    }

    #[test]
    fn should_return_none_after_exhaustion_when_calling_next() {
        // Arrange
        let results = vec![(vec![1], vec![10])];
        let mut iter = Iterator::forward(results);

        // Act
        iter.next(); // Consume the only item
        let result = iter.next(); // Try to get beyond end

        // Assert
        assert_eq!(result, None);
    }

    #[test]
    fn should_return_cloned_pair_when_calling_next() {
        // Arrange
        let results = vec![(vec![1, 2, 3], vec![10, 20, 30])];
        let mut iter = Iterator::forward(results);

        // Act
        let pair = iter.next();

        // Assert
        assert_eq!(pair, Some((vec![1, 2, 3], vec![10, 20, 30])));
    }

    #[test]
    fn should_continue_returning_none_when_calling_next_after_exhausted() {
        // Arrange
        let results = vec![(vec![1], vec![10])];
        let mut iter = Iterator::forward(results);
        iter.next(); // Exhaust

        // Act
        let first_after_exhausted = iter.next();
        let second_after_exhausted = iter.next();
        let third_after_exhausted = iter.next();

        // Assert
        assert_eq!(first_after_exhausted, None);
        assert_eq!(second_after_exhausted, None);
        assert_eq!(third_after_exhausted, None);
    }

    // ========== Iterator Current() Behavior Tests ==========
    // Tests for Iterator::current() invariants: returns reference without advancing, returns None when exhausted

    #[test]
    fn should_return_current_pair_without_advancing_when_calling_current() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];
        let iter = Iterator::forward(results);

        // Act
        let current1 = iter.current();
        let current2 = iter.current();

        // Assert - calling current() multiple times returns same value
        assert_eq!(current1, Some((&[1][..], &[10][..])));
        assert_eq!(current2, Some((&[1][..], &[10][..])));
    }

    #[test]
    fn should_return_reference_when_calling_current() {
        // Arrange
        let results = vec![(vec![1, 2], vec![10, 20])];
        let iter = Iterator::forward(results);

        // Act
        let current = iter.current();

        // Assert - should be references to the data (single unwrap to avoid double call)
        let (k, v) = current.expect("current");
        assert_eq!(k, &[1, 2][..]);
        assert_eq!(v, &[10, 20][..]);
    }

    #[test]
    fn should_return_none_when_calling_current_on_exhausted_iterator() {
        // Arrange
        let results = vec![(vec![1], vec![10])];
        let mut iter = Iterator::forward(results);
        iter.next(); // Exhaust

        // Act
        let current = iter.current();

        // Assert
        assert_eq!(current, None);
    }

    #[test]
    fn should_return_none_when_calling_current_on_empty_iterator() {
        // Arrange
        let results: Vec<(Vec<u8>, Vec<u8>)> = vec![];

        // Act
        let iter = Iterator::forward(results);
        let current = iter.current();

        // Assert
        assert_eq!(current, None);
    }

    // ========== Iterator Reverse Tests ==========
    // Tests for Iterator::reverse() invariant: results reversed, direction set to Reverse

    #[test]
    fn should_reverse_order_when_creating_reverse_iterator() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        // Act
        let mut iter = Iterator::reverse(results);

        // Assert
        assert_eq!(iter.direction(), Direction::Reverse);
        assert_eq!(iter.next(), Some((vec![3], vec![30])));
        assert_eq!(iter.next(), Some((vec![2], vec![20])));
        assert_eq!(iter.next(), Some((vec![1], vec![10])));
    }

    #[test]
    fn should_preserve_all_pairs_when_reversing() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        // Act
        let mut iter = Iterator::reverse(results);
        let collected = iter.collect_all();

        // Assert - all pairs present, just reversed
        assert_eq!(collected.len(), 3);
        assert_eq!(
            collected,
            vec![
                (vec![3], vec![30]),
                (vec![2], vec![20]),
                (vec![1], vec![10])
            ]
        );
    }

    // ========== Iterator Remaining() Tests ==========
    // Tests for Iterator::remaining() invariant: returns count of unconsumed items

    #[test]
    fn should_track_remaining_count_when_iterating() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];
        let mut iter = Iterator::forward(results);

        // Act: advance and capture remaining counts at each step
        let r0 = iter.remaining();
        iter.next();
        let r1 = iter.remaining();
        iter.next();
        let r2 = iter.remaining();
        iter.next();
        let r3 = iter.remaining();
        // Assert: remaining counts should decrease as we consume items
        assert_eq!(r0, 3);
        assert_eq!(r1, 2);
        assert_eq!(r2, 1);
        assert_eq!(r3, 0);
    }

    #[test]
    fn should_return_zero_when_remaining_called_on_exhausted_iterator() {
        // Arrange
        let results = vec![(vec![1], vec![10])];
        let mut iter = Iterator::forward(results);
        iter.next(); // Exhaust

        // Act
        let remaining = iter.remaining();

        // Assert
        assert_eq!(remaining, 0);
    }

    #[test]
    fn should_return_full_count_when_remaining_called_on_new_iterator() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        // Act
        let iter = Iterator::forward(results);

        // Assert
        assert_eq!(iter.remaining(), 3);
    }

    // ========== Iterator Exhausted() Tests ==========
    // Tests for Iterator::exhausted() invariant: true after all items consumed or when set

    #[test]
    fn should_mark_exhausted_when_all_items_consumed() {
        // Arrange
        let results = vec![(vec![1], vec![10])];
        let mut iter = Iterator::forward(results);

        // Act
        iter.next();

        // Assert
        assert!(iter.exhausted());
    }

    #[test]
    fn should_not_mark_exhausted_when_iterator_created() {
        // Arrange
        // Act
        let results = vec![(vec![1], vec![10])];
        let iter = Iterator::forward(results);

        // Assert
        assert!(!iter.exhausted());
    }

    #[test]
    fn should_mark_exhausted_when_consuming_beyond_end() {
        // Arrange
        let results = vec![(vec![1], vec![10])];
        let mut iter = Iterator::forward(results);
        iter.next();

        // Act
        iter.next(); // Try to consume beyond end

        // Assert
        assert!(iter.exhausted());
    }

    // ========== Iterator Collect All Tests ==========
    // Tests for Iterator::collect_all() invariant: consumes all remaining, sets exhausted true

    #[test]
    fn should_collect_all_remaining_pairs_when_calling_collect_all() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];
        let mut iter = Iterator::forward(results);
        iter.next(); // Skip first

        // Act
        let collected = iter.collect_all();

        // Assert
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], (vec![2], vec![20]));
        assert_eq!(collected[1], (vec![3], vec![30]));
    }

    #[test]
    fn should_exhaust_iterator_when_calling_collect_all() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];
        let mut iter = Iterator::forward(results);

        // Act
        iter.collect_all();

        // Assert
        assert!(iter.exhausted());
    }

    #[test]
    fn should_return_empty_when_collect_all_called_on_exhausted_iterator() {
        // Arrange
        let results = vec![(vec![1], vec![10])];
        let mut iter = Iterator::forward(results);
        iter.next(); // Exhaust

        // Act
        let collected = iter.collect_all();

        // Assert
        assert_eq!(collected.len(), 0);
    }

    #[test]
    fn should_collect_all_when_called_immediately() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];
        let mut iter = Iterator::forward(results);

        // Act
        let collected = iter.collect_all();

        // Assert
        assert_eq!(collected.len(), 3);
    }

    // ========== Iterator Bound Tests ==========
    // Tests for start() method: sets start bound, includes start by default

    #[test]
    fn should_filter_below_start_when_start_set() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        // Act
        let mut iter = forward_with_bounds(results, Some(&[2]), None, true);

        // Assert
        assert_eq!(iter.next(), Some((vec![2], vec![20])));
        assert_eq!(iter.next(), Some((vec![3], vec![30])));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn should_include_start_key_when_start_set() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        // Act
        let mut iter = forward_with_bounds(results, Some(&[2]), None, true);

        // Assert
        assert_eq!(iter.next(), Some((vec![2], vec![20]))); // Start key included
    }

    #[test]
    fn should_filter_no_results_when_start_beyond_all_keys() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];

        // Act
        let mut iter = forward_with_bounds(results, Some(&[9]), None, true);

        // Assert
        assert_eq!(iter.next(), None);
    }

    // ========== Iterator End Bound Tests ==========
    // Tests for end() method: sets end bound, includes end by default

    #[test]
    fn should_filter_above_end_when_end_set() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        // Act
        let mut iter = forward_with_bounds(results, None, Some(&[2]), true);

        // Assert
        assert_eq!(iter.next(), Some((vec![1], vec![10])));
        assert_eq!(iter.next(), Some((vec![2], vec![20]))); // End key included
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn should_include_end_key_when_end_set() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        // Act
        let mut iter = forward_with_bounds(results, None, Some(&[2]), true);

        // Assert - end key is inclusive
        let last = iter.collect_all().last().map(|p| p.0.clone());
        assert_eq!(last, Some(vec![2]));
    }

    #[test]
    fn should_filter_no_results_when_end_below_all_keys() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];

        // Act
        let mut iter = forward_with_bounds(results, None, Some(&[0]), true);

        // Assert
        assert_eq!(iter.next(), None);
    }

    // ========== Iterator Range Tests ==========
    // Tests for range() method: sets bounds [start, end) with end exclusive

    #[test]
    fn should_build_iterator_with_range_bounds() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
            (vec![4], vec![40]),
            (vec![5], vec![50]),
        ];

        // Act
        let mut iter = forward_with_bounds(results, Some(&[2]), Some(&[4]), true);

        // Assert - inclusive on both ends
        assert_eq!(iter.next(), Some((vec![2], vec![20])));
        assert_eq!(iter.next(), Some((vec![3], vec![30])));
        assert_eq!(iter.next(), Some((vec![4], vec![40])));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn should_respect_range_exclusive_end_when_using_range_method() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
            (vec![4], vec![40]),
            (vec![5], vec![50]),
        ];

        // Act
        let mut iter = forward_with_bounds(results, Some(&[2]), Some(&[4]), false);

        // Assert - [2, 4) means 2 inclusive, 4 exclusive
        assert_eq!(iter.next(), Some((vec![2], vec![20])));
        assert_eq!(iter.next(), Some((vec![3], vec![30])));
        assert_eq!(iter.next(), None); // 4 excluded
    }

    #[test]
    fn should_set_include_end_false_when_using_range_method() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];

        // Act
        let mut iter = forward_with_bounds(results, Some(&[1]), Some(&[3]), false);
        let collected = iter.collect_all();

        // Assert - 3 should be excluded
        assert!(!collected.iter().any(|(k, _)| k == &vec![3]));
    }

    // ========== Iterator Bound Composition Tests ==========
    // Tests for multiple bounds and direction combinations

    #[test]
    fn should_apply_inclusive_bounds_before_reversing() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
            (vec![4], vec![40]),
        ];

        // Act
        let mut iter = reverse_with_bounds(results, Some(&[2]), Some(&[4]), true);

        // Assert
        assert_eq!(iter.direction(), Direction::Reverse);
        assert_eq!(iter.next(), Some((vec![4], vec![40])));
        assert_eq!(iter.next(), Some((vec![3], vec![30])));
        assert_eq!(iter.next(), Some((vec![2], vec![20])));
    }

    #[test]
    fn should_filter_to_specified_range() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
            (vec![4], vec![40]),
            (vec![5], vec![50]),
        ];

        // Act
        let mut iter = forward_with_bounds(results, Some(&[2]), Some(&[4]), true);

        // Assert
        let collected = iter.collect_all();
        assert_eq!(collected.len(), 3); // 2, 3, 4
        assert_eq!(collected[0].0, vec![2]);
        assert_eq!(collected[2].0, vec![4]);
    }

    #[test]
    fn should_apply_bounds_before_reversing_when_range_and_reverse_combined() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
            (vec![4], vec![40]),
        ];

        // Act
        let mut iter = reverse_with_bounds(results, Some(&[2]), Some(&[4]), false);

        // Assert - should have [2, 3] after filtering, reversed to [3, 2]
        assert_eq!(iter.next(), Some((vec![3], vec![30])));
        assert_eq!(iter.next(), Some((vec![2], vec![20])));
        assert_eq!(iter.next(), None);
    }

    // ========== Edge Cases ==========
    // Tests for edge cases and boundary conditions

    #[test]
    fn should_handle_empty_results_when_creating_iterator() {
        // Arrange
        let results: Vec<(Vec<u8>, Vec<u8>)> = vec![];

        // Act
        let mut iter = Iterator::forward(results);

        // Assert
        assert!(iter.exhausted());
        assert_eq!(iter.remaining(), 0);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn should_handle_single_item_when_creating_iterator() {
        // Arrange
        let results = vec![(vec![1], vec![10])];

        // Act
        let mut iter = Iterator::forward(results);

        // Assert
        assert_eq!(iter.remaining(), 1);
        assert_eq!(iter.next(), Some((vec![1], vec![10])));
        assert_eq!(iter.remaining(), 0);
        assert!(iter.exhausted());
    }

    #[test]
    fn should_handle_large_key_values_when_iterating() {
        // Arrange
        let large_key = vec![0u8; 10000];
        let large_val = vec![1u8; 10000];
        let results = vec![(large_key.clone(), large_val.clone())];

        // Act
        let mut iter = Iterator::forward(results);
        let pair = iter.next();

        // Assert
        assert_eq!(pair, Some((large_key, large_val)));
    }

    #[test]
    fn should_handle_empty_key_value_when_iterating() {
        // Arrange
        let results = vec![(vec![], vec![])];

        // Act
        let mut iter = Iterator::forward(results);

        // Assert
        assert_eq!(iter.next(), Some((vec![], vec![])));
    }

    #[test]
    fn should_handle_identical_keys_with_different_values() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![1], vec![20]), // same key, different value
        ];

        // Act
        let mut iter = Iterator::forward(results);

        // Assert
        assert_eq!(iter.next(), Some((vec![1], vec![10])));
        assert_eq!(iter.next(), Some((vec![1], vec![20])));
    }

    #[test]
    fn should_handle_unsorted_results_when_building_iterator() {
        // Arrange
        let results = vec![
            (vec![3], vec![30]),
            (vec![1], vec![10]),
            (vec![2], vec![20]),
        ];

        // Act
        let mut iter = Iterator::forward(results);

        // Assert - should iterate in provided order, not sorted
        assert_eq!(iter.next(), Some((vec![3], vec![30])));
        assert_eq!(iter.next(), Some((vec![1], vec![10])));
        assert_eq!(iter.next(), Some((vec![2], vec![20])));
    }

    #[test]
    fn should_handle_overlapping_ranges_when_building_iterator() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
            (vec![4], vec![40]),
        ];

        // Act
        let iter1 = forward_with_bounds(results.clone(), Some(&[1]), Some(&[3]), true);
        let iter2 = forward_with_bounds(results, Some(&[2]), Some(&[4]), true);

        // Assert - both should work independently
        assert_eq!(iter1.remaining(), 3); // 1, 2, 3
        assert_eq!(iter2.remaining(), 3); // 2, 3, 4
    }
}
