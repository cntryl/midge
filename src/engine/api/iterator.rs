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

/// Default batch size for lazy-loading iterators.
const DEFAULT_LAZY_BATCH_SIZE: usize = 256;

/// A range iterator over key-value pairs
///
/// Iterators provide efficient sequential access to the database.
/// They can be created with various options (direction, bounds, etc).
///
/// Supports two modes:
/// - **Eager**: All results are buffered upfront (used by transaction scans
///   and snapshot-based reads where the full result set is already available).
/// - **Lazy**: Results are fetched in batches from a `LazySource`, reducing
///   memory pressure for large range scans. The lazy source is called to
///   fetch the next batch when the current buffer is exhausted.
pub struct Iterator {
    /// Current position within the current batch
    position: usize,
    /// Current batch of results
    results: Vec<(Vec<u8>, Vec<u8>)>,
    /// Iteration direction
    direction: Direction,
    /// Whether iteration has completed
    exhausted: bool,
    /// Optional lazy source for fetching additional batches
    lazy_source: Option<Box<dyn LazySource>>,
    /// Batch size for lazy loading
    lazy_batch_size: usize,
}

/// Trait for lazy-loading scan results in batches.
///
/// Implementations fetch the next batch of key-value pairs from the
/// underlying storage (memtable + SSTs) without loading the entire
/// result set into memory.
pub(crate) trait LazySource: Send {
    /// Fetch the next batch of results.
    ///
    /// `resume_key` is the exclusive lower bound: return pairs with keys
    /// strictly greater than this. If `None`, start from the beginning.
    ///
    /// Returns an empty vec when iteration is complete.
    fn fetch_batch(
        &mut self,
        resume_key: Option<&[u8]>,
        batch_size: usize,
        direction: Direction,
    ) -> Vec<(Vec<u8>, Vec<u8>)>;
}

impl Iterator {
    /// Create a new iterator with the given results (eager mode)
    #[allow(dead_code)] // Used by engine when creating iterators
    pub(crate) fn new(results: Vec<(Vec<u8>, Vec<u8>)>, direction: Direction) -> Self {
        Self {
            position: 0,
            results,
            direction,
            exhausted: false,
            lazy_source: None,
            lazy_batch_size: DEFAULT_LAZY_BATCH_SIZE,
        }
    }

    /// Create a lazy-loading iterator with a source for batch fetching.
    ///
    /// The first batch is fetched immediately. Subsequent batches are
    /// fetched on demand when the current buffer is exhausted.
    #[allow(dead_code)]
    pub(crate) fn lazy(
        mut source: Box<dyn LazySource>,
        direction: Direction,
        batch_size: usize,
    ) -> Self {
        let batch_size = if batch_size == 0 {
            DEFAULT_LAZY_BATCH_SIZE
        } else {
            batch_size
        };
        let initial_batch = source.fetch_batch(None, batch_size, direction);
        let exhausted = initial_batch.is_empty();
        Self {
            position: 0,
            results: initial_batch,
            direction,
            exhausted,
            lazy_source: Some(source),
            lazy_batch_size: batch_size,
        }
    }

    /// Create a forward iterator (eager mode)
    #[allow(dead_code)] // Used by engine when creating iterators
    pub(crate) fn forward(results: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self::new(results, Direction::Forward)
    }

    /// Create a reverse iterator (eager mode)
    #[allow(dead_code)] // Used by engine when creating iterators
    pub(crate) fn reverse(mut results: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        results.reverse();
        Self::new(results, Direction::Reverse)
    }

    /// Try to load the next batch from the lazy source.
    ///
    /// Returns `true` if new results were loaded, `false` if source is
    /// exhausted or no lazy source is configured.
    fn try_load_next_batch(&mut self) -> bool {
        let source = match self.lazy_source.as_mut() {
            Some(s) => s,
            None => return false,
        };

        // Determine resume key from the last element in the current batch
        let resume_key = if self.results.is_empty() {
            None
        } else {
            self.results.last().map(|(k, _)| k.as_slice())
        };

        let batch = source.fetch_batch(resume_key, self.lazy_batch_size, self.direction);

        if batch.is_empty() {
            return false;
        }

        self.results = batch;
        self.position = 0;
        true
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
        self.exhausted || (self.position >= self.results.len() && self.lazy_source.is_none())
    }

    /// Get the direction of this iterator
    #[must_use]
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// Get the count of items remaining in the current batch.
    ///
    /// For lazy iterators this only reflects the current batch, not
    /// the total remaining in the underlying data source.
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
            if !self.try_load_next_batch() {
                self.exhausted = true;
                return None;
            }
        }

        let pair = self.results[self.position].clone();
        self.position += 1;
        Some(pair)
    }
}

/// Builder for creating iterators with options
pub struct IteratorBuilder {
    /// Start key (None = unbounded)
    start: Option<Vec<u8>>,
    /// End key (None = unbounded)
    end: Option<Vec<u8>>,
    /// Iteration direction
    direction: Direction,
    /// Whether to include start key
    #[allow(dead_code)]
    include_start: bool,
    /// Whether to include end key
    include_end: bool,
}

impl IteratorBuilder {
    /// Create a new iterator builder
    pub fn new() -> Self {
        Self {
            start: None,
            end: None,
            direction: Direction::Forward,
            include_start: true,
            include_end: true,
        }
    }

    /// Set the start key (inclusive by default)
    pub fn start(mut self, key: Vec<u8>) -> Self {
        self.start = Some(key);
        self
    }

    /// Set the end key (inclusive by default)
    pub fn end(mut self, key: Vec<u8>) -> Self {
        self.end = Some(key);
        self
    }

    /// Set range bounds [start, end) (start inclusive, end exclusive)
    pub fn range(mut self, start: Vec<u8>, end: Vec<u8>) -> Self {
        self.start = Some(start);
        self.end = Some(end);
        self.include_end = false;
        self
    }

    /// Set direction to reverse
    pub fn reverse(mut self) -> Self {
        self.direction = Direction::Reverse;
        self
    }

    /// Build the iterator (takes results, in real impl would fetch from engine)
    #[allow(dead_code)] // Used by engine when creating iterators
    pub(crate) fn build(self, results: Vec<(Vec<u8>, Vec<u8>)>) -> Iterator {
        let filtered = self.filter_results(results);
        if self.direction == Direction::Forward {
            Iterator::forward(filtered)
        } else {
            Iterator::reverse(filtered)
        }
    }

    /// Filter results based on configured bounds
    #[allow(dead_code)] // Used by build method
    fn filter_results(&self, results: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<(Vec<u8>, Vec<u8>)> {
        results
            .into_iter()
            .filter(|(k, _)| {
                // Check start bound
                if let Some(ref start) = self.start {
                    let cmp = k.as_slice().cmp(start.as_slice());
                    if self.include_start {
                        if cmp == std::cmp::Ordering::Less {
                            return false;
                        }
                    } else if cmp != std::cmp::Ordering::Greater {
                        return false;
                    }
                }

                // Check end bound
                if let Some(ref end) = self.end {
                    let cmp = k.as_slice().cmp(end.as_slice());
                    if self.include_end {
                        if cmp == std::cmp::Ordering::Greater {
                            return false;
                        }
                    } else if cmp != std::cmp::Ordering::Less {
                        return false;
                    }
                }

                true
            })
            .collect()
    }
}

impl Default for IteratorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    // ========== IteratorBuilder Creation Tests ==========
    // Tests for IteratorBuilder initialization invariants: defaults set correctly

    #[test]
    fn should_initialize_builder_with_defaults_when_created() {
        // Arrange
        // (no setup required)

        // Act
        let builder = IteratorBuilder::new();

        // Assert - verify defaults through behavior
        let results = vec![(vec![1], vec![10])];
        let iter = builder.build(results);
        assert_eq!(iter.direction(), Direction::Forward);
        assert!(!iter.exhausted());
    }

    #[test]
    fn should_use_default_when_calling_default_method() {
        // Arrange
        let builder1 = IteratorBuilder::new();
        let builder2 = IteratorBuilder::default();

        // Act
        let results = vec![(vec![1], vec![10])];
        let iter1 = builder1.build(results.clone());
        let iter2 = builder2.build(results);

        // Assert - both should behave the same
        assert_eq!(iter1.direction(), iter2.direction());
        assert_eq!(iter1.remaining(), iter2.remaining());
    }

    // ========== IteratorBuilder Chaining Tests ==========
    // Tests for IteratorBuilder fluent API: methods return Self for chaining

    #[test]
    fn should_support_chaining_when_calling_builder_methods() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];

        // Act
        let iter = IteratorBuilder::new()
            .start(vec![1])
            .end(vec![2])
            .build(results);

        // Assert
        assert!(!iter.exhausted());
    }

    #[test]
    fn should_support_reverse_chaining_when_calling_reverse_method() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];

        // Act
        let iter = IteratorBuilder::new().reverse().build(results);

        // Assert
        assert_eq!(iter.direction(), Direction::Reverse);
    }

    // ========== IteratorBuilder Start Bound Tests ==========
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
        let mut iter = IteratorBuilder::new().start(vec![2]).build(results);

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
        let mut iter = IteratorBuilder::new().start(vec![2]).build(results);

        // Assert
        assert_eq!(iter.next(), Some((vec![2], vec![20]))); // Start key included
    }

    #[test]
    fn should_filter_no_results_when_start_beyond_all_keys() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];

        // Act
        let mut iter = IteratorBuilder::new().start(vec![9]).build(results);

        // Assert
        assert_eq!(iter.next(), None);
    }

    // ========== IteratorBuilder End Bound Tests ==========
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
        let mut iter = IteratorBuilder::new().end(vec![2]).build(results);

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
        let mut iter = IteratorBuilder::new().end(vec![2]).build(results);

        // Assert - end key is inclusive
        let last = iter.collect_all().last().map(|p| p.0.clone());
        assert_eq!(last, Some(vec![2]));
    }

    #[test]
    fn should_filter_no_results_when_end_below_all_keys() {
        // Arrange
        let results = vec![(vec![1], vec![10]), (vec![2], vec![20])];

        // Act
        let mut iter = IteratorBuilder::new().end(vec![0]).build(results);

        // Assert
        assert_eq!(iter.next(), None);
    }

    // ========== IteratorBuilder Range Tests ==========
    // Tests for range() method: sets bounds [start, end) with end exclusive

    #[test]
    fn should_build_iterator_with_range_bounds_when_using_builder() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
            (vec![4], vec![40]),
            (vec![5], vec![50]),
        ];

        // Act
        let builder = IteratorBuilder::new().start(vec![2]).end(vec![4]);
        let mut iter = builder.build(results);

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
        let builder = IteratorBuilder::new().range(vec![2], vec![4]);
        let mut iter = builder.build(results);

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
        let mut iter = IteratorBuilder::new()
            .range(vec![1], vec![3])
            .build(results);
        let collected = iter.collect_all();

        // Assert - 3 should be excluded
        assert!(!collected.iter().any(|(k, _)| k == &vec![3]));
    }

    // ========== IteratorBuilder Complex Composition Tests ==========
    // Tests for multiple bounds and direction combinations

    #[test]
    fn should_support_builder_chaining_with_reverse() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
            (vec![4], vec![40]),
        ];

        // Act
        let mut iter = IteratorBuilder::new()
            .start(vec![2])
            .end(vec![4])
            .reverse()
            .build(results);

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
        let mut iter = IteratorBuilder::new()
            .start(vec![2])
            .end(vec![4])
            .build(results);

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
        let mut iter = IteratorBuilder::new()
            .range(vec![2], vec![4])
            .reverse()
            .build(results);

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
        let iter1 = IteratorBuilder::new()
            .start(vec![1])
            .end(vec![3])
            .build(results.clone());
        let iter2 = IteratorBuilder::new()
            .start(vec![2])
            .end(vec![4])
            .build(results);

        // Assert - both should work independently
        assert_eq!(iter1.remaining(), 3); // 1, 2, 3
        assert_eq!(iter2.remaining(), 3); // 2, 3, 4
    }
}
