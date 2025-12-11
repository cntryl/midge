//! Iterator API - Range scanning and sequential reads
//!
//! Iterators provide efficient sequential access to key-value pairs,
//! including full range scans, prefix scans, and reverse iteration.

#[allow(unused_imports)]
use super::super::ColumnFamilyId;
#[allow(unused_imports)]
use crate::common::MidgeResult;

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
pub struct Iterator {
    /// Current position in the iteration
    position: usize,
    /// Buffered results (TODO: implement lazy loading)
    results: Vec<(Vec<u8>, Vec<u8>)>,
    /// Iteration direction
    direction: Direction,
    /// Whether iteration has completed
    exhausted: bool,
}

impl Iterator {
    /// Create a new iterator with the given results
    #[allow(dead_code)] // Used by engine when creating iterators
    pub(crate) fn new(results: Vec<(Vec<u8>, Vec<u8>)>, direction: Direction) -> Self {
        Self {
            position: 0,
            results,
            direction,
            exhausted: false,
        }
    }

    /// Create a forward iterator
    #[allow(dead_code)] // Used by engine when creating iterators
    pub(crate) fn forward(results: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self::new(results, Direction::Forward)
    }

    /// Create a reverse iterator
    #[allow(dead_code)] // Used by engine when creating iterators
    pub(crate) fn reverse(mut results: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        results.reverse();
        Self::new(results, Direction::Reverse)
    }

    /// Get the current key-value pair without advancing
    pub fn current(&self) -> Option<(&[u8], &[u8])> {
        if self.exhausted || self.position >= self.results.len() {
            return None;
        }
        let (k, v) = &self.results[self.position];
        Some((k.as_slice(), v.as_slice()))
    }

    /// Move to the next key-value pair
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.exhausted || self.position >= self.results.len() {
            self.exhausted = true;
            return None;
        }
        let pair = self.results[self.position].clone();
        self.position += 1;
        Some(pair)
    }

    /// Check if iteration is complete
    pub fn exhausted(&self) -> bool {
        self.exhausted || self.position >= self.results.len()
    }

    /// Get the direction of this iterator
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// Get the count of items remaining
    pub fn remaining(&self) -> usize {
        if self.exhausted {
            0
        } else {
            self.results.len().saturating_sub(self.position)
        }
    }

    /// Collect all remaining pairs into a vector
    pub fn collect_all(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut collected = Vec::new();
        while let Some(pair) = self.next() {
            collected.push(pair);
        }
        collected
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
    fn should_track_remaining_count_when_iterating() {
        // Arrange
        let results = vec![
            (vec![1], vec![10]),
            (vec![2], vec![20]),
            (vec![3], vec![30]),
        ];
        let mut iter = Iterator::forward(results);

        // Act & Assert
        assert_eq!(iter.remaining(), 3);
        iter.next();
        assert_eq!(iter.remaining(), 2);
        iter.next();
        assert_eq!(iter.remaining(), 1);
        iter.next();
        assert_eq!(iter.remaining(), 0);
    }

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

        // Assert - [2, 4)
        assert_eq!(iter.next(), Some((vec![2], vec![20])));
        assert_eq!(iter.next(), Some((vec![3], vec![30])));
        assert_eq!(iter.next(), None);
    }

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
}
