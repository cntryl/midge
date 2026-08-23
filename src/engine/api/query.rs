//! Query builder for scan operations
//!
//! Provides a fluent API for specifying range scans with optional filters.

use super::iterator::Direction;
use bytes::Bytes;

/// Lightweight scan query builder for range operations.
#[derive(Debug, Clone)]
pub struct Query {
    /// Lower bound (inclusive) for the range scan
    pub start: Option<Bytes>,
    /// Upper bound (exclusive) for the range scan
    pub end: Option<Bytes>,
    /// Prefix filter - scans keys starting with this prefix
    pub prefix: Option<Bytes>,
    /// Maximum number of results to return
    pub limit: Option<usize>,
    /// Iteration direction (Forward or Reverse)
    pub direction: Direction,
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

impl Query {
    /// Create a new empty query
    #[must_use]
    pub fn new() -> Self {
        Self {
            start: None,
            end: None,
            prefix: None,
            limit: None,
            direction: Direction::Forward,
        }
    }

    /// Set the start key (inclusive)
    #[must_use]
    pub fn start_key(mut self, k: Bytes) -> Self {
        self.start = Some(k);
        self
    }

    /// Set the end key (exclusive)
    #[must_use]
    pub fn end_key(mut self, k: Bytes) -> Self {
        self.end = Some(k);
        self
    }

    /// Set a prefix filter
    #[must_use]
    pub fn prefix(mut self, p: Bytes) -> Self {
        self.prefix = Some(p);
        self
    }

    /// Set the maximum number of results
    #[must_use]
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Reverse the iteration direction
    #[must_use]
    pub fn reverse(mut self) -> Self {
        self.direction = Direction::Reverse;
        self
    }

    /// Get the effective start bound for iteration
    #[inline]
    pub fn effective_start(&self) -> Option<&[u8]> {
        match (self.start.as_deref(), self.prefix.as_deref()) {
            (Some(start), Some(prefix)) => Some(start.max(prefix)),
            (Some(start), None) => Some(start),
            (None, Some(prefix)) => Some(prefix),
            (None, None) => None,
        }
    }

    /// Get the effective end bound for iteration
    #[inline]
    pub fn effective_end(&self) -> Option<Vec<u8>> {
        match (
            self.end.as_ref().map(Bytes::as_ref),
            self.prefix
                .as_ref()
                .and_then(|prefix| Self::prefix_successor(prefix)),
        ) {
            (Some(end), Some(prefix_end)) => Some(end.min(prefix_end.as_slice()).to_vec()),
            (Some(end), None) => Some(end.to_vec()),
            (None, Some(prefix_end)) => Some(prefix_end),
            (None, None) => None,
        }
    }

    /// Return the first lexicographic key outside `prefix`, if one exists.
    ///
    /// Appending `0xFF` is not a valid upper bound because descendants such
    /// as `prefix || 0xFF || suffix` compare above it. Incrementing the final
    /// non-`0xFF` byte gives the exclusive bound for every binary descendant.
    fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
        let mut successor = prefix.to_vec();
        while let Some(byte) = successor.pop() {
            if byte != u8::MAX {
                successor.push(byte + 1);
                return Some(successor);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Effective Start Tests ==========
    // Tests for effective_start() invariants: returns start if set, else prefix, else None

    #[test]
    fn should_return_start_when_start_is_set() {
        // Arrange
        let start = Bytes::from_static(b"key");
        let query = Query::new().start_key(start);

        // Act
        let effective = query.effective_start();

        // Assert
        assert_eq!(effective, Some(&b"key"[..]));
    }

    #[test]
    fn should_return_prefix_when_start_not_set_but_prefix_is() {
        // Arrange
        let prefix = Bytes::from_static(b"pre");
        let query = Query::new().prefix(prefix);

        // Act
        let effective = query.effective_start();

        // Assert
        assert_eq!(effective, Some(&b"pre"[..]));
    }

    #[test]
    fn should_return_start_over_prefix_when_both_are_set() {
        // Arrange
        let start = Bytes::from_static(b"start");
        let prefix = Bytes::from_static(b"prefix");
        let query = Query::new().prefix(prefix).start_key(start);

        // Act
        let effective = query.effective_start();

        // Assert
        assert_eq!(effective, Some(&b"start"[..]));
    }

    #[test]
    fn should_return_none_when_neither_start_nor_prefix_set() {
        // Arrange
        let query = Query::new();

        // Act
        let effective = query.effective_start();

        // Assert
        assert_eq!(effective, None);
    }

    #[test]
    fn should_return_empty_bytes_when_start_is_empty() {
        // Arrange
        let empty = Bytes::from_static(b"");
        let query = Query::new().start_key(empty);

        // Act
        let effective = query.effective_start();

        // Assert
        assert_eq!(effective, Some(&b""[..]));
    }

    // ========== Effective End Tests ==========
    // Tests for effective_end() invariants: returns end if set, else the
    // lexicographic prefix successor when one exists, else None.

    #[test]
    fn should_return_end_when_end_is_set() {
        // Arrange
        let end = Bytes::from_static(b"key");
        let query = Query::new().end_key(end);

        // Act
        let effective = query.effective_end();

        // Assert
        assert_eq!(effective, Some(b"key".to_vec()));
    }

    #[test]
    fn should_return_prefix_successor_when_end_not_set_but_prefix_is() {
        // Arrange
        let prefix = Bytes::from_static(b"pre");
        let query = Query::new().prefix(prefix);

        // Act
        let effective = query.effective_end();

        // Assert
        assert_eq!(effective, Some(b"prf".to_vec()));
    }

    #[test]
    fn should_compute_binary_prefix_successor_when_prefix_ends_in_ff() {
        // Arrange
        let query = Query::new().prefix(Bytes::from(vec![0x10, 0xff]));

        // Act
        let effective = query.effective_end();

        // Assert: [0x11] is the first key outside the [0x10, 0xff] prefix.
        assert_eq!(effective, Some(vec![0x11]));
    }

    #[test]
    fn should_leave_prefix_scan_unbounded_when_prefix_has_no_successor() {
        // Arrange
        let query = Query::new().prefix(Bytes::from(vec![0xff, 0xff]));

        // Act
        let effective = query.effective_end();

        // Assert
        assert_eq!(effective, None);
    }

    #[test]
    fn should_return_end_over_prefix_when_both_are_set() {
        // Arrange
        let end = Bytes::from_static(b"end");
        let prefix = Bytes::from_static(b"prefix");
        let query = Query::new().prefix(prefix).end_key(end);

        // Act
        let effective = query.effective_end();

        // Assert
        assert_eq!(effective, Some(b"end".to_vec()));
    }

    #[test]
    fn should_return_none_when_neither_end_nor_prefix_set() {
        // Arrange
        let query = Query::new();

        // Act
        let effective = query.effective_end();

        // Assert
        assert_eq!(effective, None);
    }

    #[test]
    fn should_return_none_when_empty_prefix_has_no_finite_upper_bound() {
        // Arrange
        let empty_prefix = Bytes::from_static(b"");
        let query = Query::new().prefix(empty_prefix);

        // Act
        let effective = query.effective_end();

        // Assert
        assert_eq!(effective, None);
    }

    #[test]
    fn should_return_empty_end_when_end_is_empty() {
        // Arrange
        let empty = Bytes::from_static(b"");
        let query = Query::new().end_key(empty);

        // Act
        let effective = query.effective_end();

        // Assert
        assert_eq!(effective, Some(vec![]));
    }

    // ========== Complex Chaining Tests ==========
    // Tests for method chaining with all combinations

    #[test]
    fn should_support_full_fluent_api_chain() {
        // Arrange
        let start = Bytes::from_static(b"a");
        let end = Bytes::from_static(b"z");

        // Act
        let query = Query::new()
            .start_key(start.clone())
            .end_key(end.clone())
            .limit(50)
            .reverse();

        // Assert
        assert_eq!(query.start, Some(start));
        assert_eq!(query.end, Some(end));
        assert_eq!(query.limit, Some(50));
        assert_eq!(query.direction, Direction::Reverse);
        assert_eq!(query.prefix, None);
    }

    #[test]
    fn should_support_prefix_with_limit_reverse() {
        // Arrange
        let prefix = Bytes::from_static(b"user:");

        // Act
        let query = Query::new().prefix(prefix.clone()).limit(100).reverse();

        // Assert
        assert_eq!(query.prefix, Some(prefix));
        assert_eq!(query.limit, Some(100));
        assert_eq!(query.direction, Direction::Reverse);
    }

    #[test]
    fn should_allow_all_methods_in_any_order() {
        // Arrange
        let q1 = Query::new()
            .limit(10)
            .reverse()
            .start_key(Bytes::from_static(b"a"));

        let q2 = Query::new()
            .start_key(Bytes::from_static(b"a"))
            .limit(10)
            .reverse();

        // Act
        // (build both queries)

        // Assert
        assert_eq!(q1.limit, q2.limit);
        assert_eq!(q1.direction, q2.direction);
        assert_eq!(q1.start, q2.start);
    }

    // ========== Edge Cases ==========

    #[test]
    fn should_handle_multiple_overwrites_in_chain() {
        // Arrange
        let k1 = Bytes::from_static(b"key1");
        let k2 = Bytes::from_static(b"key2");
        let k3 = Bytes::from_static(b"key3");

        // Act
        let query = Query::new()
            .start_key(k1)
            .start_key(k2)
            .start_key(k3.clone());

        // Assert
        assert_eq!(query.start, Some(k3));
    }

    #[test]
    fn should_work_with_binary_data() {
        // Arrange: exercise arbitrary byte values, including 0x00 and 0xFF,
        // across all three key fields simultaneously.
        let binary_start = Bytes::from_static(&[0x00, 0x01, 0x02, 0xFF]);
        let binary_end = Bytes::from_static(&[0xFF, 0xFE, 0xFD, 0x00]);
        let binary_prefix = Bytes::from_static(&[0x80, 0x81]);

        // Act
        let query = Query::new()
            .start_key(binary_start.clone())
            .end_key(binary_end.clone())
            .prefix(binary_prefix.clone());

        // Assert
        assert_eq!(query.start, Some(binary_start));
        assert_eq!(query.end, Some(binary_end));
        assert_eq!(query.prefix, Some(binary_prefix));
    }
}
