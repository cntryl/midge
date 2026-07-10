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
        self.start
            .as_ref()
            .map(std::convert::AsRef::as_ref)
            .or_else(|| self.prefix.as_ref().map(std::convert::AsRef::as_ref))
    }

    /// Get the effective end bound for iteration
    #[inline]
    pub fn effective_end(&self) -> Option<Vec<u8>> {
        match (self.end.as_ref(), self.prefix.as_ref()) {
            (Some(e), _) => Some(e.to_vec()),
            (None, Some(p)) => Self::prefix_successor(p),
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

    // ========== Query Creation Tests ==========
    // Tests for Query::new() invariants: all fields initialized with defaults

    #[test]
    fn should_initialize_all_fields_to_none_when_creating_new_query() {
        // Arrange
        // (no setup required)

        // Act
        let query = Query::new();

        // Assert
        assert_eq!(query.start, None);
        assert_eq!(query.end, None);
        assert_eq!(query.prefix, None);
        assert_eq!(query.limit, None);
        assert_eq!(query.direction, Direction::Forward);
    }

    #[test]
    fn should_create_query_with_all_default_values_when_calling_default() {
        // Arrange
        // (no setup required)

        // Act
        let query = Query::default();

        // Assert - all fields should match new()
        assert_eq!(query.start, None);
        assert_eq!(query.end, None);
        assert_eq!(query.prefix, None);
        assert_eq!(query.limit, None);
        assert_eq!(query.direction, Direction::Forward);
    }

    #[test]
    fn should_return_equivalent_queries() {
        // Arrange
        let new_query = Query::new();
        let default_query = Query::default();

        // Act
        // (compare fields)

        // Assert
        assert_eq!(new_query.start, default_query.start);
        assert_eq!(new_query.end, default_query.end);
        assert_eq!(new_query.prefix, default_query.prefix);
        assert_eq!(new_query.limit, default_query.limit);
        assert_eq!(new_query.direction, default_query.direction);
    }

    #[test]
    fn should_initialize_reverse_to_false_when_creating_query() {
        // Arrange
        // (no setup required)

        // Act
        let query = Query::new();

        // Assert
        assert_eq!(query.direction, Direction::Forward);
    }

    // ========== Query Start Key Tests ==========
    // Tests for start_key() method: sets start field, returns self for chaining

    #[test]
    fn should_set_start_key_when_calling_start_key() {
        // Arrange
        let start_bytes = Bytes::from_static(b"key");

        // Act
        let query = Query::new().start_key(start_bytes.clone());

        // Assert
        assert_eq!(query.start, Some(start_bytes));
    }

    #[test]
    fn should_return_self_for_chaining_when_calling_start_key() {
        // Arrange
        let start1 = Bytes::from_static(b"a");
        let end1 = Bytes::from_static(b"z");

        // Act
        let query = Query::new().start_key(start1.clone()).end_key(end1.clone());

        // Assert - both start and end should be set
        assert_eq!(query.start, Some(start1));
        assert_eq!(query.end, Some(end1));
    }

    #[test]
    fn should_overwrite_start_key_when_calling_start_key_twice() {
        // Arrange
        let start1 = Bytes::from_static(b"key1");
        let start2 = Bytes::from_static(b"key2");

        // Act
        let query = Query::new().start_key(start1).start_key(start2.clone());

        // Assert
        assert_eq!(query.start, Some(start2));
    }

    #[test]
    fn should_accept_empty_start_key() {
        // Arrange
        let empty = Bytes::from_static(b"");

        // Act
        let query = Query::new().start_key(empty.clone());

        // Assert
        assert_eq!(query.start, Some(empty));
    }

    #[test]
    fn should_accept_large_start_key() {
        // Arrange
        let large = Bytes::from_static(&[0u8; 10000]);

        // Act
        let query = Query::new().start_key(large.clone());

        // Assert
        assert_eq!(query.start, Some(large));
    }

    // ========== Query End Key Tests ==========
    // Tests for end_key() method: sets end field, returns self for chaining

    #[test]
    fn should_set_end_key_when_calling_end_key() {
        // Arrange
        let end_bytes = Bytes::from_static(b"zzzz");

        // Act
        let query = Query::new().end_key(end_bytes.clone());

        // Assert
        assert_eq!(query.end, Some(end_bytes));
    }

    #[test]
    fn should_return_self_for_chaining_when_calling_end_key() {
        // Arrange
        let start = Bytes::from_static(b"a");
        let end = Bytes::from_static(b"z");
        let prefix = Bytes::from_static(b"pre");

        // Act
        let query = Query::new()
            .start_key(start.clone())
            .end_key(end.clone())
            .prefix(prefix.clone());

        // Assert
        assert_eq!(query.start, Some(start));
        assert_eq!(query.end, Some(end));
        assert_eq!(query.prefix, Some(prefix));
    }

    #[test]
    fn should_overwrite_end_key_when_calling_end_key_twice() {
        // Arrange
        let end1 = Bytes::from_static(b"end1");
        let end2 = Bytes::from_static(b"end2");

        // Act
        let query = Query::new().end_key(end1).end_key(end2.clone());

        // Assert
        assert_eq!(query.end, Some(end2));
    }

    #[test]
    fn should_accept_empty_end_key() {
        // Arrange
        let empty = Bytes::from_static(b"");

        // Act
        let query = Query::new().end_key(empty.clone());

        // Assert
        assert_eq!(query.end, Some(empty));
    }

    // ========== Query Prefix Tests ==========
    // Tests for prefix() method: sets prefix field, returns self for chaining

    #[test]
    fn should_set_prefix_when_calling_prefix() {
        // Arrange
        let prefix_bytes = Bytes::from_static(b"user:");

        // Act
        let query = Query::new().prefix(prefix_bytes.clone());

        // Assert
        assert_eq!(query.prefix, Some(prefix_bytes));
    }

    #[test]
    fn should_return_self_for_chaining_when_calling_prefix() {
        // Arrange
        let prefix = Bytes::from_static(b"pre");
        let limit = 10;

        // Act
        let query = Query::new().prefix(prefix.clone()).limit(limit);

        // Assert
        assert_eq!(query.prefix, Some(prefix));
        assert_eq!(query.limit, Some(limit));
    }

    #[test]
    fn should_overwrite_prefix_when_calling_prefix_twice() {
        // Arrange
        let prefix1 = Bytes::from_static(b"pre1");
        let prefix2 = Bytes::from_static(b"pre2");

        // Act
        let query = Query::new().prefix(prefix1).prefix(prefix2.clone());

        // Assert
        assert_eq!(query.prefix, Some(prefix2));
    }

    #[test]
    fn should_accept_empty_prefix() {
        // Arrange
        let empty = Bytes::from_static(b"");

        // Act
        let query = Query::new().prefix(empty.clone());

        // Assert
        assert_eq!(query.prefix, Some(empty));
    }

    // ========== Query Limit Tests ==========
    // Tests for limit() method: sets limit field, returns self for chaining

    #[test]
    fn should_set_limit_when_calling_limit() {
        // Arrange
        // (no setup required)

        // Act
        let query = Query::new().limit(42);

        // Assert
        assert_eq!(query.limit, Some(42));
    }

    #[test]
    fn should_return_self_for_chaining_when_calling_limit() {
        // Arrange
        // Act
        let query = Query::new().limit(10).reverse();

        // Assert
        assert_eq!(query.limit, Some(10));
        assert_eq!(query.direction, Direction::Reverse);
    }

    #[test]
    fn should_overwrite_limit_when_calling_limit_twice() {
        // Arrange
        // Act
        let query = Query::new().limit(10).limit(20);

        // Assert
        assert_eq!(query.limit, Some(20));
    }

    #[test]
    fn should_accept_zero_limit() {
        // Arrange
        // Act
        let query = Query::new().limit(0);

        // Assert
        assert_eq!(query.limit, Some(0));
    }

    #[test]
    fn should_accept_large_limit() {
        // Arrange
        // Act
        let query = Query::new().limit(usize::MAX);

        // Assert
        assert_eq!(query.limit, Some(usize::MAX));
    }

    // ========== Query Reverse Tests ==========
    // Tests for reverse() method: sets reverse to true, returns self for chaining

    #[test]
    fn should_set_reverse_to_true_when_calling_reverse() {
        // Arrange
        // Act
        let query = Query::new().reverse();

        // Assert
        assert_eq!(query.direction, Direction::Reverse);
    }

    #[test]
    fn should_return_self_for_chaining_when_calling_reverse() {
        // Arrange
        let start = Bytes::from_static(b"a");

        // Act
        let query = Query::new().start_key(start.clone()).reverse();

        // Assert
        assert_eq!(query.start, Some(start));
        assert_eq!(query.direction, Direction::Reverse);
    }

    #[test]
    fn should_keep_reverse_true_when_calling_reverse_multiple_times() {
        // Arrange
        // Act
        let query = Query::new().reverse().reverse();

        // Assert
        assert_eq!(query.direction, Direction::Reverse);
    }

    #[test]
    fn should_allow_reverse_in_any_chaining_position() {
        // Arrange
        let start = Bytes::from_static(b"a");
        let end = Bytes::from_static(b"z");

        // Act
        let query1 = Query::new()
            .reverse()
            .start_key(start.clone())
            .end_key(end.clone());

        let query2 = Query::new().start_key(start).reverse().end_key(end);

        // Assert - reverse should be true in both
        assert_eq!(query1.direction, Direction::Reverse);
        assert_eq!(query2.direction, Direction::Reverse);
    }

    // ========== Query Clone Tests ==========
    // Tests for Clone trait: independent copies

    #[test]
    fn should_clone_query_with_all_fields() {
        // Arrange
        let original = Query::new()
            .start_key(Bytes::from_static(b"start"))
            .end_key(Bytes::from_static(b"end"))
            .prefix(Bytes::from_static(b"pre"))
            .limit(100)
            .reverse();

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned.start, original.start);
        assert_eq!(cloned.end, original.end);
        assert_eq!(cloned.prefix, original.prefix);
        assert_eq!(cloned.limit, original.limit);
        assert_eq!(cloned.direction, original.direction);
    }

    #[test]
    fn should_be_independent_after_cloning() {
        // Arrange
        let start_a = Bytes::from_static(b"a");
        let start_b = Bytes::from_static(b"b");
        let query1 = Query::new().start_key(start_a.clone()).limit(10);

        // Act
        let query2 = query1.clone();
        let query2_start = query2.start.clone();
        let query3 = query2.start_key(start_b.clone());

        // Assert - query1 unchanged, query2 unchanged, query3 different
        assert_eq!(query1.start, Some(start_a.clone()));
        assert_eq!(query2_start, Some(start_a));
        assert_eq!(query3.start, Some(start_b));
    }

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
    fn should_return_reference_to_start_bytes() {
        // Arrange
        let start = Bytes::from_static(b"data");
        let query = Query::new().start_key(start);

        // Act
        let effective = query.effective_start();

        // Assert - should be a reference to the actual bytes
        assert_eq!(effective.unwrap(), b"data");
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
    fn should_return_vector_from_effective_end() {
        // Arrange
        let end = Bytes::from_static(b"bytes");
        let query = Query::new().end_key(end);

        // Act
        let effective = query.effective_end();

        // Assert - should be an owned Vec, not a reference
        assert_eq!(effective, Some(vec![b'b', b'y', b't', b'e', b's']));
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

    #[test]
    fn should_increment_the_last_prefix_byte() {
        // Arrange
        let prefix = Bytes::from_static(b"test");
        let query = Query::new().prefix(prefix);

        // Act
        let effective = query.effective_end();

        // Assert
        assert_eq!(effective, Some(b"tesu".to_vec()));
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
    fn should_handle_mixed_prefix_start() {
        // Arrange
        let start = Bytes::from_static(b"start");
        let prefix = Bytes::from_static(b"prefix");

        // Act
        let query = Query::new().prefix(prefix.clone()).start_key(start.clone());

        // Assert
        assert_eq!(query.start, Some(start));
        assert_eq!(query.prefix, Some(prefix));
        // effective_start should prefer start
        assert_eq!(query.effective_start(), Some(&b"start"[..]));
    }

    #[test]
    fn should_handle_mixed_end_prefix() {
        // Arrange
        let end = Bytes::from_static(b"end");
        let prefix = Bytes::from_static(b"pre");

        // Act
        let query = Query::new().prefix(prefix.clone()).end_key(end.clone());

        // Assert
        assert_eq!(query.end, Some(end));
        assert_eq!(query.prefix, Some(prefix));
        // effective_end should prefer end
        assert_eq!(query.effective_end(), Some(b"end".to_vec()));
    }

    #[test]
    fn should_preserve_all_fields_in_complete_query() {
        // Arrange
        let start = Bytes::from_static(b"start");
        let end = Bytes::from_static(b"end");
        let prefix = Bytes::from_static(b"prefix");

        // Act
        let query = Query::new()
            .start_key(start.clone())
            .end_key(end.clone())
            .prefix(prefix.clone())
            .limit(42)
            .reverse();

        // Assert
        assert_eq!(query.start, Some(start));
        assert_eq!(query.end, Some(end));
        assert_eq!(query.prefix, Some(prefix));
        assert_eq!(query.limit, Some(42));
        assert_eq!(query.direction, Direction::Reverse);
    }

    #[test]
    fn should_work_with_binary_data() {
        // Arrange
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

    #[test]
    fn should_handle_utf8_data() {
        // Arrange
        let utf8_start = Bytes::from("日本語");
        let utf8_prefix = Bytes::from("中文");

        // Act
        let query = Query::new()
            .start_key(utf8_start.clone())
            .prefix(utf8_prefix.clone());

        // Assert
        assert_eq!(query.start, Some(utf8_start));
        assert_eq!(query.prefix, Some(utf8_prefix));
    }
}
