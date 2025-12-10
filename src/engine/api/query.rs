//! Query builder for scan operations
//!
//! Provides a fluent API for specifying range scans with optional filters.

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
    /// Iterate in reverse order (from end to start)
    pub reverse: bool,
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

impl Query {
    /// Create a new empty query
    pub fn new() -> Self {
        Self {
            start: None,
            end: None,
            prefix: None,
            limit: None,
            reverse: false,
        }
    }

    /// Set the start key (inclusive)
    pub fn start_key(mut self, k: Bytes) -> Self {
        self.start = Some(k);
        self
    }

    /// Set the end key (exclusive)
    pub fn end_key(mut self, k: Bytes) -> Self {
        self.end = Some(k);
        self
    }

    /// Set a prefix filter
    pub fn prefix(mut self, p: Bytes) -> Self {
        self.prefix = Some(p);
        self
    }

    /// Set the maximum number of results
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Reverse the iteration direction
    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    /// Get the effective start bound for iteration
    #[inline]
    pub fn effective_start(&self) -> Option<&[u8]> {
        self.start
            .as_ref()
            .map(|b| b.as_ref())
            .or_else(|| self.prefix.as_ref().map(|p| p.as_ref()))
    }

    /// Get the effective end bound for iteration
    #[inline]
    pub fn effective_end(&self) -> Option<Vec<u8>> {
        match (self.end.as_ref(), self.prefix.as_ref()) {
            (Some(e), _) => Some(e.to_vec()),
            (None, Some(p)) => {
                // For prefix, end is prefix + 0xFF to get all keys with that prefix
                let mut v = p.to_vec();
                // Try to increment the prefix by appending 0xFF
                v.push(0xFF);
                Some(v)
            }
            (None, None) => None,
        }
    }
}
