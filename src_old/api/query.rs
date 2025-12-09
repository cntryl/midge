use bytes::Bytes;

/// Lightweight scan query builder used by examples and simple callers.
#[derive(Default)]
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

impl Query {
    pub fn new() -> Self {
        Self {
            start: None,
            end: None,
            prefix: None,
            limit: None,
            reverse: false,
        }
    }
    pub fn start_key(mut self, k: Bytes) -> Self {
        self.start = Some(k);
        self
    }
    pub fn end_key(mut self, k: Bytes) -> Self {
        self.end = Some(k);
        self
    }
    pub fn prefix(mut self, p: Bytes) -> Self {
        self.prefix = Some(p);
        self
    }
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }
    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    /// Get the effective start bound for iteration.
    /// This is always the lower bound, regardless of direction.
    #[inline]
    pub fn effective_start(&self) -> Option<&[u8]> {
        self.start.as_ref().map(|b| b.as_ref()).or_else(|| {
            // For prefix scans, start is the prefix itself
            if !self.reverse {
                self.prefix.as_ref().map(|p| p.as_ref())
            } else {
                self.prefix.as_ref().map(|p| p.as_ref())
            }
        })
    }

    /// Get the effective end bound for iteration.
    /// This is always the upper bound, regardless of direction.
    #[inline]
    pub fn effective_end(&self) -> Option<Vec<u8>> {
        // Compute end from explicit bound or prefix
        match (self.end.as_ref(), self.prefix.as_ref()) {
            (Some(e), _) => Some(e.to_vec()),
            (None, Some(p)) => {
                let mut v = p.to_vec();
                v.push(0xFF);
                Some(v)
            }
            (None, None) => None,
        }
    }
}
