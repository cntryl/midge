//! Multi-way merge iterator for compaction
//!
//! Merges multiple sorted sequences of entries while maintaining order.

use bytes::Bytes;

/// Entry from a single SST during merge
#[derive(Debug, Clone)]
pub struct MergeEntry {
    pub key: Bytes,
    pub value: Bytes,
    pub seq: u64,
}

/// Multi-way merge iterator combining sorted inputs
pub struct MergeIterator {
    // TODO: Implement when merging logic is needed
}

impl MergeIterator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for MergeIterator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_merge_iterator_when_new() {
        // Arrange
        // Act
        let _iter = MergeIterator::new();

        // Assert (creation succeeded if we get here)
    }
}
