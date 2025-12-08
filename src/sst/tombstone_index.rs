//! Phase 4: Range Tombstone Index
//!
//! Provides efficient tombstone lookup without reading data blocks.
//! Tombstones are stored in separate blocks and indexed by their start/end ranges.

use bytes::Bytes;
use crate::sst::traits::RangeTombstone;
use crate::sst::format::BlockHandle;

/// Index entry for a tombstone block
///
/// Each entry describes a contiguous range of tombstones stored in a separate block.
#[derive(Clone, Debug)]
pub struct TombstoneIndexEntry {
    /// Minimum start key of all tombstones in this block
    pub min_key: Bytes,
    
    /// Maximum end key of all tombstones in this block
    pub max_key: Bytes,
    
    /// Physical location of the tombstone block
    pub block_handle: BlockHandle,
    
    /// Number of tombstones in this block
    pub count: u32,
}

impl TombstoneIndexEntry {
    /// Create a new tombstone index entry
    pub fn new(min_key: Bytes, max_key: Bytes, block_handle: BlockHandle, count: u32) -> Self {
        Self {
            min_key,
            max_key,
            block_handle,
            count,
        }
    }
    
    /// Check if this tombstone block might contain a tombstone covering the given key
    #[inline]
    pub fn might_cover(&self, key: &[u8]) -> bool {
        // A tombstone [start, end) covers key if start <= key < end
        // This block might contain such a tombstone if:
        // - The block's min_key <= key (tombstones could start at or before key)
        // - The block's max_key > key (tombstones could end after key)
        key >= self.min_key.as_ref() && key < self.max_key.as_ref()
    }
    
    /// Check if this tombstone block might intersect a range [start, end)
    #[inline]
    pub fn range_intersects(&self, start: &[u8], end: &[u8]) -> bool {
        // Blocks intersect if their ranges overlap
        start < self.max_key.as_ref() && end > self.min_key.as_ref()
    }
}

/// Tombstone index for efficient tombstone lookups
///
/// Maintains an index of tombstone blocks sorted by start key,
/// enabling fast lookups without reading all data blocks.
#[derive(Clone, Debug, Default)]
pub struct TombstoneIndex {
    entries: Vec<TombstoneIndexEntry>,
}

impl TombstoneIndex {
    /// Create a new tombstone index
    pub fn new(entries: Vec<TombstoneIndexEntry>) -> Self {
        Self { entries }
    }
    
    /// Create an empty tombstone index
    pub fn empty() -> Self {
        Self::default()
    }
    
    /// Get all index entries
    pub fn entries(&self) -> &[TombstoneIndexEntry] {
        &self.entries
    }
    
    /// Check if this index is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    /// Number of tombstone blocks
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    /// Find all tombstone blocks that might contain a tombstone covering the given key
    ///
    /// Uses binary search for efficient lookup. Returns an iterator over candidate blocks.
    pub fn find_blocks_for_key<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a TombstoneIndexEntry> + 'a {
        // TODO: Ensure `entries` is sorted by min_key and use partition_point or binary_search
        // to reduce scanning overhead for large tombstone indexes.
        // Current behavior: linear scan with `filter`.
        self.entries.iter().filter(move |entry| entry.might_cover(key))
    }
    
    /// Find all tombstone blocks that might intersect a range [start, end)
    ///
    /// Used for range scans to identify which tombstone blocks need to be checked.
    pub fn find_blocks_in_range<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
    ) -> impl Iterator<Item = &'a TombstoneIndexEntry> + 'a {
        self.entries
            .iter()
            .filter(move |entry| entry.range_intersects(start, end))
    }
    
    /// Check if a key might be covered by any tombstone (fast pre-filter)
    ///
    /// Returns true if there's ANY possibility the key is deleted.
    /// Returns false ONLY if we can definitively prove it's not deleted.
    pub fn might_be_deleted(&self, key: &[u8]) -> bool {
        self.entries.iter().any(|entry| entry.might_cover(key))
    }
}

/// Builder for constructing a tombstone index
pub struct TombstoneIndexBuilder {
    entries: Vec<TombstoneIndexEntry>,
}

impl TombstoneIndexBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    
    /// Add a tombstone block to the index
    pub fn add_block(
        &mut self,
        tombstones: &[RangeTombstone],
        block_handle: BlockHandle,
    ) {
        if tombstones.is_empty() {
            return;
        }
        
        // Find min and max keys across all tombstones in this block
        let min_key = tombstones
            .iter()
            .map(|t| t.start.as_slice())
            .min()
            .unwrap_or(&[])
            .to_vec();
        
        let max_key = tombstones
            .iter()
            .map(|t| t.end.as_slice())
            .max()
            .unwrap_or(&[])
            .to_vec();
        
        let entry = TombstoneIndexEntry::new(
            Bytes::from(min_key),
            Bytes::from(max_key),
            block_handle,
            tombstones.len() as u32,
        );
        
        self.entries.push(entry);
    }
    
    /// Build the final tombstone index
    pub fn finish(self) -> TombstoneIndex {
        TombstoneIndex::new(self.entries)
    }
}

impl Default for TombstoneIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_tombstone(start: &[u8], end: &[u8], seq: u64) -> RangeTombstone {
        RangeTombstone {
            start: start.to_vec(),
            end: end.to_vec(),
            seq,
        }
    }
    
    #[test]
    fn should_create_empty_tombstone_index() {
        // Arrange
        // (no setup needed)
        
        // Act
        let index = TombstoneIndex::empty();
        
        // Assert
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }
    
    #[test]
    fn should_create_tombstone_index_entry() {
        // Arrange
        let min_key = Bytes::from("apple");
        let max_key = Bytes::from("banana");
        let handle = BlockHandle::new(100, 256);
        
        // Act
        let entry = TombstoneIndexEntry::new(min_key.clone(), max_key.clone(), handle, 5);
        
        // Assert
        assert_eq!(entry.min_key, min_key);
        assert_eq!(entry.max_key, max_key);
        assert_eq!(entry.count, 5);
    }
    
    #[test]
    fn should_detect_key_coverage() {
        // Arrange
        let entry = TombstoneIndexEntry::new(
            Bytes::from("apple"),
            Bytes::from("cherry"),
            BlockHandle::new(0, 100),
            3,
        );
        
        // Act & Assert
        assert!(entry.might_cover(b"apple"));
        assert!(entry.might_cover(b"banana"));
        assert!(!entry.might_cover(b"cherry")); // Exclusive end
        assert!(!entry.might_cover(b"aardvark"));
        assert!(!entry.might_cover(b"zebra"));
    }
    
    #[test]
    fn should_detect_range_intersection() {
        // Arrange
        let entry = TombstoneIndexEntry::new(
            Bytes::from("b"),
            Bytes::from("e"),
            BlockHandle::new(0, 100),
            2,
        );
        
        // Act & Assert
        assert!(entry.range_intersects(b"a", b"c")); // Overlaps at start
        assert!(entry.range_intersects(b"d", b"f")); // Overlaps at end
        assert!(entry.range_intersects(b"b", b"e")); // Exact match
        assert!(entry.range_intersects(b"c", b"d")); // Fully contained
        assert!(!entry.range_intersects(b"a", b"b")); // Before
        assert!(!entry.range_intersects(b"e", b"f")); // After
    }
    
    #[test]
    fn should_build_tombstone_index_from_blocks() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        let block1 = vec![
            create_tombstone(b"a", b"c", 10),
            create_tombstone(b"b", b"d", 20),
        ];
        let block2 = vec![
            create_tombstone(b"m", b"p", 30),
            create_tombstone(b"n", b"q", 40),
        ];
        
        // Act
        builder.add_block(&block1, BlockHandle::new(0, 100));
        builder.add_block(&block2, BlockHandle::new(100, 150));
        let index = builder.finish();
        
        // Assert
        assert_eq!(index.len(), 2);
        assert_eq!(index.entries()[0].min_key, Bytes::from("a"));
        assert_eq!(index.entries()[0].max_key, Bytes::from("d"));
        assert_eq!(index.entries()[1].min_key, Bytes::from("m"));
        assert_eq!(index.entries()[1].max_key, Bytes::from("q"));
    }
    
    #[test]
    fn should_find_blocks_for_key() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"m", b"p", 20)],
            BlockHandle::new(100, 150),
        );
        let index = builder.finish();
        
        // Act
        let blocks_for_b: Vec<_> = index.find_blocks_for_key(b"b").collect();
        let blocks_for_n: Vec<_> = index.find_blocks_for_key(b"n").collect();
        let blocks_for_x: Vec<_> = index.find_blocks_for_key(b"x").collect();
        
        // Assert
        assert_eq!(blocks_for_b.len(), 1);
        assert_eq!(blocks_for_b[0].min_key, Bytes::from("a"));
        assert_eq!(blocks_for_n.len(), 1);
        assert_eq!(blocks_for_n[0].min_key, Bytes::from("m"));
        assert_eq!(blocks_for_x.len(), 0);
    }
    
    #[test]
    fn should_find_blocks_in_range() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(0, 100),
        );
        builder.add_block(
            &[create_tombstone(b"e", b"g", 20)],
            BlockHandle::new(100, 150),
        );
        builder.add_block(
            &[create_tombstone(b"m", b"p", 30)],
            BlockHandle::new(250, 200),
        );
        let index = builder.finish();
        
        // Act
        let blocks: Vec<_> = index.find_blocks_in_range(b"b", b"f").collect();
        
        // Assert
        assert_eq!(blocks.len(), 2); // First two blocks intersect [b, f)
    }
    
    #[test]
    fn should_check_if_key_might_be_deleted() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        builder.add_block(
            &[create_tombstone(b"a", b"c", 10)],
            BlockHandle::new(0, 100),
        );
        let index = builder.finish();
        
        // Act
        let might_be_deleted_b = index.might_be_deleted(b"b");
        let might_be_deleted_x = index.might_be_deleted(b"x");
        
        // Assert
        assert!(might_be_deleted_b);
        assert!(!might_be_deleted_x);
    }
    
    #[test]
    fn should_handle_empty_tombstone_list_when_adding_block() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        
        // Act
        builder.add_block(&[], BlockHandle::new(0, 100));
        let index = builder.finish();
        
        // Assert
        assert!(index.is_empty());
    }
    
    #[test]
    fn should_handle_single_tombstone_in_block() {
        // Arrange
        let mut builder = TombstoneIndexBuilder::new();
        let tombstone = vec![create_tombstone(b"x", b"z", 50)];
        
        // Act
        builder.add_block(&tombstone, BlockHandle::new(0, 100));
        let index = builder.finish();
        
        // Assert
        assert_eq!(index.len(), 1);
        assert_eq!(index.entries()[0].count, 1);
    }
}
