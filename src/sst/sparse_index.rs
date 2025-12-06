use crate::error::{MidgeError, MidgeResult};
use crate::sst::encoding::TlvBlockIterator;
use crate::sst::format::BlockHandle;
use crate::sst::block_meta::BlockMeta;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Bytes,
    pub block_handle: BlockHandle,
}

#[derive(Debug, Default, Clone)]
pub struct SparseIndex {
    entries: Vec<IndexEntry>,
}

impl SparseIndex {
    pub fn new(entries: Vec<IndexEntry>) -> Self {
        Self { entries }
    }
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Build BlockMeta entries for all blocks.
    /// 
    /// Note: This is conservative because the sparse index only stores max_key for each block.
    /// We use empty/None for fence pointers since we can't reliably determine min_key from the index alone.
    /// In Phase 2, we'll enhance this by extracting actual min_keys from block data during reads.
    pub fn build_block_metas(&self) -> Vec<BlockMeta> {
        let mut metas = Vec::new();
        for entry in self.entries.iter() {
            // For now, use empty min_key - we'll extract actual min_key from block data when needed
            let min_key = Bytes::new();
            let max_key = entry.key.clone();
            let handle = entry.block_handle;
            let meta = BlockMeta::new(min_key, max_key, handle);
            metas.push(meta);
        }
        metas
    }

    pub fn encode(&self) -> Bytes {
        let mut builder = crate::sst::format::IndexBlockBuilder::new();
        for entry in &self.entries {
            let _ = builder.add_index_entry(&entry.key, entry.block_handle);
        }
        builder.finish()
    }

    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        let iterator = TlvBlockIterator::new(data);
        let mut entries = Vec::new();
        for entry in iterator {
            // Debug: log iterator results to help diagnose missing KEY_DELTA for empty blocks

            let (key, value, _, _, _) = entry?;
            let value = value.ok_or(MidgeError::InvalidData(
                "Missing value in index entry".to_string(),
            ))?;
            let (block_handle, _) = BlockHandle::decode(value)?;
            entries.push(IndexEntry {
                key: key.into(),
                block_handle,
            });
        }
        Ok(Self::new(entries))
    }

    /// Find the block that might contain the given key.
    /// Sparse index keys represent the LAST key in each block.
    /// Returns the block whose last key is >= the search key.
    /// Uses partition_point for optimized binary search.
    #[inline]
    pub fn find_block(&self, key: &[u8]) -> Option<&BlockHandle> {
        if self.entries.is_empty() {
            return None;
        }

        // partition_point finds the first index where predicate is false
        // i.e., the first entry where entry.key >= key
        let idx = self.entries.partition_point(|e| e.key.as_ref() < key);

        // Index entries store the LAST key in each block
        // If key <= first_entry, it must be in the first block
        if idx == 0 {
            return Some(&self.entries[0].block_handle);
        }

        // If idx >= entries.len(), key is greater than all index entries
        // Return the last block (it might contain keys after the last indexed key)
        if idx >= self.entries.len() {
            return Some(&self.entries[self.entries.len() - 1].block_handle);
        }

        // Otherwise, idx points to the first entry where entry.key >= key
        // This means key <= entries[idx].key, so key might be in block idx
        // (since entries[idx].key is the LAST key in that block)
        Some(&self.entries[idx].block_handle)
    }

    /// Find blocks in a range without allocating.
    /// Returns an iterator over block handles where keys may fall in [start, end).
    ///
    /// Note: Since the sparse index stores the LAST key of each block, we must be
    /// conservative about the end bound. A block with last_key >= end might still
    /// contain keys < end, so we must include such blocks.
    #[inline]
    pub fn find_blocks_in_range<'a>(
        &'a self,
        start: &[u8],
        end: &'a [u8],
    ) -> impl Iterator<Item = &'a BlockHandle> + 'a {
        // Empty range [a, a) should return no blocks
        let (start_idx, end_idx) = if start >= end {
            (0, 0)
        } else {
            // Find start index: first block where last_key >= start.
            // This is the first block that could contain keys >= start.
            let start_idx = self.entries.partition_point(|e| e.key.as_ref() < start);

            // Find end index: partition_point returns first index where last_key >= end.
            // We need to INCLUDE that block because it may contain keys < end.
            // So we add 1 to include the block at end_idx.
            let end_idx = self.entries.partition_point(|e| e.key.as_ref() < end);
            // Include the block at end_idx if it exists (add 1 to the slice end)
            let end_idx = (end_idx + 1).min(self.entries.len());

            // If start_idx >= end_idx, return empty range
            let start_idx = start_idx.min(end_idx);

            (start_idx, end_idx)
        };

        self.entries[start_idx..end_idx]
            .iter()
            .map(|en| &en.block_handle)
    }
}

pub struct SparseIndexBuilder {
    entries: Vec<IndexEntry>,
}

impl SparseIndexBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    pub fn add_index_entry(&mut self, key: &[u8], handle: BlockHandle) {
        self.entries.push(IndexEntry {
            key: key.to_vec().into(),
            block_handle: handle,
        });
    }
    pub fn add(&mut self, key: Bytes, handle: BlockHandle) {
        self.entries.push(IndexEntry {
            key,
            block_handle: handle,
        });
    }
    pub fn finish(self) -> SparseIndex {
        SparseIndex::new(self.entries)
    }
}

impl Default for SparseIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::format::BlockHandle;
    use bytes::Bytes;

    #[test]
    fn should_return_none_for_empty_index_find_block() {
        // Arrange
        let idx = SparseIndexBuilder::new().finish();

        // Act
        let got = idx.find_block(b"any");

        // Assert
        assert!(got.is_none());
    }

    #[test]
    fn should_find_block_for_exact_key() {
        // Arrange
        let mut b = SparseIndexBuilder::new();
        b.add(Bytes::from_static(b"a"), BlockHandle::new(100, 10));
        b.add(Bytes::from_static(b"m"), BlockHandle::new(200, 20));
        let idx = b.finish();

        // Act
        let got = idx.find_block(b"m").unwrap();

        // Assert
        assert_eq!(got.offset, 200);
        assert_eq!(got.size, 20);
    }

    #[test]
    fn should_find_block_for_key_between_entries() {
        // Arrange
        // Sparse index stores LAST key in each block
        let mut b = SparseIndexBuilder::new();
        b.add(Bytes::from_static(b"a"), BlockHandle::new(100, 10)); // Block 0 ends with "a"
        b.add(Bytes::from_static(b"m"), BlockHandle::new(200, 20)); // Block 1 ends with "m"
        b.add(Bytes::from_static(b"z"), BlockHandle::new(300, 30)); // Block 2 ends with "z"
        let idx = b.finish();

        // Act: Search for "p" (between "m" and "z")
        // "p" > "m" so it's not in block 1
        // "p" <= "z" so it might be in block 2
        let got = idx.find_block(b"p").unwrap();

        // Assert: Should return block 2 (offset 300)
        assert_eq!(got.offset, 300);
    }

    #[test]
    fn should_find_blocks_in_range_from_start_to_end_exclusive() {
        // Arrange
        // Sparse index stores LAST key in each block
        let mut b = SparseIndexBuilder::new();
        b.add(Bytes::from_static(b"a"), BlockHandle::new(10, 1)); // Block 0 ends with "a"
        b.add(Bytes::from_static(b"c"), BlockHandle::new(20, 2)); // Block 1 ends with "c"
        b.add(Bytes::from_static(b"e"), BlockHandle::new(30, 3)); // Block 2 ends with "e"
        let idx = b.finish();

        // Act: Query range [b, e)
        // - Block 0 (last="a"): last_key < "b", so no keys >= "b" → skip
        // - Block 1 (last="c"): last_key >= "b", could have keys in [b, c] → include
        // - Block 2 (last="e"): last_key >= "e", could have keys in [first_key, e) → include
        let handles: Vec<u64> = idx
            .find_blocks_in_range(b"b", b"e")
            .map(|h| h.offset)
            .collect();

        // Assert: Should return blocks 1 and 2
        assert_eq!(handles, vec![20u64, 30u64]);
    }
}
