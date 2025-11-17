use crate::error::{MidgeError, MidgeResult};
use crate::sst::encoding::TlvBlockIterator;
use crate::sst::format::BlockHandle;
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
    #[inline]
    pub fn find_blocks_in_range<'a>(
        &'a self,
        start: &[u8],
        end: &'a [u8],
    ) -> impl Iterator<Item = &'a BlockHandle> + 'a {
        // Use partition_point for cleaner binary search
        let start_idx = self.entries.partition_point(|e| e.key.as_ref() < start);

        // Clamp to valid range - if start is beyond last entry, start from last
        let start_idx = if start_idx == 0 {
            0
        } else if start_idx >= self.entries.len() {
            self.entries.len().saturating_sub(1)
        } else {
            start_idx - 1
        };

        // Use iterator instead of collecting into Vec
        self.entries[start_idx..]
            .iter()
            .take_while(move |en| en.key.as_ref() < end)
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
        let mut b = SparseIndexBuilder::new();
        b.add(Bytes::from_static(b"a"), BlockHandle::new(10, 1));
        b.add(Bytes::from_static(b"c"), BlockHandle::new(20, 2));
        b.add(Bytes::from_static(b"e"), BlockHandle::new(30, 3));
        let idx = b.finish();

        // Act
        let handles: Vec<u64> = idx
            .find_blocks_in_range(b"b", b"e")
            .map(|h| h.offset)
            .collect();

        // Assert
        assert_eq!(handles, vec![10u64, 20u64]);
    }
}
