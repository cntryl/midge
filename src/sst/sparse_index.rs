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
            builder.add_index_entry(&entry.key, entry.block_handle);
        }
        builder.finish()
    }

    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        let iterator = TlvBlockIterator::new(data);
        let mut entries = Vec::new();
        for entry in iterator {
            let (key, value, _, _, _) = entry?;
            let value = value.ok_or(MidgeError::InvalidData("Missing value in index entry".to_string()))?;
            let (block_handle, _) = BlockHandle::decode(value)?;
            entries.push(IndexEntry {
                key: key.into(),
                block_handle,
            });
        }
        Ok(Self::new(entries))
    }

    /// Find the block that might contain the given key.
    /// Returns the block whose first key is <= the search key.
    /// Uses partition_point for optimized binary search.
    #[inline]
    pub fn find_block(&self, key: &[u8]) -> Option<&BlockHandle> {
        if self.entries.is_empty() {
            eprintln!("DEBUG: SparseIndex entries is empty");
            return None;
        }

        eprintln!("DEBUG: SparseIndex find_block for key: {:?}", String::from_utf8_lossy(key));
        eprintln!("DEBUG: SparseIndex entries count: {}", self.entries.len());
        for (i, e) in self.entries.iter().enumerate() {
            eprintln!("DEBUG: Entry {}: key={:?}", i, String::from_utf8_lossy(&e.key));
        }

        // partition_point finds the first index where predicate is false
        // predicate: entry.key <= key  => we want first entry.key > key
        let idx = self.entries.partition_point(|e| e.key.as_ref() <= key);

        eprintln!("DEBUG: partition_point returned idx: {}", idx);

        // idx is now the first entry GREATER than key
        // We want the last entry LESS THAN OR EQUAL to key, which is idx - 1
        // saturating_sub handles idx == 0 case (returns 0)
        let block_idx = idx.saturating_sub(1);

        eprintln!("DEBUG: block_idx: {}", block_idx);

        Some(&self.entries[block_idx].block_handle)
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
    fn should_find_previous_block_for_key_between_entries() {
        // Arrange
        let mut b = SparseIndexBuilder::new();
        b.add(Bytes::from_static(b"a"), BlockHandle::new(100, 10));
        b.add(Bytes::from_static(b"m"), BlockHandle::new(200, 20));
        b.add(Bytes::from_static(b"z"), BlockHandle::new(300, 30));
        let idx = b.finish();

        // Act
        let got = idx.find_block(b"p").unwrap();

        // Assert
        assert_eq!(got.offset, 200);
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
