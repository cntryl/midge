use crate::error::{MidgeError, MidgeResult};
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

    pub fn decode(data: &[u8]) -> MidgeResult<Self> {
        use crate::common::tlv::{parse_varint32_from_slice, tags, TlvReader};

        // Decode an index block encoded with DataBlockBuilder where value is a BlockHandle encoding
        let len = data.len();
        if len < 9 {
            // Need at least version + restart_count
            return Ok(SparseIndex::default());
        }

        // Read restart count (last 4 bytes)
        let num_restarts =
            u32::from_le_bytes([data[len - 4], data[len - 3], data[len - 2], data[len - 1]])
                as usize;
        if num_restarts == 0 {
            return Ok(SparseIndex::default());
        }

        // Calculate where entries end (before version marker + restart array)
        let restarts_start = len
            .checked_sub(4 + num_restarts * 4)
            .ok_or_else(|| MidgeError::InvalidData("index block too small".into()))?;

        // Version marker is before restart array
        let version_offset = restarts_start
            .checked_sub(1)
            .ok_or_else(|| MidgeError::InvalidData("index block too small for version".into()))?;
        let entries_end = version_offset;

        // Single TlvReader for all entries
        let reader = TlvReader::new(&data[..entries_end]);
        let mut last_key: Vec<u8> = Vec::new();
        // Pre-allocate with estimated capacity based on restart points
        let mut out: Vec<IndexEntry> = Vec::with_capacity(num_restarts.saturating_mul(4));

        // State for current entry
        let mut shared_len: Option<u32> = None;
        let mut key_delta: Option<&[u8]> = None;
        let mut value: Option<&[u8]> = None;

        // Helper closure to reconstruct and push entry
        let process_entry = |shared_len: u32,
                             key_delta: &[u8],
                             val_bytes: &[u8],
                             last_key: &mut Vec<u8>,
                             out: &mut Vec<IndexEntry>|
         -> MidgeResult<()> {
            // Reconstruct full key
            let mut key = Vec::with_capacity(shared_len as usize + key_delta.len());
            if shared_len as usize > last_key.len() {
                return Err(MidgeError::InvalidData(format!(
                    "shared_len {} exceeds last_key len {}",
                    shared_len,
                    last_key.len()
                )));
            }
            key.extend_from_slice(&last_key[..shared_len as usize]);
            key.extend_from_slice(key_delta);

            // Decode block handle from value
            let (bh, _bh_sz) = BlockHandle::decode(val_bytes)?;

            *last_key = key.clone();
            out.push(IndexEntry {
                key: Bytes::from(key),
                block_handle: bh,
            });
            Ok(())
        };

        for (tag, tag_data) in reader {
            match tag {
                tags::SHARED_PREFIX_LEN => {
                    // Process previous entry if complete
                    if let (Some(sl), Some(kd), Some(val_bytes)) = (shared_len, key_delta, value) {
                        process_entry(sl, kd, val_bytes, &mut last_key, &mut out)?;
                    }

                    // Start new entry
                    shared_len = Some(parse_varint32_from_slice(tag_data)?);
                    key_delta = None;
                    value = None;
                }
                tags::KEY_DELTA => {
                    key_delta = Some(tag_data);
                }
                tags::VALUE => {
                    value = Some(tag_data);
                }
                _ => {
                    // Skip other tags (sequence, entry_type, etc.)
                }
            }
        }

        // Process last entry
        if let (Some(sl), Some(kd), Some(val_bytes)) = (shared_len, key_delta, value) {
            process_entry(sl, kd, val_bytes, &mut last_key, &mut out)?;
        }

        Ok(SparseIndex::new(out))
    }

    /// Find the block that might contain the given key.
    /// Returns the block whose first key is <= the search key.
    /// Uses partition_point for optimized binary search.
    #[inline]
    pub fn find_block(&self, key: &[u8]) -> Option<&BlockHandle> {
        if self.entries.is_empty() {
            return None;
        }

        // partition_point finds the first index where predicate is false
        // predicate: entry.key <= key  => we want first entry.key > key
        let idx = self.entries.partition_point(|e| e.key.as_ref() <= key);

        // idx is now the first entry GREATER than key
        // We want the last entry LESS THAN OR EQUAL to key, which is idx - 1
        // saturating_sub handles idx == 0 case (returns 0)
        let block_idx = idx.saturating_sub(1);

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
