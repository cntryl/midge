use super::{BlockHandle, IndexEntries, SstFileIo};
use crate::common::{MidgeError, MidgeResult};
use crate::io::File;
use crate::sst::cache::CacheKey;
use std::sync::Arc;

impl SstFileIo {
    pub(super) fn parse_index_entries(&self) -> MidgeResult<Vec<(Vec<u8>, BlockHandle)>> {
        self.parse_index_entries_from(None)
    }

    pub(super) fn parse_index_entries_from(
        &self,
        file: Option<&dyn File>,
    ) -> MidgeResult<Vec<(Vec<u8>, BlockHandle)>> {
        let footer = self
            .footer
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("No footer".into()))?;

        let index_data = file.map_or_else(
            || self.read_block(&footer.index_handle),
            |file| self.read_block_from(file, &footer.index_handle),
        )?;
        self.decode_index_entries(&index_data)
    }

    pub(super) fn decode_index_entries(
        &self,
        index_data: &[u8],
    ) -> MidgeResult<Vec<(Vec<u8>, BlockHandle)>> {
        let mut result: Vec<(Vec<u8>, BlockHandle)> = Vec::new();
        let mut offset = 0;

        while offset < index_data.len() {
            if offset
                .checked_add(20)
                .is_none_or(|end| end > index_data.len())
            {
                return Err(MidgeError::Corruption(
                    "SST index has a truncated entry header".into(),
                ));
            }

            let key_len = u32::from_le_bytes([
                index_data[offset],
                index_data[offset + 1],
                index_data[offset + 2],
                index_data[offset + 3],
            ]) as usize;
            offset += 4;

            let key_end = offset.checked_add(key_len).ok_or_else(|| {
                MidgeError::Corruption("SST index key length overflows the block".into())
            })?;
            if key_end > index_data.len() {
                return Err(MidgeError::Corruption(
                    "SST index has a truncated key".into(),
                ));
            }

            let key = index_data[offset..key_end].to_vec();
            offset = key_end;

            if offset
                .checked_add(16)
                .is_none_or(|end| end > index_data.len())
            {
                return Err(MidgeError::Corruption(
                    "SST index has a truncated block handle".into(),
                ));
            }

            let block_offset = u64::from_le_bytes([
                index_data[offset],
                index_data[offset + 1],
                index_data[offset + 2],
                index_data[offset + 3],
                index_data[offset + 4],
                index_data[offset + 5],
                index_data[offset + 6],
                index_data[offset + 7],
            ]);
            offset += 8;

            let block_size = u64::from_le_bytes([
                index_data[offset],
                index_data[offset + 1],
                index_data[offset + 2],
                index_data[offset + 3],
                index_data[offset + 4],
                index_data[offset + 5],
                index_data[offset + 6],
                index_data[offset + 7],
            ]);
            offset += 8;

            let handle = BlockHandle::new(block_offset, block_size);
            Self::validate_block_handle(handle, self.block_region_end, "data")?;
            if result
                .last()
                .is_some_and(|(previous_key, previous_handle)| {
                    previous_key.as_slice() > key.as_slice()
                        || previous_handle
                            .offset
                            .checked_add(previous_handle.size)
                            .is_none_or(|end| end > handle.offset)
                })
            {
                return Err(MidgeError::Corruption(
                    "SST index entries are not key-ordered and non-overlapping".into(),
                ));
            }
            result.push((key, handle));
        }

        Ok(result)
    }

    pub(super) fn index_entries(&self) -> MidgeResult<IndexEntries> {
        self.index_entries_from(None)
    }

    pub(super) fn index_entries_from(&self, file: Option<&dyn File>) -> MidgeResult<IndexEntries> {
        if let Some(cached) = self.index_entries.load_full() {
            return Ok(cached);
        }

        let parsed = Arc::new(self.parse_index_entries_from(file)?);
        // Concurrent cold readers may parse the same immutable index more
        // than once. Publishing either equivalent result is safe, and avoids
        // placing a mutex in every subsequent point-read hot path.
        self.index_entries.store(Some(Arc::clone(&parsed)));
        Ok(parsed)
    }

    pub(super) fn key_outside_persisted_range(&self, key: &[u8]) -> bool {
        self.smallest_key
            .as_ref()
            .zip(self.largest_key.as_ref())
            .is_some_and(|(smallest_key, largest_key)| {
                key < smallest_key.as_slice() || key > largest_key.as_slice()
            })
    }

    pub(super) fn range_outside_persisted_bounds(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> bool {
        self.smallest_key
            .as_ref()
            .zip(self.largest_key.as_ref())
            .is_some_and(|(smallest_key, largest_key)| {
                end.is_some_and(|end| end <= smallest_key.as_slice())
                    || start.is_some_and(|start| start > largest_key.as_slice())
            })
    }

    fn trie_search_bounds(&self, last_index: usize, key: &[u8]) -> Option<(usize, usize)> {
        let trie = self.trie_reader.as_ref()?;
        let block_index = trie
            .find_block(key)
            .or_else(|| trie.seek_next(key))
            .and_then(|block_index| usize::try_from(block_index).ok())
            .filter(|block_index| *block_index <= last_index)?;
        Some((block_index.saturating_sub(1), block_index))
    }

    fn search_bounds(&self, last_index: usize, key: &[u8]) -> Option<(usize, usize)> {
        if self.key_outside_persisted_range(key) {
            return None;
        }

        if let Some(bounds) = self.trie_search_bounds(last_index, key) {
            return Some(bounds);
        }

        Some((0, last_index))
    }

    pub(super) fn candidate_block_indices(
        &self,
        index: &[(Vec<u8>, BlockHandle)],
        key: &[u8],
    ) -> Option<std::ops::RangeInclusive<usize>> {
        if index.is_empty() {
            return None;
        }

        let last_index = index.len() - 1;
        let (mut start_bound, end_bound) = self.search_bounds(last_index, key)?;

        if start_bound > end_bound {
            start_bound = end_bound;
        }
        start_bound = start_bound.saturating_sub(1);

        let mut left = start_bound;
        let mut right = end_bound + 1;
        while left < right {
            let mid = usize::midpoint(left, right);
            if index[mid].0.as_slice() <= key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        let last_candidate = if left == start_bound {
            start_bound
        } else {
            left - 1
        };
        let mut first_candidate = last_candidate;

        // A single logical key can span arbitrarily many data blocks when it
        // has large or numerous retained versions. The trie deliberately
        // keeps the last block id for duplicate block-boundary keys, so its
        // narrow search window is only a lookup accelerator. Expand through
        // the complete equal-key run before selecting the predecessor block
        // that may contain the first versions of the key.
        while first_candidate > 0 && index[first_candidate - 1].0.as_slice() == key {
            first_candidate -= 1;
        }

        if index[first_candidate].0.as_slice() == key && first_candidate > 0 {
            first_candidate -= 1;
        }

        Some(first_candidate..=last_candidate)
    }

    pub(super) fn candidate_data_blocks(
        &self,
        index: &[(Vec<u8>, BlockHandle)],
        key: &[u8],
    ) -> Vec<(usize, BlockHandle)> {
        self.candidate_block_indices(index, key)
            .map_or_else(Vec::new, |range| {
                range.map(|idx| (idx, index[idx].1)).collect()
            })
    }

    pub(super) fn read_cached_data_block(&self, handle: &BlockHandle) -> MidgeResult<bytes::Bytes> {
        self.read_cached_data_block_from(handle, None)
    }

    pub(super) fn read_cached_data_block_from(
        &self,
        handle: &BlockHandle,
        file: Option<&dyn File>,
    ) -> MidgeResult<bytes::Bytes> {
        if let Some(cache) = &self.recovery_block {
            return self.read_recovery_block(cache, handle);
        }
        let cache_key = CacheKey::for_data(self.sst_id, handle.offset);
        let read_metrics = self.diagnostics.sst_metrics();

        if let Some(ref cache) = self.block_cache {
            if let Some(cached_value) = cache.get(&cache_key) {
                read_metrics.record_block_cache_hit();
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cache_hit();
                }
                Ok(cached_value.data.as_ref().clone())
            } else {
                read_metrics.record_block_cache_miss();
                if let Some(telemetry) = crate::telemetry::Telemetry::global() {
                    telemetry.metrics().record_cache_miss();
                }
                let bytes = file.map_or_else(
                    || self.read_block(handle),
                    |file| self.read_block_from(file, handle),
                )?;
                read_metrics.record_data_block_read();
                cache.put(cache_key, &bytes);
                Ok(bytes)
            }
        } else {
            let bytes = file.map_or_else(
                || self.read_block(handle),
                |file| self.read_block_from(file, handle),
            )?;
            read_metrics.record_data_block_read();
            Ok(bytes)
        }
    }
}
