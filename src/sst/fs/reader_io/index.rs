use super::block_index::BlockIndex;
use super::{BlockHandle, IndexEntries, SstFileIo};
use crate::common::{MidgeError, MidgeResult};
use crate::io::File;
use crate::sst::cache::{CacheFill, CacheKey};
use std::sync::Arc;

impl SstFileIo {
    pub(super) fn parse_index_entries(&self) -> MidgeResult<BlockIndex> {
        self.parse_index_entries_from(None)
    }

    pub(super) fn parse_index_entries_from(
        &self,
        file: Option<&dyn File>,
    ) -> MidgeResult<BlockIndex> {
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

    pub(super) fn decode_index_entries(&self, index_data: &[u8]) -> MidgeResult<BlockIndex> {
        BlockIndex::decode(index_data, self.block_region_end)
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

    fn trie_search_bounds(&self, index: &BlockIndex, key: &[u8]) -> Option<(usize, usize)> {
        let last_index = index.len().checked_sub(1)?;
        let trie = self.trie_reader.as_ref()?;
        let block_index = trie
            .find_block(key)
            .or_else(|| trie.seek_next(key))
            .and_then(|block_index| usize::try_from(block_index).ok())
            .filter(|block_index| *block_index <= last_index)?;
        let first = block_index.saturating_sub(1);
        // A successor with duplicate boundaries points at its last block.
        // Trust a hint only when the omitted index entries cannot contain the
        // query's predecessor; otherwise search the complete sparse index.
        let lower_brackets = first == 0 || index.key(first) <= key;
        let upper_brackets = block_index == last_index || index.key(block_index + 1) > key;
        (lower_brackets && upper_brackets).then_some((first, block_index))
    }

    fn search_bounds(&self, index: &BlockIndex, key: &[u8]) -> Option<(usize, usize)> {
        if self.key_outside_persisted_range(key) {
            return None;
        }

        if let Some(bounds) = self.trie_search_bounds(index, key) {
            return Some(bounds);
        }

        Some((0, index.len().checked_sub(1)?))
    }

    /// The blocks that can hold keys in `[start, end]`, clamped to `index`;
    /// `None` when no block can. Every range reader selects blocks here.
    pub(super) fn block_span(
        &self,
        index: &BlockIndex,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Option<std::ops::RangeInclusive<usize>> {
        let last = index.len().checked_sub(1)?;
        let first_block = start
            .and_then(|key| self.candidate_block_indices(index, key))
            .map_or(0, |range| *range.start());
        let last_block = end
            .and_then(|key| self.candidate_block_indices(index, key))
            .map_or(last, |range| *range.end())
            .min(last);
        (first_block <= last_block).then_some(first_block..=last_block)
    }

    pub(super) fn candidate_block_indices(
        &self,
        index: &BlockIndex,
        key: &[u8],
    ) -> Option<std::ops::RangeInclusive<usize>> {
        if index.is_empty() {
            return None;
        }

        let (mut start_bound, end_bound) = self.search_bounds(index, key)?;

        if start_bound > end_bound {
            start_bound = end_bound;
        }
        start_bound = start_bound.saturating_sub(1);

        let mut left = start_bound;
        let mut right = end_bound + 1;
        while left < right {
            let mid = usize::midpoint(left, right);
            if index.key(mid) <= key {
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
        while first_candidate > 0 && index.key(first_candidate - 1) == key {
            first_candidate -= 1;
        }

        if index.key(first_candidate) == key && first_candidate > 0 {
            first_candidate -= 1;
        }

        Some(first_candidate..=last_candidate)
    }

    pub(super) fn candidate_data_blocks(
        &self,
        index: &BlockIndex,
        key: &[u8],
    ) -> Vec<(usize, BlockHandle)> {
        self.candidate_block_indices(index, key)
            .map_or_else(Vec::new, |range| {
                range.map(|idx| (idx, index.handle(idx))).collect()
            })
    }

    pub(super) fn read_cached_data_block(&self, handle: &BlockHandle) -> MidgeResult<bytes::Bytes> {
        self.read_cached_data_block_from(handle, self.handle.as_deref(), CacheFill::Normal)
    }

    /// Serve a data block from the cache or the file; a miss is inserted only
    /// under [`CacheFill::Normal`], so scans can read without evicting the
    /// point-read working set (#944).
    pub(super) fn read_cached_data_block_from(
        &self,
        handle: &BlockHandle,
        file: Option<&dyn File>,
        cache_fill: CacheFill,
    ) -> MidgeResult<bytes::Bytes> {
        let cache_key = CacheKey::for_data(self.sst_id, handle.offset);
        let read_metrics = self.diagnostics.sst_metrics();

        if let Some(ref cache) = self.block_cache {
            if let Some(cached_value) = cache.get(&cache_key) {
                read_metrics.record_block_cache_hit();
                self.diagnostics.record_block_cache_lookup(true);
                Ok(cached_value.data)
            } else {
                read_metrics.record_block_cache_miss();
                self.diagnostics.record_block_cache_lookup(false);
                let bytes = file.map_or_else(
                    || self.read_block(handle),
                    |file| self.read_block_from(file, handle),
                )?;
                // Immutable cached blocks carry a checked restart directory.
                // Cache hits need only constant framing and binary key probes.
                crate::sst::data_block::DataBlockLayout::parse(&bytes)?;
                read_metrics.record_data_block_read();
                if cache_fill == CacheFill::Normal {
                    cache.put(cache_key, &bytes);
                }
                Ok(bytes)
            }
        } else {
            let bytes = file.map_or_else(
                || self.read_block(handle),
                |file| self.read_block_from(file, handle),
            )?;
            crate::sst::data_block::DataBlockLayout::parse(&bytes)?;
            read_metrics.record_data_block_read();
            Ok(bytes)
        }
    }

    /// Like [`Self::read_cached_data_block_from`], but a scan that bypasses the
    /// cache reads ahead: a miss fetches up to [`READAHEAD_BYTES`] of the block
    /// region in the scan's direction into `window` and later blocks inside it
    /// are served from that buffer, so a long scan costs one read per window
    /// instead of one per block (#945). Small scans fill the cache instead and
    /// read one block at a time, since their next block is rarely wanted.
    pub(super) fn read_cached_data_block_windowed(
        &self,
        handle: &BlockHandle,
        file: Option<&dyn File>,
        cache_fill: CacheFill,
        reverse: bool,
        span: &std::ops::Range<u64>,
        window: &mut Option<ReadWindow>,
    ) -> MidgeResult<bytes::Bytes> {
        let (Some(file), CacheFill::Bypass) = (file, cache_fill) else {
            return self.read_cached_data_block_from(handle, file, cache_fill);
        };
        let cache_key = CacheKey::for_data(self.sst_id, handle.offset);
        let read_metrics = self.diagnostics.sst_metrics();
        if let Some(cache) = &self.block_cache {
            if let Some(cached_value) = cache.get(&cache_key) {
                read_metrics.record_block_cache_hit();
                self.diagnostics.record_block_cache_lookup(true);
                return Ok(cached_value.data);
            }
            read_metrics.record_block_cache_miss();
            self.diagnostics.record_block_cache_lookup(false);
        }
        Self::validate_block_handle(*handle, self.block_region_end, "referenced")?;
        let frame = if let Some(frame) = window.as_ref().and_then(|window| window.frame(handle)) {
            frame
        } else {
            let fetched = self.read_window(file, handle, reverse, span)?;
            let frame = fetched
                .frame(handle)
                .ok_or_else(|| MidgeError::Internal("read window misses its block".into()))?;
            *window = Some(fetched);
            frame
        };
        let (block, ()) = Self::decode_framed_block(&frame, |_| Ok(()))?;
        crate::sst::data_block::DataBlockLayout::parse(&block)?;
        read_metrics.record_data_block_read();
        Ok(block)
    }

    /// Fetch the window that starts at `handle` going forward, or ends at it
    /// going backward, clipped to `span`: the byte range of the blocks the scan
    /// can still visit, so a narrow range scan never reads past its own end.
    fn read_window(
        &self,
        file: &dyn File,
        handle: &BlockHandle,
        reverse: bool,
        span: &std::ops::Range<u64>,
    ) -> MidgeResult<ReadWindow> {
        let block_end = handle.offset.saturating_add(handle.size);
        let (offset, len) = if reverse {
            let offset = block_end
                .saturating_sub(READAHEAD_BYTES.max(handle.size))
                .max(span.start.min(handle.offset));
            (offset, block_end - offset)
        } else {
            let end = handle
                .offset
                .saturating_add(READAHEAD_BYTES.max(handle.size))
                .min(span.end.max(block_end))
                .min(self.block_region_end);
            (handle.offset, end - handle.offset)
        };
        let bytes = file
            .read_at(offset, len)
            .map_err(crate::io::FsError::into_midge)?;
        Ok(ReadWindow { offset, bytes })
    }
}

/// Bytes a bypassing scan read ahead of its current block.
const READAHEAD_BYTES: u64 = 256 * 1024;

pub(super) struct ReadWindow {
    offset: u64,
    bytes: bytes::Bytes,
}

impl ReadWindow {
    /// The framed block for `handle` when the window covers it entirely.
    fn frame(&self, handle: &BlockHandle) -> Option<bytes::Bytes> {
        let start = handle.offset.checked_sub(self.offset)?;
        let end = start.checked_add(handle.size)?;
        let (start, end) = (usize::try_from(start).ok()?, usize::try_from(end).ok()?);
        (end <= self.bytes.len()).then(|| self.bytes.slice(start..end))
    }
}
