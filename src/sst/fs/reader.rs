use bytes::Bytes;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use tracing::{debug, trace};

use crate::error::{MidgeError, MidgeResult};
use crate::fs;
use crate::sst::bloom::BloomFilter;
use crate::sst::encoding::{decode, decode_key_at_offset, TlvBlockIterator};
use crate::sst::format::{Block, BlockHandle, BlockType, Footer};
use crate::sst::meta_index::{linear_search_meta_index, meta_index_contains};
use crate::sst::range_tombstone::{decode_range_tombstones, is_covered_by_range_tombstone};
use crate::sst::reader_common::should_skip_key;
use crate::sst::sparse_index::SparseIndex;
use crate::sst::traits::{KeyState, RangeTombstone, SstStateReader};

use super::iterator::SstRangeIter;
use super::utils::{
    binary_search_restart_points, calculate_entries_end, decode_data_block,
    decode_data_block_paranoid, decode_internal_key_or_raw,
};

/// Helper struct for building and processing block entries during linear search

#[derive(Debug)]
pub struct SstFile {
    path: PathBuf,
    footer: Option<Footer>,
    sparse_index: Option<SparseIndex>,
    bloom_filter: Option<BloomFilter>,
    range_tombstones: Vec<RangeTombstone>,
    use_internal_keys: bool,
    paranoid_checksums: bool,
}

impl SstFile {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            footer: None,
            sparse_index: None,
            bloom_filter: None,
            range_tombstones: Vec::new(),
            use_internal_keys: false,
            paranoid_checksums: false,
        }
    }

    /// Create a new SST file reader with paranoid checksum verification enabled
    pub fn new_with_paranoid(path: PathBuf, paranoid_checksums: bool) -> Self {
        Self {
            path,
            footer: None,
            sparse_index: None,
            bloom_filter: None,
            range_tombstones: Vec::new(),
            use_internal_keys: false,
            paranoid_checksums,
        }
    }

    pub fn open(path: &Path) -> MidgeResult<Self> {
        Self::open_with_paranoid(path, false)
    }

    /// Open SST file with paranoid checksum verification
    pub fn open_with_paranoid(path: &Path, paranoid_checksums: bool) -> MidgeResult<Self> {
        debug!("Opening SST file: {}", path.display());
        let mut sst = Self::new_with_paranoid(path.to_path_buf(), paranoid_checksums);
        if let Err(e) = sst.load_metadata() {
            // Use structured logging for failures; tests/CI should configure a tracing
            // subscriber if they want to capture these diagnostics.
            tracing::error!("SST open failed: {} error: {}", path.display(), e);
            return Err(e);
        }
        debug!("SST file metadata loaded successfully");
        Ok(sst)
    }

    fn load_metadata(&mut self) -> MidgeResult<()> {
        debug!("Loading footer and index from {}", self.path.display());
        // Some platforms may briefly report NotFound for a file that was just
        // renamed into place (observed as a rare transient on Windows). Retry
        // a few times before failing to improve robustness in tests.
        let mut last_err: Option<std::io::Error> = None;
        let mut file_opt: Option<std::fs::File> = None;
        for _attempt in 0..5 {
            match OpenOptions::new().read(true).open(&self.path) {
                Ok(f) => {
                    file_opt = Some(f);
                    break;
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        // brief backoff and retry
                        std::thread::sleep(std::time::Duration::from_millis(5));
                        last_err = Some(e);
                        continue;
                    } else {
                        return Err(e.into());
                    }
                }
            }
        }
        let mut file = match file_opt {
            Some(f) => f,
            None => {
                return Err(last_err
                    .unwrap_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::NotFound, "file not found")
                    })
                    .into())
            }
        };

        // Read footer from end of file (48 bytes)
        let mut footer_data = [0u8; 48];
        fs::read_from_end(&mut file, 48, &mut footer_data)?;
        self.footer = Some(Footer::decode(&footer_data)?);
        // SAFETY: footer was just set to Some on the previous line
        let footer = self.footer.as_ref().expect("footer just set");

        trace!(
            "Footer loaded: index_handle offset={} size={}",
            footer.index_handle.offset,
            footer.index_handle.size
        );

        // Read sparse index
        let index_data = fs::read_range(
            &mut file,
            footer.index_handle.offset,
            footer.index_handle.offset + footer.index_handle.size,
        )?;
        let index_block = Block::decode(&index_data, BlockType::Index)?;

        trace!("Index block decoded, data len={}", index_block.data.len());

        self.sparse_index = Some(SparseIndex::decode(&index_block.data)?);
        debug!("Sparse index decoded successfully");

        let mut range_tombstones: Vec<RangeTombstone> = Vec::new();
        let mut use_internal = false;
        if footer.meta_index_handle.size > 0 {
            trace!(
                "Loading meta index: offset={} size={}",
                footer.meta_index_handle.offset,
                footer.meta_index_handle.size
            );
            // Read meta index
            let meta_index_data = fs::read_range(
                &mut file,
                footer.meta_index_handle.offset,
                footer.meta_index_handle.offset + footer.meta_index_handle.size,
            )?;

            let meta_index_block = Block::decode(&meta_index_data, BlockType::MetaIndex)?;
            trace!(
                "Meta index block decoded, data len={}",
                meta_index_block.data.len()
            );

            if let Some(bloom_handle) = linear_search_meta_index(
                &meta_index_block.data,
                0,
                meta_index_block.data.len(),
                b"filter.bloom",
            )? {
                trace!("Found bloom filter handle");
                let bloom_data = fs::read_range(
                    &mut file,
                    bloom_handle.offset,
                    bloom_handle.offset + bloom_handle.size,
                )?;
                let bloom_block = Block::decode(&bloom_data, BlockType::Filter)?;
                self.bloom_filter = Some(BloomFilter::decode_block(&bloom_block.data)?);
            }
            if let Some(tomb_handle) = linear_search_meta_index(
                &meta_index_block.data,
                0,
                meta_index_block.data.len(),
                b"tombstones.range",
            )? {
                trace!("Found tombstones handle");
                let tomb_data = fs::read_range(
                    &mut file,
                    tomb_handle.offset,
                    tomb_handle.offset + tomb_handle.size,
                )?;
                let tomb_block = Block::decode(&tomb_data, BlockType::Filter)?;
                range_tombstones = decode_range_tombstones(&tomb_block.data)?;
            }
            // detect internal key meta flag using presence check (value may not be a BlockHandle)
            use_internal = meta_index_contains(
                &meta_index_block.data,
                0,
                meta_index_block.data.len(),
                b"format.internal_keys",
            )?;
            debug!("Internal keys format: {}", use_internal);
        }
        self.range_tombstones = range_tombstones;
        self.use_internal_keys = use_internal;
        debug!("Metadata loading complete");
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        match self.get_state(key)? {
            KeyState::Value(v, _seq, None) => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    /// Get presence state (including tombstone) for a key in this SST.
    fn get_state_internal(&self, key: &[u8]) -> MidgeResult<KeyState> {
        trace!("get_state_internal: key={:?}", String::from_utf8_lossy(key));

        // Early-out if bloom filter or range tombstones indicate key is not present
        #[cfg(test)]
        {
            eprintln!(
                "bloom_filter present={} may_contain={}",
                self.bloom_filter.is_some(),
                self.bloom_filter
                    .as_ref()
                    .map(|bf| bf.may_contain(key))
                    .unwrap_or(false)
            );
            eprintln!("range tombstones: {}", self.range_tombstones.len());
        }

        if should_skip_key(&self.bloom_filter, &self.range_tombstones, key, u64::MAX) {
            trace!("Bloom filter or tombstone check: key not present");
            return Ok(KeyState::Absent);
        }

        let sparse_index = self
            .sparse_index
            .as_ref()
            .ok_or_else(|| MidgeError::InvalidData("SST file not properly loaded".into()))?;

        // TEMP DEBUG: print sparse index keys to help diagnose failing tests
        // NOTE: This is a temporary diagnostic change; it will be removed after debugging
        #[cfg(test)]
        {
            eprintln!(
                "SST sparse index entries (count={}):",
                sparse_index.entries().len()
            );
            for en in sparse_index.entries() {
                eprintln!("  entry key={}", String::from_utf8_lossy(en.key.as_ref()));
            }
        }

        if let Some(block_handle) = sparse_index.find_block(key) {
            if self.use_internal_keys {
                // For internal-on-disk layout, reuse the snapshot-aware logic
                // with an effectively infinite snapshot so any seq is visible.
                return self.get_state_at_internal(key, u64::MAX);
            }
            let data_block = self.read_data_block(*block_handle)?;
            return self.search_data_block_state(&data_block.data, key);
        }

        Ok(KeyState::Absent)
    }

    /// Snapshot-aware get: respects per-entry sequence number when deciding visibility.
    fn get_state_at_internal(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<KeyState> {
        // Early-out if bloom filter or range tombstones indicate key is not present
        if should_skip_key(
            &self.bloom_filter,
            &self.range_tombstones,
            key,
            snapshot_seq,
        ) {
            return Ok(KeyState::Absent);
        }
        let sparse_index = self
            .sparse_index
            .as_ref()
            .ok_or_else(|| MidgeError::InvalidData("SST file not properly loaded".into()))?;
        if let Some(block_handle) = sparse_index.find_block(key) {
            // Use snapshot-aware search to find first version with seq <= snapshot_seq
            let blk = self.read_data_block(*block_handle)?;
            let state = self.search_data_block_state_at(&blk.data, key, snapshot_seq)?;
            trace!(
                "get_state_at key={:?} seq={} state={:?}",
                key,
                snapshot_seq,
                state
            );
            Ok(state)
        } else {
            Ok(KeyState::Absent)
        }
    }

    pub fn range_iter(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<SstRangeIter> {
        let sparse_index = self
            .sparse_index
            .as_ref()
            .ok_or_else(|| MidgeError::InvalidData("SST file not properly loaded".into()))?;

        let blocks: Vec<BlockHandle> = match (start, end) {
            // Both bounds specified - use optimized range search
            (Some(s), Some(e)) => sparse_index.find_blocks_in_range(s, e).copied().collect(),

            // Start bound only - find start position and take all blocks after
            (Some(s), None) => {
                let entries = sparse_index.entries();
                let start_idx = entries
                    .binary_search_by(|en| en.key.as_ref().cmp(s))
                    .unwrap_or_else(|i| i.saturating_sub(1));
                entries[start_idx..]
                    .iter()
                    .map(|en| en.block_handle)
                    .collect()
            }

            // End bound only - take all blocks up to end position
            (None, Some(e)) => {
                let entries = sparse_index.entries();
                let end_idx = entries
                    .binary_search_by(|en| en.key.as_ref().cmp(e))
                    .map(|i| i.saturating_sub(1))
                    .unwrap_or_else(|i| i.saturating_sub(1))
                    .min(entries.len().saturating_sub(1));
                entries[..=end_idx]
                    .iter()
                    .map(|en| en.block_handle)
                    .collect()
            }

            // No bounds - return all blocks
            (None, None) => sparse_index
                .entries()
                .iter()
                .map(|en| en.block_handle)
                .collect(),
        };

        Ok(SstRangeIter::new(
            self.path.clone(),
            blocks,
            start.map(|s| s.to_vec()),
            end.map(|e| e.to_vec()),
            self.use_internal_keys,
        ))
    }

    fn parse_key_at_offset(
        &self,
        data: &[u8],
        offset: usize,
        limit: usize,
    ) -> MidgeResult<Vec<u8>> {
        // Use shared TLV parser
        let key = decode_key_at_offset(data, offset, limit)?;

        // If internal keys format, decode to get user key
        if !self.use_internal_keys {
            return Ok(key);
        }
        if let Some((user, _seq, _tomb)) = crate::common::internal_key::decode_internal_key(&key) {
            Ok(user)
        } else {
            Ok(key)
        }
    }

    #[inline]
    fn read_data_block(&self, handle: BlockHandle) -> MidgeResult<Block> {
        // OPTIMIZATION OPPORTUNITY: This could be enhanced with block caching.
        // For now, we optimize by reusing file handles and minimizing allocations.
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let block_data = fs::read_range(&mut file, handle.offset, handle.offset + handle.size)?;
        if self.paranoid_checksums {
            decode_data_block_paranoid(&block_data, true)
        } else {
            decode_data_block(&block_data)
        }
    }

    pub fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let it = self.range_iter(start, end)?;
        let mut out: Vec<(Bytes, Bytes)> = Vec::new();
        for (k, v) in it {
            out.push((k, v));
        }
        Ok(out)
    }

    /// Like scan_range but returns KeyState per key for merge logic.
    fn scan_range_state_internal(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        let sparse_index = self
            .sparse_index
            .as_ref()
            .ok_or_else(|| MidgeError::InvalidData("SST file not properly loaded".into()))?;
        let mut out: Vec<(Bytes, KeyState)> = Vec::new();

        for en in sparse_index.entries() {
            let blk = self.read_data_block(en.block_handle)?;
            let data = blk.data.as_ref();

            // Use shared TlvBlockIterator
            let iterator = TlvBlockIterator::new(data);

            for entry_result in iterator {
                let (key, value_slice, sequence, entry_type, expiration) = entry_result?;

                // Decode key to get user key, sequence, and tombstone flag
                let (user_key, seq, tomb) = if self.use_internal_keys {
                    decode_internal_key_or_raw(&key)
                } else {
                    (key.clone(), sequence, entry_type == 2)
                };

                // Apply range filters
                let in_range = start.is_none_or(|s| user_key.as_slice() >= s)
                    && end.is_none_or(|e| user_key.as_slice() < e);

                if in_range {
                    if tomb {
                        out.push((Bytes::from(user_key.clone()), KeyState::Tombstone(seq)));
                    } else if !is_covered_by_range_tombstone(
                        &self.range_tombstones,
                        &user_key,
                        u64::MAX,
                    ) {
                        if let Some(val) = value_slice {
                            out.push((
                                Bytes::from(user_key.clone()),
                                KeyState::Value(Bytes::copy_from_slice(val), seq, expiration),
                            ));
                        }
                    }
                }
            }
        }

        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// Snapshot-aware scan: include entries with seq <= snapshot and map tombstones.
    fn scan_range_state_at_internal(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        let sparse_index = self
            .sparse_index
            .as_ref()
            .ok_or_else(|| MidgeError::InvalidData("SST file not properly loaded".into()))?;
        let mut out: Vec<(Bytes, KeyState)> = Vec::new();

        for en in sparse_index.entries() {
            let blk = self.read_data_block(en.block_handle)?;
            let data = blk.data.as_ref();

            // Use shared TlvBlockIterator
            let iterator = TlvBlockIterator::new(data);

            for entry_result in iterator {
                let (key, value_slice, sequence, entry_type, _expiration) = entry_result?;

                let (user_key, seq, tomb) = if self.use_internal_keys {
                    decode_internal_key_or_raw(&key)
                } else {
                    (key.clone(), sequence, entry_type == 2)
                };

                // Apply range and snapshot filters
                let in_range = start.is_none_or(|s| user_key.as_slice() >= s)
                    && end.is_none_or(|e| user_key.as_slice() < e);

                // Snapshot isolation: only see writes with seq < snapshot_seq
                if in_range && seq < snapshot_seq {
                    let state = if tomb
                        || is_covered_by_range_tombstone(
                            &self.range_tombstones,
                            &user_key,
                            snapshot_seq,
                        ) {
                        KeyState::Tombstone(seq)
                    } else if let Some(val) = value_slice {
                        KeyState::Value(Bytes::copy_from_slice(val), seq, None)
                    } else {
                        KeyState::Tombstone(seq)
                    };
                    out.push((Bytes::from(user_key), state));
                }
            }
        }

        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    // Note: writer helpers are intentionally omitted at this layer. The writer
    // implementation will be provided separately and adapted to the trait when available.

    fn search_data_block_state(&self, data: &[u8], target_key: &[u8]) -> MidgeResult<KeyState> {
        trace!(
            "search_data_block_state: target={:?} data_len={}",
            String::from_utf8_lossy(target_key),
            data.len()
        );

        if data.len() < 5 {
            trace!("Data too small, returning Absent");
            return Ok(KeyState::Absent);
        }

        // Parse header from the end
        let total_len = data.len();
        let restart_count = u32::from_le_bytes([
            data[total_len - 4],
            data[total_len - 3],
            data[total_len - 2],
            data[total_len - 1],
        ]) as usize;
        let restarts_len = restart_count * 4;
        if total_len < 4 + restarts_len + 1 {
            return Err(MidgeError::InvalidData("Data block too small".into()));
        }
        let version = data[total_len - 4 - restarts_len - 1];
        if version != 3 {
            return Err(MidgeError::InvalidData(format!(
                "Unsupported data block version: {}",
                version
            )));
        }
        let restarts_start = total_len - 4 - restarts_len;
        let entries_end = restarts_start;

        let restart_offset = binary_search_restart_points(
            data,
            restart_count,
            restarts_start,
            entries_end,
            target_key,
            |d, offset, limit| self.parse_key_at_offset(d, offset, limit),
        );

        #[cfg(test)]
        {
            // Dump all keys in block for debugging
            use crate::sst::encoding::TlvBlockIterator;
            let iterator = TlvBlockIterator::new(data);
            eprintln!("Data block entries:");
            for entry_result in iterator {
                let (k, v, seq, t, _exp) = entry_result?;
                eprintln!(
                    "  key={} seq={} tomb={} val_len={}",
                    String::from_utf8_lossy(&k),
                    seq,
                    t,
                    v.as_ref().map(|b| b.len()).unwrap_or(0)
                );
            }
        }

        self.linear_search_data_block_state(data, restart_offset, entries_end, target_key)
    }

    fn search_data_block_state_at(
        &self,
        data: &[u8],
        target_key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<KeyState> {
        let entries_end = match calculate_entries_end(data) {
            Some(end) => end,
            None => return Ok(KeyState::Absent),
        };

        let num_restarts = u32::from_le_bytes([
            data[data.len() - 4],
            data[data.len() - 3],
            data[data.len() - 2],
            data[data.len() - 1],
        ]) as usize;
        let restarts_start = data.len() - 4 - (num_restarts * 4);

        let restart_offset = binary_search_restart_points(
            data,
            num_restarts,
            restarts_start,
            entries_end,
            target_key,
            |d, offset, limit| self.parse_key_at_offset(d, offset, limit),
        );

        self.linear_search_data_block_state_at(
            data,
            restart_offset,
            entries_end,
            target_key,
            snapshot_seq,
        )
    }

    fn linear_search_data_block_state(
        &self,
        data: &[u8],
        start_offset: usize,
        limit: usize,
        target_key: &[u8],
    ) -> MidgeResult<KeyState> {
        self.linear_search_data_block_state_impl(data, start_offset, limit, target_key, None)
    }

    fn linear_search_data_block_state_at(
        &self,
        data: &[u8],
        start_offset: usize,
        limit: usize,
        target_key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<KeyState> {
        self.linear_search_data_block_state_impl(
            data,
            start_offset,
            limit,
            target_key,
            Some(snapshot_seq),
        )
    }

    /// Unified linear search implementation for both snapshot-aware and non-snapshot-aware searches
    fn linear_search_data_block_state_impl(
        &self,
        data: &[u8],
        start_offset: usize,
        limit: usize,
        target_key: &[u8],
        snapshot_seq: Option<u64>,
    ) -> MidgeResult<KeyState> {
        self.log_search_start(target_key, snapshot_seq, start_offset, limit, data.len());

        let mut cursor = start_offset;
        let mut last_key: Vec<u8> = Vec::new();
        let mut entry_count = 0;

        while cursor < limit {
            let entry = match decode(data, cursor, limit) {
                Ok(e) => e,
                Err(_) => break,
            };
            cursor += entry.bytes_consumed;
            entry_count += 1;

            // Reconstruct full key
            let mut raw_key = Vec::with_capacity(entry.shared_len as usize + entry.key_delta.len());
            raw_key.extend_from_slice(&last_key[..entry.shared_len as usize]);
            raw_key.extend_from_slice(entry.key_delta);
            last_key = raw_key.clone();

            let (user_key, seq, tomb) = if self.use_internal_keys {
                if let Some((uk, s, t)) = crate::common::internal_key::decode_internal_key(&raw_key)
                {
                    (uk, s, t)
                } else {
                    (raw_key.clone(), entry.sequence, entry.entry_type == 2)
                }
            } else {
                (raw_key, entry.sequence, entry.entry_type == 2)
            };

            // Check snapshot visibility
            if let Some(snapshot) = snapshot_seq {
                if seq > snapshot {
                    continue; // Skip newer entries
                }
            }

            if user_key.as_slice() == target_key {
                return Ok(if tomb {
                    KeyState::Tombstone(seq)
                } else if let Some(val) = entry.value {
                    KeyState::Value(Bytes::copy_from_slice(val), seq, entry.expiration)
                } else {
                    KeyState::Tombstone(seq)
                });
            }

            if user_key.as_slice() > target_key {
                break; // Past the target key
            }
        }

        trace!(entry_count, "key not found in block");
        Ok(KeyState::Absent)
    }

    /// Log the start of a search operation
    fn log_search_start(
        &self,
        target_key: &[u8],
        snapshot_seq: Option<u64>,
        start_offset: usize,
        limit: usize,
        data_len: usize,
    ) {
        if let Some(seq) = snapshot_seq {
            debug!(
                target_key = %String::from_utf8_lossy(target_key),
                snapshot_seq = seq,
                start_offset,
                limit,
                "linear_search_data_block_state_at"
            );
        } else {
            tracing::debug!(
                "DEBUG SEARCH: Looking for key={} use_internal={}",
                String::from_utf8_lossy(target_key),
                self.use_internal_keys
            );
            tracing::debug!(
                target_key = %String::from_utf8_lossy(target_key),
                start_offset,
                limit,
                data_len,
                use_internal = self.use_internal_keys,
                "linear_search_data_block_state"
            );
        }
    }

    // linear_search_meta_index is implemented earlier in this file; keep that implementation.
}

// Implement the generic SstReader contract for the FS-backed reader
impl crate::sst::SstReader for SstFile {
    fn get(&self, key: &[u8]) -> crate::error::MidgeResult<Option<bytes::Bytes>> {
        Self::get(self, key)
    }

    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> crate::error::MidgeResult<Vec<(bytes::Bytes, bytes::Bytes)>> {
        Self::scan_range(self, start, end)
    }
}

// Implement stateful reader trait for FS-backed SstFile
impl SstStateReader for SstFile {
    fn get_state(&self, key: &[u8]) -> MidgeResult<KeyState> {
        self.get_state_internal(key)
    }

    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        self.scan_range_state_internal(start, end)
    }

    fn get_state_at(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<KeyState> {
        self.get_state_at_internal(key, snapshot_seq)
    }

    fn scan_range_state_at(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        self.scan_range_state_at_internal(start, end, snapshot_seq)
    }
}
