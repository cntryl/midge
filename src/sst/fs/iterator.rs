use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::error::MidgeResult;
use crate::sst::block_meta::BlockMeta;

use super::utils::decode_data_block;

/// Range iterator over SST file blocks with cached file handle.
///
/// The file handle is opened once on first block read and reused for all
/// subsequent reads, avoiding the overhead of repeated file open/close.
///
/// # Phase 2.5: Fence-Pointer Range Skipping
///
/// Blocks are efficiently skipped using fence pointers (min_key, max_key):
/// - If `block.max_key < range_start`, skip (block entirely before range)
/// - If `block.min_key >= range_end`, stop (block entirely after range, and all following)
/// - Otherwise, read block and filter entries by key range
///
/// This can skip 50-90% of blocks for typical window scans on streaming data.
pub struct SstRangeIter {
    path: PathBuf,
    blocks: Vec<BlockMeta>,
    blk_idx: usize,
    data: Option<Vec<u8>>,
    cursor: usize,
    entries_end: usize,
    last_key: Vec<u8>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    use_internal_keys: bool,
    /// Cached file handle for efficient sequential block reads
    cached_file: Option<File>,
    /// Metric: number of blocks skipped via fence pointers
    skipped_blocks: u64,
    /// Metric: total blocks examined
    examined_blocks: u64,
    /// Cached key hash for bloom filter probes (Phase 1.5 optimization)
    cached_key_hash: Option<u64>,
    /// Last successful block index for sequential resume (Phase 2.5 optimization)
    last_hit_block_idx: Option<usize>,
}

impl SstRangeIter {
    pub(super) fn new(
        path: PathBuf,
        blocks: Vec<BlockMeta>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        use_internal_keys: bool,
    ) -> Self {
        Self {
            path,
            blocks,
            blk_idx: 0,
            data: None,
            cursor: 0,
            entries_end: 0,
            last_key: Vec::new(),
            start,
            end,
            use_internal_keys,
            cached_file: None,
            skipped_blocks: 0,
            examined_blocks: 0,
            cached_key_hash: None,
            last_hit_block_idx: None,
        }
    }

    /// Get or create the cached file handle
    fn get_or_open_file(&mut self) -> MidgeResult<&mut File> {
        if self.cached_file.is_none() {
            let file = OpenOptions::new().read(true).open(&self.path)?;
            self.cached_file = Some(file);
        }
        // SAFETY: We just ensured cached_file is Some
        self.cached_file
            .as_mut()
            .ok_or_else(|| crate::error::MidgeError::InvalidData("file handle missing".into()))
    }

    fn load_next_block(&mut self) -> MidgeResult<bool> {
        // Skip blocks that don't intersect with the requested range (fence-pointer optimization)
        while self.blk_idx < self.blocks.len() {
            let meta = &self.blocks[self.blk_idx];
            self.examined_blocks += 1;

            // Check if block can be skipped based on fence pointers (min_key, max_key)
            let should_skip = match (&self.start, &self.end) {
                (Some(s), Some(e)) => {
                    // Block is entirely before range start or after range end
                    meta.max_key.as_ref() < s.as_slice() || meta.min_key.as_ref() >= e.as_slice()
                }
                (Some(s), None) => {
                    // Block is entirely before range start
                    meta.max_key.as_ref() < s.as_slice()
                }
                (None, Some(e)) => {
                    // Block is entirely after range end
                    meta.min_key.as_ref() >= e.as_slice()
                }
                (None, None) => false,
            };

            if should_skip {
                self.skipped_blocks += 1;
                self.blk_idx += 1;
                continue;
            }

            break;
        }

        if self.blk_idx >= self.blocks.len() {
            return Ok(false);
        }

        // Record last hit block for sequential resume optimization
        self.last_hit_block_idx = Some(self.blk_idx);

        let handle = self.blocks[self.blk_idx].handle;
        self.blk_idx += 1;

        // Use cached file handle instead of opening a new file each time
        let file = self.get_or_open_file()?;
        file.seek(SeekFrom::Start(handle.offset))?;
        let mut block_data = vec![0u8; handle.size as usize];
        file.read_exact(&mut block_data)?;

        let block = decode_data_block(&block_data)?;
        let data: Vec<u8> = block.data.to_vec();
        let len = data.len();
        if len < 8 {
            self.data = None;
            return Ok(true);
        }
        let num_restarts =
            u32::from_le_bytes([data[len - 4], data[len - 3], data[len - 2], data[len - 1]])
                as usize;
        let restarts_start = len - 4 - (num_restarts * 4);
        self.entries_end = restarts_start;
        self.cursor = 0;
        self.last_key.clear();
        self.data = Some(data);
        Ok(true)
    }

    fn parse_next_entry(&mut self) -> Option<(Bytes, Bytes)> {
        use crate::common::tlv::{tags, TlvReader};

        loop {
            // Load next block if needed
            if self.data.is_none() {
                if !(self.load_next_block().ok()?) {
                    return None;
                }
                if self.data.is_none() {
                    continue;
                }
            }

            // Clone the data to avoid borrow checker issues
            // SAFETY: checked is_none() above, so data is Some
            let data_clone = self.data.as_ref().expect("data checked above").clone();
            if self.cursor >= self.entries_end {
                self.data = None;
                continue;
            }

            // Use TLV reader to parse the entry
            let mut reader = TlvReader::new(&data_clone[self.cursor..self.entries_end]);
            let mut shared_len: Option<u32> = None;
            let mut key_delta_vec: Option<Vec<u8>> = None;
            let mut value_vec: Option<Vec<u8>> = None;
            let mut entry_type = 0u8;
            let mut entry_complete = false;
            let mut bytes_consumed = 0;
            let mut failed = false;

            while let Some((tag, tag_data)) = reader.next() {
                bytes_consumed = reader.position();
                match tag {
                    tags::SHARED_PREFIX_LEN => {
                        shared_len = Some(
                            match crate::common::tlv::parse_varint32_from_slice(tag_data) {
                                Ok(v) => v,
                                Err(_) => {
                                    failed = true;
                                    break;
                                }
                            },
                        );
                    }
                    tags::KEY_DELTA => {
                        key_delta_vec = Some(tag_data.to_vec());
                        if self.use_internal_keys {
                            entry_complete = true;
                            break;
                        }
                    }
                    tags::VALUE => {
                        value_vec = Some(tag_data.to_vec());
                    }
                    tags::ENTRY_TYPE if tag_data.len() == 1 => {
                        entry_type = tag_data[0];
                        if !self.use_internal_keys {
                            entry_complete = true;
                            break;
                        }
                    }
                    _ => {}
                }
            }

            if failed || !entry_complete || shared_len.is_none() || key_delta_vec.is_none() {
                self.data = None;
                continue;
            }

            // Reconstruct the key
            // SAFETY: checked is_none() above, so both are Some
            let sl = shared_len.expect("shared_len checked above");
            let kd = key_delta_vec.expect("key_delta_vec checked above");
            let mut key = self.last_key.clone();
            key.truncate(sl as usize);
            key.extend_from_slice(&kd);

            // Handle internal-on-disk format vs non-internal format
            let tombstone = if self.use_internal_keys {
                if let Some((_u, _seq, t)) = crate::common::internal_key::decode_internal_key(&key)
                {
                    t
                } else {
                    false
                }
            } else {
                entry_type == 2
            };

            // Apply range filters
            if let Some(s) = &self.start {
                if key.as_slice() < s.as_slice() {
                    // Update last_key and cursor before skipping
                    self.last_key = key;
                    self.cursor += bytes_consumed;
                    continue;
                }
            }
            if let Some(e) = &self.end {
                if key.as_slice() >= e.as_slice() {
                    return None;
                }
            }

            // Skip tombstones
            if tombstone {
                // Update last_key and cursor before skipping
                self.last_key = key;
                self.cursor += bytes_consumed;
                continue;
            }

            // Update cursor (last_key will be updated below)
            self.cursor += bytes_consumed;

            // Return the entry
            let val = if let Some(v) = value_vec {
                Bytes::from(v)
            } else {
                Bytes::new()
            };

            // Save key for last_key, then move into Bytes
            self.last_key = key.clone();
            return Some((Bytes::from(key), val));
        }
    }

    /// Get the number of blocks skipped via fence-pointer optimization (Phase 2.5)
    pub fn skipped_blocks(&self) -> u64 {
        self.skipped_blocks
    }

    /// Get the total number of blocks examined (skipped or read)
    pub fn examined_blocks(&self) -> u64 {
        self.examined_blocks
    }

    /// Get the block skip ratio (skipped / examined)
    pub fn block_skip_ratio(&self) -> f64 {
        if self.examined_blocks == 0 {
            0.0
        } else {
            self.skipped_blocks as f64 / self.examined_blocks as f64
        }
    }

    /// Compute and cache hash for key (Phase 1.5 iterator hash precompute)
    #[inline]
    pub fn compute_key_hash(&mut self, key: &[u8]) -> u64 {
        let hash = Self::hash_key(key);
        self.cached_key_hash = Some(hash);
        hash
    }

    /// Get cached key hash if present
    #[inline]
    pub fn cached_hash(&self) -> Option<u64> {
        self.cached_key_hash
    }

    /// Simple hash function for key (FNV-1a)
    #[inline]
    fn hash_key(key: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
        for &byte in key {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    /// Resume from last successful block index for sequential ranges (Phase 2.5 optimization)
    pub fn try_resume_from_last(&mut self) {
        if let Some(last_idx) = self.last_hit_block_idx {
            if last_idx + 1 < self.blocks.len() {
                self.blk_idx = last_idx + 1;
            }
        }
    }
}

impl Iterator for SstRangeIter {
    type Item = (Bytes, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        self.parse_next_entry()
    }
}
