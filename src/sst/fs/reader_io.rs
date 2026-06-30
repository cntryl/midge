//! Filesystem-backed SST reader using `io::Fs` abstraction (new approach)
//!
//! This reader uses the base `io::Fs` trait instead of `std::fs` directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.

use bytes::Bytes;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::sst::bloom::writer::{BloomFilterOps, BloomTestResult};
use crate::sst::bloom::{BlockBloomFilter, BloomMetrics, BloomReader};
use crate::sst::cache::{BlockCache, CacheKey};
use crate::sst::encoding;
use crate::sst::read_amp_metrics::ReadAmpMetrics;
use crate::sst::sparse_index::SparseIndexReader;
use crate::sst::types::{
    decode_range_tombstones, BlockHandle, Footer, KeyState, RangeTombstone, SstEntry, SstMetadata,
    SST_FORMAT_V1,
};

/// Stable summary of the physical contents of a single SST file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFileSummary {
    pub size_bytes: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub smallest_seq: u64,
    pub largest_seq: u64,
}

/// SST file reader using `io::Fs` abstraction
/// Identical to `SstFile` but accepts `Arc<dyn Fs>` for the filesystem backend
pub struct SstFileIo {
    path: FsPath,
    fs: Arc<dyn Fs>,
    footer: Option<Footer>,
    sst_id: u64,
    bloom_reader: Option<BloomReader>,
    block_bloom_filter: Option<BlockBloomFilter>,
    bloom_metrics: BloomMetrics,
    read_amp_metrics: ReadAmpMetrics,
    sparse_index: Option<Arc<SparseIndexReader>>,
    block_cache: Option<Arc<BlockCache>>,
    format_version: u32,
    range_tombstones: Vec<RangeTombstone>,
}

impl SstFileIo {
    /// Create a new SST reader using the provided filesystem
    #[must_use]
    pub fn new(path_str: &str, fs: Arc<dyn Fs>) -> Self {
        Self {
            path: FsPath::new(path_str),
            fs,
            footer: None,
            sst_id: 0,
            bloom_reader: None,
            block_bloom_filter: None,
            bloom_metrics: BloomMetrics::new(),
            read_amp_metrics: ReadAmpMetrics::new(),
            sparse_index: None,
            block_cache: None,
            format_version: SST_FORMAT_V1,
            range_tombstones: Vec::new(),
        }
    }

    /// Open and load metadata from an SST file
    ///
    /// # Errors
    ///
    /// Returns an error when the SST footer, metadata, or backing file cannot be read.
    pub fn open(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        let mut reader = Self::new(path_str, fs);
        reader.load_metadata()?;
        Ok(reader)
    }

    /// Open an SST file using `RealFs` (convenience method for single-file access)
    /// This creates a new `RealFs` instance for the parent directory of the SST file.
    ///
    /// # Errors
    ///
    /// Returns an error when the filesystem cannot be opened or the SST metadata cannot be read.
    pub fn open_with_real_fs(path: &std::path::Path) -> MidgeResult<Self> {
        let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let fs = Arc::new(crate::io::RealFs::new(parent)?);
        // Use filename relative to parent dir so RealFs (rooted at parent) resolves it correctly
        let path_str = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        Self::open(&path_str, fs)
    }

    /// Summarize an SST file opened via `RealFs`.
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be opened or summarized.
    pub fn summarize_with_real_fs(path: &std::path::Path) -> MidgeResult<SstFileSummary> {
        Self::open_with_real_fs(path)?.summary()
    }

    /// Enable bloom filter for this reader
    #[must_use]
    pub fn with_bloom(mut self, bloom: BloomReader) -> Self {
        self.bloom_reader = Some(bloom);
        self
    }

    /// Enable block bloom filter for this reader
    #[must_use]
    pub fn with_block_bloom(mut self, block_bloom: BlockBloomFilter) -> Self {
        self.block_bloom_filter = Some(block_bloom);
        self
    }

    /// Load block bloom filter from footer (if present)
    ///
    /// # Errors
    ///
    /// Returns an error when the block bloom handle cannot be read or decoded.
    pub fn load_block_bloom(&mut self) -> MidgeResult<()> {
        if let Some(ref footer) = self.footer {
            if let Some(block_bloom_handle) = footer.block_bloom_handle {
                let bloom_data = self.read_block(&block_bloom_handle)?;
                let block_bloom = BlockBloomFilter::deserialize(&bloom_data)?;
                self.block_bloom_filter = Some(block_bloom);
            }
        }
        Ok(())
    }

    /// Enable sparse index for this reader
    #[must_use]
    pub fn with_sparse_index(mut self, index: SparseIndexReader) -> Self {
        self.sparse_index = Some(Arc::new(index));
        self
    }

    /// Enable block cache for this reader
    #[must_use]
    pub fn with_block_cache(mut self, cache: Arc<BlockCache>) -> Self {
        self.block_cache = Some(cache);
        self
    }

    /// Set the SST ID for cache key generation
    #[must_use]
    pub fn with_sst_id(mut self, id: u64) -> Self {
        self.sst_id = id;
        self
    }

    /// Get reference to bloom metrics for this reader
    pub fn bloom_metrics(&self) -> &BloomMetrics {
        &self.bloom_metrics
    }

    /// Get reference to read amplification metrics for this reader
    pub fn read_amp_metrics(&self) -> &ReadAmpMetrics {
        &self.read_amp_metrics
    }

    /// Derive key-range and sequence metadata from the actual SST contents.
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be scanned or has no publishable entries.
    pub fn summary(&self) -> MidgeResult<SstFileSummary> {
        use crate::sst::traits::SstStateReader;

        let size_bytes = self.fs.metadata(&self.path)?.len;
        let entries = self.scan_range_state(None, None)?;

        let mut smallest_key: Option<Vec<u8>> = None;
        let mut largest_key: Option<Vec<u8>> = None;
        let mut smallest_seq: Option<u64> = None;
        let mut largest_seq: Option<u64> = None;

        for (key, state) in entries {
            let key_vec = key.to_vec();
            if smallest_key
                .as_ref()
                .is_none_or(|current| key_vec.as_slice() < current.as_slice())
            {
                smallest_key = Some(key_vec.clone());
            }
            if largest_key
                .as_ref()
                .is_none_or(|current| key_vec.as_slice() > current.as_slice())
            {
                largest_key = Some(key_vec);
            }

            let seq = match state {
                KeyState::Value(_, seq, _, _) | KeyState::Tombstone(seq) => seq,
                KeyState::Absent => continue,
            };
            smallest_seq = Some(smallest_seq.map_or(seq, |current| current.min(seq)));
            largest_seq = Some(largest_seq.map_or(seq, |current| current.max(seq)));
        }

        for tombstone in self.range_tombstones() {
            if smallest_key
                .as_ref()
                .is_none_or(|current| tombstone.start.as_slice() < current.as_slice())
            {
                smallest_key = Some(tombstone.start.clone());
            }
            if largest_key
                .as_ref()
                .is_none_or(|current| tombstone.end.as_slice() > current.as_slice())
            {
                largest_key = Some(tombstone.end.clone());
            }
            smallest_seq =
                Some(smallest_seq.map_or(tombstone.seq, |current| current.min(tombstone.seq)));
            largest_seq =
                Some(largest_seq.map_or(tombstone.seq, |current| current.max(tombstone.seq)));
        }

        Ok(SstFileSummary {
            size_bytes,
            smallest_key: smallest_key.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' contains no publishable entries",
                    self.path.0.as_str()
                ))
            })?,
            largest_key: largest_key.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' contains no publishable entries",
                    self.path.0.as_str()
                ))
            })?,
            smallest_seq: smallest_seq.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' contains no publishable sequence bounds",
                    self.path.0.as_str()
                ))
            })?,
            largest_seq: largest_seq.ok_or_else(|| {
                MidgeError::Corruption(format!(
                    "SST '{}' contains no publishable sequence bounds",
                    self.path.0.as_str()
                ))
            })?,
        })
    }

    fn load_metadata(&mut self) -> MidgeResult<()> {
        // Open file in read-only mode
        // Get file size
        let metadata = self.fs.metadata(&self.path)?;
        let file_size = metadata.len;

        if file_size < 48 {
            return Err(MidgeError::Corruption("SST file too small".into()));
        }

        // Determine footer size (72 bytes new, fall back to 56 or 48)
        let footer_size = if file_size >= 72 {
            72u64
        } else if file_size >= 56 {
            56u64
        } else {
            48u64
        };

        // Read footer from end of file
        let footer_offset = file_size - footer_size;
        let footer_data = {
            let file = self.fs.open(
                &self.path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )?;
            file.read_at(footer_offset, footer_size)?
        };

        self.footer = Some(Footer::decode(&footer_data)?);
        self.load_sst_metadata()?;

        Ok(())
    }

    fn load_sst_metadata(&mut self) -> MidgeResult<()> {
        let Some(footer) = self.footer.clone() else {
            return Ok(());
        };

        if footer.meta_index_handle.size == 0 {
            self.format_version = SST_FORMAT_V1;
            self.range_tombstones.clear();
            return Ok(());
        }

        let metadata_bytes = self.read_block(&footer.meta_index_handle)?;
        if metadata_bytes.is_empty() {
            self.format_version = SST_FORMAT_V1;
            self.range_tombstones.clear();
            return Ok(());
        }

        let metadata = SstMetadata::decode(&metadata_bytes)?;
        self.format_version = metadata.format_version;
        self.range_tombstones = match metadata.range_tombstone_handle {
            Some(handle) if handle.size > 0 => {
                let tombstone_bytes = self.read_block(&handle)?;
                decode_range_tombstones(&tombstone_bytes)?
            }
            _ => Vec::new(),
        };

        Ok(())
    }

    fn read_block(&self, handle: &BlockHandle) -> MidgeResult<bytes::Bytes> {
        let file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

        // Read block with size prefix
        let buffer = file.read_at(handle.offset, handle.size)?;

        // First 4 bytes are length prefix
        if buffer.len() < 4 {
            return Err(MidgeError::Corruption("Block too short".into()));
        }

        let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        if len + 4 > buffer.len() {
            return Err(MidgeError::Corruption("Block data truncated".into()));
        }

        let raw = &buffer[4..4 + len];
        Ok(Self::decompress_raw_block(raw))
    }

    /// Decompress a raw block payload, stripping the block trailer if present.
    ///
    /// Blocks with a valid trailer (`[data][algo:u8][crc32c:u32]`) are verified
    /// and decompressed.  Legacy blocks without a trailer (pre-v1.0.0) are
    /// returned as-is for backward compatibility.
    fn decompress_raw_block(raw: &[u8]) -> bytes::Bytes {
        use crate::sst::compression;

        // A block must be at least BLOCK_TRAILER_SIZE bytes to contain a
        // trailer.  Shorter payloads are legacy / uncompressed.
        if raw.len() < compression::BLOCK_TRAILER_SIZE {
            return bytes::Bytes::copy_from_slice(raw);
        }

        // Attempt trailer-based decompression.  If the CRC check fails this
        // may be a legacy block without a trailer so fall back to raw bytes.
        match compression::decompress_block_with_trailer(raw) {
            Ok(decompressed) => decompressed,
            Err(_) => bytes::Bytes::copy_from_slice(raw),
        }
    }

    /// Readahead window size: read up to this many blocks in a single IO operation
    /// for cold-cache range scans. Tuned for typical SSD latency/throughput tradeoffs.
    const READAHEAD_WINDOW_BLOCKS: usize = 32;

    /// Read multiple contiguous blocks in a single IO operation for cold-cache scans.
    ///
    /// This is the core optimization for range scan readahead:
    /// - Reads from `handles[0].offset` to `handles[last].offset + handles[last].size`
    /// - Slices the buffer to extract individual block data
    /// - Preserves existing error handling and alignment rules
    ///
    /// Returns a Vec of decoded block data (Bytes), one per handle.
    fn read_blocks_contiguous(&self, handles: &[BlockHandle]) -> MidgeResult<Vec<bytes::Bytes>> {
        if handles.is_empty() {
            return Ok(Vec::new());
        }

        // Single block: use existing path
        if handles.len() == 1 {
            let block_data = self.read_block(&handles[0])?;
            return Ok(vec![block_data]);
        }

        // Compute contiguous read range
        let first = &handles[0];
        let last = &handles[handles.len() - 1];
        let read_start = first.offset;
        let read_end = last.offset + last.size;
        let total_len = read_end - read_start;

        // Open file once for the entire window
        let file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

        // Single contiguous read for all blocks in window
        let buffer = file.read_at(read_start, total_len)?;

        // Extract individual blocks from the buffer
        let mut result = Vec::with_capacity(handles.len());
        for handle in handles {
            // Compute offset within the buffer
            let buf_offset = usize::try_from(handle.offset - read_start).map_err(|_| {
                MidgeError::Corruption("Block offset exceeds addressable memory".into())
            })?;
            let handle_size = usize::try_from(handle.size).map_err(|_| {
                MidgeError::Corruption("Block size exceeds addressable memory".into())
            })?;
            let buf_end = buf_offset + handle_size;

            if buf_end > buffer.len() {
                return Err(MidgeError::Corruption(
                    "Block extends past read buffer".into(),
                ));
            }

            let block_slice = &buffer[buf_offset..buf_end];

            // Parse length prefix (same logic as read_block)
            if block_slice.len() < 4 {
                return Err(MidgeError::Corruption("Block too short".into()));
            }

            let len = u32::from_le_bytes([
                block_slice[0],
                block_slice[1],
                block_slice[2],
                block_slice[3],
            ]) as usize;

            if len + 4 > block_slice.len() {
                return Err(MidgeError::Corruption("Block data truncated".into()));
            }

            let raw = &block_slice[4..4 + len];
            result.push(Self::decompress_raw_block(raw));
        }

        Ok(result)
    }

    fn scan_index(&self) -> MidgeResult<Vec<(Vec<u8>, BlockHandle)>> {
        let footer = self
            .footer
            .as_ref()
            .ok_or_else(|| MidgeError::Corruption("No footer".into()))?;

        let index_data = self.read_block(&footer.index_handle)?;
        let mut result = Vec::new();
        let mut offset = 0;

        while offset < index_data.len() {
            if offset + 20 > index_data.len() {
                break;
            }

            let key_len = u32::from_le_bytes([
                index_data[offset],
                index_data[offset + 1],
                index_data[offset + 2],
                index_data[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_len > index_data.len() {
                break;
            }

            let key = index_data[offset..offset + key_len].to_vec();
            offset += key_len;

            if offset + 16 > index_data.len() {
                break;
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

            result.push((key, BlockHandle::new(block_offset, block_size)));
        }

        Ok(result)
    }

    fn scan_block_entries_from_bytes(
        &self,
        block_data: &bytes::Bytes,
    ) -> MidgeResult<Vec<SstEntry>> {
        let mut result = Vec::new();
        let mut offset = 0;
        let mut previous_key = Vec::new();

        while offset < block_data.len() {
            let (entry, next_offset) =
                encoding::decode_with_format(block_data.as_ref(), offset, self.format_version)?;

            let shared_len = entry.shared_len as usize;
            if shared_len > previous_key.len() {
                return Err(MidgeError::Corruption(
                    "Invalid shared prefix length in SST entry".into(),
                ));
            }

            let mut full_key = Vec::with_capacity(shared_len + entry.key_delta.len());
            full_key.extend_from_slice(&previous_key[..shared_len]);
            full_key.extend_from_slice(entry.key_delta);

            let value_bytes = if let Some(val_off) = entry.value_offset {
                let val_len = match entry.value {
                    Some(v) => v.len(),
                    None => {
                        return Err(MidgeError::Corruption(
                            "value offset present without value".into(),
                        ))
                    }
                };
                Some(block_data.slice(val_off..val_off + val_len))
            } else {
                None
            };

            previous_key = full_key.clone();
            result.push(SstEntry::new(
                full_key,
                value_bytes,
                entry.sequence,
                entry.entry_type as u8,
                entry.expiration,
            ));
            offset = next_offset;
        }

        Ok(result)
    }

    fn scan_block_from_bytes(
        &self,
        block_data: &bytes::Bytes,
    ) -> MidgeResult<Vec<(bytes::Bytes, Option<bytes::Bytes>)>> {
        Ok(self
            .scan_block_entries_from_bytes(block_data)?
            .into_iter()
            .map(|entry| (Bytes::from(entry.key), entry.value))
            .collect())
    }

    fn state_from_entry(entry: SstEntry) -> KeyState {
        if entry.is_tombstone() {
            KeyState::Tombstone(entry.sequence)
        } else if let Some(value) = entry.value {
            KeyState::Value(value, entry.sequence, entry.expiration, entry.op_type)
        } else {
            KeyState::Absent
        }
    }

    /// Check block bloom filter with proper metrics and failure-safe semantics
    fn check_block_bloom(&self, block_idx: usize, key: &[u8]) -> bool {
        if let Some(ref block_bloom) = self.block_bloom_filter {
            self.bloom_metrics.record_check();

            match block_bloom.might_contain_in_block(block_idx, key) {
                BloomTestResult::DefinitelyNotPresent => {
                    self.bloom_metrics.record_negative();
                    self.bloom_metrics.record_block_skipped();
                    false
                }
                BloomTestResult::MightBePresent => true,
            }
        } else {
            // No bloom filter - default to MAYBE (safe)
            true
        }
    }
}

impl crate::sst::SstReader for SstFileIo {
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        let mut blocks_read = 0u64;

        // Step 1: Check SST-level bloom filter (negative lookup)
        if let Some(ref bloom) = self.bloom_reader {
            match bloom.contains(key) {
                BloomTestResult::DefinitelyNotPresent => {
                    self.read_amp_metrics.record_read(1, 0, 0);
                    return Ok(None);
                }
                BloomTestResult::MightBePresent => {
                    // Continue to index lookup
                }
            }
        }

        let index = self.scan_index()?;
        blocks_read += 1; // Index block read

        // Step 2: Use sparse index or binary search
        let block_handle = if let Some(ref sparse_idx) = self.sparse_index {
            let block_range = sparse_idx.find_block_range(key);

            let mut found_handle = None;
            for (idx, (first_key, handle)) in index.iter().enumerate() {
                if idx < block_range.start_block {
                    continue;
                }
                if idx > block_range.end_block {
                    break;
                }

                if key <= first_key.as_slice() || idx == block_range.end_block {
                    if !self.check_block_bloom(idx, key) {
                        self.read_amp_metrics.record_read(1, 0, blocks_read);
                        return Ok(None);
                    }
                    found_handle = Some(*handle);
                    break;
                }
            }

            found_handle
        } else {
            let mut found_handle = None;
            for (idx, (first_key, handle)) in index.iter().enumerate() {
                if key <= first_key.as_slice() || idx == index.len() - 1 {
                    if !self.check_block_bloom(idx, key) {
                        self.read_amp_metrics.record_read(1, 0, blocks_read);
                        return Ok(None);
                    }
                    found_handle = Some(*handle);
                    break;
                }
            }

            found_handle
        };

        if let Some(handle) = block_handle {
            blocks_read += 1;
            let cache_key = CacheKey::for_data(self.sst_id, handle.offset);

            let block_data = if let Some(ref cache) = self.block_cache {
                if let Some(cached_value) = cache.get(&cache_key) {
                    cached_value.data.as_ref().clone()
                } else {
                    let bytes = self.read_block(&handle)?;
                    cache.put(cache_key, &bytes);
                    bytes
                }
            } else {
                self.read_block(&handle)?
            };

            let entries = self.scan_block_from_bytes(&block_data)?;
            for (entry_key, value) in entries {
                if entry_key.as_ref() == key {
                    self.read_amp_metrics.record_read(1, 0, blocks_read);
                    return Ok(value);
                }
            }
        }

        self.read_amp_metrics.record_read(1, 0, blocks_read);
        Ok(None)
    }

    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let index = self.scan_index()?;
        let mut result = Vec::new();

        // Collect just the block handles for readahead
        let handles: Vec<BlockHandle> = index.iter().map(|(_, h)| *h).collect();

        // Process blocks in readahead windows for cold-cache efficiency
        for window_start in (0..handles.len()).step_by(Self::READAHEAD_WINDOW_BLOCKS) {
            let window_end = (window_start + Self::READAHEAD_WINDOW_BLOCKS).min(handles.len());
            let window_handles = &handles[window_start..window_end];

            // Single contiguous IO for entire window
            let block_data_vec = self.read_blocks_contiguous(window_handles)?;

            // Process each block's data
            for block_data in block_data_vec {
                let entries = self.scan_block_from_bytes(&block_data)?;
                for (key, value) in entries {
                    if let Some(s) = start {
                        if key.as_ref() < s {
                            continue;
                        }
                    }
                    if let Some(e) = end {
                        if key.as_ref() >= e {
                            continue;
                        }
                    }

                    if let Some(val) = value {
                        result.push((key.clone(), val));
                    }
                }
            }
        }

        Ok(result)
    }
}

impl crate::sst::SstStateReader for SstFileIo {
    fn get_state(&self, key: &[u8]) -> MidgeResult<crate::sst::types::KeyState> {
        let mut best_match: Option<SstEntry> = None;
        let index = self.scan_index()?;

        for (_first_key, handle) in &index {
            let block_data = self.read_block(handle)?;
            for entry in self.scan_block_entries_from_bytes(&block_data)? {
                if entry.key.as_slice() == key
                    && best_match
                        .as_ref()
                        .is_none_or(|current| entry.sequence > current.sequence)
                {
                    best_match = Some(entry);
                }
            }
        }

        Ok(best_match.map_or(crate::sst::types::KeyState::Absent, Self::state_from_entry))
    }

    fn get_state_at(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<crate::sst::types::KeyState> {
        let mut best_match: Option<SstEntry> = None;
        let index = self.scan_index()?;

        for (_first_key, handle) in &index {
            let block_data = self.read_block(handle)?;
            for entry in self.scan_block_entries_from_bytes(&block_data)? {
                if entry.key.as_slice() != key {
                    continue;
                }
                if snapshot_seq != u64::MAX && entry.sequence > snapshot_seq {
                    continue;
                }
                if best_match
                    .as_ref()
                    .is_none_or(|current| entry.sequence > current.sequence)
                {
                    best_match = Some(entry);
                }
            }
        }

        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

        Ok(match best_match {
            Some(entry) if entry.is_tombstone() => KeyState::Tombstone(entry.sequence),
            Some(entry) if entry.is_expired(now_millis) => KeyState::Tombstone(entry.sequence),
            Some(entry) => Self::state_from_entry(entry),
            None => KeyState::Absent,
        })
    }

    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, crate::sst::types::KeyState)>> {
        let index = self.scan_index()?;
        let mut result = Vec::new();

        for (_first_key, handle) in index {
            let block_data = self.read_block(&handle)?;
            for entry in self.scan_block_entries_from_bytes(&block_data)? {
                if let Some(s) = start {
                    if entry.key.as_slice() < s {
                        continue;
                    }
                }
                if let Some(e) = end {
                    if entry.key.as_slice() >= e {
                        continue;
                    }
                }

                let key = Bytes::from(entry.key.clone());
                result.push((key, Self::state_from_entry(entry)));
            }
        }

        Ok(result)
    }

    fn range_tombstones(&self) -> Vec<crate::sst::types::RangeTombstone> {
        self.range_tombstones.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_new_reader_with_io_fs() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());

        // Act
        let reader = SstFileIo::new("test.sst", fs);

        // Assert
        assert!(reader.footer.is_none());
    }

    #[test]
    fn should_have_proper_type_safety() {
        // Arrange
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());

        // Act
        let reader = SstFileIo::new("test.sst", fs);

        // Assert
        assert!(reader.footer.is_none());
    }

    #[test]
    fn should_chain_with_sst_id() {
        // Arrange
        let fs = Arc::new(crate::io::MockFs::new());
        let reader = SstFileIo::new("test.sst", fs);

        // Act
        let with_id = reader.with_sst_id(42);

        // Assert
        assert_eq!(with_id.sst_id, 42);
    }
}
