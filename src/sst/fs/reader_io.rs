//! Filesystem-backed SST reader using `io::Fs` abstraction (new approach)
//!
//! This reader uses the base `io::Fs` trait instead of `std::fs` directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.

use bytes::Bytes;
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::sst::bloom::writer::{BloomFilterOps, BloomTestResult};
use crate::sst::bloom::{BlockBloomFilter, BloomMetrics, BloomReader};
use crate::sst::cache::{BlockCache, CacheKey};
use crate::sst::encoding;
use crate::sst::index::tuner::IndexKind;
use crate::sst::read_amp_metrics::ReadAmpMetrics;
use crate::sst::sparse_index::SparseIndexReader;
use crate::sst::trie::TrieReader;
use crate::sst::types::{
    decode_range_tombstones, BlockHandle, Footer, KeyState, RangeTombstone, SstEntry, SstMetadata,
    SST_FORMAT_V1,
};

type IndexEntries = Arc<Vec<(Vec<u8>, BlockHandle)>>;

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
    trie_reader: Option<Arc<TrieReader>>,
    block_cache: Option<Arc<BlockCache>>,
    index_entries: Mutex<Option<IndexEntries>>,
    format_version: u32,
    index_kind: IndexKind,
    smallest_key: Option<Vec<u8>>,
    largest_key: Option<Vec<u8>>,
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
            trie_reader: None,
            block_cache: None,
            index_entries: Mutex::new(None),
            format_version: SST_FORMAT_V1,
            index_kind: IndexKind::Sparse,
            smallest_key: None,
            largest_key: None,
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

    pub(crate) fn sst_id(&self) -> u64 {
        self.sst_id
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
            self.index_kind = IndexKind::Sparse;
            self.smallest_key = None;
            self.largest_key = None;
            self.trie_reader = None;
            self.range_tombstones.clear();
            return Ok(());
        }

        let metadata_bytes = self.read_block(&footer.meta_index_handle)?;
        if metadata_bytes.is_empty() {
            self.format_version = SST_FORMAT_V1;
            self.index_kind = IndexKind::Sparse;
            self.smallest_key = None;
            self.largest_key = None;
            self.trie_reader = None;
            self.range_tombstones.clear();
            return Ok(());
        }

        let metadata = SstMetadata::decode(&metadata_bytes)?;
        self.format_version = metadata.format_version;
        self.index_kind = metadata.index_kind;
        self.smallest_key = metadata
            .key_range
            .as_ref()
            .map(|range| range.smallest_key.clone());
        self.largest_key = metadata
            .key_range
            .as_ref()
            .map(|range| range.largest_key.clone());
        self.range_tombstones = match metadata.range_tombstone_handle {
            Some(handle) if handle.size > 0 => {
                let tombstone_bytes = self.read_block(&handle)?;
                decode_range_tombstones(&tombstone_bytes)?
            }
            _ => Vec::new(),
        };
        self.trie_reader = match (self.index_kind, footer.trie_handle) {
            (IndexKind::Trie, Some(handle)) => {
                let trie_bytes = self.read_block(&handle)?;
                Some(Arc::new(TrieReader::new(&trie_bytes)?))
            }
            (IndexKind::Trie, None) => {
                return Err(MidgeError::Corruption(
                    "Trie-selected SST metadata is missing trie footer handle".into(),
                ));
            }
            (IndexKind::Sparse, Some(_handle)) => {
                return Err(MidgeError::Corruption(
                    "Sparse-selected SST metadata should not carry trie footer handle".into(),
                ));
            }
            (IndexKind::Sparse, None) => None,
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
        Self::decompress_raw_block(raw, self.uses_block_trailers())
    }

    /// Decompress a raw block payload, stripping the block trailer if present.
    ///
    /// Blocks with a valid trailer (`[data][algo:u8][crc32c:u32]`) are verified
    /// and decompressed.  Legacy blocks without a trailer (pre-v1.0.0) are
    /// returned as-is for backward compatibility.
    fn uses_block_trailers(&self) -> bool {
        self.footer
            .as_ref()
            .is_some_and(|footer| footer.meta_index_handle.size > 0)
    }

    fn decompress_raw_block(raw: &[u8], strict_trailer: bool) -> MidgeResult<bytes::Bytes> {
        use crate::sst::compression;

        // A block must be at least BLOCK_TRAILER_SIZE bytes to contain a
        // trailer.  Shorter payloads are legacy / uncompressed.
        if raw.len() < compression::BLOCK_TRAILER_SIZE {
            if strict_trailer {
                return Err(MidgeError::Corruption(
                    "current-format SST block too short for trailer".into(),
                ));
            }
            return Ok(bytes::Bytes::copy_from_slice(raw));
        }

        match compression::decompress_block_with_trailer(raw) {
            Ok(decompressed) => Ok(decompressed),
            Err(error) if strict_trailer => Err(error),
            Err(_) => Ok(bytes::Bytes::copy_from_slice(raw)),
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
        let strict_trailer = self.uses_block_trailers();
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
            result.push(Self::decompress_raw_block(raw, strict_trailer)?);
        }

        Ok(result)
    }

    fn parse_index_entries(&self) -> MidgeResult<Vec<(Vec<u8>, BlockHandle)>> {
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

    fn index_entries(&self) -> MidgeResult<IndexEntries> {
        if let Some(cached) = self
            .index_entries
            .lock()
            .map_err(|_| MidgeError::Internal("SST index cache lock poisoned".into()))?
            .as_ref()
            .cloned()
        {
            return Ok(cached);
        }

        let parsed = Arc::new(self.parse_index_entries()?);
        *self
            .index_entries
            .lock()
            .map_err(|_| MidgeError::Internal("SST index cache lock poisoned".into()))? =
            Some(Arc::clone(&parsed));
        Ok(parsed)
    }

    fn key_outside_persisted_range(&self, key: &[u8]) -> bool {
        self.smallest_key
            .as_ref()
            .zip(self.largest_key.as_ref())
            .is_some_and(|(smallest_key, largest_key)| {
                key < smallest_key.as_slice() || key > largest_key.as_slice()
            })
    }

    fn range_outside_persisted_bounds(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> bool {
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
            .map_or(last_index, |block_index| block_index.min(last_index));
        Some((block_index.saturating_sub(1), block_index))
    }

    fn search_bounds(&self, last_index: usize, key: &[u8]) -> Option<(usize, usize)> {
        if self.key_outside_persisted_range(key) {
            return None;
        }

        if let Some(bounds) = self.trie_search_bounds(last_index, key) {
            return Some(bounds);
        }

        if let Some(ref sparse_idx) = self.sparse_index {
            let block_range = sparse_idx.find_block_range(key);
            return Some((
                block_range.start_block.min(last_index),
                block_range.end_block.min(last_index),
            ));
        }

        Some((0, last_index))
    }

    fn candidate_block_indices(
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

        while first_candidate > start_bound && index[first_candidate - 1].0.as_slice() == key {
            first_candidate -= 1;
        }

        if index[first_candidate].0.as_slice() == key && first_candidate > start_bound {
            first_candidate -= 1;
        }

        Some(first_candidate..=last_candidate)
    }

    fn candidate_data_blocks(
        &self,
        index: &[(Vec<u8>, BlockHandle)],
        key: &[u8],
    ) -> Vec<(usize, BlockHandle)> {
        self.candidate_block_indices(index, key)
            .map_or_else(Vec::new, |range| {
                range.map(|idx| (idx, index[idx].1)).collect()
            })
    }

    fn read_cached_data_block(&self, handle: &BlockHandle) -> MidgeResult<bytes::Bytes> {
        let cache_key = CacheKey::for_data(self.sst_id, handle.offset);
        let read_metrics = crate::sst::read_path_metrics::global_sst_read_metrics();

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
                let bytes = self.read_block(handle)?;
                read_metrics.record_data_block_read();
                cache.put(cache_key, &bytes);
                Ok(bytes)
            }
        } else {
            let bytes = self.read_block(handle)?;
            read_metrics.record_data_block_read();
            Ok(bytes)
        }
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
                    crate::sst::read_path_metrics::global_sst_read_metrics().record_bloom_reject();
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
        if self.key_outside_persisted_range(key) {
            self.read_amp_metrics.record_read(0, 0, 0);
            return Ok(None);
        }

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

        let index = self.index_entries()?;
        blocks_read += 1; // Index block read

        // Step 2: Use sparse index or binary search to identify only blocks
        // whose key interval can contain this key. Adjacent duplicate-key
        // blocks are included so MVCC versions split across block boundaries
        // remain visible to snapshot reads.
        let candidate_blocks = self.candidate_data_blocks(index.as_ref(), key);
        crate::sst::read_path_metrics::global_sst_read_metrics()
            .record_candidate_blocks_checked(candidate_blocks.len());

        for (idx, handle) in candidate_blocks {
            if !self.check_block_bloom(idx, key) {
                continue;
            }
            blocks_read += 1;
            let block_data = self.read_cached_data_block(&handle)?;

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
        if self.range_outside_persisted_bounds(start, end) {
            return Ok(Vec::new());
        }

        let index = self.index_entries()?;
        let mut result = Vec::new();

        let start_block = start
            .and_then(|start_key| self.candidate_block_indices(index.as_ref(), start_key))
            .map_or(0, |range| *range.start());
        let end_block = end
            .and_then(|end_key| self.candidate_block_indices(index.as_ref(), end_key))
            .map_or_else(|| index.len().saturating_sub(1), |range| *range.end());

        if index.is_empty() || start_block >= index.len() || start_block > end_block {
            return Ok(Vec::new());
        }

        let handles: Vec<BlockHandle> = index[start_block..=end_block]
            .iter()
            .map(|(_, handle)| *handle)
            .collect();

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
        if self.key_outside_persisted_range(key) {
            return Ok(crate::sst::types::KeyState::Absent);
        }

        let mut best_match: Option<SstEntry> = None;
        let index = self.index_entries()?;

        let candidate_blocks = self.candidate_data_blocks(index.as_ref(), key);
        crate::sst::read_path_metrics::global_sst_read_metrics()
            .record_candidate_blocks_checked(candidate_blocks.len());

        for (_idx, handle) in candidate_blocks {
            let block_data = self.read_cached_data_block(&handle)?;
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
        if self.key_outside_persisted_range(key) {
            self.read_amp_metrics.record_read(0, 0, 0);
            return Ok(KeyState::Absent);
        }

        let mut best_match: Option<SstEntry> = None;

        if let Some(ref bloom) = self.bloom_reader {
            if matches!(bloom.contains(key), BloomTestResult::DefinitelyNotPresent) {
                self.read_amp_metrics.record_read(1, 0, 0);
                return Ok(KeyState::Absent);
            }
        }

        let index = self.index_entries()?;
        let mut blocks_read = 1u64;

        let candidate_blocks = self.candidate_data_blocks(index.as_ref(), key);
        crate::sst::read_path_metrics::global_sst_read_metrics()
            .record_candidate_blocks_checked(candidate_blocks.len());

        for (idx, handle) in candidate_blocks {
            if !self.check_block_bloom(idx, key) {
                continue;
            }

            blocks_read += 1;
            let block_data = self.read_cached_data_block(&handle)?;
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

        self.read_amp_metrics.record_read(1, 0, blocks_read);

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
        if self.range_outside_persisted_bounds(start, end) {
            return Ok(Vec::new());
        }

        let index = self.index_entries()?;
        let mut result = Vec::new();

        let start_block = start
            .and_then(|start_key| self.candidate_block_indices(index.as_ref(), start_key))
            .map_or(0, |range| *range.start());
        let end_block = end
            .and_then(|end_key| self.candidate_block_indices(index.as_ref(), end_key))
            .map_or_else(|| index.len().saturating_sub(1), |range| *range.end());

        if index.is_empty() || start_block >= index.len() || start_block > end_block {
            return Ok(Vec::new());
        }

        for (_first_key, handle) in &index[start_block..=end_block] {
            let block_data = self.read_block(handle)?;
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
    use crate::io::traits::{DirEntry, Metadata};
    use crate::io::{Durability, File, Fs, FsPath, FsResult, OpenOptions};
    use crate::sst::traits::{SstFactory, SstReader, SstStateReader};
    use std::collections::HashSet;
    use std::sync::Mutex;

    struct CountingFs {
        inner: crate::io::RealFs,
        reads: Arc<Mutex<Vec<(u64, u64)>>>,
    }

    impl CountingFs {
        fn new(root: &std::path::Path) -> FsResult<Self> {
            Ok(Self {
                inner: crate::io::RealFs::new(root)?,
                reads: Arc::new(Mutex::new(Vec::new())),
            })
        }

        fn clear_reads(&self) {
            self.reads.lock().expect("read log lock").clear();
        }

        fn reads(&self) -> Vec<(u64, u64)> {
            self.reads.lock().expect("read log lock").clone()
        }
    }

    struct CountingFile<'a> {
        inner: Box<dyn File + 'a>,
        reads: Arc<Mutex<Vec<(u64, u64)>>>,
    }

    impl File for CountingFile<'_> {
        fn read_at(&self, offset: u64, len: u64) -> FsResult<Bytes> {
            self.reads
                .lock()
                .expect("read log lock")
                .push((offset, len));
            self.inner.read_at(offset, len)
        }

        fn write_at(&mut self, offset: u64, data: Bytes) -> FsResult<()> {
            self.inner.write_at(offset, data)
        }

        fn append(&mut self, data: Bytes) -> FsResult<u64> {
            self.inner.append(data)
        }

        fn len(&self) -> FsResult<u64> {
            self.inner.len()
        }

        fn sync(&mut self, dur: Durability) -> FsResult<()> {
            self.inner.sync(dur)
        }

        fn close(self: Box<Self>) -> FsResult<()> {
            self.inner.close()
        }
    }

    impl Fs for CountingFs {
        fn open(&self, path: &FsPath, opts: OpenOptions) -> FsResult<Box<dyn File + '_>> {
            Ok(Box::new(CountingFile {
                inner: self.inner.open(path, opts)?,
                reads: Arc::clone(&self.reads),
            }))
        }

        fn open_persistent_handle(
            &self,
            path: &FsPath,
            opts: OpenOptions,
        ) -> FsResult<Box<dyn File>> {
            self.inner.open_persistent_handle(path, opts)
        }

        fn remove_file(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_file(path)
        }

        fn exists(&self, path: &FsPath) -> FsResult<bool> {
            self.inner.exists(path)
        }

        fn metadata(&self, path: &FsPath) -> FsResult<Metadata> {
            self.inner.metadata(path)
        }

        fn create_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.create_dir_all(path)
        }

        fn list_dir(&self, path: &FsPath) -> FsResult<Vec<DirEntry>> {
            self.inner.list_dir(path)
        }

        fn remove_dir_all(&self, path: &FsPath) -> FsResult<()> {
            self.inner.remove_dir_all(path)
        }

        fn sync_dir(&self, path: &FsPath, dur: Durability) -> FsResult<()> {
            self.inner.sync_dir(path, dur)
        }

        fn rename_atomic(&self, from: &FsPath, to: &FsPath) -> FsResult<()> {
            self.inner.rename_atomic(from, to)
        }
    }

    fn write_unique_key_sst(temp_dir: &tempfile::TempDir, name: &str) -> MidgeResult<()> {
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        let value = vec![b'x'; 256];

        for i in 0..96u64 {
            let key = format!("key_{i:04}");
            writer.add_with_meta(key.as_bytes(), Some(&value), i + 1, 0, None)?;
        }

        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(name))
    }

    fn write_keyed_sst(
        temp_dir: &tempfile::TempDir,
        name: &str,
        block_size: usize,
        keys: &[Vec<u8>],
    ) -> MidgeResult<()> {
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, block_size);
        let mut writer = factory.create()?;
        let value = vec![b'v'; 256];

        for (index, key) in keys.iter().enumerate() {
            writer.add_with_meta(
                key,
                Some(&value),
                u64::try_from(index + 1).unwrap_or(u64::MAX),
                0,
                None,
            )?;
        }

        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(name))
    }

    fn structured_keys() -> Vec<Vec<u8>> {
        (0..192)
            .map(|index| format!("tenant/shared/static-segment/{index:04}").into_bytes())
            .collect()
    }

    fn open_counting_reader(
        temp_dir: &tempfile::TempDir,
        name: &str,
    ) -> MidgeResult<(Arc<CountingFs>, SstFileIo)> {
        let counting_fs = Arc::new(CountingFs::new(temp_dir.path())?);
        let fs: Arc<dyn Fs> = counting_fs.clone();
        let reader = SstFileIo::open(name, fs)?;
        Ok((counting_fs, reader))
    }

    fn data_block_reads(reads: &[(u64, u64)], index: &[(Vec<u8>, BlockHandle)]) -> Vec<(u64, u64)> {
        let data_offsets = index
            .iter()
            .map(|(_key, handle)| handle.offset)
            .collect::<HashSet<_>>();
        reads
            .iter()
            .copied()
            .filter(|(offset, _len)| data_offsets.contains(offset))
            .collect()
    }

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

    #[test]
    fn should_get_state_at_read_only_candidate_block_when_key_present() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        write_unique_key_sst(&temp_dir, "candidate.sst")?;
        let (counting_fs, reader) = open_counting_reader(&temp_dir, "candidate.sst")?;
        let index = reader.index_entries()?;
        assert!(
            index.len() >= 3,
            "test SST should contain multiple data blocks"
        );

        let target_block_idx = 1;
        let target_handle = index[target_block_idx].1;
        let target_block = reader.read_block(&target_handle)?;
        let entries = reader.scan_block_entries_from_bytes(&target_block)?;
        assert!(
            entries.len() >= 2,
            "target block should contain multiple keys"
        );
        let target_key = entries[1].key.clone();

        counting_fs.clear_reads();

        // Act
        let state = reader.get_state_at(&target_key, u64::MAX)?;

        // Assert
        assert!(matches!(state, KeyState::Value(_, _, _, _)));
        let reads = data_block_reads(&counting_fs.reads(), index.as_ref());
        assert_eq!(reads, vec![(target_handle.offset, target_handle.size)]);
        Ok(())
    }

    #[test]
    fn should_get_state_at_read_only_candidate_block_when_key_missing() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        write_unique_key_sst(&temp_dir, "candidate-missing.sst")?;
        let (counting_fs, reader) = open_counting_reader(&temp_dir, "candidate-missing.sst")?;
        let index = reader.index_entries()?;
        assert!(
            index.len() >= 3,
            "test SST should contain multiple data blocks"
        );

        let target_block_idx = 1;
        let target_handle = index[target_block_idx].1;
        let target_block = reader.read_block(&target_handle)?;
        let entries = reader.scan_block_entries_from_bytes(&target_block)?;
        assert!(
            entries.len() >= 2,
            "target block should contain multiple keys"
        );
        let mut missing_key = entries[1].key.clone();
        missing_key.push(b'a');

        counting_fs.clear_reads();

        // Act
        let state = reader.get_state_at(&missing_key, u64::MAX)?;

        // Assert
        assert_eq!(state, KeyState::Absent);
        let reads = data_block_reads(&counting_fs.reads(), index.as_ref());
        assert_eq!(reads, vec![(target_handle.offset, target_handle.size)]);
        Ok(())
    }

    #[test]
    fn should_select_trie_metadata_for_structured_keys() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let keys = structured_keys();
        write_keyed_sst(&temp_dir, "structured.sst", 4096, &keys)?;

        // Act
        let reader = SstFileIo::open(
            "structured.sst",
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
        )?;

        // Assert
        assert_eq!(reader.index_kind, IndexKind::Trie);
        assert!(reader.trie_reader.is_some());
        assert_eq!(reader.smallest_key.as_deref(), Some(keys[0].as_slice()));
        assert_eq!(
            reader.largest_key.as_deref(),
            Some(keys[keys.len() - 1].as_slice())
        );
        Ok(())
    }

    #[test]
    fn should_keep_sparse_metadata_for_small_ssts() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let keys = (0..64)
            .map(|index| format!("random-key-{index:04}").into_bytes())
            .collect::<Vec<_>>();
        write_keyed_sst(&temp_dir, "small.sst", 4096, &keys)?;

        // Act
        let reader = SstFileIo::open(
            "small.sst",
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
        )?;

        // Assert
        assert_eq!(reader.index_kind, IndexKind::Sparse);
        assert!(reader.trie_reader.is_none());
        Ok(())
    }

    #[test]
    fn should_use_trie_accelerator_when_structured_key_is_present() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let keys = structured_keys();
        write_keyed_sst(&temp_dir, "structured-candidate.sst", 4096, &keys)?;
        let (counting_fs, reader) = open_counting_reader(&temp_dir, "structured-candidate.sst")?;
        assert_eq!(reader.index_kind, IndexKind::Trie);
        let index = reader.index_entries()?;
        let target_block_idx = 1;
        let target_handle = index[target_block_idx].1;
        let target_block = reader.read_block(&target_handle)?;
        let entries = reader.scan_block_entries_from_bytes(&target_block)?;
        let target_key = entries[1].key.clone();

        counting_fs.clear_reads();

        // Act
        let state = reader.get_state_at(&target_key, u64::MAX)?;

        // Assert
        assert!(matches!(state, KeyState::Value(_, _, _, _)));
        let reads = data_block_reads(&counting_fs.reads(), index.as_ref());
        assert_eq!(reads, vec![(target_handle.offset, target_handle.size)]);
        Ok(())
    }

    #[test]
    fn should_skip_range_scan_when_requested_keys_are_outside_persisted_bounds() -> MidgeResult<()>
    {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let keys = structured_keys();
        write_keyed_sst(&temp_dir, "range-bounds.sst", 4096, &keys)?;
        let (counting_fs, reader) = open_counting_reader(&temp_dir, "range-bounds.sst")?;
        counting_fs.clear_reads();

        // Act
        let rows = reader.scan_range(Some(b"zzz"), Some(b"zzzz"))?;

        // Assert
        assert!(rows.is_empty());
        assert!(counting_fs.reads().is_empty());
        Ok(())
    }

    #[test]
    fn should_fail_open_when_trie_block_is_corrupted() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let keys = structured_keys();
        write_keyed_sst(&temp_dir, "corrupt-trie.sst", 4096, &keys)?;
        let reader = SstFileIo::open(
            "corrupt-trie.sst",
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
        )?;
        let trie_handle = reader
            .footer
            .as_ref()
            .and_then(|footer| footer.trie_handle)
            .expect("structured SST should persist a trie block");
        let path = temp_dir.path().join("corrupt-trie.sst");
        let mut bytes = std::fs::read(&path)?;
        let corrupt_offset = usize::try_from(trie_handle.offset + 4).unwrap_or(usize::MAX);
        bytes[corrupt_offset] ^= 0xFF;
        std::fs::write(&path, bytes)?;

        // Act
        let Err(error) = SstFileIo::open(
            "corrupt-trie.sst",
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
        ) else {
            panic!("corrupted trie block should fail to open");
        };

        // Assert
        assert!(matches!(error, MidgeError::Corruption(_)));
        Ok(())
    }

    #[test]
    fn should_get_state_at_return_newest_visible_version_across_duplicate_blocks() -> MidgeResult<()>
    {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"aaa", Some(&vec![b'a'; 256]), 100, 0, None)?;
        for seq in 1..=32u64 {
            let value = vec![u8::try_from(seq).unwrap_or(u8::MAX); 512];
            writer.add_with_meta(b"dup", Some(&value), seq, 0, None)?;
        }
        writer.add_with_meta(b"zzz", Some(&vec![b'z'; 256]), 100, 0, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("versions.sst"))?;

        let reader = SstFileIo::open(
            "versions.sst",
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
        )?;
        let index = reader.index_entries()?;
        let duplicate_blocks = index
            .iter()
            .filter(|(first_key, _handle)| first_key.as_slice() == b"dup")
            .count();
        assert!(
            duplicate_blocks >= 2,
            "duplicate versions should span multiple blocks"
        );

        // Act
        let state_at_5 = reader.get_state_at(b"dup", 5)?;
        let latest = reader.get_state_at(b"dup", u64::MAX)?;

        // Assert
        match state_at_5 {
            KeyState::Value(value, seq, _, _) => {
                assert_eq!(seq, 5);
                assert_eq!(value[0], 5);
            }
            other => panic!("expected visible value at seq 5, got {other:?}"),
        }
        match latest {
            KeyState::Value(value, seq, _, _) => {
                assert_eq!(seq, 32);
                assert_eq!(value[0], 32);
            }
            other => panic!("expected latest visible value, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn should_preserve_tombstone_ttl_semantics_when_get_state_at_reads() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"dead", Some(b"old"), 4, 0, None)?;
        writer.add_with_meta(b"dead", None, 9, 2, None)?;
        writer.add_with_meta(b"ttl", Some(b"expired"), 11, 0, Some(1))?;
        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("state.sst"))?;
        let reader = SstFileIo::open(
            "state.sst",
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
        )?;

        // Act
        let old_dead = reader.get_state_at(b"dead", 4)?;
        let deleted_dead = reader.get_state_at(b"dead", u64::MAX)?;
        let expired = reader.get_state_at(b"ttl", u64::MAX)?;

        // Assert
        assert!(matches!(old_dead, KeyState::Value(_, 4, _, _)));
        assert_eq!(deleted_dead, KeyState::Tombstone(9));
        assert_eq!(expired, KeyState::Tombstone(11));
        Ok(())
    }

    #[test]
    fn should_reject_current_format_block_with_crc_mismatch() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"crc-key", Some(b"crc-value"), 7, 0, None)?;
        let path = temp_dir.path().join("crc.sst");
        crate::sst::fs::finish_writer_to_path(writer, &path)?;

        {
            use std::io::{Read, Seek, Write};

            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)?;
            file.seek(std::io::SeekFrom::Start(4))?;
            let mut byte = [0_u8; 1];
            file.read_exact(&mut byte)?;
            file.seek(std::io::SeekFrom::Start(4))?;
            file.write_all(&[byte[0] ^ 0x01])?;
            file.sync_all()?;
        }

        let reader = SstFileIo::open(
            "crc.sst",
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
        )?;

        // Act
        let error = reader
            .get_state_at(b"crc-key", u64::MAX)
            .expect_err("current-format SST blocks must enforce CRC");

        // Assert
        assert!(
            error.to_string().contains("CRC32C mismatch"),
            "expected CRC mismatch error, got {error}"
        );
        Ok(())
    }
}
