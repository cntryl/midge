//! Filesystem-backed SST reader using io::Fs abstraction (new approach)
//!
//! This reader uses the base io::Fs trait instead of std::fs directly,
//! allowing for swappable implementations (Real, Mock, Chaos) for testing.

use bytes::Bytes;
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::sst::bloom::writer::{BloomFilterOps, BloomTestResult};
use crate::sst::bloom::{BlockBloomFilter, BloomMetrics, BloomReader};
use crate::sst::cache::{BlockCache, CacheKey};
use crate::sst::encoding;
use crate::sst::read_amp_metrics::ReadAmpMetrics;
use crate::sst::sparse_index::SparseIndexReader;
use crate::sst::traits::SstReader;
use crate::sst::types::{BlockHandle, Footer};

/// SST file reader using io::Fs abstraction
/// Identical to SstFile but accepts Arc<dyn Fs> for the filesystem backend
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
}

impl SstFileIo {
    /// Create a new SST reader using the provided filesystem
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
        }
    }

    /// Open and load metadata from an SST file
    pub fn open(path_str: &str, fs: Arc<dyn Fs>) -> MidgeResult<Self> {
        let mut reader = Self::new(path_str, fs);
        reader.load_metadata()?;
        Ok(reader)
    }

    /// Open an SST file using RealFs (convenience method for single-file access)
    /// This creates a new RealFs instance for the parent directory of the SST file.
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

    /// Enable bloom filter for this reader
    pub fn with_bloom(mut self, bloom: BloomReader) -> Self {
        self.bloom_reader = Some(bloom);
        self
    }

    /// Enable block bloom filter for this reader
    pub fn with_block_bloom(mut self, block_bloom: BlockBloomFilter) -> Self {
        self.block_bloom_filter = Some(block_bloom);
        self
    }

    /// Load block bloom filter from footer (if present)
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
    pub fn with_sparse_index(mut self, index: SparseIndexReader) -> Self {
        self.sparse_index = Some(Arc::new(index));
        self
    }

    /// Enable block cache for this reader
    pub fn with_block_cache(mut self, cache: Arc<BlockCache>) -> Self {
        self.block_cache = Some(cache);
        self
    }

    /// Set the SST ID for cache key generation
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

    fn load_metadata(&mut self) -> MidgeResult<()> {
        // Open file in read-only mode
        let file = self.fs.open(
            &self.path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )?;

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
        let footer_data = file.read_at(footer_offset, footer_size)?;

        self.footer = Some(Footer::decode(&footer_data)?);

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

        Ok(bytes::Bytes::copy_from_slice(&buffer[4..4 + len]))
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

    fn scan_block(
        &self,
        handle: &BlockHandle,
    ) -> MidgeResult<Vec<(bytes::Bytes, Option<bytes::Bytes>)>> {
        let block_data = self.read_block(handle)?;
        self.scan_block_from_bytes(&block_data)
    }

    fn scan_block_from_bytes(
        &self,
        block_data: &bytes::Bytes,
    ) -> MidgeResult<Vec<(bytes::Bytes, Option<bytes::Bytes>)>> {
        let mut result = Vec::new();
        let mut offset = 0;

        while offset < block_data.len() {
            if let Ok((entry, next_offset)) = encoding::decode(block_data.as_ref(), offset) {
                // Zero-copy: create Bytes slices that reference the original block buffer
                let key_start = entry.key_offset;
                let key_len = entry.key_delta.len();
                let key_bytes = block_data.slice(key_start..key_start + key_len);

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

                result.push((key_bytes, value_bytes));
                offset = next_offset;
            } else {
                break;
            }
        }

        Ok(result)
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
                    cache.put(cache_key, bytes.clone());
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

        for (_first_key, handle) in index {
            let entries = self.scan_block(&handle)?;
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

        Ok(result)
    }
}

impl crate::sst::SstStateReader for SstFileIo {
    fn get_state(&self, key: &[u8]) -> MidgeResult<crate::sst::types::KeyState> {
        match self.get(key)? {
            Some(value) => Ok(crate::sst::types::KeyState::Value(value, 0, None, 0)),
            None => Ok(crate::sst::types::KeyState::Absent),
        }
    }

    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, crate::sst::types::KeyState)>> {
        let pairs = self.scan_range(start, end)?;
        Ok(pairs
            .into_iter()
            .map(|(k, v)| (k.clone(), crate::sst::types::KeyState::Value(v, 0, None, 0)))
            .collect())
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
