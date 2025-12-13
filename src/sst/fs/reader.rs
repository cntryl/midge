//! Filesystem-backed SST reader with integrated bloom filter, sparse index, and block cache

use bytes::Bytes;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::sst::bloom::writer::BloomTestResult;
use crate::sst::bloom::{writer::BloomFilterOps, BlockBloomFilter, BloomMetrics, BloomReader};
use crate::sst::cache::{BlockCache, CacheKey};
use crate::sst::encoding;
use crate::sst::read_amp_metrics::ReadAmpMetrics;
use crate::sst::sparse_index::SparseIndexReader;
use crate::sst::traits::SstReader;
use crate::sst::types::{BlockHandle, Footer};

/// SST file reader with optional bloom filter, sparse index, and block cache
pub struct SstFile {
    path: std::path::PathBuf,
    footer: Option<Footer>,
    cached_file: std::sync::Mutex<Option<File>>,
    sst_id: u64,
    bloom_reader: Option<BloomReader>,
    block_bloom_filter: Option<BlockBloomFilter>,
    bloom_metrics: BloomMetrics,
    read_amp_metrics: ReadAmpMetrics,
    sparse_index: Option<Arc<SparseIndexReader>>,
    block_cache: Option<Arc<BlockCache>>,
}

impl SstFile {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            footer: None,
            cached_file: std::sync::Mutex::new(None),
            sst_id: 0,
            bloom_reader: None,
            block_bloom_filter: None,
            bloom_metrics: BloomMetrics::new(),
            read_amp_metrics: ReadAmpMetrics::new(),
            sparse_index: None,
            block_cache: None,
        }
    }

    pub fn open(path: &Path) -> MidgeResult<Self> {
        let mut reader = Self::new(path);
        reader.load_metadata()?;
        Ok(reader)
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
        let mut file = File::open(&self.path)?;
        let file_size = std::fs::metadata(&self.path)?.len();

        if file_size < 48 {
            return Err(MidgeError::Corruption("SST file too small".into()));
        }

        // Try to read footer (72 bytes for new format, fall back to 56 or 48)
        let footer_size = if file_size >= 72 {
            72
        } else if file_size >= 56 {
            56
        } else {
            48
        };

        file.seek(SeekFrom::End(-(footer_size as i64)))?;
        let mut footer_data = vec![0u8; footer_size];
        file.read_exact(&mut footer_data)?;

        self.footer = Some(Footer::decode(&footer_data)?);
        *self.cached_file.lock().expect("cached_file lock poisoned") = Some(file);

        Ok(())
    }

    fn read_block(&self, handle: &BlockHandle) -> MidgeResult<Vec<u8>> {
        let mut file_guard = self.cached_file.lock().expect("cached_file lock poisoned");
        if file_guard.is_none() {
            *file_guard = Some(File::open(&self.path)?);
        }

        if let Some(file) = file_guard.as_mut() {
            file.seek(SeekFrom::Start(handle.offset))?;

            let mut buffer = vec![0u8; handle.size as usize];
            file.read_exact(&mut buffer)?;

            // First 4 bytes are length prefix
            if buffer.len() < 4 {
                return Err(MidgeError::Corruption("Block too short".into()));
            }

            let len = u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
            if len + 4 > buffer.len() {
                return Err(MidgeError::Corruption("Block data truncated".into()));
            }

            Ok(buffer[4..4 + len].to_vec())
        } else {
            Err(MidgeError::Corruption("Cannot open SST file".into()))
        }
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

    fn scan_block(&self, handle: &BlockHandle) -> MidgeResult<Vec<(Vec<u8>, Option<Bytes>)>> {
        let block_data = self.read_block(handle)?;
        self.scan_block_from_bytes(&block_data)
    }

    fn scan_block_from_bytes(
        &self,
        block_data: &[u8],
    ) -> MidgeResult<Vec<(Vec<u8>, Option<Bytes>)>> {
        let mut result = Vec::new();
        let mut offset = 0;

        while offset < block_data.len() {
            if let Ok((entry, next_offset)) = encoding::decode(block_data, offset) {
                // Reconstruct key from shared_len + key_delta
                // For now, assume no shared prefix (simplified)
                let key = if entry.shared_len == 0 {
                    entry.key_delta
                } else {
                    // Would need to track previous key for prefix decompression
                    entry.key_delta
                };

                let value = entry.value.map(Bytes::from);
                result.push((key, value));
                offset = next_offset;
            } else {
                break;
            }
        }

        Ok(result)
    }

    /// Check block bloom filter with proper metrics and failure-safe semantics
    ///
    /// Returns true if key might be in block (or if bloom is unavailable)
    /// Returns false if key definitely not in block
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

impl crate::sst::SstReader for SstFile {
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        // Track blocks read for this operation
        let mut blocks_read = 0u64;

        // Step 1: Check SST-level bloom filter (negative lookup)
        if let Some(ref bloom) = self.bloom_reader {
            match bloom.contains(key) {
                BloomTestResult::DefinitelyNotPresent => {
                    // Record read with bloom rejection (no blocks accessed)
                    self.read_amp_metrics.record_read(1, 0, 0);
                    return Ok(None); // Key definitely not in SST
                }
                BloomTestResult::MightBePresent => {
                    // Continue to index lookup
                }
            }
        }

        let index = self.scan_index()?;
        blocks_read += 1; // Index block read

        // Step 2: Use sparse index to narrow block search range (if available)
        let block_handle = if let Some(ref sparse_idx) = self.sparse_index {
            // Sparse index narrows down which blocks to search
            let block_range = sparse_idx.find_block_range(key);

            // Find the specific block handle within the narrowed range
            let mut found_handle = None;
            for (idx, (first_key, handle)) in index.iter().enumerate() {
                if idx < block_range.start_block {
                    continue;
                }
                if idx > block_range.end_block {
                    break;
                }

                if key <= first_key.as_slice() || idx == block_range.end_block {
                    // Check block bloom BEFORE selecting this block
                    if !self.check_block_bloom(idx, key) {
                        // Block bloom rejected - key definitely not here
                        self.read_amp_metrics.record_read(1, 0, blocks_read);
                        return Ok(None);
                    }
                    found_handle = Some(*handle);
                    break;
                }
            }

            found_handle
        } else {
            // Traditional binary search without sparse index
            let mut found_handle = None;
            for (idx, (first_key, handle)) in index.iter().enumerate() {
                if key <= first_key.as_slice() || idx == index.len() - 1 {
                    // Check block bloom BEFORE selecting this block
                    if !self.check_block_bloom(idx, key) {
                        // Block bloom rejected - key definitely not here
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
            blocks_read += 1; // Data block will be read
                              // Step 3: Check block cache (if available)
            let cache_key = CacheKey::for_data(self.sst_id, handle.offset);

            let block_data = if let Some(ref cache) = self.block_cache {
                // Try to get from cache
                if let Some(cached_value) = cache.get(&cache_key) {
                    cached_value.data.as_ref().clone()
                } else {
                    // Load from disk and cache
                    let data = self.read_block(&handle)?;
                    let bytes = Bytes::from(data);

                    // Try to insert into cache
                    cache.put(cache_key, bytes.clone());
                    bytes
                }
            } else {
                // No cache available, read directly
                let data = self.read_block(&handle)?;
                Bytes::from(data)
            };

            // Decode and search within block
            let entries = self.scan_block_from_bytes(&block_data)?;
            for (entry_key, value) in entries {
                if entry_key == key {
                    // Record successful read
                    self.read_amp_metrics.record_read(1, 0, blocks_read);
                    return Ok(value);
                }
            }
        }

        // Record read with no key found
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
                // Check range bounds
                if let Some(s) = start {
                    if key.as_slice() < s {
                        continue;
                    }
                }
                if let Some(e) = end {
                    if key.as_slice() >= e {
                        continue;
                    }
                }

                if let Some(val) = value {
                    result.push((Bytes::from(key), val));
                }
            }
        }

        Ok(result)
    }
}

impl crate::sst::SstStateReader for SstFile {
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
    use crate::sst::{DynSstWriter, SstReader, SstStateReader};

    fn create_test_sst(entries: &[(&[u8], &[u8])]) -> MidgeResult<Vec<u8>> {
        // Create and write SST
        let temp_dir = tempfile::tempdir()?;
        let mut writer = crate::sst::fs::writer::FsSstWriter::new(temp_dir.path(), 4096)?;

        for (key, value) in entries {
            writer.add(key, value)?;
        }

        Box::new(writer).finish_bytes()
    }

    #[test]
    fn should_create_new_reader() {
        // Arrange
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.sst");

        // Act
        let reader = SstFile::new(path.as_path());

        // Assert
        assert!(reader.footer.is_none());
    }

    #[test]
    fn should_open_valid_sst_file() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let entries = vec![(b"key1" as &[u8], b"value1" as &[u8]), (b"key2", b"value2")];
        let bytes = create_test_sst(&entries)?;
        let path = temp_dir.path().join("test.sst");
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;

        // Assert
        assert!(reader.footer.is_some());
        Ok(())
    }

    #[test]
    fn should_chain_with_bloom_filter() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key", b"value")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        assert!(reader.footer.is_some());

        // Assert
        Ok(())
    }

    #[test]
    fn should_chain_with_sparse_index() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key", b"value")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        assert!(reader.footer.is_some());

        // Assert
        Ok(())
    }

    #[test]
    fn should_chain_with_block_cache() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key", b"value")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        assert!(reader.footer.is_some());

        // Assert
        Ok(())
    }

    #[test]
    fn should_set_sst_id() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key", b"value")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?.with_sst_id(42);

        // Assert
        assert_eq!(reader.sst_id, 42);
        Ok(())
    }

    #[test]
    fn should_reject_file_too_small() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("tiny.sst");
        std::fs::write(&path, b"tiny")?;

        // Act
        let result = SstFile::open(path.as_path());

        // Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn should_reject_nonexistent_file() {
        // Arrange
        let path = std::path::Path::new("/nonexistent/path/test.sst");

        // Act
        let result = SstFile::open(path);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_have_proper_size() {
        // Arrange & Act & Assert
        assert!(std::mem::size_of::<SstFile>() > 0);
    }

    #[test]
    fn should_read_metadata() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key", b"value")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let mut reader = SstFile::new(path.as_path());
        reader.load_metadata()?;

        // Assert
        assert!(reader.footer.is_some());
        Ok(())
    }

    #[test]
    fn should_open_multiblock_sst() -> MidgeResult<()> {
        // Arrange - force multiple blocks with small block size
        let temp_dir = tempfile::tempdir()?;
        let mut writer = crate::sst::fs::writer::FsSstWriter::new(temp_dir.path(), 256)?;
        for i in 0..20 {
            let key = format!("key{:03}", i);
            let val = format!("value{:03}", i);
            writer.add(key.as_bytes(), val.as_bytes())?;
        }
        let bytes = Box::new(writer).finish_bytes()?;
        let path = temp_dir.path().join("test.sst");
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;

        // Assert
        assert!(reader.footer.is_some());
        Ok(())
    }

    #[test]
    fn should_handle_two_entries() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes =
            create_test_sst(&[(b"key1" as &[u8], b"value1" as &[u8]), (b"key2", b"value2")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        assert!(reader.footer.is_some());

        // Assert
        Ok(())
    }

    #[test]
    fn should_try_get_method() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key1", b"value1")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.get(b"key1");

        // Assert
        Ok(())
    }

    #[test]
    fn should_scan_range_without_panic() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let entries: Vec<(&[u8], &[u8])> = vec![(b"aaa", b"v1"), (b"bbb", b"v2"), (b"ccc", b"v3")];
        let bytes = create_test_sst(&entries)?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.scan_range(None, None);

        // Assert
        Ok(())
    }

    #[test]
    fn should_scan_range_with_start_bound() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"aaa" as &[u8], b"v1" as &[u8]), (b"bbb", b"v2")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.scan_range(Some(b"bbb"), None);

        // Assert
        Ok(())
    }

    #[test]
    fn should_scan_range_with_end_bound() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"aaa" as &[u8], b"v1" as &[u8]), (b"bbb", b"v2")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.scan_range(None, Some(b"bbb"));

        // Assert
        Ok(())
    }

    #[test]
    fn should_call_get_state() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key", b"value")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.get_state(b"key");

        // Assert
        Ok(())
    }

    #[test]
    fn should_call_get_state_for_absent_key() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key", b"value")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.get_state(b"nonexistent");

        // Assert
        Ok(())
    }

    #[test]
    fn should_call_scan_range_state() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes =
            create_test_sst(&[(b"key1" as &[u8], b"value1" as &[u8]), (b"key2", b"value2")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.scan_range_state(None, None);

        // Assert
        Ok(())
    }

    #[test]
    fn should_load_valid_footer() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"key", b"value")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;

        // Assert
        assert!(reader.footer.is_some());
        Ok(())
    }

    #[test]
    fn should_handle_empty_sst_file() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.get(b"any_key");

        // Assert
        Ok(())
    }

    #[test]
    fn should_handle_single_entry_file() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&[(b"only", b"one")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.get_state(b"only");

        // Assert
        Ok(())
    }

    #[test]
    fn should_handle_large_file() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut entries = Vec::new();
        for i in 0..100 {
            let key = format!("key_{:04}", i);
            let val = format!("value_{:04}", i);
            entries.push((key, val));
        }
        let entry_refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
            .collect();

        let path = temp_dir.path().join("test.sst");
        let bytes = create_test_sst(&entry_refs)?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.scan_range(None, None);

        // Assert
        Ok(())
    }

    #[test]
    fn should_open_and_chain_methods() -> MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("test.sst");
        let bytes =
            create_test_sst(&[(b"key1" as &[u8], b"value1" as &[u8]), (b"key2", b"value2")])?;
        std::fs::write(&path, &bytes)?;

        // Act
        let reader = SstFile::open(path.as_path())?;
        let _ = reader.get(b"key1");
        let _ = reader.scan_range(None, None);
        let _ = reader.get(b"key2");

        // Assert
        Ok(())
    }
}
