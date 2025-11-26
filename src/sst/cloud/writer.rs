//! Cloud-backed SST writer implementation.
//!
//! This writer builds SSTs in memory (similar to SstMemWriter) and then
//! uploads the complete SST blob to cloud storage via the StorageBackend.

use crate::cloud::StorageBackend;
use crate::common::codec::CompressionType;
use crate::error::MidgeResult;
use crate::sst::writer_common::{SstImageBuilder, WriterConfig, WriterState};
use bytes::Bytes;
use std::sync::Arc;

use super::reader::SstCloudReader;

/// Cloud-backed SST writer that builds data blocks and index in memory,
/// then uploads the complete SST to cloud storage.
pub struct SstCloudWriter {
    backend: Arc<dyn StorageBackend>,
    key_prefix: String,
    state: WriterState,
    blocks: Vec<(Bytes, Bytes)>, // (last_key, encoded block)
}

impl SstCloudWriter {
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        key_prefix: String,
        compression: CompressionType,
        block_size: usize,
    ) -> Self {
        let config = WriterConfig::new(block_size, compression);
        Self {
            backend,
            key_prefix,
            state: WriterState::new(config),
            blocks: Vec::new(),
        }
    }

    pub fn new_with_internal(
        backend: Arc<dyn StorageBackend>,
        key_prefix: String,
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
    ) -> Self {
        let config = WriterConfig::new(block_size, compression).with_internal_keys(use_internal);
        Self {
            backend,
            key_prefix,
            state: WriterState::new(config),
            blocks: Vec::new(),
        }
    }

    pub fn new_with_bloom(
        backend: Arc<dyn StorageBackend>,
        key_prefix: String,
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
        bloom_bits_per_key: u32,
    ) -> Self {
        let config = WriterConfig::new(block_size, compression)
            .with_internal_keys(use_internal)
            .with_bloom_bits(bloom_bits_per_key);
        Self {
            backend,
            key_prefix,
            state: WriterState::new(config),
            blocks: Vec::new(),
        }
    }

    fn flush_block_if_needed(&mut self) -> Option<(Bytes, Bytes)> {
        self.state.flush_current_block()
    }

    /// Add a range tombstone to this SST.
    pub fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) {
        self.state.add_range_tombstone(start, end, seq);
    }

    /// Add with explicit metadata for advanced usage.
    pub fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: u8,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        if self.state.should_flush_block(key, value) {
            if let Some((last_key, encoded)) = self.flush_block_if_needed() {
                self.blocks.push((last_key, encoded));
            }
        }

        self.state
            .add_entry(key, value, seq, op_type, expiration)?;
        Ok(())
    }

    /// Finalize the SST and upload to cloud storage, returning the cloud key.
    pub fn finish_to_cloud(self, sst_id: &str) -> MidgeResult<String> {
        let cloud_key = format!("{}/{}.sst", self.key_prefix, sst_id);
        let backend = self.backend.clone();
        let raw = self.finish_bytes_internal()?;

        // Respect global upload rate limiter (if set) before performing the upload.
        let limiter = crate::common::rate_limiter::global_rate_limiter();
        limiter.request(raw.len() as u64);

        backend.put_blob(&cloud_key, Bytes::from(raw))?;
        Ok(cloud_key)
    }

    /// Finalize the SST and return raw bytes without uploading.
    pub fn finish_bytes(self) -> MidgeResult<Vec<u8>> {
        self.finish_bytes_internal()
    }

    fn finish_bytes_internal(mut self) -> MidgeResult<Vec<u8>> {
        // Flush any remaining data in the current block
        if let Some((last_key, encoded)) = self.flush_block_if_needed() {
            self.blocks.push((last_key, encoded));
        }

        let builder = SstImageBuilder::new(self.blocks, self.state);
        builder.build()
    }
}

impl crate::sst::SstWriter for SstCloudWriter {
    type Reader = SstCloudReader;

    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.add_with_meta(key, Some(value), 0, 0, None)
    }

    fn finish(self) -> MidgeResult<Self::Reader> {
        let backend = self.backend.clone();
        let raw = self.finish_bytes_internal()?;
        SstCloudReader::from_bytes(backend, raw)
    }
}

impl SstCloudWriter {
    /// Convenience inherent methods to ease usage in tests/examples without importing the trait
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        <Self as crate::sst::SstWriter>::add(self, key, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::MockCloudBackend;
    use crate::sst::SstReader; // Import trait for get() method

    #[test]
    fn should_create_cloud_writer_successfully() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let compression = CompressionType::None;
        let block_size = 4096;

        // Act
        let writer =
            SstCloudWriter::new(backend, "test-prefix".to_string(), compression, block_size);

        // Assert
        assert_eq!(writer.state.config.block_size, 4096);
    }

    #[test]
    fn should_add_entries_to_cloud_writer() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer =
            SstCloudWriter::new(backend, "test".to_string(), CompressionType::None, 4096);

        // Act
        let result1 = writer.add(b"key1", b"value1");
        let result2 = writer.add(b"key2", b"value2");

        // Assert
        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }

    #[test]
    fn should_finish_bytes_successfully() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer =
            SstCloudWriter::new(backend, "test".to_string(), CompressionType::None, 4096);
        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();

        // Act
        let result = writer.finish_bytes();

        // Assert
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(bytes.len() >= 48); // At least footer size
    }

    #[test]
    fn should_upload_to_cloud_storage() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = SstCloudWriter::new(
            backend.clone(),
            "sst".to_string(),
            CompressionType::None,
            4096,
        );
        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();

        // Act
        let result = writer.finish_to_cloud("test-sst-001");

        // Assert
        assert!(result.is_ok());
        let cloud_key = result.unwrap();
        assert_eq!(cloud_key, "sst/test-sst-001.sst");

        // Verify blob exists in cloud
        let blob = backend.get_blob(&cloud_key);
        assert!(blob.is_ok());
    }

    #[test]
    fn should_create_writer_with_bloom_filter() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = SstCloudWriter::new_with_bloom(
            backend.clone(),
            "sst".to_string(),
            CompressionType::None,
            4096,
            false,
            10, // bits per key
        );

        // Act
        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        let cloud_key = writer.finish_to_cloud("bloom-test").unwrap();

        // Assert - verify we can open and read back
        let reader =
            crate::sst::cloud::reader::SstCloudReader::open(backend.clone(), &cloud_key).unwrap();
        assert_eq!(reader.get(b"key1").unwrap(), Some(Bytes::from("value1")));
        assert_eq!(reader.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn should_handle_empty_sst() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let writer =
            SstCloudWriter::new(backend.clone(), "sst".to_string(), CompressionType::None, 4096);

        // Act - finish without adding any entries
        let result = writer.finish_bytes();

        // Assert - empty SST should still produce valid bytes
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(bytes.len() >= 48); // At least footer
    }

    #[test]
    fn should_write_with_internal_keys() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = SstCloudWriter::new_with_internal(
            backend.clone(),
            "sst".to_string(),
            CompressionType::None,
            4096,
            true,
        );

        // Act - add entries with internal key semantics (descending seq for same user key)
        writer
            .add_with_meta(b"key1", Some(b"v2"), 20, 0, None)
            .unwrap();
        writer
            .add_with_meta(b"key1", Some(b"v1"), 10, 0, None)
            .unwrap();
        let cloud_key = writer.finish_to_cloud("internal-test").unwrap();

        // Assert - verify upload succeeded
        let blob = backend.get_blob(&cloud_key);
        assert!(blob.is_ok());
    }

    #[test]
    fn should_add_range_tombstones() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer =
            SstCloudWriter::new(backend.clone(), "sst".to_string(), CompressionType::None, 4096);

        // Act
        writer.add(b"a", b"A").unwrap();
        writer.add(b"b", b"B").unwrap();
        writer.add(b"c", b"C").unwrap();
        writer.add_range_tombstone(b"b", b"c", 100);
        let result = writer.finish_bytes();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_large_values() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer =
            SstCloudWriter::new(backend.clone(), "sst".to_string(), CompressionType::Lz4, 4096);

        let large_value = vec![b'X'; 100_000]; // 100KB value

        // Act
        writer.add(b"large_key", &large_value).unwrap();
        let cloud_key = writer.finish_to_cloud("large-test").unwrap();

        // Assert - verify roundtrip
        let reader =
            crate::sst::cloud::reader::SstCloudReader::open(backend.clone(), &cloud_key).unwrap();
        let result = reader.get(b"large_key").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 100_000);
    }
}
