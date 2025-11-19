//! Factory implementations for creating cloud-backed SST readers and writers.

use crate::cloud::StorageBackend;
use crate::error::MidgeResult;
use crate::sst::traits::{SstReaderFactory, SstStateReader};
use std::sync::Arc;

use super::reader::SstCloudReader;
use super::writer::SstCloudWriter;

// Adapter implementing DynSstWriter for the cloud writer
struct CloudDynWriter {
    writer: SstCloudWriter,
}

impl crate::sst::DynSstWriter for CloudDynWriter {
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        crate::sst::SstWriter::add(&mut self.writer, key, value)
    }

    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        tombstone: bool,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        self.writer
            .add_with_meta(key, value, seq, tombstone, expiration)
    }

    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
        self.writer.finish_bytes()
    }

    fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
        self.writer.add_range_tombstone(start, end, seq);
        Ok(())
    }
}

/// Factory that creates cloud-backed SST writers.
///
/// This factory creates writers that build SSTs in memory and can either:
/// 1. Return the raw bytes via `finish_bytes()`, or
/// 2. Upload to cloud storage via `finish_to_cloud()`
#[derive(Clone)]
pub struct CloudSstFactory {
    backend: Arc<dyn StorageBackend>,
    key_prefix: String,
}

impl CloudSstFactory {
    pub fn new(backend: Arc<dyn StorageBackend>, key_prefix: String) -> Self {
        Self {
            backend,
            key_prefix,
        }
    }
}

impl crate::sst::SstFactory for CloudSstFactory {
    fn create(
        &self,
        compression: crate::common::codec::CompressionType,
        block_size: usize,
        use_internal: bool,
    ) -> Box<dyn crate::sst::DynSstWriter> {
        Box::new(CloudDynWriter {
            writer: SstCloudWriter::new_with_internal(
                self.backend.clone(),
                self.key_prefix.clone(),
                compression,
                block_size,
                use_internal,
            ),
        })
    }

    fn create_with_bloom(
        &self,
        compression: crate::common::codec::CompressionType,
        block_size: usize,
        use_internal: bool,
        bloom_bits_per_key: u32,
    ) -> Box<dyn crate::sst::DynSstWriter> {
        Box::new(CloudDynWriter {
            writer: SstCloudWriter::new_with_bloom(
                self.backend.clone(),
                self.key_prefix.clone(),
                compression,
                block_size,
                use_internal,
                bloom_bits_per_key,
            ),
        })
    }
}

/// Cloud-backed reader factory that opens readers from cloud storage keys.
///
/// This factory interprets the path as a cloud storage key and fetches the
/// SST blob from the backend.
pub struct CloudSstReaderFactory {
    backend: Arc<dyn StorageBackend>,
    paranoid_checksums: bool,
}

impl CloudSstReaderFactory {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            paranoid_checksums: false,
        }
    }

    pub fn new_with_paranoid(backend: Arc<dyn StorageBackend>, paranoid_checksums: bool) -> Self {
        Self {
            backend,
            paranoid_checksums,
        }
    }
}

impl SstReaderFactory for CloudSstReaderFactory {
    fn open(&self, path: &std::path::Path) -> MidgeResult<Box<dyn SstStateReader>> {
        // HYBRID MODE: Check local cache first, fall back to cloud download
        // This enables CloudBacked storage mode where SSTs are written locally
        // AND uploaded to cloud for durability/disaster recovery

        let data = if path.exists() {
            // Read from local cache (fast path)
            std::fs::read(path).map_err(|e| {
                crate::error::MidgeError::internal(format!(
                    "Failed to read local SST {}: {}",
                    path.display(),
                    e
                ))
            })?
        } else {
            // Download from cloud (slow path - cache miss or node failure recovery)
            // Extract filename from path and construct cloud key
            let filename = path.file_name().and_then(|n| n.to_str()).ok_or_else(|| {
                crate::error::MidgeError::internal(format!("Invalid SST path: {}", path.display()))
            })?;

            // Cloud key format: "sst/<filename>" (relative path, not absolute)
            let cloud_key = format!("sst/{}", filename);

            tracing::debug!(
                "SST not in local cache, downloading from cloud: {}",
                cloud_key
            );

            self.backend.get_blob(&cloud_key)?.to_vec()
        };

        // Construct cloud key for reader metadata (used for logging/debugging)
        let cloud_key = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|f| format!("sst/{}", f));

        let r = if self.paranoid_checksums {
            SstCloudReader::from_bytes_with_key_paranoid(
                self.backend.clone(),
                data,
                cloud_key,
                true,
            )?
        } else {
            SstCloudReader::from_bytes_with_key(self.backend.clone(), data, cloud_key)?
        };
        Ok(Box::new(r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::MockCloudBackend;
    use crate::sst::{SstFactory, SstReaderFactory};

    #[test]
    fn should_create_cloud_sst_factory() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Act
        let factory = CloudSstFactory::new(backend, "sst".to_string());

        // Assert
        let writer = factory.create(crate::common::codec::CompressionType::None, 4096, false);
        assert!(writer.finish_bytes().is_ok());
    }

    #[test]
    fn should_create_writer_via_factory() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let factory = CloudSstFactory::new(backend, "sst".to_string());

        // Act
        let mut writer = factory.create(crate::common::codec::CompressionType::None, 4096, false);

        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        let result = writer.finish_bytes();

        // Assert
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(bytes.len() >= 48);
    }

    #[test]
    fn should_support_full_roundtrip() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let write_factory = CloudSstFactory::new(backend.clone(), "sst".to_string());

        // Act
        let mut writer =
            write_factory.create(crate::common::codec::CompressionType::None, 4096, false);
        writer.add(b"apple", b"A").unwrap();
        writer.add(b"banana", b"B").unwrap();
        writer.add(b"cherry", b"C").unwrap();
        let bytes = writer.finish_bytes().unwrap();

        // Upload to cloud
        let cloud_key = "sst/roundtrip-test.sst";
        backend
            .put_blob(cloud_key, bytes::Bytes::from(bytes))
            .unwrap();

        // Act - Read
        let read_factory = CloudSstReaderFactory::new(backend);
        let reader = read_factory.open(std::path::Path::new(cloud_key)).unwrap();

        // Assert
        let state_a = reader.get_state(b"apple").unwrap();
        let state_b = reader.get_state(b"banana").unwrap();
        let state_c = reader.get_state(b"cherry").unwrap();
        let state_x = reader.get_state(b"nonexistent").unwrap();

        match state_a {
            crate::sst::traits::KeyState::Value(v, _, _) => assert_eq!(v, bytes::Bytes::from("A")),
            _ => panic!("Expected Value for apple"),
        }

        match state_b {
            crate::sst::traits::KeyState::Value(v, _, _) => assert_eq!(v, bytes::Bytes::from("B")),
            _ => panic!("Expected Value for banana"),
        }

        match state_c {
            crate::sst::traits::KeyState::Value(v, _, _) => assert_eq!(v, bytes::Bytes::from("C")),
            _ => panic!("Expected Value for cherry"),
        }

        match state_x {
            crate::sst::traits::KeyState::Absent => {}
            _ => panic!("Expected Absent for nonexistent"),
        }
    }
}
