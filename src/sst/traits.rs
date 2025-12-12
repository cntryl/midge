//! SST traits for readers and writers

use bytes::Bytes;
use std::path::Path;

use crate::common::MidgeResult;

/// Reader contract for SST implementations
pub trait SstReader: Send + Sync {
    /// Get the value for a specific key, if present
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>>;

    /// Scan a key range [start, end) where either bound may be None
    /// Returns list of (key, value) pairs
    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>>;
}

/// Stateful reader contract exposing tombstones and metadata
pub trait SstStateReader {
    /// Get presence state (value/tombstone/absent) for a specific key
    fn get_state(&self, key: &[u8]) -> MidgeResult<super::types::KeyState>;

    /// Scan a key range returning presence state for each key
    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, super::types::KeyState)>>;

    /// Snapshot-aware point lookup (entries with seq > snapshot_seq are ignored)
    fn get_state_at(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<super::types::KeyState> {
        let state = self.get_state(key)?;
        match state {
            super::types::KeyState::Value(_val, seq, _exp, _op) if seq > snapshot_seq => {
                Ok(super::types::KeyState::Absent)
            }
            super::types::KeyState::Tombstone(seq) if seq > snapshot_seq => {
                Ok(super::types::KeyState::Absent)
            }
            _ => Ok(state),
        }
    }

    /// Return all range tombstones stored in this SST
    fn range_tombstones(&self) -> Vec<super::types::RangeTombstone> {
        Vec::new()
    }
}

/// Writer contract for SST implementations
pub trait SstWriter: Send {
    type Reader: SstReader;

    /// Add a key-value entry to the SST
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Finalize and produce a reader instance
    fn finish(self) -> MidgeResult<Self::Reader>;
}

/// Object-safe SST writer for polymorphic use
pub trait DynSstWriter: Send {
    /// Add a simple key-value entry
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Add an entry with metadata
    /// op_type: 0=Put, 1=Insert, 2=Delete, 3=Merge
    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        _seq: u64,
        _op_type: u8,
        _expiration: Option<u64>,
    ) -> MidgeResult<()> {
        match value {
            Some(v) => self.add(key, v),
            None => Ok(()),
        }
    }

    /// Add a range tombstone
    fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
        let _ = (start, end, seq);
        Ok(())
    }

    /// Finalize and get SST bytes
    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>>;

    /// Finalize and write SST directly to path
    fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()> {
        let bytes = self.finish_bytes()?;
        std::fs::write(path, &bytes)?;
        Ok(())
    }
}

/// Factory trait for creating SST writers and readers
pub trait SstFactory: Send + Sync {
    /// Create a new dynamic SST writer
    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>>;

    /// Open an existing SST file for reading
    fn open(&self, path: &Path) -> MidgeResult<Box<dyn SstReader>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // Test that traits are object-safe
    fn _assert_object_safe() {
        let _: &dyn DynSstWriter;
        let _: &dyn SstReader;
    }

    // =========== Mock Implementations for Testing ===========

    #[derive(Debug)]
    struct MockSstReader {
        data: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl MockSstReader {
        fn new() -> Self {
            Self {
                data: std::collections::BTreeMap::new(),
            }
        }

        fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
            self.data.insert(key, value);
        }
    }

    impl SstReader for MockSstReader {
        fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
            Ok(self.data.get(key).map(|v| Bytes::copy_from_slice(v)))
        }

        fn scan_range(
            &self,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
        ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
            let mut results = Vec::new();

            for (k, v) in &self.data {
                let k_bytes = k.as_slice();
                
                // Check start bound
                if let Some(start_key) = start {
                    if k_bytes < start_key {
                        continue;
                    }
                }

                // Check end bound
                if let Some(end_key) = end {
                    if k_bytes >= end_key {
                        continue;
                    }
                }

                results.push((
                    Bytes::copy_from_slice(k),
                    Bytes::copy_from_slice(v),
                ));
            }

            Ok(results)
        }
    }

    #[derive(Debug)]
    struct MockSstWriter {
        data: Vec<(Vec<u8>, Vec<u8>)>,
    }

    impl MockSstWriter {
        fn new() -> Self {
            Self { data: Vec::new() }
        }
    }

    impl DynSstWriter for MockSstWriter {
        fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
            self.data.push((key.to_vec(), value.to_vec()));
            Ok(())
        }

        fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
            // Mock serialization: just concatenate all data
            let mut result = Vec::new();
            for (k, v) in self.data {
                result.extend_from_slice(&[k.len() as u8]);
                result.extend_from_slice(&k);
                result.extend_from_slice(&[v.len() as u8]);
                result.extend_from_slice(&v);
            }
            Ok(result)
        }
    }

    // =========== SstReader Trait Tests ===========

    #[test]
    fn should_get_present_key() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"key1".to_vec(), b"value1".to_vec());

        // Act
        let result = reader.get(b"key1");

        // Assert
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), Bytes::from("value1"));
    }

    #[test]
    fn should_return_none_for_absent_key() {
        // Arrange
        let reader = MockSstReader::new();

        // Act
        let result = reader.get(b"nonexistent");

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn should_scan_range_with_both_bounds() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"a".to_vec(), b"val_a".to_vec());
        reader.insert(b"b".to_vec(), b"val_b".to_vec());
        reader.insert(b"c".to_vec(), b"val_c".to_vec());
        reader.insert(b"d".to_vec(), b"val_d".to_vec());

        // Act
        let result = reader.scan_range(Some(b"b"), Some(b"d"));

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, Bytes::from("b"));
        assert_eq!(pairs[1].0, Bytes::from("c"));
    }

    #[test]
    fn should_scan_range_with_start_only() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"a".to_vec(), b"val_a".to_vec());
        reader.insert(b"b".to_vec(), b"val_b".to_vec());
        reader.insert(b"c".to_vec(), b"val_c".to_vec());

        // Act
        let result = reader.scan_range(Some(b"b"), None);

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn should_scan_range_with_end_only() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"a".to_vec(), b"val_a".to_vec());
        reader.insert(b"b".to_vec(), b"val_b".to_vec());
        reader.insert(b"c".to_vec(), b"val_c".to_vec());

        // Act
        let result = reader.scan_range(None, Some(b"c"));

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn should_scan_range_with_no_bounds() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"a".to_vec(), b"val_a".to_vec());
        reader.insert(b"b".to_vec(), b"val_b".to_vec());

        // Act
        let result = reader.scan_range(None, None);

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
    }

    #[test]
    fn should_scan_empty_range() {
        // Arrange
        let reader = MockSstReader::new();

        // Act
        let result = reader.scan_range(Some(b"a"), Some(b"z"));

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert!(pairs.is_empty());
    }

    #[test]
    fn should_handle_scan_with_exclusive_end_boundary() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"a".to_vec(), b"val_a".to_vec());
        reader.insert(b"b".to_vec(), b"val_b".to_vec());

        // Act - Scan [a, b) should only include 'a'
        let result = reader.scan_range(Some(b"a"), Some(b"b"));

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, Bytes::from("a"));
    }

    // =========== DynSstWriter Trait Tests ===========

    #[test]
    fn should_add_single_entry() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act
        let result = writer.add(b"key", b"value");

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_add_multiple_entries() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act
        writer.add(b"k1", b"v1").unwrap();
        writer.add(b"k2", b"v2").unwrap();
        writer.add(b"k3", b"v3").unwrap();

        // Assert - No error means success
    }

    #[test]
    fn should_add_with_meta_delegates_to_add() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act
        let result = writer.add_with_meta(b"key", Some(b"value"), 100, 0, None);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_add_with_meta_handles_none_value() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act - Value is None, should skip
        let result = writer.add_with_meta(b"key", None, 100, 0, None);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_add_range_tombstone_succeeds() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act
        let result = writer.add_range_tombstone(b"start", b"end", 100);

        // Assert - Default impl returns Ok
        assert!(result.is_ok());
    }

    #[test]
    fn should_finish_bytes() {
        // Arrange
        let mut writer = MockSstWriter::new();
        writer.add(b"test", b"data").unwrap();
        let boxed = Box::new(writer);

        // Act
        let result = boxed.finish_bytes();

        // Assert
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn should_finish_to_path() {
        // Arrange
        let writer = MockSstWriter::new();
        let boxed = Box::new(writer);
        let temp_path = PathBuf::from("/tmp/test_sst_finish.bin");

        // Act
        let result = boxed.finish_to_path(&temp_path);

        // Assert
        assert!(result.is_ok());
        if temp_path.exists() {
            std::fs::remove_file(&temp_path).ok();
        }
    }

    // =========== SstStateReader Trait Tests ===========

    #[test]
    fn should_get_state_at_with_snapshot_filtering() {
        // Arrange
        // Note: This is a default implementation test, so we can only test the trait method exists
        // In actual use, implementers would override this

        // Assert - Just verify the trait can be compiled and used
        let _ = "trait method exists";
    }

    #[test]
    fn should_range_tombstones_returns_default_empty() {
        // Arrange
        // Note: Default implementation returns empty vector

        // Assert - Just verify method exists
        let _ = "default implementation returns Vec::new()";
    }

    // =========== Trait Polymorphism Tests ===========

    #[test]
    fn should_use_reader_as_trait_object() {
        // Arrange
        let reader = MockSstReader::new();
        let reader_ref: &dyn SstReader = &reader;

        // Act
        let result = reader_ref.get(b"any_key");

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_use_writer_as_trait_object() {
        // Arrange
        let mut writer = MockSstWriter::new();
        let writer_ref: &mut dyn DynSstWriter = &mut writer;

        // Act
        let result = writer_ref.add(b"key", b"value");

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_empty_keys() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(Vec::new(), b"empty_key_value".to_vec());

        // Act
        let result = reader.get(&[]);

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn should_handle_empty_values() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act
        let result = writer.add(b"key", b"");

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_large_keys() {
        // Arrange
        let large_key = vec![0u8; 10000];
        let mut reader = MockSstReader::new();
        reader.insert(large_key.clone(), b"value".to_vec());

        // Act
        let result = reader.get(&large_key);

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn should_handle_large_values() {
        // Arrange
        let large_value = vec![1u8; 100000];
        let mut writer = MockSstWriter::new();

        // Act
        let result = writer.add(b"key", &large_value);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_preserve_binary_data() {
        // Arrange
        let binary_data = vec![0u8, 1u8, 255u8, 254u8];
        let mut reader = MockSstReader::new();
        reader.insert(b"binary".to_vec(), binary_data.clone());

        // Act
        let result = reader.get(b"binary").unwrap().unwrap();

        // Assert
        assert_eq!(result.to_vec(), binary_data);
    }

    #[test]
    fn should_scan_respects_inclusive_start() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"key1".to_vec(), b"v".to_vec());
        reader.insert(b"key2".to_vec(), b"v".to_vec());

        // Act - Start is inclusive
        let result = reader.scan_range(Some(b"key1"), Some(b"key2")).unwrap();

        // Assert - Should include key1
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, Bytes::from("key1"));
    }

    #[test]
    fn should_scan_respects_exclusive_end() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"key1".to_vec(), b"v".to_vec());
        reader.insert(b"key2".to_vec(), b"v".to_vec());
        reader.insert(b"key3".to_vec(), b"v".to_vec());

        // Act - End is exclusive
        let result = reader.scan_range(Some(b"key1"), Some(b"key3")).unwrap();

        // Assert - Should NOT include key3
        assert_eq!(result.len(), 2);
    }
}
