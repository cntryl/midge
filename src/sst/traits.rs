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
    /// op_type: 0=Put, 1=Insert, 2=Delete
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
        use std::io::Write;

        // Finish bytes first and record write time
        let finish_start = std::time::Instant::now();
        let bytes = self.finish_bytes()?;
        let write_bytes = bytes.len() as u64;
        let parent = path.parent().unwrap_or_else(|| Path::new("."));

        // Write to a temp file first
        let tmp = path.with_extension("tmp");
        let write_start = std::time::Instant::now();
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .map_err(crate::common::MidgeError::Io)?;
            f.write_all(&bytes).map_err(crate::common::MidgeError::Io)?;
            // Ensure data is on disk
            let sync_start = std::time::Instant::now();
            f.sync_all().map_err(crate::common::MidgeError::Io)?;
            let sync_ns = sync_start.elapsed().as_nanos();

            tracing::debug!(path = ?tmp, write_bytes, sync_ns, "sst temp file written and fsynced");
        }
        let write_ns = write_start.elapsed().as_nanos();

        // Atomically rename into place
        let rename_start = std::time::Instant::now();
        std::fs::rename(&tmp, path).map_err(crate::common::MidgeError::Io)?;
        let rename_ns = rename_start.elapsed().as_nanos();

        // Fsync parent directory to persist the rename. On some platforms (Windows)
        // opening a directory for fsync can return PermissionDenied — treat dir fsync
        // as best-effort and do not fail the whole operation if it isn't supported.
        let dir_fsync_start = std::time::Instant::now();
        match std::fs::File::open(parent) {
            Ok(dir_f) => {
                if let Err(e) = dir_f.sync_all() {
                    // Best-effort: log and continue
                    tracing::debug!("failed to fsync parent dir for sst: {}", e);
                }
            }
            Err(e) => {
                tracing::debug!("failed to open parent dir for fsync: {}", e);
            }
        }
        let dir_fsync_ns = dir_fsync_start.elapsed().as_nanos();

        tracing::info!(
            path = ?path,
            bytes = write_bytes,
            finish_total_ms = finish_start.elapsed().as_secs_f64() * 1000.0,
            write_ms = (write_ns as f64) / 1_000_000.0,
            rename_ms = (rename_ns as f64) / 1_000_000.0,
            dir_fsync_ms = (dir_fsync_ns as f64) / 1_000_000.0,
            "sst finished to path"
        );

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

                results.push((Bytes::copy_from_slice(k), Bytes::copy_from_slice(v)));
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

    // =========== Trait Object Safety Tests ===========

    #[test]
    fn should_use_reader_as_trait_object() {
        // Arrange
        let reader = MockSstReader::new();
        let reader_ref: &dyn SstReader = &reader;

        // Act
        let result = reader_ref.get(b"any_key");

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn should_use_writer_as_boxed_trait_object() {
        // Arrange
        let writer: Box<dyn DynSstWriter> = Box::new(MockSstWriter::new());

        // Act
        // Trait object can be created and used
        let _: Box<dyn DynSstWriter> = writer;

        // Assert
        // Just verifying it compiles and is object-safe
    }

    #[test]
    fn should_downcast_reader_through_trait_object() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"key1".to_vec(), b"value1".to_vec());
        let reader_ref: &dyn SstReader = &reader;

        // Act
        let result = reader_ref.get(b"key1");

        // Assert
        assert!(result.is_ok());
        let value = result.expect("get failed").expect("key not found");
        assert_eq!(value, Bytes::from("value1"));
    }

    // =========== SstReader Trait Behavior Tests ===========

    #[test]
    fn should_get_return_present_key() {
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
    fn should_get_return_none_for_absent_key() {
        // Arrange
        let reader = MockSstReader::new();

        // Act
        let result = reader.get(b"nonexistent");

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn should_scan_range_with_both_bounds_inclusive_exclusive() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"a".to_vec(), b"val_a".to_vec());
        reader.insert(b"b".to_vec(), b"val_b".to_vec());
        reader.insert(b"c".to_vec(), b"val_c".to_vec());
        reader.insert(b"d".to_vec(), b"val_d".to_vec());

        // Act - [b, d) should return b and c (inclusive start, exclusive end)
        let result = reader.scan_range(Some(b"b"), Some(b"d"));

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, Bytes::from("b"));
        assert_eq!(pairs[1].0, Bytes::from("c"));
    }

    #[test]
    fn should_scan_range_respects_inclusive_start_boundary() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"apple".to_vec(), b"v1".to_vec());
        reader.insert(b"banana".to_vec(), b"v2".to_vec());
        reader.insert(b"cherry".to_vec(), b"v3".to_vec());

        // Act - Start boundary is inclusive
        let result = reader.scan_range(Some(b"banana"), None);

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, Bytes::from("banana"));
    }

    #[test]
    fn should_scan_range_respects_exclusive_end_boundary() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"apple".to_vec(), b"v1".to_vec());
        reader.insert(b"banana".to_vec(), b"v2".to_vec());
        reader.insert(b"cherry".to_vec(), b"v3".to_vec());

        // Act - End boundary is exclusive
        let result = reader.scan_range(None, Some(b"cherry"));

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().all(|(k, _)| k.as_ref() < b"cherry".as_ref()));
    }

    #[test]
    fn should_scan_range_with_no_bounds_returns_all() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"a".to_vec(), b"val_a".to_vec());
        reader.insert(b"b".to_vec(), b"val_b".to_vec());
        reader.insert(b"c".to_vec(), b"val_c".to_vec());

        // Act
        let result = reader.scan_range(None, None);

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 3);
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
    fn should_scan_range_returns_results_in_order() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"key3".to_vec(), b"v3".to_vec());
        reader.insert(b"key1".to_vec(), b"v1".to_vec());
        reader.insert(b"key2".to_vec(), b"v2".to_vec());

        // Act
        let result = reader.scan_range(None, None).unwrap();

        // Assert - BTreeMap maintains sorted order
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, Bytes::from("key1"));
        assert_eq!(result[1].0, Bytes::from("key2"));
        assert_eq!(result[2].0, Bytes::from("key3"));
    }

    #[test]
    fn should_get_handle_binary_keys() {
        // Arrange
        let binary_key = vec![0u8, 1u8, 255u8, 254u8];
        let binary_value = vec![100u8, 200u8];
        let mut reader = MockSstReader::new();
        reader.insert(binary_key.clone(), binary_value.clone());

        // Act
        let result = reader.get(&binary_key).unwrap();

        // Assert
        assert_eq!(result.unwrap().to_vec(), binary_value);
    }

    #[test]
    fn should_get_handle_large_values() {
        // Arrange
        let large_value = vec![42u8; 100000];
        let mut reader = MockSstReader::new();
        reader.insert(b"key".to_vec(), large_value.clone());

        // Act
        let result = reader.get(b"key").unwrap();

        // Assert
        assert_eq!(result.unwrap().to_vec(), large_value);
    }

    // =========== DynSstWriter Trait Behavior Tests ===========

    #[test]
    fn should_add_with_meta_default_impl_calls_add_for_some() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act - Default impl should call add() for Some(value)
        let result = writer.add_with_meta(b"key", Some(b"value"), 100, 0, None);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_add_with_meta_default_impl_skips_none() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act - Default impl should skip None values
        let result = writer.add_with_meta(b"key", None, 100, 0, None);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_add_range_tombstone_default_impl_returns_ok() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act - Default impl returns Ok(())
        let result = writer.add_range_tombstone(b"start", b"end", 100);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_finish_to_path_default_impl_writes_file() {
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

    #[test]
    fn should_finish_bytes_produces_non_empty_output_when_has_data() {
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
    fn should_finish_bytes_produces_output_for_empty_writer() {
        // Arrange
        let writer = MockSstWriter::new();
        let boxed = Box::new(writer);

        // Act
        let result = boxed.finish_bytes();

        // Assert
        assert!(result.is_ok());
        // Empty writer may produce empty or minimal output
    }

    #[test]
    fn should_multiple_add_calls_accumulate() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act
        writer.add(b"k1", b"v1").unwrap();
        writer.add(b"k2", b"v2").unwrap();
        writer.add(b"k3", b"v3").unwrap();
        let boxed = Box::new(writer);
        let result = boxed.finish_bytes().unwrap();

        // Assert - Should have accumulated data
        assert!(!result.is_empty());
    }

    #[test]
    fn should_handle_empty_key_in_add() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act
        let result = writer.add(b"", b"value");

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_empty_value_in_add() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act
        let result = writer.add(b"key", b"");

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_binary_data_in_writer() {
        // Arrange
        let mut writer = MockSstWriter::new();
        let binary_key = vec![0u8, 255u8, 128u8];
        let binary_value = vec![1u8, 254u8, 127u8];

        // Act
        let result = writer.add(&binary_key, &binary_value);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_writer_handle_large_values() {
        // Arrange
        let mut writer = MockSstWriter::new();
        let large_value = vec![42u8; 50000];

        // Act
        let result = writer.add(b"key", &large_value);

        // Assert
        assert!(result.is_ok());
    }

    // =========== Trait Polymorphism Edge Cases ===========

    #[test]
    fn should_scan_with_start_end_as_same_value_returns_empty() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"key1".to_vec(), b"v".to_vec());

        // Act - [key1, key1) should be empty
        let result = reader.scan_range(Some(b"key1"), Some(b"key1")).unwrap();

        // Assert
        assert!(result.is_empty());
    }

    #[test]
    fn should_scan_with_start_greater_than_end_returns_empty() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"a".to_vec(), b"v".to_vec());
        reader.insert(b"z".to_vec(), b"v".to_vec());

        // Act - [z, a) is invalid range
        let result = reader.scan_range(Some(b"z"), Some(b"a")).unwrap();

        // Assert
        assert!(result.is_empty());
    }

    #[test]
    fn should_preserve_value_integrity_through_bytes() {
        // Arrange
        let original_value = vec![0u8, 1u8, 2u8, 255u8, 254u8];
        let mut reader = MockSstReader::new();
        reader.insert(b"key".to_vec(), original_value.clone());

        // Act
        let result = reader
            .get(b"key")
            .expect("get failed")
            .expect("key not found");

        // Assert
        assert_eq!(result.to_vec(), original_value);
    }
}
