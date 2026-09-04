//! SST traits for readers and writers

use bytes::Bytes;
use std::path::Path;

use crate::common::MidgeResult;

/// One owned logical version yielded by an SST's raw compaction cursor.
///
/// This is an internal/diagnostic SST contract rather than part of Midge's
/// stable engine API. Values retain their persisted TTL metadata verbatim.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct RawSstVersion {
    pub key: Vec<u8>,
    pub seq: u64,
    pub is_tombstone: bool,
    pub value: Option<Vec<u8>>,
    pub expiration: Option<u64>,
}

/// Owned, fallible stream of raw SST versions in key-ascending,
/// sequence-descending order.
pub type RawSstVersionCursor =
    Box<dyn Iterator<Item = MidgeResult<RawSstVersion>> + Send + 'static>;

struct MaterializedRawVersionCursor {
    versions: std::vec::IntoIter<RawSstVersion>,
    _reservation: Option<crate::common::resource_budget::ResourceReservation>,
}

impl Iterator for MaterializedRawVersionCursor {
    type Item = MidgeResult<RawSstVersion>;

    fn next(&mut self) -> Option<Self::Item> {
        self.versions.next().map(Ok)
    }
}

/// Reader contract for SST implementations
pub trait SstReader: Send + Sync {
    /// Get the value for a specific key, if present
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be read or decoded.
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>>;

    /// Scan a key range [start, end) where either bound may be None
    /// Returns list of (key, value) pairs
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be scanned or decoded.
    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>>;
}

/// Stateful reader contract exposing tombstones and metadata
pub trait SstStateReader: Send + Sync {
    /// Get presence state (value/tombstone/absent) for a specific key
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be read or decoded.
    fn get_state(&self, key: &[u8]) -> MidgeResult<super::types::KeyState>;

    /// Scan a key range returning presence state for each key
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be scanned or decoded.
    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, super::types::KeyState)>>;

    /// Snapshot-aware range lookup with a caller-owned TTL clock.
    fn scan_range_state_with_time(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        _now_millis: u64,
    ) -> MidgeResult<Vec<(Bytes, super::types::KeyState)>> {
        self.scan_range_state(start, end)
    }

    /// Scan persisted state without interpreting TTL expiration. Recovery and
    /// compaction use this to preserve raw value-plus-expiration metadata.
    fn scan_range_raw_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, super::types::KeyState)>>;

    /// Consume this reader and stream persisted logical versions without
    /// interpreting TTL expiration.
    ///
    /// Filesystem readers override this compatibility implementation with a
    /// block-at-a-time cursor. Implementations used only by compatibility
    /// callers may retain the materializing fallback.
    ///
    /// # Errors
    ///
    /// Returns an error when the requested range cannot be read or decoded.
    fn raw_version_cursor(
        self: Box<Self>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    ) -> MidgeResult<RawSstVersionCursor> {
        self.raw_version_cursor_with_budget(start, end, None)
    }

    /// Budgeted form of [`SstStateReader::raw_version_cursor`] used by
    /// compaction. Implementations must reserve retained cursor buffers before
    /// growing them whenever a budget is supplied.
    ///
    /// # Errors
    ///
    /// Returns `MidgeError::ResourceLimit` before retaining data that cannot fit.
    fn raw_version_cursor_with_budget(
        self: Box<Self>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        budget: Option<crate::common::resource_budget::ResourceBudget>,
    ) -> MidgeResult<RawSstVersionCursor> {
        let states = self.scan_range_raw_state(start.as_deref(), end.as_deref())?;
        let versions = states
            .into_iter()
            .filter_map(|(key, state)| match state {
                super::types::KeyState::Absent => None,
                super::types::KeyState::Tombstone(seq) => Some(RawSstVersion {
                    key: key.to_vec(),
                    seq,
                    is_tombstone: true,
                    value: None,
                    expiration: None,
                }),
                super::types::KeyState::Value(value, seq, expiration, _op_type) => {
                    Some(RawSstVersion {
                        key: key.to_vec(),
                        seq,
                        is_tombstone: false,
                        value: Some(value.to_vec()),
                        expiration,
                    })
                }
            })
            .collect::<Vec<_>>();
        let retained_bytes = versions.iter().fold(0usize, |total, version| {
            total
                .saturating_add(version.key.capacity())
                .saturating_add(version.value.as_ref().map_or(0, Vec::capacity))
                .saturating_add(std::mem::size_of::<RawSstVersion>())
        });
        let reservation = budget
            .map(|budget| budget.reserve(retained_bytes, "compatibility raw-version cursor"))
            .transpose()?;
        Ok(Box::new(MaterializedRawVersionCursor {
            versions: versions.into_iter(),
            _reservation: reservation,
        }))
    }

    /// Snapshot-aware point lookup (entries with seq > `snapshot_seq` are ignored)
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be read or decoded.
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

    /// Snapshot-aware lookup with a caller-owned TTL clock.
    fn get_state_at_with_time(
        &self,
        key: &[u8],
        snapshot_seq: u64,
        _now_millis: u64,
    ) -> MidgeResult<super::types::KeyState> {
        self.get_state_at(key, snapshot_seq)
    }

    /// Return all range tombstones stored in this SST
    fn range_tombstones(&self) -> Vec<super::types::RangeTombstone> {
        Vec::new()
    }

    /// Return the retained bytes required to clone this SST's range tombstones.
    /// Budgeted compaction reserves this amount before requesting the clone.
    fn range_tombstone_memory_usage(&self) -> usize {
        0
    }
}

/// Combined reader contract used by the SST factory.
pub trait SstReaderExt: SstReader + SstStateReader {}

impl<T> SstReaderExt for T where T: SstReader + SstStateReader {}

/// Object-safe SST writer for polymorphic use
pub trait DynSstWriter: Send {
    /// Best-effort retained/encoded size used for soft compaction rollover.
    /// Implementations that cannot estimate return zero and therefore retain
    /// the compatibility single-output behavior.
    fn estimated_size_bytes(&self) -> usize {
        0
    }

    /// Add a simple key-value entry
    ///
    /// # Errors
    ///
    /// Returns an error when the key-value pair cannot be appended to the SST.
    fn add(&mut self, key: &[u8], value: &[u8]) -> MidgeResult<()>;

    /// Add an entry with metadata
    /// `op_type`: 0=Put, 1=Insert, 2=Delete
    ///
    /// # Errors
    ///
    /// Returns an error when the entry cannot be appended to the SST.
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

    /// Add an entry that is already sorted by key ascending and sequence
    /// descending for equal keys.
    ///
    /// The default preserves compatibility with writers that only implement
    /// `add_with_meta`. Filesystem writers use this signal to encode and spill
    /// complete data blocks incrementally during compaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the entry cannot be appended to the SST.
    fn add_sorted_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: u8,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        self.add_with_meta(key, value, seq, op_type, expiration)
    }

    /// Add a range tombstone
    ///
    /// # Errors
    ///
    /// Returns an error when the range tombstone cannot be appended to the SST.
    fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
        let _ = (start, end, seq);
        Err(crate::common::MidgeError::NotSupported(
            "this SST writer does not support range tombstones".to_string(),
        ))
    }

    /// Finalize and atomically persist this SST directly to `path`.
    ///
    /// The compatibility default uses [`DynSstWriter::finish_bytes`].
    /// Filesystem streaming writers override it so compaction never
    /// reconstructs the completed SST in one byte vector.
    ///
    /// # Errors
    ///
    /// Returns an error when finalization or atomic persistence fails.
    fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()> {
        let bytes = self.finish_bytes()?;
        crate::sst::fs::persist_sst_bytes_to_path(&bytes, path)
    }

    /// Finalize and get SST bytes
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be finalized.
    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>>;
}

/// Factory trait for creating SST writers and readers
pub trait SstFactory: Send + Sync {
    /// Create a new dynamic SST writer
    ///
    /// # Errors
    ///
    /// Returns an error when the writer cannot be created.
    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>>;

    /// Create a writer for a budgeted compaction operation.
    ///
    /// Filesystem writers preserve readable legacy raw entries that exceed the
    /// admission limit for new writes. Such blocks remain uncompressed; entry
    /// buffers and compression workspaces still require budget reservations.
    ///
    /// The default preserves compatibility for non-filesystem factories.
    fn create_for_compaction(
        &self,
        _budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Box<dyn DynSstWriter>> {
        self.create()
    }

    /// Open a reader whose retained metadata is charged to compaction.
    ///
    /// Compatibility factories may use the ordinary reader path. Production
    /// filesystem factories override this method and reserve before metadata
    /// block reads and decoding.
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be opened within the supplied budget.
    fn open_for_compaction(
        &self,
        path: &Path,
        _budget: crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<Box<dyn SstReaderExt>> {
        self.open(path)
    }

    /// Open an existing SST file for reading
    ///
    /// # Errors
    ///
    /// Returns an error when the SST cannot be opened or decoded.
    fn open(&self, path: &Path) -> MidgeResult<Box<dyn SstReaderExt>>;
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

    impl SstStateReader for MockSstReader {
        fn get_state(&self, key: &[u8]) -> MidgeResult<crate::sst::types::KeyState> {
            Ok(match self.data.get(key) {
                Some(value) => {
                    crate::sst::types::KeyState::Value(Bytes::copy_from_slice(value), 0, None, 0)
                }
                None => crate::sst::types::KeyState::Absent,
            })
        }

        fn scan_range_state(
            &self,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
        ) -> MidgeResult<Vec<(Bytes, crate::sst::types::KeyState)>> {
            Ok(self
                .scan_range(start, end)?
                .into_iter()
                .map(|(key, value)| (key, crate::sst::types::KeyState::Value(value, 0, None, 0)))
                .collect())
        }

        fn scan_range_raw_state(
            &self,
            start: Option<&[u8]>,
            end: Option<&[u8]>,
        ) -> MidgeResult<Vec<(Bytes, crate::sst::types::KeyState)>> {
            self.scan_range_state(start, end)
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
                result.extend_from_slice(&[u8::try_from(k.len()).unwrap_or(u8::MAX)]);
                result.extend_from_slice(&k);
                result.extend_from_slice(&[u8::try_from(v.len()).unwrap_or(u8::MAX)]);
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
        let reader_ref: &dyn SstReaderExt = &reader;

        // Act
        let result = reader_ref.get(b"any_key");

        // Assert
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn should_downcast_reader_through_trait_object() {
        // Arrange
        let mut reader = MockSstReader::new();
        reader.insert(b"key1".to_vec(), b"value1".to_vec());
        let reader_ref: &dyn SstReaderExt = &reader;

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
    fn should_apply_bounded_range_given_mock_reader() {
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
    fn should_return_suffix_given_inclusive_start_bound_when_using_mock_reader() {
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
    fn should_return_prefix_given_exclusive_end_bound_when_using_mock_reader() {
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
        let large_value = vec![42u8; 100_000];
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
    fn should_reject_range_tombstone_when_writer_does_not_support_it() {
        // Arrange
        let mut writer = MockSstWriter::new();

        // Act - unsupported range tombstones must fail closed.
        let result = writer.add_range_tombstone(b"start", b"end", 100);

        // Assert
        assert!(matches!(
            result,
            Err(crate::common::MidgeError::NotSupported(_))
        ));
    }

    #[test]
    fn should_finish_writer_to_path_helper_writes_file() {
        // Arrange
        let writer = MockSstWriter::new();
        let boxed: Box<dyn DynSstWriter> = Box::new(writer);
        let temp_path = PathBuf::from("/tmp/test_sst_finish.bin");

        // Act
        let result = crate::sst::fs::finish_writer_to_path(boxed, &temp_path);

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

        // Assert - the mock encodes each entry as length-prefixed bytes, so
        // finishing with zero entries deterministically produces zero bytes.
        assert_eq!(result.unwrap(), Vec::<u8>::new());
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
