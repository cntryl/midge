//! Cloud-backed SST reader implementation.
//!
//! This reader fetches SST blobs from cloud storage and provides the same
//! interface as filesystem and in-memory SST readers.

use crate::cloud::StorageBackend;
use crate::error::MidgeResult;
use crate::sst::bloom::BloomFilter;
use crate::sst::encoding::TlvBlockIterator;
use crate::sst::format::{Block, BlockHandle, Footer};
use crate::sst::range_tombstone::is_covered_by_range_tombstone;
use crate::sst::reader_common::{
    read_data_block_from_bytes, read_data_block_from_bytes_paranoid,
    search_data_block as common_search_data_block, should_skip_key, SstMetadata,
};
use crate::sst::sparse_index::SparseIndex;
use crate::sst::traits::{KeyState, RangeTombstone, SstStateReader};
use bytes::Bytes;
use std::sync::Arc;

/// Cloud-backed SST reader
pub struct SstCloudReader {
    #[allow(dead_code)]
    backend: Arc<dyn StorageBackend>,
    cloud_key: Option<String>,
    data: Vec<u8>,
    _footer: Footer,
    sparse_index: SparseIndex,
    bloom_filter: Option<BloomFilter>,
    range_tombstones: Vec<RangeTombstone>,
    use_internal_keys: bool,
    paranoid_checksums: bool,
}

impl SstCloudReader {
    /// Create a reader from a cloud storage key
    pub fn open(backend: Arc<dyn StorageBackend>, cloud_key: &str) -> MidgeResult<Self> {
        let data = backend.get_blob(cloud_key)?;
        Self::from_bytes_with_key(backend, data.to_vec(), Some(cloud_key.to_string()))
    }

    /// Create a reader from raw bytes (e.g., for testing or caching)
    pub fn from_bytes(backend: Arc<dyn StorageBackend>, raw: Vec<u8>) -> MidgeResult<Self> {
        Self::from_bytes_with_key(backend, raw, None)
    }

    pub(crate) fn from_bytes_with_key(
        backend: Arc<dyn StorageBackend>,
        raw: Vec<u8>,
        cloud_key: Option<String>,
    ) -> MidgeResult<Self> {
        Self::from_bytes_with_key_paranoid(backend, raw, cloud_key, false)
    }

    /// Create reader with paranoid checksum verification
    pub(crate) fn from_bytes_with_key_paranoid(
        backend: Arc<dyn StorageBackend>,
        raw: Vec<u8>,
        cloud_key: Option<String>,
        paranoid_checksums: bool,
    ) -> MidgeResult<Self> {
        // Use common metadata parsing logic
        let metadata = SstMetadata::from_bytes(&raw)?;

        Ok(Self {
            backend,
            cloud_key,
            data: raw,
            _footer: metadata.footer,
            sparse_index: metadata.sparse_index,
            bloom_filter: metadata.bloom_filter,
            range_tombstones: metadata.range_tombstones,
            use_internal_keys: metadata.use_internal_keys,
            paranoid_checksums,
        })
    }

    fn read_data_block(&self, handle: BlockHandle) -> MidgeResult<Block> {
        let off = handle.offset as usize;
        let sz = handle.size as usize;
        let raw = &self.data[off..off + sz];
        if self.paranoid_checksums {
            read_data_block_from_bytes_paranoid(raw, true)
        } else {
            read_data_block_from_bytes(raw)
        }
    }

    #[allow(dead_code)]
    fn search_data_block(&self, data: &[u8], target_key: &[u8]) -> MidgeResult<Option<Bytes>> {
        common_search_data_block(data, target_key, self.use_internal_keys)
    }

    /// Snapshot-aware point lookup
    pub fn get_at(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<Option<Bytes>> {
        // Early-out if bloom filter or range tombstones indicate key is not present
        if should_skip_key(
            &self.bloom_filter,
            &self.range_tombstones,
            key,
            snapshot_seq,
        ) {
            return Ok(None);
        }

        if let Some(bh) = self.sparse_index.find_block(key) {
            let blk = self.read_data_block(*bh)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, _expiration) = result?;

                let mut actual_key = raw_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&actual_key)
                    {
                        actual_key = user;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if actual_key.as_slice() == key {
                    // Snapshot isolation: only see writes with seq < snapshot_seq
                    if actual_seq < snapshot_seq && !tomb {
                        return Ok(value_opt.map(Bytes::copy_from_slice));
                    } else {
                        return Ok(None);
                    }
                }

                if actual_key.as_slice() > key {
                    break;
                }
            }
        }

        Ok(None)
    }

    /// Get the serialized bloom filter bytes
    pub fn get_bloom_filter_bytes(&self) -> Option<Vec<u8>> {
        self.bloom_filter.as_ref().map(|bf| bf.encode().to_vec())
    }

    /// Get the cloud storage key for this SST (if opened from cloud)
    pub fn cloud_key(&self) -> Option<&str> {
        self.cloud_key.as_deref()
    }
}

impl SstStateReader for SstCloudReader {
    fn get_state(&self, key: &[u8]) -> MidgeResult<KeyState> {
        // Early-out if bloom filter or range tombstones indicate key is not present
        if should_skip_key(&self.bloom_filter, &self.range_tombstones, key, u64::MAX) {
            return Ok(KeyState::Absent);
        }

        // Check for exact range tombstone match
        for rt in &self.range_tombstones {
            if key >= rt.start.as_slice() && key < rt.end.as_slice() {
                return Ok(KeyState::Tombstone(rt.seq));
            }
        }

        if let Some(bh) = self.sparse_index.find_block(key) {
            let blk = self.read_data_block(*bh)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, expiration) = result?;

                let mut actual_key = raw_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&actual_key)
                    {
                        actual_key = user;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if actual_key.as_slice() == key {
                    if tomb {
                        return Ok(KeyState::Tombstone(actual_seq));
                    }
                    return Ok(KeyState::Value(
                        Bytes::copy_from_slice(value_opt.unwrap_or(&[])),
                        actual_seq,
                        expiration,
                    ));
                }

                if actual_key.as_slice() > key {
                    break;
                }
            }
        }

        Ok(KeyState::Absent)
    }

    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        let mut results = Vec::new();

        for entry in self.sparse_index.entries() {
            let last_key = &entry.key;
            let block_handle = entry.block_handle;

            if let Some(s) = start {
                if last_key.as_ref() < s {
                    continue;
                }
            }

            let blk = self.read_data_block(block_handle)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, expiration) = result?;

                let mut actual_key = raw_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&actual_key)
                    {
                        actual_key = user;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if let Some(s) = start {
                    if actual_key.as_slice() < s {
                        continue;
                    }
                }

                if let Some(e) = end {
                    if actual_key.as_slice() >= e {
                        return Ok(results);
                    }
                }

                let state = if tomb {
                    KeyState::Tombstone(actual_seq)
                } else {
                    KeyState::Value(
                        Bytes::copy_from_slice(value_opt.unwrap_or(&[])),
                        actual_seq,
                        expiration,
                    )
                };

                results.push((Bytes::copy_from_slice(&actual_key), state));
            }

            if let Some(e) = end {
                if last_key.as_ref() >= e {
                    break;
                }
            }
        }

        Ok(results)
    }

    fn get_state_at(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<KeyState> {
        // Bloom filter check
        if let Some(bf) = &self.bloom_filter {
            if !bf.may_contain(key) {
                return Ok(KeyState::Absent);
            }
        }

        // Range tombstone check
        if is_covered_by_range_tombstone(&self.range_tombstones, key, snapshot_seq) {
            return Ok(KeyState::Tombstone(snapshot_seq));
        }

        if let Some(bh) = self.sparse_index.find_block(key) {
            let blk = self.read_data_block(*bh)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, expiration) = result?;

                let mut actual_key = raw_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&actual_key)
                    {
                        actual_key = user;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if actual_key.as_slice() == key {
                    if actual_seq > snapshot_seq {
                        return Ok(KeyState::Absent);
                    }
                    if tomb {
                        return Ok(KeyState::Tombstone(actual_seq));
                    }
                    return Ok(KeyState::Value(
                        Bytes::copy_from_slice(value_opt.unwrap_or(&[])),
                        actual_seq,
                        expiration,
                    ));
                }

                if actual_key.as_slice() > key {
                    break;
                }
            }
        }

        Ok(KeyState::Absent)
    }

    fn scan_range_state_at(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
    ) -> MidgeResult<Vec<(Bytes, KeyState)>> {
        let mut results = Vec::new();

        for entry in self.sparse_index.entries() {
            let last_key = &entry.key;
            let block_handle = entry.block_handle;

            if let Some(s) = start {
                if last_key.as_ref() < s {
                    continue;
                }
            }

            let blk = self.read_data_block(block_handle)?;
            let iter = TlvBlockIterator::new(&blk.data);

            for result in iter {
                let (raw_key, value_opt, seq, entry_type, expiration) = result?;

                let mut actual_key = raw_key;
                let mut actual_seq = seq;
                let mut tomb = entry_type == 2;

                if self.use_internal_keys {
                    if let Some((user, s, t)) =
                        crate::common::internal_key::decode_internal_key(&actual_key)
                    {
                        actual_key = user;
                        actual_seq = s;
                        tomb = t;
                    }
                }

                if let Some(s) = start {
                    if actual_key.as_slice() < s {
                        continue;
                    }
                }

                if let Some(e) = end {
                    if actual_key.as_slice() >= e {
                        return Ok(results);
                    }
                }

                // Skip entries with seq > snapshot
                if actual_seq > snapshot_seq {
                    continue;
                }

                // Check if covered by range tombstone
                if is_covered_by_range_tombstone(&self.range_tombstones, &actual_key, snapshot_seq)
                {
                    continue;
                }

                let state = if tomb {
                    KeyState::Tombstone(actual_seq)
                } else {
                    KeyState::Value(
                        Bytes::copy_from_slice(value_opt.unwrap_or(&[])),
                        actual_seq,
                        expiration,
                    )
                };

                results.push((Bytes::copy_from_slice(&actual_key), state));
            }

            if let Some(e) = end {
                if last_key.as_ref() >= e {
                    break;
                }
            }
        }

        Ok(results)
    }
}

impl crate::sst::SstReader for SstCloudReader {
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        match self.get_state(key)? {
            KeyState::Value(v, _, _) => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let state_results = self.scan_range_state(start, end)?;
        let results = state_results
            .into_iter()
            .filter_map(|(k, state)| match state {
                KeyState::Value(v, _, _) => Some((k, v)),
                _ => None,
            })
            .collect();
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::MockCloudBackend;
    use crate::sst::SstReader;

    #[test]
    fn should_read_from_cloud_storage() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = crate::sst::cloud::SstCloudWriter::new(
            backend.clone(),
            "sst".to_string(),
            crate::common::codec::CompressionType::None,
            4096,
        );

        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        let cloud_key = writer.finish_to_cloud("test-001").unwrap();

        // Act
        let reader = SstCloudReader::open(backend, &cloud_key);

        // Assert
        assert!(reader.is_ok());
    }

    #[test]
    fn should_get_value_successfully() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = crate::sst::cloud::SstCloudWriter::new(
            backend.clone(),
            "sst".to_string(),
            crate::common::codec::CompressionType::None,
            4096,
        );

        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        let cloud_key = writer.finish_to_cloud("test-001").unwrap();
        let reader = SstCloudReader::open(backend, &cloud_key).unwrap();

        // Act
        let result = reader.get(b"key1");

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(Bytes::from("value1")));
    }

    #[test]
    fn should_return_none_for_missing_key() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = crate::sst::cloud::SstCloudWriter::new(
            backend.clone(),
            "sst".to_string(),
            crate::common::codec::CompressionType::None,
            4096,
        );

        writer.add(b"key1", b"value1").unwrap();
        let cloud_key = writer.finish_to_cloud("test-001").unwrap();
        let reader = SstCloudReader::open(backend, &cloud_key).unwrap();

        // Act
        let result = reader.get(b"nonexistent");

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn should_scan_range_successfully() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = crate::sst::cloud::SstCloudWriter::new(
            backend.clone(),
            "sst".to_string(),
            crate::common::codec::CompressionType::None,
            4096,
        );

        writer.add(b"a", b"A").unwrap();
        writer.add(b"b", b"B").unwrap();
        writer.add(b"c", b"C").unwrap();
        let cloud_key = writer.finish_to_cloud("test-001").unwrap();
        let reader = SstCloudReader::open(backend, &cloud_key).unwrap();

        // Act
        let result = reader.scan_range(Some(b"a"), Some(b"c"));

        // Assert
        assert!(result.is_ok());
        let pairs = result.unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, Bytes::from("a"));
        assert_eq!(pairs[1].0, Bytes::from("b"));
    }

    #[test]
    fn should_use_bloom_filter_to_skip_absent_keys() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = crate::sst::cloud::SstCloudWriter::new_with_bloom(
            backend.clone(),
            "sst".to_string(),
            crate::common::codec::CompressionType::None,
            4096,
            false,
            10,
        );

        writer.add(b"key1", b"value1").unwrap();
        writer.add(b"key2", b"value2").unwrap();
        let cloud_key = writer.finish_to_cloud("test-001").unwrap();
        let reader = SstCloudReader::open(backend, &cloud_key).unwrap();

        // Act
        let result = reader.get(b"definitely_not_there");

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn should_handle_snapshot_reads() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = crate::sst::cloud::SstCloudWriter::new_with_internal(
            backend.clone(),
            "sst".to_string(),
            crate::common::codec::CompressionType::None,
            4096,
            true,
        );

        // Add keys in descending sequence order (required for internal mode)
        writer
            .add_with_meta(b"key1", Some(b"v2"), 20, false, None)
            .unwrap();
        writer
            .add_with_meta(b"key1", Some(b"v1"), 10, false, None)
            .unwrap();
        let cloud_key = writer.finish_to_cloud("test-001").unwrap();
        let reader = SstCloudReader::open(backend, &cloud_key).unwrap();

        // Act
        let result_at_15 = reader.get_at(b"key1", 15);
        let result_at_25 = reader.get_at(b"key1", 25);

        // Assert
        assert!(result_at_15.is_ok());
        assert!(result_at_25.is_ok());
    }

    #[test]
    fn should_read_from_bytes() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let mut writer = crate::sst::cloud::SstCloudWriter::new(
            backend.clone(),
            "sst".to_string(),
            crate::common::codec::CompressionType::None,
            4096,
        );

        writer.add(b"key1", b"value1").unwrap();
        let bytes = writer.finish_bytes().unwrap();

        // Act
        let reader = SstCloudReader::from_bytes(backend, bytes);

        // Assert
        assert!(reader.is_ok());
        let r = reader.unwrap();
        assert_eq!(r.get(b"key1").unwrap(), Some(Bytes::from("value1")));
    }
}
