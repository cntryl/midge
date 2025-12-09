use crate::error::{MidgeError, MidgeResult};
use crate::wal::WalRecord;
use std::sync::Arc;

use super::shared::{CloudStorageBackend, WalSegment};

/// Reader for cloud WAL segments.
///
/// Downloads and parses WAL segments from cloud storage, providing sequential
/// replay for crash recovery and debugging.
pub struct CloudWalReader {
    backend: Arc<dyn CloudStorageBackend>,
}

impl CloudWalReader {
    /// Create a new cloud WAL reader.
    pub fn new(backend: Arc<dyn CloudStorageBackend>) -> Self {
        Self { backend }
    }

    /// List all segment sequence IDs available in cloud storage.
    ///
    /// Returns a sorted list of segment numbers that can be read.
    pub fn list_segments(&self) -> MidgeResult<Vec<u64>> {
        // List blobs with the WAL segment prefix
        match self.backend.list_blobs("wal_segment_") {
            Ok(keys) => {
                let mut ids = Vec::new();
                for k in keys {
                    // Parse segment ID from key like "wal_segment_000042"
                    if let Some(num_str) = k.strip_prefix("wal_segment_") {
                        // Remove any file extension if present
                        let num_str = num_str.split('.').next().unwrap_or(num_str);
                        if let Ok(id) = num_str.parse::<u64>() {
                            ids.push(id);
                        }
                    }
                }
                ids.sort_unstable();
                Ok(ids)
            }
            Err(e) => Err(e),
        }
    }

    /// Read a specific segment by its sequence ID.
    ///
    /// Downloads the segment from cloud storage and deserializes the records.
    pub fn read_segment(&self, id: u64) -> MidgeResult<WalSegment> {
        let key = format!("wal_segment_{:06}", id);
        let data = self.backend.get_blob(&key)?;

        // Deserialize the records from bincode format
        let records: Vec<WalRecord> = bincode::deserialize(&data)
            .map_err(|e| MidgeError::internal(format!("Failed to deserialize segment: {}", e)))?;

        WalSegment::new(id, records)
    }

    /// Replay all segments in order, calling the callback for each record.
    ///
    /// This is used for crash recovery: replaying the WAL to rebuild state.
    pub fn replay_all<F>(&self, mut callback: F) -> MidgeResult<usize>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        let segments = self.list_segments()?;
        let mut total_records = 0;

        for segment_id in segments {
            let segment = self.read_segment(segment_id)?;
            for record in &segment.records {
                callback(record)?;
                total_records += 1;
            }
        }

        Ok(total_records)
    }

    /// Replay segments starting from a specific segment ID.
    ///
    /// Useful for incremental recovery or syncing from a known checkpoint.
    pub fn replay_from<F>(&self, start_segment: u64, mut callback: F) -> MidgeResult<usize>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        let all_segments = self.list_segments()?;
        let segments_to_replay: Vec<u64> = all_segments
            .into_iter()
            .filter(|&id| id >= start_segment)
            .collect();

        let mut total_records = 0;
        for segment_id in segments_to_replay {
            let segment = self.read_segment(segment_id)?;
            for record in &segment.records {
                callback(record)?;
                total_records += 1;
            }
        }

        Ok(total_records)
    }

    /// Get the highest segment ID available in cloud storage.
    ///
    /// Returns None if no segments exist.
    pub fn latest_segment_id(&self) -> MidgeResult<Option<u64>> {
        let segments = self.list_segments()?;
        Ok(segments.last().copied())
    }

    /// Check if a specific segment exists in cloud storage.
    pub fn segment_exists(&self, id: u64) -> MidgeResult<bool> {
        let key = format!("wal_segment_{:06}", id);
        // Use head_blob to check existence
        Ok(self.backend.head_blob(&key)?.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::MockCloudBackend;
    use crate::wal::WalOpKind;
    use bytes::Bytes;

    #[test]
    fn should_list_empty_segments_when_no_data_exists() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let reader = CloudWalReader::new(backend);

        // Act
        let segments = reader.list_segments().unwrap();

        // Assert
        assert_eq!(segments.len(), 0);
    }

    #[test]
    fn should_read_segment_after_upload() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let records = vec![WalRecord {
            cf_id: 0,
            op: WalOpKind::Put,
            key: Bytes::from("test_key"),
            value: Some(Bytes::from("test_value")),
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        }];
        let segment = WalSegment::new(1, records.clone()).unwrap();
        let data = segment.serialize().unwrap();
        backend.put_blob("wal_segment_000001", data.into()).unwrap();

        let reader = CloudWalReader::new(backend);

        // Act
        let read_segment = reader.read_segment(1).unwrap();

        // Assert
        assert_eq!(read_segment.sequence, 1);
        assert_eq!(read_segment.records.len(), 1);
        assert_eq!(read_segment.records[0].key, Bytes::from("test_key"));
    }

    #[test]
    fn should_list_multiple_segments_in_order() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Upload segments out of order
        for id in [3, 1, 2] {
            let segment = WalSegment::new(id, vec![]).unwrap();
            let data = segment.serialize().unwrap();
            backend
                .put_blob(&format!("wal_segment_{:06}", id), data.into())
                .unwrap();
        }

        let reader = CloudWalReader::new(backend);

        // Act
        let segments = reader.list_segments().unwrap();

        // Assert
        assert_eq!(segments, vec![1, 2, 3]);
    }

    #[test]
    fn should_replay_all_records_in_sequence() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());

        // Create two segments with records
        for seg_id in 1..=2 {
            let records: Vec<WalRecord> = (1..=3)
                .map(|i| WalRecord {
                    cf_id: 0,
                    op: WalOpKind::Put,
                    key: Bytes::from(format!("key_{}_{}", seg_id, i)),
                    value: Some(Bytes::from("value")),
                    seq: (seg_id - 1) * 3 + i,
                    expiration: None,
                    range_end: None,
                    txn_id: None,
                    compression: None,
                })
                .collect();

            let segment = WalSegment::new(seg_id, records).unwrap();
            let data = segment.serialize().unwrap();
            backend
                .put_blob(&format!("wal_segment_{:06}", seg_id), data.into())
                .unwrap();
        }

        let reader = CloudWalReader::new(backend);
        let mut replayed_keys = Vec::new();

        // Act
        let count = reader
            .replay_all(|record| {
                replayed_keys.push(record.key.clone());
                Ok(())
            })
            .unwrap();

        // Assert
        assert_eq!(count, 6);
        assert_eq!(replayed_keys.len(), 6);
        assert_eq!(replayed_keys[0], Bytes::from("key_1_1"));
        assert_eq!(replayed_keys[5], Bytes::from("key_2_3"));
    }
}
