//! Flush coordination for MidgeEngine.
//!
//! This module handles memtable flushing coordination including:
//! - Memtable rollover and flush queueing

use crate::api::column_family::{ColumnFamilyId, DEFAULT_CF_ID};
use crate::core::runtime::{RuntimeTask, RuntimeTaskKind};
use crate::error::MidgeResult;
use std::sync::Arc;
use tracing::error;

use super::MidgeEngine;

impl MidgeEngine {
    /// Roll over memtable and queue flush job for the specified column family.
    ///
    /// This freezes the current active memtable and queues it for background flushing.
    /// WAL is rotated to a new file for future writes.
    ///
    /// # Arguments
    ///
    /// * `cf_id` - Column family ID to flush (use DEFAULT_CF_ID for default CF)
    ///
    /// # Returns
    ///
    /// Sequence number at rollover time for tracking flush progress
    pub(crate) fn rollover_and_queue_flush(&self, cf_id: ColumnFamilyId) -> MidgeResult<u64> {
        let cf_target = cf_id.as_u32();
        let request_flush = |job| {
            // Phase 6: All flushes now routed through EngineRuntime for unified coordination
            let runtime = Arc::clone(&self.runtime);
            let flush_coord = Arc::clone(&self.flush_coordinator);
            let description = format!("flush_cf({cf_target})");
            runtime.submit(RuntimeTask::new(
                RuntimeTaskKind::Flush,
                description,
                Box::new(move || {
                    if let Err(err) = flush_coord.request_flush(job) {
                        error!(%err, "runtime flush task failed");
                    }
                }),
            ))
        };

        crate::core::persistence::flush::rollover_and_queue_flush(
            cf_id,
            self.wal_coordinator.writer_lock(),
            self.wal_coordinator.factory(),
            &self.db_path.join("wal"),
            || {
                if cf_id == DEFAULT_CF_ID {
                    let entries =
                        self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
                    let range_tombstones =
                        self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
                    (entries, range_tombstones)
                } else {
                    // For non-default CFs, use with_cf_memtable_mut
                    let entries = self
                        .with_cf_memtable_mut(cf_id, |mt| mt.drain_with_meta_internal())
                        .unwrap_or_default();
                    let range_tombstones = self
                        .with_cf_memtable_mut(cf_id, |mt| mt.drain_range_tombstones())
                        .unwrap_or_default();
                    (entries, range_tombstones)
                }
            },
            request_flush,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::column_family::DEFAULT_CF_ID;
    use std::sync::Arc;

    fn create_test_engine() -> Arc<MidgeEngine> {
        let opts = crate::MidgeOptions {
            storage_mode: crate::StorageMode::Memory,
            ..Default::default()
        };
        Arc::new(MidgeEngine::open(opts).expect("Failed to create test engine"))
    }

    #[test]
    fn should_rollover_default_cf_successfully() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Write some data to the memtable
        engine.put(&cf, b"key1", b"value1").unwrap();
        engine.put(&cf, b"key2", b"value2").unwrap();

        // Act
        let result = engine.rollover_and_queue_flush(DEFAULT_CF_ID);

        // Assert
        assert!(result.is_ok());
        let seq = result.unwrap();
        assert!(seq > 0, "Sequence number should be greater than 0");
    }

    #[test]
    fn should_rollover_custom_cf_successfully() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine
            .create_column_family("test_cf", crate::api::ColumnFamilyConfig::default())
            .unwrap();

        // Write some data to the custom CF memtable
        engine.put(&cf, b"key1", b"value1").unwrap();
        engine.put(&cf, b"key2", b"value2").unwrap();

        // Act
        let result = engine.rollover_and_queue_flush(cf.id());

        // Assert
        assert!(result.is_ok());
        let seq = result.unwrap();
        assert!(seq > 0, "Sequence number should be greater than 0");
    }

    #[test]
    fn should_increment_sequence_number_after_rollover() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        engine.put(&cf, b"key1", b"value1").unwrap();
        let seq_before = engine.rollover_and_queue_flush(DEFAULT_CF_ID).unwrap();

        // Act
        engine.put(&cf, b"key2", b"value2").unwrap();
        let seq_after = engine.rollover_and_queue_flush(DEFAULT_CF_ID).unwrap();

        // Assert
        assert!(
            seq_after > seq_before,
            "Sequence number should increase after rollover"
        );
    }

    #[test]
    fn should_rollover_empty_memtable_successfully() {
        // Arrange
        let engine = create_test_engine();

        // Act
        let result = engine.rollover_and_queue_flush(DEFAULT_CF_ID);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_drain_memtable_entries_during_rollover() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Write data
        engine.put(&cf, b"key1", b"value1").unwrap();
        engine.put(&cf, b"key2", b"value2").unwrap();

        // Act
        engine.rollover_and_queue_flush(DEFAULT_CF_ID).unwrap();

        // Assert
        let value1 = engine.get(&cf, b"key1").unwrap();
        let value2 = engine.get(&cf, b"key2").unwrap();
        assert!(value1.is_some());
        assert!(value2.is_some());
    }

    #[test]
    fn should_handle_delete_range_tombstones_during_rollover() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Write data and delete range
        engine.put(&cf, b"key1", b"value1").unwrap();
        engine.put(&cf, b"key2", b"value2").unwrap();
        engine.delete_range(&cf, b"key1", b"key3").unwrap();

        // Act
        let result = engine.rollover_and_queue_flush(DEFAULT_CF_ID);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_handle_multiple_rollover_calls() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Act
        engine.put(&cf, b"key1", b"value1").unwrap();
        let seq1 = engine.rollover_and_queue_flush(DEFAULT_CF_ID).unwrap();

        engine.put(&cf, b"key2", b"value2").unwrap();
        let seq2 = engine.rollover_and_queue_flush(DEFAULT_CF_ID).unwrap();

        engine.put(&cf, b"key3", b"value3").unwrap();
        let seq3 = engine.rollover_and_queue_flush(DEFAULT_CF_ID).unwrap();

        // Assert
        assert!(seq1 < seq2);
        assert!(seq2 < seq3);
    }

    #[test]
    fn should_isolate_rollover_between_column_families() {
        // Arrange
        let engine = create_test_engine();
        let default_cf = engine.default_column_family();
        let custom_cf = engine
            .create_column_family("test_cf", crate::api::ColumnFamilyConfig::default())
            .unwrap();

        // Write to both CFs
        engine.put(&default_cf, b"key1", b"value1").unwrap();
        engine.put(&custom_cf, b"key2", b"value2").unwrap();

        // Act
        let seq_default = engine.rollover_and_queue_flush(DEFAULT_CF_ID).unwrap();

        // Assert
        let value_custom = engine.get(&custom_cf, b"key2").unwrap();
        assert!(value_custom.is_some());
        assert!(seq_default > 0);
    }

    #[test]
    fn should_rotate_wal_during_rollover() {
        // Arrange
        let temp_dir = tempfile::TempDir::new().unwrap();
        let opts = crate::MidgeOptions {
            storage_mode: crate::StorageMode::LocalDisk {
                db_path: temp_dir.path().to_path_buf(),
            },
            ..Default::default()
        };
        let engine = Arc::new(MidgeEngine::open(opts).expect("Failed to create test engine"));
        let cf = engine.default_column_family();

        // Write data to create WAL entries
        engine.put(&cf, b"key1", b"value1").unwrap();

        // Act
        let result = engine.rollover_and_queue_flush(DEFAULT_CF_ID);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_preserve_data_after_rollover() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Write data before rollover
        engine.put(&cf, b"before_key", b"before_value").unwrap();

        // Act
        engine.rollover_and_queue_flush(DEFAULT_CF_ID).unwrap();

        // Write data after rollover
        engine.put(&cf, b"after_key", b"after_value").unwrap();

        // Assert
        let before_value = engine.get(&cf, b"before_key").unwrap();
        let after_value = engine.get(&cf, b"after_key").unwrap();
        assert_eq!(
            before_value,
            Some(bytes::Bytes::from(b"before_value".to_vec()))
        );
        assert_eq!(
            after_value,
            Some(bytes::Bytes::from(b"after_value".to_vec()))
        );
    }

    #[test]
    fn should_handle_large_memtable_rollover() {
        // Arrange
        let engine = create_test_engine();
        let cf = engine.default_column_family();

        // Write many entries
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            engine.put(&cf, key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Act
        let result = engine.rollover_and_queue_flush(DEFAULT_CF_ID);

        // Assert
        assert!(result.is_ok());
        let seq = result.unwrap();
        assert!(seq > 0, "Sequence number should be greater than 0");
    }
}
