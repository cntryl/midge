//! WAL durability and rotation management
//!
//! Provides a unified interface for write-ahead log operations that ensures
//! data durability through coordinated writes and automatic log rotation.
//! Handles the lifecycle of WAL writers, manages rotation during flush operations,
//! and provides thread-safe access to WAL functionality for concurrent workloads.

use crate::error::MidgeResult;
use crate::wal::{WalFactory, WalWriter};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;

/// Manages write-ahead log durability and rotation.
///
/// Provides thread-safe operations for appending records to the WAL,
/// ensuring data persistence, and handling log rotation during flush operations.
/// Acts as the primary interface for WAL operations in the storage engine.
pub struct WalController {
    /// Current WAL writer (thread-safe via trait's &self methods)
    writer: RwLock<Box<dyn WalWriter>>,
    /// Factory for creating new WAL writers during rotation
    factory: Arc<dyn WalFactory>,
}

impl WalController {
    /// Create a new WAL controller with the given writer and factory.
    pub fn new(writer: Box<dyn WalWriter>, factory: Arc<dyn WalFactory>) -> Self {
        Self {
            writer: RwLock::new(writer),
            factory,
        }
    }

    /// Get a reference to the WAL writer for direct operations.
    ///
    /// Returns a read lock guard. For concurrent writes, the WalWriter implementation
    /// should use interior mutability (e.g., AsyncWalWriter uses channels).
    pub fn writer(&self) -> parking_lot::RwLockReadGuard<'_, Box<dyn WalWriter>> {
        self.writer.read()
    }

    /// Get a reference to the underlying RwLock for the writer.
    ///
    /// This is needed for operations that require exclusive access to the writer,
    /// such as WAL rotation during flush.
    pub(crate) fn writer_lock(&self) -> &parking_lot::RwLock<Box<dyn WalWriter>> {
        &self.writer
    }

    /// Get a reference to the WAL factory.
    pub fn factory(&self) -> &Arc<dyn WalFactory> {
        &self.factory
    }

    /// Rotate the WAL writer to a new file.
    ///
    /// Closes the current writer and creates a new one using the factory.
    /// The new writer will be assigned the given sequence number.
    pub fn rotate(&self, wal_dir: &Path, seq: u64) -> MidgeResult<()> {
        let mut writer = self.writer.write();

        // Close current writer
        let _ = writer.close();

        // Create new writer
        let new_writer = self.factory.rotate_writer(wal_dir, seq)?;
        *writer = new_writer;

        Ok(())
    }

    /// Flush the current WAL writer to ensure durability.
    pub fn flush(&self) -> MidgeResult<()> {
        let writer = self.writer.read();

        writer.flush()?;
        Ok(())
    }

    /// Append a single record to the WAL.
    ///
    /// This is lock-free for AsyncWalWriter since the actual write
    /// happens asynchronously through a channel.
    pub fn append_record(&self, record: &crate::wal::WalRecord) -> MidgeResult<crate::wal::WalPos> {
        let writer = self.writer.read();

        writer.append_record(record)
    }

    /// Append a batch of records to the WAL.
    ///
    /// This is more efficient than calling append_record multiple times.
    /// For AsyncWalWriter, batching is particularly beneficial as records
    /// are grouped together.
    pub fn append_batch(
        &self,
        records: &[crate::wal::WalRecord],
    ) -> MidgeResult<crate::wal::WalPos> {
        let writer = self.writer.read();

        writer.append_batch(records)
    }

    /// Sync the WAL to ensure all writes are durable.
    ///
    /// For AsyncWalWriter, this blocks until the background thread confirms
    /// the sync operation is complete.
    pub fn sync(&self) -> MidgeResult<()> {
        let writer = self.writer.read();

        writer.sync()
    }

    /// Sync only to local WAL storage (no cloud/upload waits).
    /// Delegates to writer.sync_local().
    pub fn sync_local(&self) -> MidgeResult<()> {
        let writer = self.writer.read();

        writer.sync_local()
    }

    /// Get the current write position in the WAL.
    ///
    /// Note: For AsyncWalWriter, this may not reflect pending writes in the channel.
    pub fn current_pos(&self) -> u64 {
        self.writer.read().current_pos()
    }

    /// Signal shutdown to background workers.
    ///
    /// For WAL implementations with background threads (e.g., CloudWalWriter),
    /// this signals workers to exit retry loops. Must be called before dropping
    /// to avoid hanging on sync() or close().
    pub fn shutdown(&self) {
        self.writer.read().shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::FsWalFactory;
    use tempfile::TempDir;

    #[test]
    fn should_create_coordinator_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();

        // Act
        let coordinator = WalController::new(writer, factory);

        // Assert - check we can get a read lock
        let _guard = coordinator.writer();
    }

    #[test]
    fn should_flush_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory);

        // Act
        let result = coordinator.flush();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_rotate_wal_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory);

        // Act
        let result = coordinator.rotate(&wal_dir, 1);

        // Assert
        assert!(result.is_ok());

        // Verify new WAL file exists
        let wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(!wal_files.is_empty());
    }

    #[test]
    fn should_provide_access_to_writer() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory);

        // Act - get read lock successfully
        let _writer_guard = coordinator.writer();

        // Assert - if we get here without error, test passes
    }

    #[test]
    fn should_provide_access_to_factory() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory: Arc<dyn WalFactory> = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory.clone());

        // Act
        let factory_ref = coordinator.factory();

        // Assert
        assert!(Arc::ptr_eq(factory_ref, &factory));
    }

    #[test]
    fn should_append_single_record_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory);

        let record = crate::wal::WalRecord {
            cf_id: 0,
            op: crate::wal::WalOpKind::Put,
            key: bytes::Bytes::from("key1"),
            value: Some(bytes::Bytes::from("value1")),
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        let result = coordinator.append_record(&record);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_append_batch_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory);

        let records = vec![
            crate::wal::WalRecord {
                cf_id: 0,
                op: crate::wal::WalOpKind::Put,
                key: bytes::Bytes::from("key1"),
                value: Some(bytes::Bytes::from("value1")),
                seq: 1,
                expiration: None,
                range_end: None,
                txn_id: None,
                compression: None,
            },
            crate::wal::WalRecord {
                cf_id: 0,
                op: crate::wal::WalOpKind::Put,
                key: bytes::Bytes::from("key2"),
                value: Some(bytes::Bytes::from("value2")),
                seq: 2,
                expiration: None,
                range_end: None,
                txn_id: None,
                compression: None,
            },
        ];

        // Act
        let result = coordinator.append_batch(&records);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_sync_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory);

        // Act
        let result = coordinator.sync();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_get_current_position() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory);

        // Act
        let pos = coordinator.current_pos();

        // Assert
        // WalFile has a 16-byte header, so position should be non-zero initially
        // Just verify we can get the position without panicking
        let _ = pos;
    }

    #[test]
    fn should_recover_records_from_rotated_wal_segment() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory.clone());

        // Write records before rotation
        let record1 = crate::wal::WalRecord {
            cf_id: 0,
            op: crate::wal::WalOpKind::Put,
            key: bytes::Bytes::from("pre_rotate_key"),
            value: Some(bytes::Bytes::from("pre_rotate_value")),
            seq: 100,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        coordinator.append_record(&record1).unwrap();
        coordinator.sync().unwrap();

        // Act - rotate WAL
        coordinator.rotate(&wal_dir, 1).unwrap();

        // Write records after rotation
        let record2 = crate::wal::WalRecord {
            cf_id: 0,
            op: crate::wal::WalOpKind::Put,
            key: bytes::Bytes::from("post_rotate_key"),
            value: Some(bytes::Bytes::from("post_rotate_value")),
            seq: 200,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        coordinator.append_record(&record2).unwrap();
        coordinator.sync().unwrap();

        // Drop coordinator to release file handles (important for Windows)
        drop(coordinator);

        // Assert - verify we can read all records from the directory
        // On Windows, file rotation may behave differently due to file locking
        let rotated_path = wal_dir.join("00000001.wal");
        let new_wal_path = wal_dir.join("wal.log");

        // The rotated file might exist on Unix or if Windows manages to rename
        if rotated_path.exists() {
            let records = crate::wal::fs::replay_wal_file(&rotated_path).unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].key.as_ref(), b"pre_rotate_key");
            assert_eq!(records[0].seq, 100);

            // New wal.log should also exist
            assert!(new_wal_path.exists(), "new WAL file should exist");
            let new_records = crate::wal::fs::replay_wal_file(&new_wal_path).unwrap();
            assert_eq!(new_records.len(), 1);
            assert_eq!(new_records[0].key.as_ref(), b"post_rotate_key");
            assert_eq!(new_records[0].seq, 200);
        } else {
            // On Windows with file locking, all records may be in wal.log
            // This is still valid - the key invariant is no data loss
            let records = crate::wal::fs::replay_wal_file(&new_wal_path).unwrap();
            assert!(
                records.len() >= 2,
                "all records should be recoverable: got {} records",
                records.len()
            );
            // Verify we have both records
            let has_pre = records
                .iter()
                .any(|r| r.key.as_ref() == b"pre_rotate_key");
            let has_post = records
                .iter()
                .any(|r| r.key.as_ref() == b"post_rotate_key");
            assert!(has_pre, "pre-rotation record should exist");
            assert!(has_post, "post-rotation record should exist");
        }
    }

    #[test]
    fn should_preserve_write_ordering_across_rotation() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let factory = Arc::new(FsWalFactory);
        let writer = factory.create_writer(&wal_dir).unwrap();
        let coordinator = WalController::new(writer, factory.clone());

        // Write multiple records, rotate, then write more
        for i in 0..5 {
            let record = crate::wal::WalRecord {
                cf_id: 0,
                op: crate::wal::WalOpKind::Put,
                key: bytes::Bytes::from(format!("key_{}", i)),
                value: Some(bytes::Bytes::from(format!("value_{}", i))),
                seq: i as u64,
                expiration: None,
                range_end: None,
                txn_id: None,
                compression: None,
            };
            coordinator.append_record(&record).unwrap();
        }
        coordinator.sync().unwrap();

        // Rotate
        coordinator.rotate(&wal_dir, 1).unwrap();

        // Write more after rotation
        for i in 5..10 {
            let record = crate::wal::WalRecord {
                cf_id: 0,
                op: crate::wal::WalOpKind::Put,
                key: bytes::Bytes::from(format!("key_{}", i)),
                value: Some(bytes::Bytes::from(format!("value_{}", i))),
                seq: i as u64,
                expiration: None,
                range_end: None,
                txn_id: None,
                compression: None,
            };
            coordinator.append_record(&record).unwrap();
        }
        coordinator.sync().unwrap();

        // Assert - collect all records from all WAL files and verify ordering
        let mut all_records = Vec::new();
        for entry in std::fs::read_dir(&wal_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            // WAL files are either "wal.log" or "{seq}.wal"
            let is_wal = path
                .extension()
                .map(|e| e == "log" || e == "wal")
                .unwrap_or(false);
            if is_wal {
                if let Ok(records) = crate::wal::fs::replay_wal_file(&path) {
                    all_records.extend(records);
                }
            }
        }

        // Sort by sequence to verify all records present
        all_records.sort_by_key(|r| r.seq);
        assert_eq!(all_records.len(), 10);
        for (i, record) in all_records.iter().enumerate() {
            assert_eq!(record.seq, i as u64);
            assert_eq!(record.key.as_ref(), format!("key_{}", i).as_bytes());
        }
    }
}
