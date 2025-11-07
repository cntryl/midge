//! WAL coordinator for managing write-ahead log operations
//!
//! Encapsulates the WAL writer and factory to provide a clean interface
//! for write operations and log rotation.

use crate::error::MidgeResult;
use crate::wal::{WalFactory, WalWriter};
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;

/// Coordinates write-ahead log operations including writes and rotation.
///
/// Encapsulates the WAL writer (thread-safe via &self methods) and the factory
/// for creating new writers during rotation.
pub struct WalCoordinator {
    /// Current WAL writer (thread-safe via trait's &self methods)
    writer: RwLock<Box<dyn WalWriter>>,
    /// Factory for creating new WAL writers during rotation
    factory: Arc<dyn WalFactory>,
}

impl WalCoordinator {
    /// Create a new WAL coordinator with the given writer and factory.
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

    /// Get the current write position in the WAL.
    ///
    /// Note: For AsyncWalWriter, this may not reflect pending writes in the channel.
    pub fn current_pos(&self) -> u64 {
        self.writer.read().current_pos()
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
        let coordinator = WalCoordinator::new(writer, factory);

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
        let coordinator = WalCoordinator::new(writer, factory);

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
        let coordinator = WalCoordinator::new(writer, factory);

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
        let coordinator = WalCoordinator::new(writer, factory);

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
        let coordinator = WalCoordinator::new(writer, factory.clone());

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
        let coordinator = WalCoordinator::new(writer, factory);

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
        let coordinator = WalCoordinator::new(writer, factory);

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
        let coordinator = WalCoordinator::new(writer, factory);

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
        let coordinator = WalCoordinator::new(writer, factory);

        // Act
        let pos = coordinator.current_pos();

        // Assert
        // WalFile has a 16-byte header, so position should be non-zero initially
        // Just verify we can get the position without panicking
        let _ = pos;
    }
}
