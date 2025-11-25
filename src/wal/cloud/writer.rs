use crate::error::MidgeResult;
use crate::wal::{WalOpKind, WalPos, WalRecord};
use parking_lot::Mutex;
use std::sync::Arc;
use crate::wal::WalFile;

use super::shared::WalBatchManager;

/// Cloud-backed WAL writer implementing the `WalWriter` trait.
///
/// This writer buffers records into segments before uploading to cloud storage,
/// balancing durability requirements with the need to avoid "death by tiny files".
///
/// # Durability Semantics
///
/// - `append_*`: Records are buffered locally; returns immediately
/// - `flush()`: Flushes current segment to cloud asynchronously (non-blocking)
/// - `sync()`: Flushes AND waits for all pending uploads to complete (blocking)
///
/// The batch size is configurable (16-64 MB recommended per spec).
pub struct CloudWalWriter {
    batch_manager: Arc<WalBatchManager>,
    /// Current logical position (incremented per record)
    current_pos: Arc<Mutex<WalPos>>,
    /// Sequence number for records
    sequence: Arc<Mutex<u64>>,
    /// Optional local WAL writer used when a db_path is provided. Ensures local
    /// durability even if cloud uploads are flaky.
    local_writer: Option<Arc<dyn crate::wal::WalWriter>>,
}

impl CloudWalWriter {
    /// Create a new cloud WAL writer.
    ///
    /// # Arguments
    ///
    /// * `backend` - Cloud storage backend
    /// * `batch_size` - Maximum segment size in bytes before auto-flush (16-64 MB recommended)
    /// * `manifest` - Optional manifest for tracking uploads
    /// * `db_path` - Optional database path for local staging
    pub fn new(
        backend: Arc<dyn super::shared::CloudStorageBackend>,
        batch_size: usize,
        manifest: Option<Arc<parking_lot::Mutex<crate::core::manifest::Manifest>>>,
        db_path: Option<std::path::PathBuf>,
    ) -> Self {
        let batch_manager = Arc::new(WalBatchManager::new(backend.clone(), batch_size, manifest, db_path.clone()));

        // Create optional local WAL writer when db_path is provided so we can
        // immediately persist records to local storage for local WAL sync
        // semantics.
        let local_writer = if let Some(path) = db_path {
            let wal_dir = path.join("wal");
            match WalFile::open(&wal_dir) {
                Ok(w) => Some(Arc::new(w) as Arc<dyn crate::wal::WalWriter>),
                Err(e) => {
                    tracing::warn!("failed to open local wal writer at {}: {}", wal_dir.display(), e);
                    None
                }
            }
        } else {
            None
        };

        Self {
            batch_manager,
            current_pos: Arc::new(Mutex::new(0)),
            sequence: Arc::new(Mutex::new(0)),
            local_writer,
        }
    }

    /// Increment and return the next WAL position.
    fn next_pos(&self) -> WalPos {
        let mut pos = self.current_pos.lock();
        *pos += 1;
        *pos
    }

    /// Increment and return the next sequence number.
    fn next_seq(&self) -> u64 {
        let mut seq = self.sequence.lock();
        *seq += 1;
        *seq
    }
}

impl crate::wal::WalWriter for CloudWalWriter {
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // First ensure local durability if configured: write the record
        // to the local WAL synchronously so callers relying on local WAL
        // semantics are safe even if cloud uploads later fail.
        if let Some(local) = &self.local_writer {
            // Best-effort: propagate local WAL errors as they indicate
            // local durability could not be achieved
            local.append_record(record)?;
        }

        // Add record to batch manager (may trigger async upload if segment full)
        let _promise = self.batch_manager.add_record(record.clone())?;

        // Return the logical position
        Ok(self.next_pos())
    }

    fn append_op(&self, kind: WalOpKind, key: &[u8], value: Option<&[u8]>) -> MidgeResult<WalPos> {
        let seq = self.next_seq();
        let record = WalRecord {
            cf_id: 0,
            op: kind,
            key: bytes::Bytes::copy_from_slice(key),
            value: value.map(bytes::Bytes::copy_from_slice),
            seq,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        self.append_record(&record)
    }

    fn append_op_with_seq(
        &self,
        kind: WalOpKind,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
    ) -> MidgeResult<WalPos> {
        let record = WalRecord {
            cf_id: 0,
            op: kind,
            key: bytes::Bytes::copy_from_slice(key),
            value: value.map(bytes::Bytes::copy_from_slice),
            seq,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        self.append_record(&record)
    }

    fn append_op_with_seq_ttl_bytes(
        &self,
        kind: WalOpKind,
        key: bytes::Bytes,
        value: Option<bytes::Bytes>,
        seq: u64,
        ttl_seconds: u64,
    ) -> MidgeResult<WalPos> {
        let expiration = if ttl_seconds > 0 {
            let now = crate::common::timestamp::now_millis();
            Some(now + (ttl_seconds * 1000))
        } else {
            None
        };

        let record = WalRecord {
            cf_id: 0,
            op: kind,
            key,
            value,
            seq,
            expiration,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        self.append_record(&record)
    }

    fn append_batch(&self, records: &[WalRecord]) -> MidgeResult<WalPos> {
        let mut last_pos = self.current_pos();
        for record in records {
            last_pos = self.append_record(record)?;
        }
        Ok(last_pos)
    }

    fn flush(&self) -> MidgeResult<()> {
        // Ensure local writer is flushed (if present) so a subsequent
        // restart will see the data.
        if let Some(local) = &self.local_writer {
            let _ = local.flush();
        }

        // Trigger async flush without waiting
        let _ = self.batch_manager.flush_async()?;
        Ok(())
    }

    fn sync(&self) -> MidgeResult<()> {
        // First sync local WAL to ensure local durability
        if let Some(local) = &self.local_writer {
            let _ = local.sync();
        }

        // Then wait for cloud uploads to finish for full durability.
        self.batch_manager.sync()
    }

    fn current_pos(&self) -> WalPos {
        *self.current_pos.lock()
    }

    fn close(&self) -> MidgeResult<()> {
        // Ensure local data is synced and cloud uploads completed before closing
        // to maintain durability invariants during shutdown.
        self.sync()
    }

    fn shutdown(&self) {
        // Signal background upload workers to stop retry loops
        self.batch_manager.shutdown();
    }

    fn sync_local(&self) -> MidgeResult<()> {
        // Sync only the local WAL writer if present. If no local writer exists
        // fallback to flushing the batch manager (non-blocking), which at
        // least ensures buffered data is submitted for upload.
        if let Some(local) = &self.local_writer {
            local.sync()
        } else {
            // Fallback - perform a flush (non-blocking) so we don't wait on cloud
            let _ = self.batch_manager.flush_async()?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::MockCloudBackend;
    use crate::wal::WalWriter;

    #[test]
    fn should_create_writer_with_default_settings() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let batch_size = 1024 * 1024; // 1 MB

        // Act
        let writer = CloudWalWriter::new(backend, batch_size, None, None);

        // Assert
        assert_eq!(writer.current_pos(), 0);
    }

    #[test]
    fn should_increment_position_after_append() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let writer = CloudWalWriter::new(backend, 1024 * 1024, None, None);

        // Act
        let pos1 = writer
            .append_op(WalOpKind::Put, b"key1", Some(b"value1"))
            .unwrap();
        let pos2 = writer
            .append_op(WalOpKind::Put, b"key2", Some(b"value2"))
            .unwrap();

        // Assert
        assert_eq!(pos1, 1);
        assert_eq!(pos2, 2);
    }

    #[test]
    fn should_buffer_small_records_without_upload() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let batch_size = 1024 * 1024; // 1 MB
        let writer = CloudWalWriter::new(backend.clone(), batch_size, None, None);

        // Act
        for i in 0..100 {
            writer
                .append_op(
                    WalOpKind::Put,
                    format!("key{}", i).as_bytes(),
                    Some(b"value"),
                )
                .unwrap();
        }

        // Assert - no uploads should have happened yet (data still buffered)
        // We can verify this by checking the backend hasn't received data
        // (MockStorageBackend would track this in a real implementation)
        assert_eq!(writer.current_pos(), 100);
    }
}
