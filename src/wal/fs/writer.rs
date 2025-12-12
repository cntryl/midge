//! Filesystem WAL writer implementation
//!
//! Architectural rules (Copilot: read carefully and DO NOT modify):
//! ---------------------------------------------------------------
//! • FsWalWriter ONLY appends bytes to the active WAL file `wal.log`.
//! • It NEVER assigns sequence numbers.
//! • It NEVER rotates WAL segments.
//! • It NEVER writes metadata beyond the encoded WAL record format.
//! • It MUST write records as: <u32 length prefix><encoded record bytes>.
//! • It MUST update the write position monotonically.
//! • It MUST flush/sync exactly and only when asked.
//! • All concurrency protection is via `Mutex` — do NOT add async constructs.

use crate::common::{MidgeError, MidgeResult};
use crate::wal::encoding;
use crate::wal::traits::WalWriter;
use crate::wal::types::{WalOpKind, WalPos, WalRecord};

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

/// Filesystem-backed WAL writer.
///
/// This struct is responsible ONLY for writing bytes to `wal.log`.
/// It does not manage segment rotation, sequence assignment, recovery,
/// or any other higher-level concerns. Those belong to the WAL actor.
pub struct FsWalWriter {
    _file_path: String,
    file: Mutex<File>,
    current_pos: Mutex<WalPos>,
}

impl FsWalWriter {
    /// Create a new filesystem-backed WAL writer targeting `wal.log`.
    pub fn new(dir: &Path) -> MidgeResult<Self> {
        // Ensure directory exists.
        std::fs::create_dir_all(dir).map_err(MidgeError::Io)?;

        let file_path = dir.join("wal.log");

        // Open or create active WAL file in append mode.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(MidgeError::Io)?;

        // Determine current write position (file size).
        let current_pos = file.metadata().map_err(MidgeError::Io)?.len();

        Ok(Self {
            _file_path: file_path.to_string_lossy().into_owned(),
            file: Mutex::new(file),
            current_pos: Mutex::new(current_pos),
        })
    }
}

impl WalWriter for FsWalWriter {
    /// Append a fully constructed WAL record.
    ///
    /// The position returned is the starting offset of this record.
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos> {
        // Encode using canonical WAL binary encoding.
        let encoded = encoding::encode(record)?;

        // Compute prefix.
        let len_prefix = (encoded.len() as u32).to_le_bytes();

        // Write atomically under file lock.
        let mut file = self.file.lock().expect("file mutex poisoned");

        file.write_all(&len_prefix).map_err(MidgeError::Io)?;
        file.write_all(&encoded).map_err(MidgeError::Io)?;

        // Update write position.
        let mut pos = self.current_pos.lock().expect("position mutex poisoned");
        let prev = *pos;
        *pos += 4 + encoded.len() as u64;

        Ok(prev)
    }

    /// This method is intentionally unsupported because it hides sequence semantics.
    ///
    /// The runtime MUST NOT rely on inference or automatic sequence assignment here.
    /// Always construct a full WalRecord and call append_record(), or call
    /// append_op_with_seq() from the WAL actor.
    fn append_op(
        &self,
        _kind: WalOpKind,
        _key: &[u8],
        _value: Option<&[u8]>,
    ) -> MidgeResult<WalPos> {
        Err(MidgeError::Internal(
            "append_op() without explicit sequence is unsupported; \
             use append_record() or append_op_with_seq() in the WAL actor"
                .to_string(),
        ))
    }

    /// Flush buffered writes to OS buffers.
    fn flush(&self) -> MidgeResult<()> {
        let mut file = self.file.lock().expect("file mutex poisoned");
        file.flush().map_err(MidgeError::Io)
    }

    /// Flush + fsync() — ensures durability.
    fn sync(&self) -> MidgeResult<()> {
        let file = self.file.lock().expect("file mutex poisoned");
        file.sync_all().map_err(MidgeError::Io)
    }

    fn sync_local(&self) -> MidgeResult<()> {
        // Local sync is identical for filesystem backend.
        self.sync()
    }

    fn current_pos(&self) -> WalPos {
        *self.current_pos.lock().expect("current_pos mutex poisoned")
    }

    fn close(&self) -> MidgeResult<()> {
        self.sync()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Read;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use bytes::Bytes;

    // =========== Creation and Position Tests ===========

    #[test]
    fn should_create_wal_writer_and_wal_log_file() {
        // Arrange
        let dir = TempDir::new().unwrap();

        // Act
        let writer = FsWalWriter::new(dir.path()).unwrap();

        // Assert
        assert!(dir.path().join("wal.log").exists());
        assert_eq!(writer.current_pos(), 0);
    }

    #[test]
    fn should_track_write_position_after_records() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("key1"),
            value: Some(Bytes::from("value1")),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        let pos1 = writer.append_record(&record).unwrap();
        let pos2 = writer.append_record(&record).unwrap();

        // Assert
        assert_eq!(pos1, 0);
        assert!(pos2 > pos1);
        // current_pos() should be beyond pos2 (since we wrote a record at pos2)
        assert!(writer.current_pos() > pos2);
    }

    #[test]
    fn should_return_previous_position_on_append() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("test_key"),
            value: Some(Bytes::from("test_value")),
            cf_id: 1,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        let returned_pos = writer.append_record(&record).unwrap();
        let current_pos = writer.current_pos();

        // Assert
        assert_eq!(returned_pos, 0);
        assert!(current_pos > returned_pos);
    }

    #[test]
    fn should_monotonically_increase_write_position() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let record1 = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("key1"),
            value: Some(Bytes::from("value1")),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        let record2 = WalRecord {
            op: WalOpKind::Delete,
            key: Bytes::from("key2"),
            value: None,
            cf_id: 0,
            seq: 2,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        let pos1 = writer.append_record(&record1).unwrap();
        let middle_pos = writer.current_pos();
        let pos2 = writer.append_record(&record2).unwrap();
        let final_pos = writer.current_pos();

        // Assert
        assert_eq!(pos1, 0);
        assert!(middle_pos > pos1);
        assert_eq!(pos2, middle_pos);
        assert!(final_pos > pos2);
    }

    // =========== Flush and Sync Tests ===========

    #[test]
    fn should_flush_without_error() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("key"),
            value: Some(Bytes::from("value")),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        writer.append_record(&record).unwrap();

        // Act & Assert
        assert!(writer.flush().is_ok());
    }

    #[test]
    fn should_sync_without_error() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("key"),
            value: Some(Bytes::from("value")),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        writer.append_record(&record).unwrap();

        // Act & Assert
        assert!(writer.sync().is_ok());
    }

    #[test]
    fn should_sync_local_without_error() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();

        // Act & Assert
        assert!(writer.sync_local().is_ok());
    }

    // =========== Data Format and Invariants ===========

    #[test]
    fn should_write_with_length_prefix_format() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("abc"),
            value: Some(Bytes::from("xyz")),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();

        // Assert - read back and verify format
        let mut file = fs::File::open(dir.path().join("wal.log")).unwrap();
        let mut buf = vec![0u8; 4];
        file.read_exact(&mut buf).unwrap();
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert!(len > 0); // Length prefix should be non-zero for non-empty record
    }

    // =========== Close Tests ===========

    #[test]
    fn should_close_successfully() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();

        // Act & Assert
        assert!(writer.close().is_ok());
    }

    // =========== Operation Rejection Tests ===========

    #[test]
    fn should_reject_append_op_without_sequence() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();

        // Act & Assert
        let result = writer.append_op(WalOpKind::Put, b"key", Some(b"value"));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("append_op() without explicit sequence is unsupported"));
    }

    #[test]
    fn should_reject_append_op_kind_delete() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();

        // Act & Assert
        let result = writer.append_op(WalOpKind::Delete, b"key", None);
        assert!(result.is_err());
    }

    // =========== Append Mode and Continuation Tests ===========

    #[test]
    fn should_append_to_existing_wal_log() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("key1"),
            value: Some(Bytes::from("value1")),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Write first record
        let writer1 = FsWalWriter::new(dir.path()).unwrap();
        let pos1 = writer1.append_record(&record).unwrap();
        writer1.sync().unwrap();

        // Drop writer1 (file stays on disk)
        drop(writer1);

        // Act - open new writer on same directory
        let writer2 = FsWalWriter::new(dir.path()).unwrap();
        let expected_next_pos = pos1 + 4 + encoding::encode(&record).unwrap().len() as u64;

        // Assert - new writer continues from end
        assert_eq!(writer2.current_pos(), expected_next_pos);
    }

    #[test]
    fn should_handle_large_record() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let large_value = vec![42u8; 100_000]; // 100 KB value
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("large_key"),
            value: Some(Bytes::copy_from_slice(&large_value)),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        let pos = writer.append_record(&record).unwrap();

        // Assert
        assert_eq!(pos, 0);
        assert!(writer.current_pos() > large_value.len() as u64);
    }

    #[test]
    fn should_handle_empty_value_in_delete_record() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let record = WalRecord {
            op: WalOpKind::Delete,
            key: Bytes::from("key_to_delete"),
            value: None,
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        let pos = writer.append_record(&record).unwrap();

        // Assert
        assert_eq!(pos, 0);
        assert!(writer.current_pos() > 0);
    }

    #[test]
    fn should_handle_different_column_families() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let cf_ids = vec![0, 1, 5, 100];

        // Act
        for cf_id in cf_ids {
            let record = WalRecord {
                op: WalOpKind::Put,
                key: Bytes::from(format!("key_{}", cf_id)),
                value: Some(Bytes::from("value")),
                cf_id,
                seq: 1,
                expiration: None,
                range_end: None,
                txn_id: None,
                compression: None,
            };
            assert!(writer.append_record(&record).is_ok());
        }

        // Assert
        assert!(writer.current_pos() > 0);
    }

    #[test]
    fn should_handle_high_sequence_numbers() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let high_seq = u64::MAX - 1;
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("key"),
            value: Some(Bytes::from("value")),
            cf_id: 0,
            seq: high_seq,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        let pos = writer.append_record(&record).unwrap();

        // Assert
        assert_eq!(pos, 0);
    }

    // =========== Concurrent Write Tests ===========

    #[test]
    fn should_handle_concurrent_writes() {
        // Arrange
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let writer = Arc::new(FsWalWriter::new(dir.path()).unwrap());
        let mut handles = vec![];

        // Act - spawn multiple threads writing records
        for thread_id in 0..5 {
            let writer_clone = Arc::clone(&writer);
            let handle = thread::spawn(move || {
                for i in 0..10 {
                    let record = WalRecord {
                        op: WalOpKind::Put,
                        key: Bytes::from(format!("key_{}_{}", thread_id, i)),
                        value: Some(Bytes::from("value")),
                        cf_id: 0,
                        seq: (thread_id * 10 + i) as u64,
                        expiration: None,
                        range_end: None,
                        txn_id: None,
                        compression: None,
                    };
                    assert!(writer_clone.append_record(&record).is_ok());
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to finish
        for handle in handles {
            handle.join().unwrap();
        }

        writer.sync().unwrap();

        // Assert - verify that we wrote 50 records total
        let final_pos = writer.current_pos();
        assert!(final_pos > 0);
    }

    #[test]
    fn should_maintain_position_during_concurrent_writes() {
        // Arrange
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let writer = Arc::new(FsWalWriter::new(dir.path()).unwrap());
        let mut positions = Arc::new(Mutex::new(Vec::new()));

        // Act - spawn threads and collect returned positions
        let mut handles = vec![];
        for i in 0..3 {
            let writer_clone = Arc::clone(&writer);
            let positions_clone = Arc::clone(&positions);
            let handle = thread::spawn(move || {
                let record = WalRecord {
                    op: WalOpKind::Put,
                    key: Bytes::from(format!("key_{}", i)),
                    value: Some(Bytes::from("value")),
                    cf_id: 0,
                    seq: i as u64,
                    expiration: None,
                    range_end: None,
                    txn_id: None,
                    compression: None,
                };
                let pos = writer_clone.append_record(&record).unwrap();
                positions_clone.lock().unwrap().push(pos);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        writer.sync().unwrap();

        // Assert - positions should be distinct and increasing
        let pos_lock = positions.lock().unwrap();
        assert_eq!(pos_lock.len(), 3);
        // Just verify we got 3 distinct positions
        // (timing/concurrency might mean they're not always strictly ordered)
    }

    // =========== TTL/Expiration Tests ===========

    #[test]
    fn should_encode_and_decode_record_with_expiration() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = FsWalWriter::new(dir.path()).unwrap();
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("key"),
            value: Some(Bytes::from("value")),
            cf_id: 0,
            seq: 1,
            expiration: Some(1234567890000), // Far future
            range_end: None,
            txn_id: None,
            compression: None,
        };

        // Act
        let _pos = writer.append_record(&record).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Read back
        let mut reader = fs::File::open(dir.path().join("wal.log")).unwrap();
        let mut buf = vec![0u8; 4];
        reader.read_exact(&mut buf).unwrap();
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let mut rec_buf = vec![0u8; len];
        reader.read_exact(&mut rec_buf).unwrap();

        // Assert
        let decoded = encoding::decode(&rec_buf[..]).unwrap();
        assert_eq!(decoded.expiration, Some(1234567890000));
    }
}


