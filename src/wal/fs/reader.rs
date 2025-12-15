//! Filesystem WAL reader implementation
//!
//! Architectural invariants (Copilot: DO NOT VIOLATE):
//! --------------------------------------------------
//! • FsWalReader reads **only** from the active WAL file `wal.log`.
//! • It must treat EOF mid-record as **corruption**, not success.
//! • It must not assume the file ends cleanly — RocksDB, Pebble,
//!   TiKV, and LMDB all rely on readers being strict and defensive.
//! • It must use the canonical format:
//!       <u32 length prefix><encoded record bytes>
//! • It must NOT attempt to fix, truncate, or adjust the file.
//! • It must update `current_pos` monotonically.
//!
//! This reader is intentionally synchronous and blocking —
//! higher-level async abstractions belong in the runtime.

use crate::common::{MidgeError, MidgeResult};
use crate::storage::abstraction::{
    OpenMode, OpenOptions, Storage, StorageError, StorageErrorKind, StorageFile, StoragePath,
};
use crate::wal::encoding;
use crate::wal::traits::{WalReader, WalReaderDyn};
use crate::wal::types::{WalPos, WalRecord};

fn map_storage_error(err: StorageError) -> MidgeError {
    match err.kind {
        StorageErrorKind::NotFound => MidgeError::NotFound,
        StorageErrorKind::Unsupported => MidgeError::NotSupported(err.message),
        StorageErrorKind::Corruption => MidgeError::Corruption(err.message),
        StorageErrorKind::InvalidInput => MidgeError::InvalidArgument(err.message),
        _ => MidgeError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            err.to_string(),
        )),
    }
}

/// Filesystem-backed WAL reader.
///
/// This struct provides low-level, corruption-aware reading semantics.
pub struct FsWalReader {
    file: Box<dyn StorageFile>,
    current_pos: WalPos,
}

impl FsWalReader {
    /// Open `wal.log` in read-only mode.
    pub fn new(storage: &dyn Storage, dir: &StoragePath) -> MidgeResult<Self> {
        let path = super::join(dir, "wal.log");
        let file = storage
            .open_file(
                &path,
                OpenOptions {
                    mode: OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                    append: false,
                },
            )
            .map_err(map_storage_error)?;

        Ok(Self {
            file,
            current_pos: 0,
        })
    }
}

impl WalReader for FsWalReader {
    /// Read a single WAL record at an explicit offset.
    ///
    /// Returns:
    /// - Ok(Some(record)) if a valid record is found
    /// - Ok(None) if clean EOF at `pos`
    /// - Err(Corruption) if EOF occurs mid-record
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        // Read 4-byte length prefix
        let len_bytes = self.file.read_at(pos, 4).map_err(map_storage_error)?;
        if len_bytes.is_empty() {
            return Ok(None);
        }
        if len_bytes.len() < 4 {
            return Err(MidgeError::Corruption(format!(
                "Incomplete WAL length prefix at pos {} (got {} bytes)",
                pos,
                len_bytes.len()
            )));
        }

        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&len_bytes[..4]);

        let len = u32::from_le_bytes(len_buf) as usize;

        // Read the encoded record bytes
        let buf = self
            .file
            .read_at(pos + 4, len as u64)
            .map_err(map_storage_error)?;
        if buf.len() < len {
            return Err(MidgeError::Corruption(format!(
                "Incomplete WAL record at pos {} (len={}, got={})",
                pos,
                len,
                buf.len()
            )));
        }

        let record = encoding::decode(&buf[..])?;
        self.current_pos = pos + 4 + len as u64;

        Ok(Some(record))
    }

    /// Replay WAL from `start` forward, invoking callback for each record.
    ///
    /// Stops at:
    /// - clean EOF → success
    /// - corruption → error
    /// - callback error → error
    fn replay<F>(&mut self, start: WalPos, mut cb: F) -> MidgeResult<()>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>,
    {
        let mut pos = start;

        loop {
            // Read the length prefix
            let len_bytes = self.file.read_at(pos, 4).map_err(map_storage_error)?;
            if len_bytes.is_empty() {
                break; // Clean EOF
            }
            if len_bytes.len() < 4 {
                return Err(MidgeError::Corruption(format!(
                    "Incomplete WAL length prefix at pos {} (got {} bytes)",
                    pos,
                    len_bytes.len()
                )));
            }

            let mut len_buf = [0u8; 4];
            len_buf.copy_from_slice(&len_bytes[..4]);

            let len = u32::from_le_bytes(len_buf) as usize;

            // Read record payload
            let buf = self
                .file
                .read_at(pos + 4, len as u64)
                .map_err(map_storage_error)?;
            if buf.len() < len {
                return Err(MidgeError::Corruption(format!(
                    "Incomplete WAL record at pos {} (len={}, got={})",
                    pos,
                    len,
                    buf.len()
                )));
            }

            // Decode
            let record = encoding::decode(&buf[..])?;

            // Dispatch to callback
            cb(&record)?;

            pos += 4 + len as u64;
        }

        self.current_pos = pos;
        Ok(())
    }

    fn close(&mut self) -> MidgeResult<()> {
        // File closes automatically — nothing to do.
        Ok(())
    }
}

/// Object-safe wrapper for trait objects.
impl WalReaderDyn for FsWalReader {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>> {
        WalReader::read_at(self, pos)
    }

    fn replay_boxed(
        &mut self,
        start: WalPos,
        cb: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    ) -> MidgeResult<()> {
        self.replay(start, cb)
    }

    fn close(&mut self) -> MidgeResult<()> {
        WalReader::close(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::{build_temp_local_storage, TempLocalStorage};
    use crate::wal::fs::FsWalWriter;
    use crate::wal::traits::{WalReader, WalWriter};
    use crate::wal::types::{WalOpKind, WalRecord};
    use bytes::Bytes;

    fn new_writer(temp: &TempLocalStorage) -> FsWalWriter {
        FsWalWriter::new(temp.storage.as_ref(), &temp.root).unwrap()
    }

    fn new_reader(temp: &TempLocalStorage) -> MidgeResult<FsWalReader> {
        FsWalReader::new(temp.storage.as_ref(), &temp.root)
    }

    // =========== Reader Creation and Initialization Tests ===========

    #[test]
    fn should_create_reader_for_existing_wal_log() {
        // Arrange
        let temp = build_temp_local_storage().unwrap();

        // Create a writer to write some data
        let writer = new_writer(&temp);
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
        writer.sync().unwrap();
        drop(writer);

        // Act
    let reader = new_reader(&temp);

        // Assert
        assert!(reader.is_ok());
    }

    #[test]
    fn should_fail_to_create_reader_for_missing_wal_log() {
        // Arrange
        let temp = build_temp_local_storage().unwrap();

        // Act
        let reader = new_reader(&temp);

        // Assert
        assert!(reader.is_err());
    }

    // =========== Read At Position Tests ===========

    #[test]
    fn should_read_record_at_position_zero() {
        // Arrange
        let temp = build_temp_local_storage().unwrap();
        let writer = new_writer(&temp);
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("test_key"),
            value: Some(Bytes::from("test_value")),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&temp).unwrap();
        let read_record = WalReader::read_at(&mut reader, 0).unwrap();

        // Assert
        assert!(read_record.is_some());
        let r = read_record.unwrap();
        assert_eq!(r.key, Bytes::from("test_key"));
        assert_eq!(r.value, Some(Bytes::from("test_value")));
    }

    #[test]
    fn should_return_none_on_clean_eof() {
        // Arrange
        let temp = build_temp_local_storage().unwrap();
        let writer = new_writer(&temp);
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
        let pos = writer.append_record(&record).unwrap();
        let encoded = encoding::encode(&record).unwrap();
        let end_pos = pos + 4 + encoded.len() as u64;
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&temp).unwrap();
        let result = WalReader::read_at(&mut reader, end_pos).unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_detect_corruption_on_partial_record() {
        // Arrange
        let temp = build_temp_local_storage().unwrap();

        // Write a complete record
        let writer = new_writer(&temp);
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
        writer.sync().unwrap();
        drop(writer);

        // Truncate the file to simulate corruption (via storage abstraction):
        // rewrite only the first half of the file into a newly truncated file.
        let wal_log = super::join(&temp.root, "wal.log");
        let file_ro = temp
            .storage
            .open_file(
                &wal_log,
                crate::storage::abstraction::OpenOptions {
                    mode: crate::storage::abstraction::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                    append: false,
                },
            )
            .unwrap();
        let len = file_ro.len().unwrap();
        let half = (len / 2).max(1);
        let half_bytes = file_ro.read_at(0, half).unwrap();

        let mut file_trunc = temp
            .storage
            .open_file(
                &wal_log,
                crate::storage::abstraction::OpenOptions {
                    mode: crate::storage::abstraction::OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                    append: false,
                },
            )
            .unwrap();
        let _ = file_trunc.write_at(0, &half_bytes).unwrap();

        // Act
        let mut reader = new_reader(&temp).unwrap();
        let result = WalReader::read_at(&mut reader, 0);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Incomplete"));
    }

    #[test]
    fn should_update_current_position_after_read() {
        // Arrange
        let temp = build_temp_local_storage().unwrap();
        let writer = new_writer(&temp);
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
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&temp).unwrap();
        let _ = WalReader::read_at(&mut reader, 0).unwrap();
        let pos_after = reader.current_pos;

        // Assert
        assert!(pos_after > 0);
    }

    // =========== Replay Tests ===========

    #[test]
    fn should_replay_all_records_from_start() {
        // Arrange
        let temp = build_temp_local_storage().unwrap();
        let writer = new_writer(&temp);
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
        writer.append_record(&record1).unwrap();
        writer.append_record(&record2).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&dir).unwrap();
        let mut count = 0;
        let mut keys = Vec::new();
        let result = WalReader::replay(&mut reader, 0, |record| {
            count += 1;
            keys.push(record.key.to_vec());
            Ok(())
        });

        // Assert
        assert!(result.is_ok());
        assert_eq!(count, 2);
        assert_eq!(keys[0], b"key1");
        assert_eq!(keys[1], b"key2");
    }

    #[test]
    fn should_replay_from_middle_position() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = new_writer(&dir);
        let record1 = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("first"),
            value: Some(Bytes::from("value1")),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        let record2 = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("second"),
            value: Some(Bytes::from("value2")),
            cf_id: 0,
            seq: 2,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        let _pos1 = writer.append_record(&record1).unwrap();
        let pos2 = writer.append_record(&record2).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Act - replay from second record
        let mut reader = new_reader(&dir).unwrap();
        let mut count = 0;
        let result = WalReader::replay(&mut reader, pos2, |_| {
            count += 1;
            Ok(())
        });

        // Assert
        assert!(result.is_ok());
        assert_eq!(count, 1);
    }

    #[test]
    fn should_stop_replay_on_callback_error() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = new_writer(&dir);
        for i in 0..5 {
            let record = WalRecord {
                op: WalOpKind::Put,
                key: Bytes::from(format!("key{}", i)),
                value: Some(Bytes::from("value")),
                cf_id: 0,
                seq: i,
                expiration: None,
                range_end: None,
                txn_id: None,
                compression: None,
            };
            writer.append_record(&record).unwrap();
        }
        writer.sync().unwrap();
        drop(writer);

        // Act - replay with callback that fails on 3rd record
        let mut reader = new_reader(&dir).unwrap();
        let mut count = 0;
        let result = WalReader::replay(&mut reader, 0, |_| {
            count += 1;
            if count == 3 {
                Err(MidgeError::Internal("test error".into()))
            } else {
                Ok(())
            }
        });

        // Assert
        assert!(result.is_err());
        assert_eq!(count, 3);
    }

    #[test]
    fn should_handle_empty_file_replay() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = new_writer(&dir);
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&dir).unwrap();
        let mut count = 0;
        let result = WalReader::replay(&mut reader, 0, |_| {
            count += 1;
            Ok(())
        });

        // Assert
        assert!(result.is_ok());
        assert_eq!(count, 0);
    }

    // =========== Close Tests ===========

    #[test]
    fn should_close_without_error() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = new_writer(&dir);
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
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&dir).unwrap();
        let result = WalReader::close(&mut reader);

        // Assert
        assert!(result.is_ok());
    }

    // =========== Data Integrity Tests ===========

    #[test]
    fn should_preserve_binary_key_and_value() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = new_writer(&dir);
        let binary_key = vec![0u8, 1u8, 255u8, 254u8];
        let binary_value = vec![127u8, 128u8, 64u8, 32u8];
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::copy_from_slice(&binary_key),
            value: Some(Bytes::copy_from_slice(&binary_value)),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&dir).unwrap();
        let read_record = WalReader::read_at(&mut reader, 0).unwrap().unwrap();

        // Assert
        assert_eq!(read_record.key.as_ref(), &binary_key[..]);
        assert_eq!(read_record.value.unwrap().as_ref(), &binary_value[..]);
    }

    #[test]
    fn should_handle_large_records() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = new_writer(&dir);
        let large_value = vec![42u8; 100_000]; // 100 KB
        let record = WalRecord {
            op: WalOpKind::Put,
            key: Bytes::from("large"),
            value: Some(Bytes::copy_from_slice(&large_value)),
            cf_id: 0,
            seq: 1,
            expiration: None,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&dir).unwrap();
        let read_record = WalReader::read_at(&mut reader, 0).unwrap().unwrap();

        // Assert
        assert_eq!(read_record.value.unwrap().len(), large_value.len());
    }

    #[test]
    fn should_handle_multiple_sequential_reads() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = new_writer(&dir);
        let records = vec![
            WalRecord {
                op: WalOpKind::Put,
                key: Bytes::from("key1"),
                value: Some(Bytes::from("value1")),
                cf_id: 0,
                seq: 1,
                expiration: None,
                range_end: None,
                txn_id: None,
                compression: None,
            },
            WalRecord {
                op: WalOpKind::Put,
                key: Bytes::from("key2"),
                value: Some(Bytes::from("value2")),
                cf_id: 0,
                seq: 2,
                expiration: None,
                range_end: None,
                txn_id: None,
                compression: None,
            },
        ];
        let mut positions = Vec::new();
        for record in &records {
            let pos = writer.append_record(record).unwrap();
            positions.push(pos);
        }
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&dir).unwrap();
        let r1 = WalReader::read_at(&mut reader, positions[0])
            .unwrap()
            .unwrap();
        let r2 = WalReader::read_at(&mut reader, positions[1])
            .unwrap()
            .unwrap();

        // Assert
        assert_eq!(r1.key, Bytes::from("key1"));
        assert_eq!(r2.key, Bytes::from("key2"));
    }

    #[test]
    fn should_handle_delete_records_without_value() {
        // Arrange
        let dir = TempDir::new().unwrap();
        let writer = new_writer(&dir);
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
        writer.append_record(&record).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // Act
        let mut reader = new_reader(&dir).unwrap();
        let read_record = WalReader::read_at(&mut reader, 0).unwrap().unwrap();

        // Assert
        assert_eq!(read_record.op, WalOpKind::Delete);
        assert!(read_record.value.is_none());
    }
}
