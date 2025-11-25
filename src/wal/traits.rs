//! WAL trait definitions
//!
//! Clean trait contracts for WAL implementations.
//! Types are defined in `types.rs`.

use crate::error::MidgeResult;
use crate::wal::types::{WalOpKind, WalPos, WalRecord};
use std::path::Path;

/// Writer contract for a WAL implementation.
///
/// Implementations must provide append semantics and durability controls.
pub trait WalWriter: Send + Sync {
    /// Append a pre-encoded record to the log and return the position where
    /// the record was written.
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos>;

    /// Convenience: append a single operation (op kind, key, optional value).
    /// Returns the position where the operation was appended.
    fn append_op(&self, kind: WalOpKind, key: &[u8], value: Option<&[u8]>) -> MidgeResult<WalPos>;

    /// Append an operation with an explicit sequence number. Default implementation
    /// falls back to `append_op` for implementations that don't track sequence.
    fn append_op_with_seq(
        &self,
        kind: WalOpKind,
        key: &[u8],
        value: Option<&[u8]>,
        _seq: u64,
    ) -> MidgeResult<WalPos> {
        self.append_op(kind, key, value)
    }

    /// Append an operation with an explicit sequence number and TTL.
    /// ttl_seconds: 0 means no expiration, otherwise the number of seconds until expiration.
    fn append_op_with_seq_ttl(
        &self,
        kind: WalOpKind,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        ttl_seconds: u64,
    ) -> MidgeResult<WalPos> {
        if ttl_seconds == 0 {
            return self.append_op_with_seq(kind, key, value, seq);
        }

        let expiration = if ttl_seconds > 0 {
            let now = crate::common::timestamp::now_millis();
            Some(now + (ttl_seconds * 1000))
        } else {
            None
        };

        let record = WalRecord {
            cf_id: 0,
            op: kind,
            key: bytes::Bytes::copy_from_slice(key),
            value: value.map(bytes::Bytes::copy_from_slice),
            seq,
            expiration,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        self.append_record(&record)
    }

    /// OPTIMIZED: Append with Bytes (zero-copy, just clones Arc pointers)
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
            key,   // No copy! Just move the Bytes (Arc pointer)
            value, // No copy!
            seq,
            expiration,
            range_end: None,
            txn_id: None,
            compression: None,
        };
        self.append_record(&record)
    }

    /// OPTIMIZED: Batch append multiple records in a single write.
    /// This allows the WAL implementation to optimize encoding and I/O.
    fn append_batch(&self, records: &[WalRecord]) -> MidgeResult<WalPos> {
        // Default implementation: fall back to individual appends
        let mut last_pos = 0;
        for record in records {
            last_pos = self.append_record(record)?;
        }
        Ok(last_pos)
    }

    /// Flush any buffered data to the underlying storage (but not necessarily
    /// fsync). Implementations should ensure records are visible after flush.
    fn flush(&self) -> MidgeResult<()>;

    /// Ensure durability to permanent storage (fsync or equivalent).
    fn sync(&self) -> MidgeResult<()>;

    /// Current append position in the WAL.
    fn current_pos(&self) -> WalPos;

    /// Close the WAL writer and release resources.
    fn close(&self) -> MidgeResult<()>;

    /// Signal shutdown to background workers (optional, no-op by default).
    ///
    /// For WAL implementations with background upload threads (e.g., CloudWalWriter),
    /// this signals workers to stop retry loops and exit cleanly. Must be called
    /// before dropping the writer to avoid hanging on sync() or close().
    ///
    /// Default implementation does nothing (suitable for synchronous WAL writers).
    fn shutdown(&self) {
        // Default: no-op for synchronous implementations
    }
}

/// Reader contract for WAL implementations.
///
/// Readers provide random access reads and a replay facility for recovery.
pub trait WalReader {
    /// Read a record located at `pos`. Returns `Ok(None)` if the position is
    /// beyond EOF.
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>>;

    /// Replay records starting at `start` (inclusive). The callback is invoked
    /// for each record in order. Returning an Err from the callback aborts the
    /// replay and returns the error upward.
    fn replay<F>(&mut self, start: WalPos, cb: F) -> MidgeResult<()>
    where
        F: FnMut(&WalRecord) -> MidgeResult<()>;

    /// Close the reader and release resources.
    fn close(&mut self) -> MidgeResult<()>;
}

/// Object-safe wrapper for WAL readers.
///
/// The existing `WalReader` trait has generic methods which make it non-object-safe;
/// this small adapter trait exposes the same capability using a boxed callback and
/// can be returned from factories as a trait object.
pub trait WalReaderDyn: Send {
    fn read_at(&mut self, pos: WalPos) -> MidgeResult<Option<WalRecord>>;
    fn replay_boxed(
        &mut self,
        start: WalPos,
        cb: &mut dyn FnMut(&WalRecord) -> MidgeResult<()>,
    ) -> MidgeResult<()>;
    fn close(&mut self) -> MidgeResult<()>;
}

/// Factory abstraction to create WAL writers and readers.
pub trait WalFactory: Send + Sync {
    /// Create a new WAL writer for the given directory.
    fn create_writer(&self, dir: &Path) -> MidgeResult<Box<dyn WalWriter>>;

    /// Create a new WAL writer with optional test hooks for fault injection.
    fn create_writer_with_hooks(
        &self,
        dir: &Path,
        test_hooks: Option<crate::common::test_hooks::TestHooks>,
    ) -> MidgeResult<Box<dyn WalWriter>> {
        // Default implementation ignores hooks for backward compatibility
        let _ = test_hooks;
        self.create_writer(dir)
    }

    /// Create a new WAL reader for the given directory.
    fn create_reader(&self, dir: &Path) -> MidgeResult<Box<dyn WalReaderDyn>>;

    /// Rotate the active WAL file (e.g., rename active wal.log to wal-<seq>.log)
    /// and return a new writer for the active WAL.
    fn rotate_writer(&self, dir: &Path, seq: u64) -> MidgeResult<Box<dyn WalWriter>>;
}

// Convenience re-exports from implementations
pub use crate::wal::fs::Wal as WalFile;
pub use crate::wal::mem::WalMem;
pub use crate::wal::mem::WalMem as WalMemWriter;
pub use crate::wal::mem::WalMemReader as WalMemReaderHandle;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::column_family::ColumnFamilyId;
    use bytes::Bytes;

    #[test]
    fn should_default_to_cf_zero_given_new_record() {
        // Arrange - Create a WAL record without specifying CF

        // Act
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from("key"),
            Some(Bytes::from("value")),
            100,
        );

        // Assert
        assert_eq!(record.cf_id, 0);
        assert_eq!(record.column_family_id().as_u32(), 0);
        assert_eq!(record.seq, 100);
    }

    #[test]
    fn should_use_custom_cf_given_new_cf_record() {
        // Arrange
        let cf_id = ColumnFamilyId::new(5);

        // Act
        let record = WalRecord::new_cf(cf_id, WalOpKind::Delete, Bytes::from("key"), None, 200);

        // Assert
        assert_eq!(record.cf_id, 5);
        assert_eq!(record.column_family_id(), cf_id);
        assert_eq!(record.seq, 200);
    }

    #[test]
    fn should_roundtrip_record_given_serialization() {
        // Arrange
        let record = WalRecord::new_cf(
            ColumnFamilyId::new(3),
            WalOpKind::Put,
            Bytes::from("test_key"),
            Some(Bytes::from("test_value")),
            42,
        );

        // Act
        let encoded = bincode::serialize(&record).expect("serialize");
        let decoded: WalRecord = bincode::deserialize(&encoded).expect("deserialize");

        // Assert
        assert_eq!(decoded.cf_id, 3);
        assert_eq!(decoded.op, WalOpKind::Put);
        assert_eq!(decoded.key, Bytes::from("test_key"));
        assert_eq!(decoded.value, Some(Bytes::from("test_value")));
        assert_eq!(decoded.seq, 42);
    }

    #[test]
    fn should_maintain_backward_compatibility_given_default_cf() {
        // Arrange - For backward compatibility with old WAL files that don't have cf_id,
        // we can manually construct records with cf_id = 0 when reading old format.
        // This test verifies that new records default to cf_id = 0.

        // Act
        let record = WalRecord::new(
            WalOpKind::Put,
            Bytes::from("key"),
            Some(Bytes::from("value")),
            100,
        );

        // Assert - New records default to cf_id = 0
        assert_eq!(record.cf_id, 0);

        // Can serialize and deserialize with cf_id included
        let encoded = bincode::serialize(&record).expect("serialize");
        let decoded: WalRecord = bincode::deserialize(&encoded).expect("deserialize");
        assert_eq!(decoded.cf_id, 0);
        assert_eq!(decoded.seq, 100);
    }
}
