//! WAL trait definitions
//!
//! Clean trait contracts for WAL implementations.

use crate::common::MidgeResult;
use crate::storage::abstraction::{Storage, StoragePath};
use crate::wal::types::{WalOpKind, WalPos, WalRecord};

/// Writer contract for a WAL implementation.
///
/// Implementations must provide append semantics and durability controls.
pub trait WalWriter: Send + Sync {
    /// Append a pre-encoded record to the log and return the position where
    /// the record was written.
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos>;

    /// Convenience: append a single operation (op kind, key, optional value).
    /// Returns the position where the operation was appended.
    ///
    /// ⚠️ **WARNING**: This method does not take a sequence number and implementations
    /// may assign an invalid default (e.g., 0), breaking ordering guarantees.
    /// **DO NOT USE** - prefer `append_op_with_seq()` or `append_record()` instead.
    /// Many implementations return an error from this method.
    fn append_op(&self, kind: WalOpKind, key: &[u8], value: Option<&[u8]>) -> MidgeResult<WalPos>;

    /// Append an operation with an explicit sequence number.
    fn append_op_with_seq(
        &self,
        kind: WalOpKind,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
    ) -> MidgeResult<WalPos> {
        let record = WalRecord::new(
            kind,
            bytes::Bytes::copy_from_slice(key),
            value.map(bytes::Bytes::copy_from_slice),
            seq,
            0, // writer_epoch: default impls use epoch 0 (callers should use append_record directly)
        );
        self.append_record(&record)
    }

    /// Append with Bytes (zero-copy)
    fn append_op_bytes(
        &self,
        kind: WalOpKind,
        key: bytes::Bytes,
        value: Option<bytes::Bytes>,
        seq: u64,
    ) -> MidgeResult<WalPos> {
        let record = WalRecord::new(kind, key, value, seq, 0);
        self.append_record(&record)
    }

    /// Batch append multiple records in a single write.
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

    /// Sync only to *local* WAL storage (fsync/local durability) without
    /// waiting for any external/cloud uploads.
    fn sync_local(&self) -> MidgeResult<()> {
        self.sync()
    }

    /// Current append position in the WAL.
    fn current_pos(&self) -> WalPos;

    /// Close the WAL writer and release resources.
    fn close(&self) -> MidgeResult<()>;

    /// Signal shutdown to background workers (optional, no-op by default).
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
/// this small adapter trait exposes the same capability using a boxed callback.
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
    fn create_writer(
        &self,
        storage: &dyn Storage,
        dir: &StoragePath,
    ) -> MidgeResult<Box<dyn WalWriter>>;

    /// Create a new WAL reader for the given directory.
    fn create_reader(
        &self,
        storage: &dyn Storage,
        dir: &StoragePath,
    ) -> MidgeResult<Box<dyn WalReaderDyn>>;

    /// Rotate the active WAL file (e.g., rename active wal.log to `wal-<seq>.log`)
    /// and return a new writer for the active WAL.
    fn rotate_writer(
        &self,
        storage: &dyn Storage,
        dir: &StoragePath,
        seq: u64,
    ) -> MidgeResult<Box<dyn WalWriter>>;
}
