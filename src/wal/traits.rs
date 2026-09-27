//! WAL trait definitions
//!
//! Clean trait contracts for WAL implementations.

use crate::common::{MidgeError, MidgeResult};
use crate::wal::types::{WalPos, WalRecord};
use std::time::Duration;

/// Failed append with an operation-specific proof that no bytes remain.
///
/// An unchanged logical append position alone is insufficient: a failed write
/// may leave a partial frame, or a timed-out worker may still be writing.
#[doc(hidden)]
#[derive(Debug)]
pub struct WalAppendError {
    pub error: MidgeError,
    pub unchanged: bool,
}

impl WalAppendError {
    pub(crate) fn unknown(error: MidgeError) -> Self {
        Self {
            error,
            unchanged: false,
        }
    }

    pub(crate) fn unchanged(error: MidgeError) -> Self {
        Self {
            error,
            unchanged: true,
        }
    }
}

/// Writer contract for a WAL implementation.
///
/// Implementations must provide append semantics and durability controls.
///
/// Every append takes a fully built [`WalRecord`], so the caller — the only
/// party that knows the live writer epoch — always stamps it. There is
/// deliberately no convenience append that builds a record from loose fields:
/// such a helper has no epoch to stamp, and recovery exempts epoch 0 from
/// stale-writer fencing, so its records could survive a failover and overwrite
/// a newer writer's data on replay.
pub trait WalWriter: Send + Sync {
    /// Append a pre-encoded record to the log and return the position where
    /// the record was written.
    ///
    /// # Errors
    ///
    /// Returns an error when the record cannot be appended to the WAL.
    fn append_record(&self, record: &WalRecord) -> MidgeResult<WalPos>;

    /// Append one record, retaining disk admission unless failure proves no growth.
    ///
    /// # Errors
    ///
    /// Returns the append error and whether its physical effects were rolled back.
    #[doc(hidden)]
    fn append_record_accounted(&self, record: &WalRecord) -> Result<WalPos, WalAppendError> {
        self.append_record(record).map_err(WalAppendError::unknown)
    }

    /// Batch append multiple records in a single write.
    /// This allows the WAL implementation to optimize encoding and I/O.
    ///
    /// # Errors
    ///
    /// Returns an error when any record in the batch cannot be appended.
    fn append_batch(&self, records: &[WalRecord]) -> MidgeResult<WalPos> {
        // Default implementation: fall back to individual appends
        let mut last_pos = 0;
        for record in records {
            last_pos = self.append_record(record)?;
        }
        Ok(last_pos)
    }

    /// Append a batch, retaining disk admission unless failure proves no growth.
    ///
    /// # Errors
    ///
    /// Returns the append error and whether its physical effects were rolled back.
    #[doc(hidden)]
    fn append_batch_accounted(&self, records: &[WalRecord]) -> Result<WalPos, WalAppendError> {
        self.append_batch(records).map_err(WalAppendError::unknown)
    }

    /// Ensure durability to permanent storage (fsync or equivalent).
    ///
    /// # Errors
    ///
    /// Returns an error when the WAL cannot be durably synced.
    fn sync(&self) -> MidgeResult<()>;

    /// Ensure durability, returning when the configured wait deadline elapses.
    ///
    /// Implementations with asynchronous writers should wait on their
    /// completion condition rather than blocking the caller indefinitely.
    fn sync_with_timeout(&self, timeout: Duration) -> MidgeResult<()> {
        let _ = timeout;
        self.sync()
    }

    /// Current append position in the WAL.
    fn current_pos(&self) -> WalPos;
}

// Reading a WAL back is recovery's job, not a writer-side capability: the
// recovery path (`crate::wal::recovery`) owns frame scanning, torn-tail
// tolerance, and writer-epoch fencing. No reader trait is published here,
// because a generic "read one record" API cannot express those rules and an
// embedder using it would bypass fencing entirely.

#[cfg(test)]
mod tests {
    use super::WalWriter;
    use crate::io::{Fs, FsPath};
    use crate::wal::types::{WalOpKind, WalRecord};
    use std::sync::Arc;

    /// Decode the writer epoch of every frame the writer persisted.
    fn persisted_writer_epochs(fs: &Arc<crate::io::MockFs>, path_str: &str) -> Vec<u64> {
        let path = FsPath::new(path_str);
        let file = fs
            .open(
                &path,
                crate::io::OpenOptions {
                    mode: crate::io::OpenMode::ReadOnly,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .expect("open persisted WAL");
        let frames = crate::wal::frame::FileFrames::new(&*file, &path);
        let mut epochs = Vec::new();
        let mut pos = 0;
        loop {
            match crate::wal::frame::next_frame(
                &frames,
                &path,
                pos,
                crate::wal::frame::FrameLimits::default(),
            ) {
                Ok(crate::wal::frame::FrameStep::Eof) => break,
                Ok(crate::wal::frame::FrameStep::Frame { payload, next_pos }) => {
                    let record =
                        crate::wal::encoding::decode(payload.as_ref()).expect("decode WAL frame");
                    epochs.push(record.writer_epoch);
                    pos = next_pos;
                }
                Err(error) => panic!("unexpected WAL frame error: {}", error.into_error()),
            }
        }
        epochs
    }

    #[test]
    fn should_persist_caller_epoch_when_writer_epoch_is_nonzero() {
        // Arrange: the writer trait has no record-building convenience append,
        // so the only way in stamps the caller's live epoch. Epoch 0 is exempt
        // from stale-writer fencing during replay, so a record written by an
        // epoch-3 writer must never land on disk as epoch 0.
        let fs = Arc::new(crate::io::MockFs::new());
        let writer = crate::wal::fs::FsWalWriterIo::new("wal.log", Arc::clone(&fs) as Arc<dyn Fs>)
            .expect("create WAL writer");
        let record = WalRecord::new(
            WalOpKind::Put,
            bytes::Bytes::from_static(b"key"),
            Some(bytes::Bytes::from_static(b"value")),
            1,
            3,
        );

        // Act
        writer.append_record(&record).expect("append record");
        drop(writer);

        // Assert
        assert_eq!(persisted_writer_epochs(&fs, "wal.log"), vec![3]);
    }
}
