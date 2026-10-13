//! WAL trait definitions
//!
//! Clean trait contracts for WAL implementations.

use crate::common::{MidgeError, MidgeResult};
use crate::wal::format::{SealedFacts, SegmentTail};
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
/// A writer appends to one segment, whose header names the writer epoch
/// (#843); records carry no epoch of their own.
pub(crate) type WriteAuthority =
    std::sync::Arc<dyn Fn(&mut dyn FnMut()) -> MidgeResult<()> + Send + Sync>;

pub trait WalWriter: Send + Sync {
    /// Install the authority fence held around each background storage effect.
    fn set_write_authority(&self, _authority: Option<WriteAuthority>) {}

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

    /// The records acknowledged into the segment so far.
    fn tail(&self) -> SegmentTail;

    /// The facts that seal the segment as it stands: its digest and the
    /// records in it. Waits, at most `timeout`, for queued writes to land;
    /// performs no I/O of its own.
    ///
    /// # Errors
    ///
    /// The writer's failure, or `Timeout` when writes do not drain.
    fn seal_facts(&self, timeout: Duration) -> MidgeResult<SealedFacts>;

    /// Write the footer sealing `facts`, which must still describe the
    /// segment, and sync it. The writer appends nothing afterwards.
    ///
    /// # Errors
    ///
    /// The write or sync error; the footer may or may not be durable.
    fn write_footer(&self, facts: &SealedFacts, timeout: Duration) -> MidgeResult<()>;
}

/// The answers of a test writer that holds no segment: it tracks no
/// records and refuses to seal.
#[cfg(test)]
pub(crate) mod unsealable {
    use crate::common::{MidgeError, MidgeResult};
    use crate::wal::format::{SealedFacts, SegmentTail};

    pub(crate) fn tail() -> SegmentTail {
        SegmentTail::default()
    }

    pub(crate) fn seal() -> MidgeResult<SealedFacts> {
        Err(MidgeError::Internal(
            "this test WAL writer holds no segment to seal".to_string(),
        ))
    }

    pub(crate) fn write_footer() -> MidgeResult<()> {
        Err(MidgeError::Internal(
            "this test WAL writer holds no segment to seal".to_string(),
        ))
    }
}

// Reading a WAL back is recovery's job, not a writer-side capability: the
// recovery path (`crate::wal::recovery`) owns frame scanning, torn-tail
// tolerance, and the segment chain. No reader trait is published here,
// because a generic "read one record" API cannot express those rules.
