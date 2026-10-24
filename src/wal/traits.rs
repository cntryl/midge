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
pub(crate) type WriteAuthority = std::sync::Arc<dyn crate::wal::writer_authority::WriterAuthority>;

/// A writer that can explicitly attach native authority to submitted work.
#[doc(hidden)]
pub trait GroupedWalWriter: Send + Sync {
    /// Admit one explicit logical operation.
    ///
    /// # Errors
    /// Returns the authority or admission deadline failure before any effect.
    fn begin_group(&self, timeout: Duration) -> MidgeResult<crate::lease::WriterEffectGroup>;
    /// Submit a batch with an owned child of the admitted group.
    ///
    /// # Errors
    /// Returns the append failure and its proof of unchanged storage.
    fn append_batch_in_group(
        &self,
        records: &[WalRecord],
        group: &crate::lease::WriterEffectGroup,
    ) -> Result<WalPos, WalAppendError>;
    /// Sync with a separately admitted child and the existing per-I/O budget.
    ///
    /// # Errors
    /// Returns the authority, filesystem or acknowledgement failure.
    fn sync_in_group(
        &self,
        timeout: Duration,
        group: &crate::lease::WriterEffectGroup,
    ) -> MidgeResult<()>;
}

pub trait WalWriter: Send + Sync {
    /// Install the authority fence held around each background storage effect.
    fn set_write_authority(&self, _authority: Option<WriteAuthority>) {}

    /// Native request grouping is optional; legacy and provider writers retain
    /// their normal per-effect authority path.
    fn writer_group_capability(&self) -> Option<&dyn GroupedWalWriter> {
        None
    }

    /// Append one record, returning its starting position.
    ///
    /// # Errors
    ///
    /// `unchanged` may be true only if this operation left no physical bytes
    /// behind and cannot still write them later. Pre-enqueue rejection or a
    /// completed, durably synced rollback can provide that proof; an unchanged
    /// logical position or an acknowledgement timeout cannot.
    #[doc(hidden)]
    fn append_record_accounted(&self, record: &WalRecord) -> Result<WalPos, WalAppendError>;

    /// Append a batch in one submitted write, returning its last record's
    /// starting position. An empty batch returns `current_pos()` without I/O.
    ///
    /// # Errors
    ///
    /// The same physical unchanged-proof contract as `append_record_accounted`
    /// applies to the entire batch, including partial writes.
    #[doc(hidden)]
    fn append_batch_accounted(&self, records: &[WalRecord]) -> Result<WalPos, WalAppendError>;

    /// Ensure durability, returning when the configured wait deadline elapses.
    ///
    /// Implementations with asynchronous writers should wait on their
    /// completion condition rather than blocking the caller indefinitely.
    ///
    /// # Errors
    /// Returns the sync failure or a timeout when completion is not known
    /// within `timeout`. A timeout does not prove that no I/O occurred.
    fn sync_with_timeout(&self, timeout: Duration) -> MidgeResult<()>;

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
