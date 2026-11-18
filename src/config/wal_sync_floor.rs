use crate::common::{MidgeError, MidgeResult};
use std::time::Duration;

/// Opt-in amortized WAL sync floor for local storage (#904).
///
/// By default every successful strict WAL fsync is followed by a durable
/// ledger edit that records the exact synced length of the open segment (the
/// "floor"). With this option on, that edit is skipped while both bounds hold,
/// so a strict group costs one fsync instead of two. Acknowledged writes are
/// still fsynced before they are acknowledged; only the recorded floor lags.
///
/// The floor is always recorded exactly at rotation, clean shutdown, DDL,
/// verification, backup and salvage.
///
/// The cost is lost-write detection inside the window: bytes between the
/// recorded floor and the true acknowledged end are classified as a torn tail
/// on recovery, so damage or truncation there is truncated and the engine
/// opens, instead of failing a strict open. An intact tail is replayed in
/// full. The window is at most `max_unrecorded_bytes` of acknowledged bytes
/// and `max_unrecorded_delay` after the oldest unrecorded acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AmortizedSyncFloor {
    /// Record the floor once this many acknowledged bytes are unrecorded,
    /// within `1..=64 MiB`.
    pub max_unrecorded_bytes: u64,
    /// Record the floor once the oldest unrecorded acknowledgement is this
    /// old, within `1 ms..=10 s`.
    pub max_unrecorded_delay: Duration,
}

impl Default for AmortizedSyncFloor {
    fn default() -> Self {
        Self {
            max_unrecorded_bytes: Self::DEFAULT_BYTES,
            max_unrecorded_delay: Self::DEFAULT_DELAY,
        }
    }
}

impl AmortizedSyncFloor {
    const DEFAULT_BYTES: u64 = 256 * 1024;
    const DEFAULT_DELAY: Duration = Duration::from_millis(20);
    const BYTES_RANGE: std::ops::RangeInclusive<u64> = 1..=64 * 1024 * 1024;
    const MIN_DELAY: Duration = Duration::from_millis(1);
    const MAX_DELAY: Duration = Duration::from_secs(10);

    pub(crate) fn validate(&self) -> MidgeResult<()> {
        if !Self::BYTES_RANGE.contains(&self.max_unrecorded_bytes) {
            return Err(MidgeError::InvalidArgument(format!(
                "amortized sync floor max_unrecorded_bytes must be within 1..=67108864, got {}",
                self.max_unrecorded_bytes
            )));
        }
        if self.max_unrecorded_delay < Self::MIN_DELAY
            || self.max_unrecorded_delay > Self::MAX_DELAY
        {
            return Err(MidgeError::InvalidArgument(format!(
                "amortized sync floor max_unrecorded_delay must be within 1ms..=10s, got {:?}",
                self.max_unrecorded_delay
            )));
        }
        Ok(())
    }
}
