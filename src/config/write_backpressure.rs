use crate::common::{MidgeError, MidgeResult};
use std::time::Duration;

/// Optional admission slowdown and background write limits. Defaults preserve
/// immediate admission and the existing hard stalls.
#[derive(Debug, Clone, Default)]
pub struct WriteBackpressureConfig {
    /// Published and reserved L0 generations at which admission slows down.
    pub l0_soft_limit: Option<usize>,
    /// Total admitted memtable bytes at which admission slows down.
    pub memory_soft_limit: Option<usize>,
    /// Estimated bytes above level targets at which admission slows down.
    pub pending_compaction_soft_limit: Option<u64>,
    /// Estimated bytes above level targets at which admission rejects writes.
    pub pending_compaction_hard_limit: Option<u64>,
    /// Maximum caller-side delay per submission. WAL and provider time are
    /// separately bounded by the original enclosing response deadline.
    pub max_admission_delay: Duration,
    /// Shared flush/compaction encoded output bytes per second. No limit when
    /// absent. Hard pressure bypasses throttling to preserve flush progress.
    pub background_write_bytes_per_second: Option<u64>,
}

impl WriteBackpressureConfig {
    pub(crate) fn has_soft_limits(&self) -> bool {
        self.l0_soft_limit.is_some()
            || self.memory_soft_limit.is_some()
            || self.pending_compaction_soft_limit.is_some()
    }

    pub(crate) fn validate(&self, l0_hard: usize, memory_hard: usize) -> MidgeResult<()> {
        let invalid = self.l0_soft_limit.is_some_and(|n| n == 0 || n >= l0_hard)
            || self
                .memory_soft_limit
                .is_some_and(|n| n == 0 || n >= memory_hard)
            || self.pending_compaction_soft_limit.is_some_and(|n| {
                n == 0
                    || self
                        .pending_compaction_hard_limit
                        .is_none_or(|hard| n >= hard)
            })
            || self.pending_compaction_hard_limit == Some(0)
            || self.background_write_bytes_per_second == Some(0)
            || (self.has_soft_limits() && self.max_admission_delay.is_zero())
            || std::time::Instant::now()
                .checked_add(self.max_admission_delay)
                .is_none();
        if invalid {
            return Err(MidgeError::InvalidArgument(
                "write soft limits must be positive and below hard limits; slowdown and I/O rate must be positive when enabled".into(),
            ));
        }
        Ok(())
    }
}
