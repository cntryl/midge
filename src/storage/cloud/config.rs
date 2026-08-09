use crate::common::{MidgeError, MidgeResult};
use std::time::Duration;

/// Cloud-backed write tuning used by database open options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudWritePolicy {
    pub eventual_flush_segment_gap: u64,
    pub wal_seal_min_segment_bytes: usize,
    pub wal_seal_max_flush_delay: Duration,
    pub wal_seal_max_pending_writes: usize,
}

impl Default for CloudWritePolicy {
    fn default() -> Self {
        Self {
            eventual_flush_segment_gap: 128,
            wal_seal_min_segment_bytes: 16 * 1024 * 1024,
            wal_seal_max_flush_delay: Duration::from_millis(500),
            wal_seal_max_pending_writes: 10_000,
        }
    }
}

impl CloudWritePolicy {
    pub(crate) fn validate(&self) -> MidgeResult<()> {
        if self.eventual_flush_segment_gap == 0 {
            return Err(MidgeError::InvalidArgument(
                "cloud eventual-flush segment gap must be greater than zero".to_string(),
            ));
        }
        if self.wal_seal_min_segment_bytes == 0 {
            return Err(MidgeError::InvalidArgument(
                "cloud WAL seal minimum segment bytes must be greater than zero".to_string(),
            ));
        }
        if self.wal_seal_max_flush_delay.is_zero() {
            return Err(MidgeError::InvalidArgument(
                "cloud WAL seal maximum flush delay must be greater than zero".to_string(),
            ));
        }
        if self.wal_seal_max_pending_writes == 0 {
            return Err(MidgeError::InvalidArgument(
                "cloud WAL seal maximum pending writes must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Cloud storage and shutdown controls owned by the cloud subsystem.
#[derive(Debug, Clone)]
pub(crate) struct CloudWritePolicyConfig {
    pub(crate) policy: CloudWritePolicy,
    pub(crate) storage_io_timeout: Duration,
    pub(crate) shutdown_drain_timeout: Duration,
    pub(crate) simulated_local_budget_bytes: Option<u64>,
}

impl Default for CloudWritePolicyConfig {
    fn default() -> Self {
        Self {
            policy: CloudWritePolicy::default(),
            storage_io_timeout: crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
            shutdown_drain_timeout: Duration::from_secs(30),
            simulated_local_budget_bytes: None,
        }
    }
}

impl CloudWritePolicyConfig {
    pub(crate) fn set_policy(&mut self, policy: CloudWritePolicy) {
        self.policy = policy;
    }
}
