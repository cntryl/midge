use crate::common::{MidgeError, MidgeResult};
use std::time::Duration;

/// Bounded transient retries. Mutation replay requires an independent
/// provider or caller reconciliation proof even when retries are configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CloudRetryPolicy {
    pub max_retries: u32,
    pub base_delay: Duration,
}

impl Default for CloudRetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(50),
        }
    }
}

impl CloudRetryPolicy {
    pub(crate) fn delay(self, attempt: u32) -> Duration {
        let ceiling = self.base_delay.saturating_mul(1 << attempt.min(4));
        let nanos = u64::try_from(ceiling.as_nanos()).unwrap_or(u64::MAX);
        Duration::from_nanos(rand::random_range(0..=nanos))
    }
}

/// Cloud-backed write tuning used by database open options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudWritePolicy {
    /// Transient read and reconciled immutable-publication retry budget.
    pub transient_retry: CloudRetryPolicy,
    /// Flush a column family whose active memtable started this many WAL
    /// segments ago, bounding the cloud WAL catalog. Local mode bounds WAL
    /// retention in bytes instead (four memtable flush triggers' worth).
    pub eventual_flush_segment_gap: u64,
    pub wal_seal_min_segment_bytes: usize,
    pub wal_seal_max_flush_delay: Duration,
    pub wal_seal_max_pending_writes: usize,
}

impl Default for CloudWritePolicy {
    fn default() -> Self {
        Self {
            transient_retry: CloudRetryPolicy::default(),
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
                "eventual-flush segment gap must be greater than zero".to_string(),
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
    pub(crate) shutdown_drain_timeout: Duration,
    pub(crate) local_storage_budget_bytes: Option<u64>,
}

impl Default for CloudWritePolicyConfig {
    fn default() -> Self {
        Self {
            policy: CloudWritePolicy::default(),
            shutdown_drain_timeout: crate::config::DEFAULT_CLOUD_SHUTDOWN_DRAIN_TIMEOUT,
            local_storage_budget_bytes: None,
        }
    }
}

impl CloudWritePolicyConfig {
    pub(crate) fn set_policy(&mut self, policy: CloudWritePolicy) {
        self.policy = policy;
    }
}
