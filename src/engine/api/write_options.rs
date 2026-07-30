//! Write options for explicit durability control
//!
//! Provides explicit control over write durability semantics.
//! Callers must always specify durability policy - there is intentionally no
//! `Default` implementation.

/// Write options - MUST be explicitly provided for all commits
///
/// Deliberately no `Default` impl to force explicit choices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    /// Durability policy
    policy: DurabilityPolicy,
}

/// Durability policy - explicit choices only
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityPolicy {
    /// fsync immediately - full durability
    Sync,
    /// Write to OS buffer - fast but not durable on crash
    Buffered,
    /// Best-effort persistence - fastest but no durability on crash before flush
    /// Only use for bulk loads or testing
    BestEffort,
    /// Local visibility plus asynchronous cloud durability.
    CloudAsync,
    /// Force immediate WAL seal + rotate + upload, then block until cloud upload
    /// completes.
    CloudStrict,
}

impl WriteOptions {
    /// Create `WriteOptions` with Sync policy
    #[must_use]
    pub fn sync() -> Self {
        Self {
            policy: DurabilityPolicy::Sync,
        }
    }

    /// Create `WriteOptions` with Buffered policy
    #[must_use]
    pub fn buffered() -> Self {
        Self {
            policy: DurabilityPolicy::Buffered,
        }
    }

    /// Create `WriteOptions` with `BestEffort` policy
    ///
    /// Best-effort persistence: data written to memtable with no WAL overhead.
    /// Data is visible for reads and can be flushed to SST, but provides NO durability guarantee
    /// on engine crash before `flush_cf()` is called.
    ///
    /// # WARNING - Data Loss Risk
    /// - On engine crash before flush: data in memtable is lost
    /// - Should ONLY be used for non-critical setup phases (before measured workload)
    /// - NEVER use for production data or measured performance runs requiring durability
    /// - Recovery strategy: reload data from source (e.g., re-run load phase)
    ///
    /// # Safe Usage Pattern
    /// 1. Load initial dataset with `best_effort()` (setup phase - not measured)
    /// 2. Call `engine.flush_cf()` to persist loaded data to SST
    /// 3. Switch to `buffered()` or `sync()` for measured workload
    /// 4. If engine restarts, reload initial dataset from scratch
    ///
    #[must_use]
    pub fn best_effort() -> Self {
        Self {
            policy: DurabilityPolicy::BestEffort,
        }
    }

    /// Create `WriteOptions` with `CloudAsync` policy.
    ///
    /// This is the normal cloud-backed durability mode: the commit becomes
    /// visible once the local WAL/memtable barrier completes, and cloud upload
    /// continues in the background.
    #[must_use]
    pub fn cloud_async() -> Self {
        Self {
            policy: DurabilityPolicy::CloudAsync,
        }
    }

    /// Create `WriteOptions` with `CloudStrict` policy.
    ///
    /// Forces immediate WAL seal + rotate + cloud upload, then blocks until
    /// upload completes. Use ONLY when cloud durability is explicitly required.
    /// Regular `CloudAsync` mode uses background uploads and never blocks commits.
    #[must_use]
    pub fn cloud_strict() -> Self {
        Self {
            policy: DurabilityPolicy::CloudStrict,
        }
    }

    /// Get durability policy
    #[must_use]
    pub fn policy(&self) -> DurabilityPolicy {
        self.policy
    }

    /// Check if this is sync mode
    #[must_use]
    pub fn is_sync(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::Sync)
    }

    /// Check if using best-effort persistence (no durability guarantee)
    #[must_use]
    pub fn is_best_effort(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::BestEffort)
    }

    /// Check if this uses asynchronous cloud durability.
    #[must_use]
    pub fn is_cloud_async(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::CloudAsync)
    }

    /// Check if this requires strict cloud durability
    #[must_use]
    pub fn is_cloud_strict(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::CloudStrict)
    }

    /// Convert API-level durability policy to WAL-level durability policy.
    ///
    /// Maps the user-facing API durability choices to runtime WAL durability policies:
    /// - Sync → Strict (fsync after every write)
    /// - Buffered → Batched (periodic fsync)
    /// - `BestEffort` → `BestEffort` (skip WAL entirely)
    /// - `CloudAsync` / `CloudStrict` → `CloudAsync` (commit finalization decides
    ///   whether to wait for cloud acknowledgment)
    pub(crate) fn to_wal_durability_policy(self) -> crate::wal::DurabilityPolicy {
        match self.policy {
            DurabilityPolicy::Sync => crate::wal::DurabilityPolicy::Strict,
            DurabilityPolicy::Buffered => crate::wal::DurabilityPolicy::Batched,
            DurabilityPolicy::BestEffort => crate::wal::DurabilityPolicy::BestEffort,
            DurabilityPolicy::CloudAsync | DurabilityPolicy::CloudStrict => {
                crate::wal::DurabilityPolicy::CloudAsync
            }
        }
    }
}

pub(crate) fn effective_wal_durability_policy(
    cloud_mode: bool,
    opts: WriteOptions,
) -> crate::common::MidgeResult<crate::wal::DurabilityPolicy> {
    if cloud_mode {
        return match opts.policy() {
            DurabilityPolicy::BestEffort => Ok(crate::wal::DurabilityPolicy::BestEffort),
            DurabilityPolicy::CloudAsync | DurabilityPolicy::CloudStrict => {
                Ok(crate::wal::DurabilityPolicy::CloudAsync)
            }
            DurabilityPolicy::Sync | DurabilityPolicy::Buffered => Err(
                crate::common::MidgeError::InvalidArgument(
                    "sync() and buffered() are local-only; use cloud_async() or cloud_strict() for cloud-backed storage".to_string(),
                ),
            ),
        };
    }

    if opts.is_cloud_async() || opts.is_cloud_strict() {
        return Err(crate::common::MidgeError::InvalidArgument(
            "cloud_async() and cloud_strict() require cloud-backed storage".to_string(),
        ));
    }

    Ok(opts.to_wal_durability_policy())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_map_every_local_write_option_to_expected_wal_policy_given_local_storage_when_committing(
    ) {
        // Arrange
        let options = [
            (WriteOptions::sync(), crate::wal::DurabilityPolicy::Strict),
            (
                WriteOptions::buffered(),
                crate::wal::DurabilityPolicy::Batched,
            ),
            (
                WriteOptions::best_effort(),
                crate::wal::DurabilityPolicy::BestEffort,
            ),
        ];

        // Act
        let mapped = options.map(|(opts, _)| effective_wal_durability_policy(false, opts));

        // Assert
        for ((_, expected), actual) in options.into_iter().zip(mapped) {
            assert_eq!(actual.expect("local option should be accepted"), expected);
        }
    }

    #[test]
    fn should_reject_sync_buffered_options_given_cloud_storage_when_committing() {
        // Arrange
        let options = [WriteOptions::sync(), WriteOptions::buffered()];

        // Act
        let errors = options.map(|opts| effective_wal_durability_policy(true, opts));

        // Assert
        assert!(errors.into_iter().all(|result| result.is_err()));
    }

    #[test]
    fn should_reject_cloud_options_given_local_storage_when_committing() {
        // Arrange
        let options = [WriteOptions::cloud_async(), WriteOptions::cloud_strict()];

        // Act
        let errors = options.map(|opts| effective_wal_durability_policy(false, opts));

        // Assert
        assert!(errors.into_iter().all(|result| result.is_err()));
    }
}
