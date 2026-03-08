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
    /// CloudFirst strict mode - force immediate WAL seal + rotate + upload,
    /// then block until cloud upload completes. Use ONLY when cloud durability
    /// is explicitly required. Regular CloudFirst uses background uploads and
    /// never blocks commits.
    CloudStrict,
}

impl WriteOptions {
    /// Create WriteOptions with Sync policy
    pub fn sync() -> Self {
        Self {
            policy: DurabilityPolicy::Sync,
        }
    }

    /// Create WriteOptions with Buffered policy
    pub fn buffered() -> Self {
        Self {
            policy: DurabilityPolicy::Buffered,
        }
    }

    /// Create WriteOptions with BestEffort policy
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
    pub fn best_effort() -> Self {
        Self {
            policy: DurabilityPolicy::BestEffort,
        }
    }

    /// Create WriteOptions with CloudStrict policy.
    ///
    /// Forces immediate WAL seal + rotate + cloud upload, then blocks until
    /// upload completes. Use ONLY when cloud durability is explicitly required.
    /// Regular CloudFirst mode uses background uploads and never blocks commits.
    pub fn cloud_strict() -> Self {
        Self {
            policy: DurabilityPolicy::CloudStrict,
        }
    }

    /// Get durability policy
    pub fn policy(&self) -> DurabilityPolicy {
        self.policy
    }

    /// Check if this is sync mode
    pub fn is_sync(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::Sync)
    }

    /// Check if using best-effort persistence (no durability guarantee)
    pub fn is_best_effort(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::BestEffort)
    }

    /// Check if this requires strict cloud durability
    pub fn is_cloud_strict(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::CloudStrict)
    }

    /// Convert API-level durability policy to WAL-level durability policy.
    ///
    /// Maps the user-facing API durability choices to runtime WAL durability policies:
    /// - Sync → Strict (fsync after every write)
    /// - Buffered → Batched (periodic fsync)
    /// - BestEffort → BestEffort (skip WAL entirely)
    /// - CloudStrict → CloudFirst (wait for cloud acknowledgment)
    pub(crate) fn to_wal_durability_policy(self) -> crate::wal::DurabilityPolicy {
        match self.policy {
            DurabilityPolicy::Sync => crate::wal::DurabilityPolicy::Strict,
            DurabilityPolicy::Buffered => crate::wal::DurabilityPolicy::Batched,
            DurabilityPolicy::BestEffort => crate::wal::DurabilityPolicy::BestEffort,
            DurabilityPolicy::CloudStrict => crate::wal::DurabilityPolicy::CloudFirst,
        }
    }
}
