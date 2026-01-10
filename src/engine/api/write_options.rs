//! Write options for explicit durability control
//!
//! Provides explicit control over write durability semantics.
//! Callers must always specify durability policy - no defaults.

/// Write options - MUST be explicitly provided for all commits
///
/// Deliberately NO Default impl to force explicit choices
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
    /// Skip WAL entirely - fastest but completely non-durable
    /// Only use for bulk loads or testing
    NoWAL,
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

    /// Create WriteOptions with NoWAL policy (dangerous)
    pub fn no_wal() -> Self {
        Self {
            policy: DurabilityPolicy::NoWAL,
        }
    }

    /// The recommended WriteOptions for production use
    ///
    /// This is an alias for sync() - full durability with immediate fsync.
    /// Use this when you want safe defaults without ambiguity.
    pub fn recommended() -> Self {
        Self::sync()
    }

    /// Get durability policy
    pub fn policy(&self) -> DurabilityPolicy {
        self.policy
    }

    /// Check if this is sync mode
    pub fn is_sync(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::Sync)
    }

    /// Check if WAL is disabled
    pub fn is_no_wal(&self) -> bool {
        matches!(self.policy, DurabilityPolicy::NoWAL)
    }

    /// Builder-style: set policy to Sync
    pub fn with_sync(mut self, sync: bool) -> Self {
        if sync {
            self.policy = DurabilityPolicy::Sync;
        } else {
            self.policy = DurabilityPolicy::Buffered;
        }
        self
    }
}
