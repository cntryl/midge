//! Shared public configuration and status types.
//!
//! These types are intentionally owned below `engine` so storage, lease,
//! metadata, and runtime code can depend on configuration without depending on
//! the public engine facade.

use std::path::PathBuf;

pub use crate::storage::providers::{
    AzureCredentialSource, CloudCredentialSource, CloudProviderConfig, GcsApiStyle,
    GcsCredentialSource, S3CredentialSource,
};

/// Storage backend specification - MUST be explicit
///
/// This enum enforces unambiguous storage selection. There are NO defaults,
/// NO inference, and NO magic switching between backends.
///
/// Each variant clearly answers: "Where does this database live?"
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Storage {
    /// In-memory storage - no persistence
    ///
    /// Data is lost when the engine is dropped or process exits.
    /// Use for: testing, caching, ephemeral workloads
    InMemory,

    /// Local filesystem storage
    ///
    /// Data persists to local disk at the specified path.
    /// Use for: traditional deployments, single-node databases
    Local {
        /// Filesystem path to database directory
        path: PathBuf,
    },

    /// Real cloud object storage.
    ///
    /// Uses a hybrid model with a local cache/staging directory plus a
    /// provider-backed object store.
    Cloud {
        /// Local cache/staging path for performance
        local_cache_path: PathBuf,
        /// Provider and credential configuration.
        provider: CloudProviderConfig,
        /// Object key prefix (e.g., "databases/myapp/")
        prefix: String,
    },

    /// Filesystem-backed cloud simulation.
    ///
    /// This is intentionally separate from real cloud mode so tests can keep a
    /// deterministic in-process object-store stand-in without implying that a
    /// provider endpoint is being used.
    CloudSimulated {
        /// Local cache/staging path for performance
        local_cache_path: PathBuf,
        /// Bucket/container name (provider-specific terminology)
        bucket: String,
        /// Object key prefix (e.g., "databases/myapp/")
        prefix: String,
    },
}

/// Recovery policy for engine open.
///
/// `Strict` fails engine open if manifest, intent-log, or WAL recovery cannot
/// establish a trustworthy state. `Salvage` preserves legacy best-effort
/// behavior and may open in a degraded state after logging warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecoveryPolicy {
    #[default]
    Strict,
    Salvage,
}

/// High-level engine health state for operators and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum EngineHealth {
    Healthy,
    Degraded,
    SalvageMode,
    WriteStalled,
    Corrupt,
}
