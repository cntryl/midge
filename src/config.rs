//! Shared public configuration and status types.
//!
//! These types are intentionally owned below `engine` so storage, lease,
//! metadata, and runtime code can depend on configuration without depending on
//! the public engine facade.

use std::path::PathBuf;
use std::time::Duration;

pub(crate) const DEFAULT_STORAGE_IO_TIMEOUT: Duration = Duration::from_secs(30);

mod provider;

pub use provider::{
    AzureCredentialSource, CloudCredentialSource, CloudProviderConfig, GcsApiStyle,
    GcsCredentialSource, S3CredentialSource,
};

/// One provider-backed bucket/container and object namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudStorageLocation {
    provider: CloudProviderConfig,
    prefix: String,
}

impl CloudStorageLocation {
    /// Define one cloud storage location.
    pub fn new(provider: CloudProviderConfig, prefix: impl Into<String>) -> Self {
        Self {
            provider,
            prefix: prefix.into(),
        }
    }

    /// Return the provider configuration.
    #[must_use]
    pub fn provider(&self) -> &CloudProviderConfig {
        &self.provider
    }

    /// Return the object namespace.
    #[must_use]
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Independently managed cloud storage classes.
///
/// WAL, SST, and mutable control objects use separate provider locations so
/// each class can have an independent versioning and lifecycle policy. The
/// control location contains the primary lease, recovery metadata, and DDL
/// registry. Configure it without object versioning whenever possible.
///
/// Midge never age-expires current WAL, SST, or metadata objects. Provider
/// lifecycle rules must only bound noncurrent versions, delete markers, and
/// incomplete multipart uploads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudStorageBuckets {
    wal: CloudStorageLocation,
    sst: CloudStorageLocation,
    control: CloudStorageLocation,
}

impl CloudStorageBuckets {
    /// Define independent WAL, SST, and control storage locations.
    #[must_use]
    pub fn new(
        wal: CloudStorageLocation,
        sst: CloudStorageLocation,
        control: CloudStorageLocation,
    ) -> Self {
        Self { wal, sst, control }
    }

    /// Return the sealed-WAL storage location.
    #[must_use]
    pub fn wal(&self) -> &CloudStorageLocation {
        &self.wal
    }

    /// Return the immutable-SST storage location.
    #[must_use]
    pub fn sst(&self) -> &CloudStorageLocation {
        &self.sst
    }

    /// Return the lease and metadata storage location.
    #[must_use]
    pub fn control(&self) -> &CloudStorageLocation {
        &self.control
    }
}

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
        /// Required, independently managed WAL, SST, and control locations.
        buckets: Box<CloudStorageBuckets>,
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
