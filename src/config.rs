//! Shared public configuration and status types.
//!
//! These types are intentionally owned below `engine` so storage, lease,
//! metadata, and runtime code can depend on configuration without depending on
//! the public engine facade.

use std::path::PathBuf;
use std::time::Duration;

pub(crate) const DEFAULT_STORAGE_IO_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const DEFAULT_RUNTIME_RESPONSE_TIMEOUT: Duration = Duration::from_mins(1);
pub(crate) const DEFAULT_CLOUD_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_mins(1);
const RUNTIME_RESPONSE_TIMEOUT_MARGIN: Duration = Duration::from_secs(30);

/// Derive the enclosing runtime deadline from the storage deadline.
///
/// The margin gives the storage layer room to fail first with a clean terminal
/// error, so the caller sees that error rather than an ambiguous runtime
/// timeout.
///
/// Known limitation: one margin covers one storage operation, not every
/// callback in a multi-step request. Strict WAL acknowledgement, foreground DDL,
/// and direct manifest mirroring share an `OperationDeadline` derived from the
/// waiting caller. Accepted flush, WAL-prune, and reclamation work can continue
/// through callerless workers or maintenance retries after that caller leaves.
/// Compaction publication still performs several provider operations without a
/// shared caller deadline, so operators on slow providers should expect a
/// runtime timeout to be reachable on that path.
/// `abandoned_runtime_requests_total` and
/// `late_runtime_responses_total` expose aggregate timeout behavior, but cannot
/// determine the outcome of an individual request.
pub(crate) fn default_runtime_response_timeout(storage_io_timeout: Duration) -> Duration {
    DEFAULT_RUNTIME_RESPONSE_TIMEOUT.max(
        storage_io_timeout
            .checked_add(RUNTIME_RESPONSE_TIMEOUT_MARGIN)
            .unwrap_or(Duration::MAX),
    )
}

pub(crate) fn validate_memtable_limits(
    memtable_size_limit: usize,
    memtable_flush_threshold: usize,
) -> crate::common::MidgeResult<()> {
    if memtable_size_limit == 0 {
        return Err(crate::common::MidgeError::InvalidArgument(
            "memtable size limit must be greater than zero".to_string(),
        ));
    }
    if memtable_flush_threshold == 0 {
        return Err(crate::common::MidgeError::InvalidArgument(
            "memtable flush threshold must be greater than zero".to_string(),
        ));
    }
    if memtable_flush_threshold > memtable_size_limit {
        return Err(crate::common::MidgeError::InvalidArgument(format!(
            "memtable flush threshold ({memtable_flush_threshold} bytes) exceeds size limit ({memtable_size_limit} bytes)"
        )));
    }
    Ok(())
}

pub(crate) mod cloud_validation;
mod provider;

pub use cloud_validation::{
    CloudCheckCode, CloudCheckOutcome, CloudPreflightOptions, CloudProviderKind, CloudStorageRole,
    CloudValidationFinding, CloudValidationMode, CloudValidationReport,
};
pub use provider::{
    AwsS3Config, AzureBlobConfig, AzureCredentialSource, CloudProviderConfig, GcsApiStyle,
    GcsConfig, GcsCredentialSource, OciCredentialSource, OciObjectStorageConfig,
    S3CompatibleConfig, S3CredentialSource,
};

/// One provider-backed bucket/container and object namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudStorageLocation {
    provider: CloudProviderConfig,
    prefix: String,
}

impl CloudStorageLocation {
    /// Define one cloud storage location.
    pub fn new(provider: impl Into<CloudProviderConfig>, prefix: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            prefix: prefix.into().trim_matches('/').to_string(),
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

    /// Validate configuration without resolving credentials or performing I/O.
    #[must_use]
    pub fn validate(&self) -> CloudValidationReport {
        cloud_validation::validate_location(self, &[CloudStorageRole::Standalone])
    }

    /// Run an explicit, read-only deployment preflight.
    #[must_use]
    pub fn preflight(&self, options: CloudPreflightOptions) -> CloudValidationReport {
        cloud_validation::preflight_location(self, &[CloudStorageRole::Standalone], options)
    }
}

/// Resolved cloud storage locations for each object class.
///
/// By default WAL, SST, and mutable control objects share one provider
/// location. Individual classes can be overridden when separate IAM,
/// ownership, or lifecycle boundaries are operationally useful.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudStorageTopology {
    wal: CloudStorageLocation,
    sst: CloudStorageLocation,
    control: CloudStorageLocation,
}

impl CloudStorageTopology {
    /// Route WAL, SST, and control objects through one shared location.
    #[must_use]
    pub fn new(shared: CloudStorageLocation) -> Self {
        Self {
            wal: shared.clone(),
            sst: shared.clone(),
            control: shared,
        }
    }

    /// Override the sealed-WAL storage location.
    #[must_use]
    pub fn with_wal(mut self, location: CloudStorageLocation) -> Self {
        self.wal = location;
        self
    }

    /// Override the immutable-SST storage location.
    #[must_use]
    pub fn with_sst(mut self, location: CloudStorageLocation) -> Self {
        self.sst = location;
        self
    }

    /// Override the lease and metadata storage location.
    #[must_use]
    pub fn with_control(mut self, location: CloudStorageLocation) -> Self {
        self.control = location;
        self
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

    /// Validate all locations and aggregate role-qualified failures.
    #[must_use]
    pub fn validate(&self) -> CloudValidationReport {
        cloud_validation::validate_topology(self)
    }

    /// Preflight unique locations concurrently under one overall deadline.
    #[must_use]
    pub fn preflight(&self, options: CloudPreflightOptions) -> CloudValidationReport {
        cloud_validation::preflight_topology(self, options)
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
        /// Resolved WAL, SST, and control locations.
        topology: Box<CloudStorageTopology>,
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
