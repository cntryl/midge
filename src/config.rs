//! Engine and runtime configuration.

use std::path::PathBuf;
use std::time::Duration;

pub(crate) const DEFAULT_RUNTIME_RESPONSE_TIMEOUT: Duration = Duration::from_mins(1);
pub(crate) const DEFAULT_CLOUD_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_mins(1);
pub(crate) const DEFAULT_MAX_IMMUTABLE_MEMTABLES: usize = 10;
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
) -> cntryl_midge_core::common::MidgeResult<()> {
    if memtable_size_limit == 0 {
        return Err(cntryl_midge_core::common::MidgeError::InvalidArgument(
            "memtable size limit must be greater than zero".to_string(),
        ));
    }
    if memtable_flush_threshold == 0 {
        return Err(cntryl_midge_core::common::MidgeError::InvalidArgument(
            "memtable flush threshold must be greater than zero".to_string(),
        ));
    }
    if memtable_flush_threshold > memtable_size_limit {
        return Err(cntryl_midge_core::common::MidgeError::InvalidArgument(format!(
            "memtable flush threshold ({memtable_flush_threshold} bytes) exceeds size limit ({memtable_size_limit} bytes)"
        )));
    }
    Ok(())
}

mod compaction_levels;
mod wal_sync_floor;
mod write_backpressure;
pub use compaction_levels::CompactionLevelConfig;
pub use wal_sync_floor::AmortizedSyncFloor;
pub use write_backpressure::WriteBackpressureConfig;

use cntryl_midge_storage::config::CloudStorageTopology;

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
