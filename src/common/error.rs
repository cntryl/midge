//! Error types for Midge

use std::fmt;
use std::io;

/// Result type for Midge operations
pub type MidgeResult<T> = Result<T, MidgeError>;

/// Main error type for Midge
#[derive(Debug)]
pub enum MidgeError {
    /// IO error
    Io(std::io::Error),

    /// Key not found
    NotFound,

    /// Invalid argument
    InvalidArgument(String),

    /// Corruption detected
    Corruption(String),

    /// Operation not supported
    NotSupported(String),

    /// Internal error (should not happen)
    Internal(String),

    /// Invalid path
    InvalidPath,

    /// Operation could not complete because the underlying storage is full.
    NoSpace(String),

    /// Recovery failed and the engine refused to continue in strict mode.
    RecoveryFailed(String),

    /// On-disk data or configuration is incompatible with this build.
    CompatibilityError(String),

    /// Write stall - memtable full or compaction lagging behind
    /// Application must apply backpressure
    WriteStall(String),

    /// Memory mode violation - attempted disk I/O in memory-only mode
    MemoryModeViolation(String),

    /// Writer fenced — epoch is stale, another leader has taken over
    Fenced(String),

    /// A different writer currently owns the requested storage lease.
    LeaseHeld(String),

    /// The lease backend could not be reached or could not complete acquisition.
    LeaseUnavailable(String),

    /// Persisted lease state could not be interpreted (malformed or ambiguous).
    LeaseIndeterminate(String),

    /// The lease's fencing epoch counter cannot advance any further.
    LeaseEpochExhausted,

    /// Transaction write conflict detected under strict conflict policy
    WriteConflict(String),

    /// A cooperative operation was cancelled before it could publish a result.
    Aborted(String),

    /// The operation cannot proceed while an owned resource is still active.
    Busy(String),

    /// The operation did not complete before its caller-provided deadline.
    Timeout(String),

    /// A bounded resource pool cannot admit more work.
    ResourceLimit(String),
}

impl fmt::Display for MidgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MidgeError::Io(e) => write!(f, "IO error: {e}"),
            MidgeError::NotFound => write!(f, "Not found"),
            MidgeError::InvalidArgument(msg) => write!(f, "Invalid argument: {msg}"),
            MidgeError::Corruption(msg) => write!(f, "Corruption: {msg}"),
            MidgeError::NotSupported(msg) => write!(f, "Not supported: {msg}"),
            MidgeError::Internal(msg) => write!(f, "Internal error: {msg}"),
            MidgeError::InvalidPath => write!(f, "Invalid path"),
            MidgeError::NoSpace(msg) => write!(f, "No space left on device: {msg}"),
            MidgeError::RecoveryFailed(msg) => write!(f, "Recovery failed: {msg}"),
            MidgeError::CompatibilityError(msg) => write!(f, "Compatibility error: {msg}"),
            MidgeError::WriteStall(msg) => write!(f, "Write stall: {msg}"),
            MidgeError::MemoryModeViolation(msg) => write!(f, "Memory mode violation: {msg}"),
            MidgeError::Fenced(msg) => write!(f, "Fenced: writer epoch is stale: {msg}"),
            MidgeError::LeaseHeld(msg) => write!(f, "Writer lease held: {msg}"),
            MidgeError::LeaseUnavailable(msg) => write!(f, "Writer lease unavailable: {msg}"),
            MidgeError::LeaseIndeterminate(msg) => {
                write!(f, "Writer lease state is indeterminate: {msg}")
            }
            MidgeError::LeaseEpochExhausted => {
                write!(f, "Writer lease fencing epoch is exhausted")
            }
            MidgeError::WriteConflict(msg) => write!(f, "Write conflict: {msg}"),
            MidgeError::Aborted(msg) => write!(f, "Aborted: {msg}"),
            MidgeError::Busy(msg) => write!(f, "Busy: {msg}"),
            MidgeError::Timeout(msg) => write!(f, "Timeout: {msg}"),
            MidgeError::ResourceLimit(msg) => write!(f, "Resource limit: {msg}"),
        }
    }
}

impl std::error::Error for MidgeError {}

impl MidgeError {
    /// Reconstruct this error for terminal-state replay without erasing its
    /// public variant or message.
    pub(crate) fn replay(&self) -> Self {
        match self {
            Self::Io(error) => Self::Io(error.raw_os_error().map_or_else(
                || io::Error::new(error.kind(), error.to_string()),
                io::Error::from_raw_os_error,
            )),
            Self::NotFound => Self::NotFound,
            Self::InvalidArgument(message) => Self::InvalidArgument(message.clone()),
            Self::Corruption(message) => Self::Corruption(message.clone()),
            Self::NotSupported(message) => Self::NotSupported(message.clone()),
            Self::Internal(message) => Self::Internal(message.clone()),
            Self::InvalidPath => Self::InvalidPath,
            Self::NoSpace(message) => Self::NoSpace(message.clone()),
            Self::RecoveryFailed(message) => Self::RecoveryFailed(message.clone()),
            Self::CompatibilityError(message) => Self::CompatibilityError(message.clone()),
            Self::WriteStall(message) => Self::WriteStall(message.clone()),
            Self::MemoryModeViolation(message) => Self::MemoryModeViolation(message.clone()),
            Self::Fenced(message) => Self::Fenced(message.clone()),
            Self::LeaseHeld(message) => Self::LeaseHeld(message.clone()),
            Self::LeaseUnavailable(message) => Self::LeaseUnavailable(message.clone()),
            Self::LeaseIndeterminate(message) => Self::LeaseIndeterminate(message.clone()),
            Self::LeaseEpochExhausted => Self::LeaseEpochExhausted,
            Self::WriteConflict(message) => Self::WriteConflict(message.clone()),
            Self::Aborted(message) => Self::Aborted(message.clone()),
            Self::Busy(message) => Self::Busy(message.clone()),
            Self::Timeout(message) => Self::Timeout(message.clone()),
            Self::ResourceLimit(message) => Self::ResourceLimit(message.clone()),
        }
    }
}

impl From<io::Error> for MidgeError {
    fn from(err: io::Error) -> Self {
        let raw_code = err.raw_os_error();
        let text = err.to_string().to_ascii_lowercase();
        if matches!(raw_code, Some(28 | 112))
            || text.contains("no space")
            || text.contains("disk full")
        {
            MidgeError::NoSpace(err.to_string())
        } else {
            MidgeError::Io(err)
        }
    }
}
