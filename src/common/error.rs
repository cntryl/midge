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

/// How a failure should be acted on, as opposed to what produced it.
///
/// This is the single classification authority. Callers must ask the error
/// rather than re-deriving policy with `matches!`, so that adding a
/// `MidgeError` variant is a compile error here instead of a silent
/// misclassification at ~37 call sites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    /// The caller asked for something invalid. Retrying is pointless.
    Caller,
    /// An environmental failure that may succeed on a later attempt.
    Transient,
    /// A bounded resource is temporarily full. Retry after release, and do
    /// not report as a defect.
    Backpressure,
    /// This writer no longer holds, or never held, durable authority.
    Fenced,
    /// An engine invariant was violated. Durable data may still be intact, so
    /// this is a defect to report rather than a reason to stop accepting writes.
    Defect,
    /// Durable state cannot be trusted. The engine must stop, not retry.
    Fatal,
}

impl MidgeError {
    /// Classify this failure for retry, backpressure, fencing, or halt.
    ///
    /// Exhaustive by construction: a new variant will not compile until it
    /// is classified here.
    #[must_use]
    pub fn severity(&self) -> Severity {
        match self {
            Self::NotFound
            | Self::InvalidArgument(_)
            | Self::NotSupported(_)
            | Self::InvalidPath
            | Self::MemoryModeViolation(_)
            | Self::WriteConflict(_) => Severity::Caller,

            Self::Io(_) | Self::LeaseHeld(_) | Self::LeaseUnavailable(_) | Self::Aborted(_) => {
                Severity::Transient
            }

            Self::NoSpace(_) | Self::WriteStall(_) | Self::Busy(_) | Self::Timeout(_) => {
                Severity::Backpressure
            }

            Self::Fenced(_) | Self::LeaseEpochExhausted | Self::LeaseIndeterminate(_) => {
                Severity::Fenced
            }

            // ResourceLimit also represents exhausted identity spaces,
            // address-space overflow, and invalid configured capacities. It
            // therefore cannot be globally classified as retryable pressure.
            // Temporary contention is tracked internally by ResourceBudget.
            Self::Internal(_) | Self::ResourceLimit(_) => Severity::Defect,

            Self::Corruption(_) | Self::RecoveryFailed(_) | Self::CompatibilityError(_) => {
                Severity::Fatal
            }
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::{MidgeError, Severity};

    #[test]
    fn should_classify_as_backpressure_when_error_is_transient_admission_pressure() {
        // Arrange: the pool-pressure family the runtime must retry rather than
        // surface as a defect.
        let errors = [
            MidgeError::Busy("publication turn".into()),
            MidgeError::WriteStall("memtable full".into()),
        ];

        // Act
        for error in errors {
            let severity = error.severity();

            // Assert
            assert_eq!(
                severity,
                Severity::Backpressure,
                "{error} must classify as backpressure"
            );
        }
    }

    #[test]
    fn should_classify_as_fatal_when_error_indicates_unrecoverable_data_loss() {
        // Arrange
        let errors = [
            MidgeError::Corruption("bad crc".into()),
            MidgeError::RecoveryFailed("torn manifest".into()),
        ];

        // Act
        let severities = errors.map(|error| error.severity());

        // Assert
        assert_eq!(severities, [Severity::Fatal, Severity::Fatal]);
    }

    #[test]
    fn should_classify_as_fenced_when_writer_lost_authority() {
        // Arrange
        let error = MidgeError::Fenced("stale epoch".into());

        // Act
        let severity = error.severity();

        // Assert
        assert_eq!(severity, Severity::Fenced);
    }

    #[test]
    fn should_classify_resource_limit_as_non_retryable_when_limit_may_be_permanent() {
        // Arrange
        let error = MidgeError::ResourceLimit("identity space exhausted".into());

        // Act
        let severity = error.severity();

        // Assert
        assert_eq!(severity, Severity::Defect);
    }
}
