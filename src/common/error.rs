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

    /// Write stall - memtable full or compaction lagging behind
    /// Application must apply backpressure
    WriteStall(String),

    /// Memory mode violation - attempted disk I/O in memory-only mode
    MemoryModeViolation(String),

    /// Writer fenced — epoch is stale, another leader has taken over
    Fenced(String),
}

impl fmt::Display for MidgeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MidgeError::Io(e) => write!(f, "IO error: {}", e),
            MidgeError::NotFound => write!(f, "Not found"),
            MidgeError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            MidgeError::Corruption(msg) => write!(f, "Corruption: {}", msg),
            MidgeError::NotSupported(msg) => write!(f, "Not supported: {}", msg),
            MidgeError::Internal(msg) => write!(f, "Internal error: {}", msg),
            MidgeError::InvalidPath => write!(f, "Invalid path"),
            MidgeError::WriteStall(msg) => write!(f, "Write stall: {}", msg),
            MidgeError::MemoryModeViolation(msg) => write!(f, "Memory mode violation: {}", msg),
            MidgeError::Fenced(msg) => write!(f, "Fenced: writer epoch is stale: {}", msg),
        }
    }
}

impl std::error::Error for MidgeError {}

impl From<io::Error> for MidgeError {
    fn from(err: io::Error) -> Self {
        MidgeError::Io(err)
    }
}

impl From<crate::lease::LeaseError> for MidgeError {
    fn from(err: crate::lease::LeaseError) -> Self {
        MidgeError::Fenced(err.to_string())
    }
}
