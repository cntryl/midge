//! Error types for Midge

use std::fmt;
use std::io;

/// Result type for Midge operations
pub type MidgeResult<T> = Result<T, MidgeError>;

/// Main error type for Midge
#[derive(Debug)]
pub enum MidgeError {
    /// IO error
    Io(io::Error),
    
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
        }
    }
}

impl std::error::Error for MidgeError {}

impl From<io::Error> for MidgeError {
    fn from(err: io::Error) -> Self {
        MidgeError::Io(err)
    }
}
