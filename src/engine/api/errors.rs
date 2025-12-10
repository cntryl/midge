//! Engine API Errors
//!
//! API-level error types for engine operations.
//! These wrap the common MidgeError with additional context.

use crate::common::MidgeError;

/// Result type for engine API operations
pub type ApiResult<T> = Result<T, ApiError>;

/// API-level error
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// Key not found
    NotFound,
    /// Key already exists
    AlreadyExists,
    /// Invalid operation
    InvalidOperation(String),
    /// Transaction conflict
    TransactionConflict,
    /// Internal engine error
    Internal(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::NotFound => write!(f, "Key not found"),
            ApiError::AlreadyExists => write!(f, "Key already exists"),
            ApiError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            ApiError::TransactionConflict => write!(f, "Transaction conflict"),
            ApiError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for ApiError {}

impl From<MidgeError> for ApiError {
    fn from(err: MidgeError) -> Self {
        ApiError::Internal(err.to_string())
    }
}
