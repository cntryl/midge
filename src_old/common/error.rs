//! Core error types for Midge database

use thiserror::Error;

/// Result type alias for Midge operations
pub type MidgeResult<T> = Result<T, MidgeError>;

/// Midge database error types
#[derive(Error, Debug)]
pub enum MidgeError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Key not found: {key}")]
    KeyNotFound { key: String },

    #[error("Key already exists: {key}")]
    KeyExists { key: String },

    #[error("Transaction conflict: {message}")]
    TransactionConflict { message: String },

    #[error("Deadlock detected: transaction {victim_txn_id} aborted (cycle: {cycle:?})")]
    Deadlock { victim_txn_id: u64, cycle: Vec<u64> },

    #[error("Database is closed")]
    DatabaseClosed,

    #[error("Database is locked by another process")]
    DatabaseLocked,

    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Corruption detected: {message}")]
    Corruption { message: String },

    #[error("Compaction error: {message}")]
    CompactionError { message: String },

    #[error("Compression error: {message}")]
    CompressionError { message: String },

    #[error("WAL error: {message}")]
    WalError { message: String },

    #[error("Internal error: {message}")]
    Internal { message: String },

    #[error("Database opened in read-only mode")]
    ReadOnly,

    #[error("Cloud storage error: {message}")]
    CloudError { message: String },

    #[cfg(any(
        feature = "cloud-aws",
        feature = "cloud-azure",
        feature = "cloud-gcp",
        feature = "cloud-oci"
    ))]
    #[error("HTTP error: {0}")]
    Http(String),
}

#[cfg(any(
    feature = "cloud-aws",
    feature = "cloud-azure",
    feature = "cloud-gcp",
    feature = "cloud-oci"
))]
impl From<ureq::Error> for MidgeError {
    fn from(err: ureq::Error) -> Self {
        Self::Http(format!("HTTP error: {}", err))
    }
}

impl MidgeError {
    pub fn corruption(message: impl Into<String>) -> Self {
        Self::Corruption {
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::InvalidConfig {
            message: message.into(),
        }
    }

    pub fn cloud_error(message: impl Into<String>) -> Self {
        Self::CloudError {
            message: message.into(),
        }
    }

    pub fn transaction_conflict(message: impl Into<String>) -> Self {
        Self::TransactionConflict {
            message: message.into(),
        }
    }

    pub fn deadlock(victim_txn_id: u64, cycle: Vec<u64>) -> Self {
        Self::Deadlock {
            victim_txn_id,
            cycle,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn should_create_corruption_error_given_message_when_using_helper() {
        // Arrange
        let message = "checksum mismatch";

        // Act
        let error = MidgeError::corruption(message);

        // Assert
        match error {
            MidgeError::Corruption { message: msg } => {
                assert_eq!(msg, "checksum mismatch");
            }
            _ => panic!("Expected Corruption variant"),
        }
    }

    #[test]
    fn should_create_internal_error_given_message_when_using_helper() {
        // Arrange
        let message = "unexpected state";

        // Act
        let error = MidgeError::internal(message);

        // Assert
        match error {
            MidgeError::Internal { message: msg } => {
                assert_eq!(msg, "unexpected state");
            }
            _ => panic!("Expected Internal variant"),
        }
    }

    #[test]
    fn should_create_invalid_config_error_given_message_when_using_helper() {
        // Arrange
        let message = "block_size must be positive";

        // Act
        let error = MidgeError::invalid_config(message);

        // Assert
        match error {
            MidgeError::InvalidConfig { message: msg } => {
                assert_eq!(msg, "block_size must be positive");
            }
            _ => panic!("Expected InvalidConfig variant"),
        }
    }

    #[test]
    fn should_create_cloud_error_given_message_when_using_helper() {
        // Arrange
        let message = "upload failed";

        // Act
        let error = MidgeError::cloud_error(message);

        // Assert
        match error {
            MidgeError::CloudError { message: msg } => {
                assert_eq!(msg, "upload failed");
            }
            _ => panic!("Expected CloudError variant"),
        }
    }

    #[test]
    fn should_convert_from_io_error_when_using_from() {
        // Arrange
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");

        // Act
        let error: MidgeError = io_err.into();

        // Assert
        assert!(matches!(error, MidgeError::Io(_)));
    }

    #[test]
    fn should_convert_lz4_error_to_compression_error() {
        // Arrange
        let data = vec![0u8; 10];

        // Act
        let result = lz4_flex::decompress_size_prepended(&data);

        // Assert
        assert!(result.is_err());
        let error = MidgeError::CompressionError {
            message: result.unwrap_err().to_string(),
        };
        assert!(matches!(error, MidgeError::CompressionError { .. }));
    }

    #[test]
    fn should_display_corruption_error_with_message() {
        // Arrange
        let error = MidgeError::corruption("data corrupted");

        // Act
        let display = format!("{}", error);

        // Assert
        assert_eq!(display, "Corruption detected: data corrupted");
    }

    #[test]
    fn should_display_internal_error_with_message() {
        // Arrange
        let error = MidgeError::internal("internal panic");

        // Act
        let display = format!("{}", error);

        // Assert
        assert_eq!(display, "Internal error: internal panic");
    }

    #[test]
    fn should_display_database_closed_error() {
        // Arrange
        let error = MidgeError::DatabaseClosed;

        // Act
        let display = format!("{}", error);

        // Assert
        assert_eq!(display, "Database is closed");
    }

    #[test]
    fn should_display_readonly_error() {
        // Arrange
        let error = MidgeError::ReadOnly;

        // Act
        let display = format!("{}", error);

        // Assert
        assert_eq!(display, "Database opened in read-only mode");
    }

    #[test]
    fn should_accept_string_in_corruption_helper() {
        // Arrange
        let message = String::from("msg1");

        // Act
        let error = MidgeError::corruption(message);

        // Assert
        assert!(matches!(error, MidgeError::Corruption { .. }));
    }

    #[test]
    fn should_accept_string_in_internal_helper() {
        // Arrange
        let message = String::from("msg2");

        // Act
        let error = MidgeError::internal(message);

        // Assert
        assert!(matches!(error, MidgeError::Internal { .. }));
    }

    #[test]
    fn should_accept_string_in_invalid_config_helper() {
        // Arrange
        let message = String::from("msg3");

        // Act
        let error = MidgeError::invalid_config(message);

        // Assert
        assert!(matches!(error, MidgeError::InvalidConfig { .. }));
    }

    #[test]
    fn should_accept_string_in_cloud_error_helper() {
        // Arrange
        let message = String::from("msg4");

        // Act
        let error = MidgeError::cloud_error(message);

        // Assert
        assert!(matches!(error, MidgeError::CloudError { .. }));
    }
}
