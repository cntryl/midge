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

#[cfg(test)]
mod tests {
    use super::*;

    // ========== ApiError Variants Tests ==========
    // Tests for ApiError enum variants: NotFound, AlreadyExists, InvalidOperation, etc.

    #[test]
    fn should_create_not_found_error() {
        // Arrange
        // (no setup required)

        // Act
        let error = ApiError::NotFound;

        // Assert
        assert_eq!(error, ApiError::NotFound);
    }

    #[test]
    fn should_create_already_exists_error() {
        // Arrange
        // (no setup required)

        // Act
        let error = ApiError::AlreadyExists;

        // Assert
        assert_eq!(error, ApiError::AlreadyExists);
    }

    #[test]
    fn should_create_transaction_conflict_error() {
        // Arrange
        // (no setup required)

        // Act
        let error = ApiError::TransactionConflict;

        // Assert
        assert_eq!(error, ApiError::TransactionConflict);
    }

    #[test]
    fn should_create_invalid_operation_error_with_message() {
        // Arrange
        let message = "operation not allowed".to_string();

        // Act
        let error = ApiError::InvalidOperation(message.clone());

        // Assert
        assert_eq!(error, ApiError::InvalidOperation(message));
    }

    #[test]
    fn should_create_internal_error_with_message() {
        // Arrange
        let message = "something went wrong".to_string();

        // Act
        let error = ApiError::Internal(message.clone());

        // Assert
        assert_eq!(error, ApiError::Internal(message));
    }

    // ========== Display Trait Tests ==========
    // Tests for Display implementation: formatted output

    #[test]
    fn should_display_not_found_error() {
        // Arrange
        // (no setup required)

        // Act
        let error = ApiError::NotFound;
        let displayed = format!("{}", error);

        // Assert
        assert_eq!(displayed, "Key not found");
    }

    #[test]
    fn should_display_already_exists_error() {
        // Arrange
        // Act
        let error = ApiError::AlreadyExists;
        let displayed = format!("{}", error);

        // Assert
        assert_eq!(displayed, "Key already exists");
    }

    #[test]
    fn should_display_transaction_conflict_error() {
        // Arrange
        // Act
        let error = ApiError::TransactionConflict;
        let displayed = format!("{}", error);

        // Assert
        assert_eq!(displayed, "Transaction conflict");
    }

    #[test]
    fn should_display_invalid_operation_error_with_message() {
        // Arrange
        let message = "test reason";
        let error = ApiError::InvalidOperation(message.to_string());

        // Act
        let displayed = format!("{}", error);

        // Assert
        assert!(displayed.contains("Invalid operation"));
        assert!(displayed.contains(message));
    }

    #[test]
    fn should_display_internal_error_with_message() {
        // Arrange
        let message = "test failure";
        let error = ApiError::Internal(message.to_string());

        // Act
        let displayed = format!("{}", error);

        // Assert
        assert!(displayed.contains("Internal error"));
        assert!(displayed.contains(message));
    }

    // ========== Debug Trait Tests ==========
    // Tests for Debug implementation

    #[test]
    fn should_debug_format_not_found() {
        // Arrange
        // Act
        let error = ApiError::NotFound;
        let debug_str = format!("{:?}", error);

        // Assert
        assert!(debug_str.contains("NotFound"));
    }

    #[test]
    fn should_debug_format_already_exists() {
        // Arrange
        // Act
        let error = ApiError::AlreadyExists;
        let debug_str = format!("{:?}", error);

        // Assert
        assert!(debug_str.contains("AlreadyExists"));
    }

    #[test]
    fn should_debug_format_invalid_operation() {
        // Arrange
        let error = ApiError::InvalidOperation("test".to_string());

        // Act
        let debug_str = format!("{:?}", error);

        // Assert
        assert!(debug_str.contains("InvalidOperation"));
    }

    // ========== Clone Tests ==========
    // Tests for Clone trait: independent copies

    #[test]
    fn should_clone_not_found_error() {
        // Arrange
        let original = ApiError::NotFound;

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned, ApiError::NotFound);
    }

    #[test]
    fn should_clone_invalid_operation_with_message() {
        // Arrange
        let message = "test message".to_string();
        let original = ApiError::InvalidOperation(message.clone());

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned, ApiError::InvalidOperation(message));
    }

    #[test]
    fn should_clone_internal_error_with_message() {
        // Arrange
        let message = "internal message".to_string();
        let original = ApiError::Internal(message.clone());

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned, ApiError::Internal(message));
    }

    // ========== PartialEq Tests ==========
    // Tests for equality semantics

    #[test]
    fn should_be_equal_when_both_not_found() {
        // Arrange
        let error1 = ApiError::NotFound;
        let error2 = ApiError::NotFound;

        // Act
        // Assert
        assert_eq!(error1, error2);
    }

    #[test]
    fn should_not_be_equal_when_different_variants() {
        // Arrange
        let error1 = ApiError::NotFound;
        let error2 = ApiError::AlreadyExists;

        // Act
        // Assert
        assert_ne!(error1, error2);
    }

    #[test]
    fn should_be_equal_when_same_invalid_operation_message() {
        // Arrange
        let msg = "test".to_string();
        let error1 = ApiError::InvalidOperation(msg.clone());
        let error2 = ApiError::InvalidOperation(msg);

        // Act
        // Assert
        assert_eq!(error1, error2);
    }

    #[test]
    fn should_not_be_equal_when_different_invalid_operation_messages() {
        // Arrange
        let error1 = ApiError::InvalidOperation("msg1".to_string());
        let error2 = ApiError::InvalidOperation("msg2".to_string());

        // Act
        // Assert
        assert_ne!(error1, error2);
    }

    // ========== From MidgeError Tests ==========
    // Tests for From<MidgeError> conversion

    #[test]
    fn should_convert_from_midge_error() {
        // Arrange
        let midge_error = MidgeError::Internal("test error".to_string());

        // Act
        let api_error: ApiError = midge_error.into();

        // Assert
        match api_error {
            ApiError::Internal(msg) => {
                assert!(msg.contains("test error") || msg.contains("Internal error"))
            }
            _ => panic!("Expected Internal error"),
        }
    }

    // ========== ApiResult Type Tests ==========
    // Tests for ApiResult type alias

    #[test]
    fn should_create_ok_api_result() {
        // Arrange
        // (no setup required)

        // Act
        let result: ApiResult<i32> = Ok(42);

        // Assert
        assert!(result.is_ok());
        assert_eq!(result, Ok(42));
    }

    #[test]
    fn should_create_err_api_result() {
        // Arrange
        // (no setup required)

        // Act
        let result: ApiResult<i32> = Err(ApiError::NotFound);

        // Assert
        assert!(result.is_err());
        assert_eq!(result, Err(ApiError::NotFound));
    }

    #[test]
    fn should_map_ok_api_result() {
        // Arrange
        let result: ApiResult<i32> = Ok(10);

        // Act
        let mapped = result.map(|x| x * 2);

        // Assert
        assert_eq!(mapped.unwrap(), 20);
    }

    #[test]
    fn should_map_err_api_result() {
        // Arrange
        let result: ApiResult<i32> = Err(ApiError::NotFound);

        // Act
        let mapped = result.map(|x| x * 2);

        // Assert
        assert_eq!(mapped.unwrap_err(), ApiError::NotFound);
    }

    // ========== Error Trait Tests ==========
    // Tests for std::error::Error trait

    #[test]
    fn should_implement_error_trait() {
        // Arrange
        let error: Box<dyn std::error::Error> = Box::new(ApiError::NotFound);

        // Act
        // Assert
        assert_eq!(error.to_string(), "Key not found");
    }

    #[test]
    fn should_handle_error_in_result_chain() {
        // Arrange
        let result: ApiResult<()> = Err(ApiError::AlreadyExists);

        // Act
        let final_result: ApiResult<()> = result.or(Ok(()));

        // Assert
        assert!(final_result.is_ok());
    }

    // ========== Edge Cases ==========

    #[test]
    fn should_handle_empty_message_in_invalid_operation() {
        // Arrange
        // Act
        let error = ApiError::InvalidOperation(String::new());

        // Assert
        assert_eq!(error, ApiError::InvalidOperation(String::new()));
    }

    #[test]
    fn should_handle_large_message_in_internal_error() {
        // Arrange
        let large_msg = "x".repeat(10000);

        // Act
        let error = ApiError::Internal(large_msg.clone());

        // Assert
        assert_eq!(error, ApiError::Internal(large_msg));
    }

    #[test]
    fn should_handle_special_characters_in_message() {
        // Arrange
        let msg = "error: 日本語 \n\t\r".to_string();

        // Act
        let error = ApiError::InvalidOperation(msg.clone());

        // Assert
        assert_eq!(error, ApiError::InvalidOperation(msg));
    }
}
