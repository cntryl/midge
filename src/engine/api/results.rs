//! Result types for advanced operations
//!
//! Result enums for operations like Compare-and-Swap and Insert that
//! need to communicate both success and what was present before.

use bytes::Bytes;

/// Result of an Insert operation (which fails if key already exists)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResult {
    /// Key was successfully inserted
    Ok,
    /// Key already exists with this value
    AlreadyExists(Bytes),
}

impl InsertResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, InsertResult::Ok)
    }
}

/// Result of a Compare-and-Swap operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasResult {
    /// CAS succeeded - value was swapped
    Swapped,
    /// CAS failed - expected value didn't match; returns actual value
    Mismatch(Option<Bytes>),
}

impl CasResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, CasResult::Swapped)
    }

    pub fn is_mismatch(&self) -> bool {
        matches!(self, CasResult::Mismatch(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== InsertResult Tests ==========
    // Tests for InsertResult invariants: Ok variant and AlreadyExists variant

    #[test]
    fn should_return_ok_when_insert_succeeded() {
        // Arrange
        // (no setup required)

        // Act
        let result = InsertResult::Ok;

        // Assert
        assert_eq!(result, InsertResult::Ok);
    }

    #[test]
    fn should_detect_ok_variant_when_calling_is_ok() {
        // Arrange
        // (no setup required)

        // Act
        let result = InsertResult::Ok;

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_return_already_exists_when_key_present() {
        // Arrange
        let value = Bytes::from_static(b"existing_value");

        // Act
        let result = InsertResult::AlreadyExists(value.clone());

        // Assert
        assert_eq!(result, InsertResult::AlreadyExists(value));
    }

    #[test]
    fn should_detect_already_exists_variant_when_calling_is_ok() {
        // Arrange
        let value = Bytes::from_static(b"value");

        // Act
        let result = InsertResult::AlreadyExists(value);

        // Assert
        assert!(!result.is_ok());
    }

    #[test]
    fn should_preserve_bytes_in_already_exists_variant() {
        // Arrange
        let bytes = Bytes::from(&[0x01, 0x02, 0x03][..]);

        // Act
        let result = InsertResult::AlreadyExists(bytes.clone());

        // Assert
        match result {
            InsertResult::AlreadyExists(stored) => assert_eq!(stored, bytes),
            _ => panic!("Expected AlreadyExists variant"),
        }
    }

    #[test]
    fn should_handle_empty_bytes_in_already_exists() {
        // Arrange
        let empty = Bytes::from_static(b"");

        // Act
        let result = InsertResult::AlreadyExists(empty.clone());

        // Assert
        assert_eq!(result, InsertResult::AlreadyExists(empty));
    }

    #[test]
    fn should_clone_insert_result_ok() {
        // Arrange
        let original = InsertResult::Ok;

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned, InsertResult::Ok);
    }

    #[test]
    fn should_clone_insert_result_already_exists() {
        // Arrange
        let value = Bytes::from_static(b"value");
        let original = InsertResult::AlreadyExists(value.clone());

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned, InsertResult::AlreadyExists(value));
    }

    #[test]
    fn should_debug_format_insert_result_ok() {
        // Arrange
        // (no setup required)

        // Act
        let result = InsertResult::Ok;
        let debug_str = format!("{:?}", result);

        // Assert
        assert!(debug_str.contains("Ok"));
    }

    #[test]
    fn should_debug_format_insert_result_already_exists() {
        // Arrange
        let value = Bytes::from_static(b"val");

        // Act
        let result = InsertResult::AlreadyExists(value);
        let debug_str = format!("{:?}", result);

        // Assert
        assert!(debug_str.contains("AlreadyExists"));
    }

    // ========== CasResult Tests ==========
    // Tests for CasResult invariants: Swapped variant and Mismatch variant

    #[test]
    fn should_return_swapped_when_cas_succeeded() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Swapped;

        // Assert
        assert_eq!(result, CasResult::Swapped);
    }

    #[test]
    fn should_detect_swapped_variant_when_calling_is_ok() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Swapped;

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_detect_swapped_variant_not_mismatch() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Swapped;

        // Assert
        assert!(!result.is_mismatch());
    }

    #[test]
    fn should_return_mismatch_with_none_when_key_not_found() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Mismatch(None);

        // Assert
        assert_eq!(result, CasResult::Mismatch(None));
    }

    #[test]
    fn should_return_mismatch_with_value_when_key_found() {
        // Arrange
        let value = Bytes::from_static(b"actual_value");

        // Act
        let result = CasResult::Mismatch(Some(value.clone()));

        // Assert
        assert_eq!(result, CasResult::Mismatch(Some(value)));
    }

    #[test]
    fn should_detect_mismatch_variant_when_calling_is_mismatch() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Mismatch(None);

        // Assert
        assert!(result.is_mismatch());
    }

    #[test]
    fn should_not_detect_swapped_as_mismatch() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Swapped;

        // Assert
        assert!(!result.is_mismatch());
    }

    #[test]
    fn should_not_detect_mismatch_as_ok() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Mismatch(None);

        // Assert
        assert!(!result.is_ok());
    }

    #[test]
    fn should_detect_mismatch_variant_when_value_present() {
        // Arrange
        let value = Bytes::from_static(b"value");

        // Act
        let result = CasResult::Mismatch(Some(value.clone()));

        // Assert
        assert!(result.is_mismatch());
    }

    #[test]
    fn should_not_detect_mismatch_as_ok_when_value_present() {
        // Arrange
        let value = Bytes::from_static(b"value");

        // Act
        let result = CasResult::Mismatch(Some(value));

        // Assert
        assert!(!result.is_ok());
    }

    #[test]
    fn should_preserve_bytes_in_mismatch_variant() {
        // Arrange
        let bytes = Bytes::from(&[0xFF, 0xEE, 0xDD][..]);

        // Act
        let result = CasResult::Mismatch(Some(bytes.clone()));

        // Assert
        match result {
            CasResult::Mismatch(Some(stored)) => assert_eq!(stored, bytes),
            _ => panic!("Expected Mismatch with Some variant"),
        }
    }

    #[test]
    fn should_clone_cas_result_swapped() {
        // Arrange
        let original = CasResult::Swapped;

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned, CasResult::Swapped);
    }

    #[test]
    fn should_clone_cas_result_mismatch_none() {
        // Arrange
        let original = CasResult::Mismatch(None);

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned, CasResult::Mismatch(None));
    }

    #[test]
    fn should_clone_cas_result_mismatch_some() {
        // Arrange
        let value = Bytes::from_static(b"value");
        let original = CasResult::Mismatch(Some(value.clone()));

        // Act
        let cloned = original.clone();

        // Assert
        assert_eq!(cloned, CasResult::Mismatch(Some(value)));
    }

    #[test]
    fn should_debug_format_cas_result_swapped() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Swapped;
        let debug_str = format!("{:?}", result);

        // Assert
        assert!(debug_str.contains("Swapped"));
    }

    #[test]
    fn should_debug_format_cas_result_mismatch() {
        // Arrange
        // (no setup required)

        // Act
        let result = CasResult::Mismatch(None);
        let debug_str = format!("{:?}", result);

        // Assert
        assert!(debug_str.contains("Mismatch"));
    }

    #[test]
    fn should_handle_large_bytes_in_mismatch() {
        // Arrange
        let large_bytes = Bytes::from(vec![42u8; 10000]);

        // Act
        let result = CasResult::Mismatch(Some(large_bytes.clone()));

        // Assert
        match result {
            CasResult::Mismatch(Some(stored)) => assert_eq!(stored.len(), 10000),
            _ => panic!("Expected Mismatch with bytes"),
        }
    }

    #[test]
    fn should_handle_binary_data_in_mismatch() {
        // Arrange
        let binary = Bytes::from(&[0x00, 0xFF, 0x80, 0x01][..]);

        // Act
        let result = CasResult::Mismatch(Some(binary.clone()));

        // Assert
        assert_eq!(result, CasResult::Mismatch(Some(binary)));
    }
}
