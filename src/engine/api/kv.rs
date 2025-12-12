//! Key-Value API Types
//!
//! Common types used throughout the KV API.

use bytes::Bytes;

/// Key type alias
pub type Key = Bytes;

/// Value type alias
pub type Value = Bytes;

/// Key-value pair
pub type KvPair = (Key, Value);

/// Optional value (None means deleted/not found)
pub type OptionalValue = Option<Value>;

/// Convert byte slice to Key
pub fn key_from_slice(slice: &[u8]) -> Key {
    Bytes::copy_from_slice(slice)
}

/// Convert byte slice to Value
pub fn value_from_slice(slice: &[u8]) -> Value {
    Bytes::copy_from_slice(slice)
}

/// Convert byte vector to Key
pub fn key_from_vec(vec: Vec<u8>) -> Key {
    Bytes::from(vec)
}

/// Convert byte vector to Value
pub fn value_from_vec(vec: Vec<u8>) -> Value {
    Bytes::from(vec)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========== Key Type Tests ==========
    // Tests for Key type alias: handles Bytes correctly

    #[test]
    fn should_create_key_from_slice() {
        // Arrange
        let slice = b"test_key";

        // Act
        let key = key_from_slice(slice);

        // Assert
        assert_eq!(key, Bytes::from_static(b"test_key"));
    }

    #[test]
    fn should_create_key_from_empty_slice() {
        // Arrange & Act
        let key = key_from_slice(b"");

        // Assert
        assert_eq!(key, Bytes::from_static(b""));
    }

    #[test]
    fn should_create_key_from_large_slice() {
        // Arrange
        let slice = vec![42u8; 10000];

        // Act
        let key = key_from_slice(&slice);

        // Assert
        assert_eq!(key.len(), 10000);
    }

    #[test]
    fn should_create_key_from_binary_data() {
        // Arrange
        let slice = &[0x00, 0xFF, 0x80, 0x01];

        // Act
        let key = key_from_slice(slice);

        // Assert
        assert_eq!(key.as_ref(), slice);
    }

    #[test]
    fn should_create_key_from_vec() {
        // Arrange
        let vec = vec![1, 2, 3, 4];

        // Act
        let key = key_from_vec(vec);

        // Assert
        assert_eq!(key.as_ref(), &[1, 2, 3, 4]);
    }

    #[test]
    fn should_create_key_from_empty_vec() {
        // Arrange & Act
        let key = key_from_vec(vec![]);

        // Assert
        assert_eq!(key.len(), 0);
    }

    #[test]
    fn should_preserve_data_integrity_when_creating_key_from_vec() {
        // Arrange
        let original_data = vec![100u8; 5000];

        // Act
        let key = key_from_vec(original_data.clone());

        // Assert
        assert_eq!(key.as_ref(), original_data.as_slice());
    }

    // ========== Value Type Tests ==========
    // Tests for Value type alias: handles Bytes correctly

    #[test]
    fn should_create_value_from_slice() {
        // Arrange
        let slice = b"test_value";

        // Act
        let value = value_from_slice(slice);

        // Assert
        assert_eq!(value, Bytes::from_static(b"test_value"));
    }

    #[test]
    fn should_create_value_from_empty_slice() {
        // Arrange & Act
        let value = value_from_slice(b"");

        // Assert
        assert_eq!(value, Bytes::from_static(b""));
    }

    #[test]
    fn should_create_value_from_large_slice() {
        // Arrange
        let slice = vec![99u8; 8000];

        // Act
        let value = value_from_slice(&slice);

        // Assert
        assert_eq!(value.len(), 8000);
    }

    #[test]
    fn should_create_value_from_binary_data() {
        // Arrange
        let slice = &[0xFF, 0x00, 0x7F, 0x80];

        // Act
        let value = value_from_slice(slice);

        // Assert
        assert_eq!(value.as_ref(), slice);
    }

    #[test]
    fn should_create_value_from_vec() {
        // Arrange
        let vec = vec![5, 6, 7, 8];

        // Act
        let value = value_from_vec(vec);

        // Assert
        assert_eq!(value.as_ref(), &[5, 6, 7, 8]);
    }

    #[test]
    fn should_create_value_from_empty_vec() {
        // Arrange & Act
        let value = value_from_vec(vec![]);

        // Assert
        assert_eq!(value.len(), 0);
    }

    #[test]
    fn should_preserve_data_integrity_when_creating_value_from_vec() {
        // Arrange
        let original_data = vec![200u8; 3000];

        // Act
        let value = value_from_vec(original_data.clone());

        // Assert
        assert_eq!(value.as_ref(), original_data.as_slice());
    }

    // ========== KvPair Type Tests ==========
    // Tests for KvPair type alias: handles (Key, Value) tuples

    #[test]
    fn should_create_kv_pair() {
        // Arrange
        let key = key_from_slice(b"key");
        let value = value_from_slice(b"value");

        // Act
        let pair: KvPair = (key.clone(), value.clone());

        // Assert
        assert_eq!(pair.0, key);
        assert_eq!(pair.1, value);
    }

    #[test]
    fn should_access_key_from_pair() {
        // Arrange
        let pair: KvPair = (key_from_slice(b"k"), value_from_slice(b"v"));

        // Act & Assert
        assert_eq!(pair.0.as_ref(), b"k");
    }

    #[test]
    fn should_access_value_from_pair() {
        // Arrange
        let pair: KvPair = (key_from_slice(b"k"), value_from_slice(b"v"));

        // Act & Assert
        assert_eq!(pair.1.as_ref(), b"v");
    }

    // ========== OptionalValue Type Tests ==========
    // Tests for OptionalValue type alias: handles Option<Value>

    #[test]
    fn should_create_optional_value_some() {
        // Arrange & Act
        let opt_value: OptionalValue = Some(value_from_slice(b"value"));

        // Assert
        assert!(opt_value.is_some());
        assert_eq!(opt_value.unwrap().as_ref(), b"value");
    }

    #[test]
    fn should_create_optional_value_none() {
        // Arrange & Act
        let opt_value: OptionalValue = None;

        // Assert
        assert!(opt_value.is_none());
    }

    #[test]
    fn should_handle_optional_value_mapping() {
        // Arrange
        let opt_value: OptionalValue = Some(value_from_slice(b"original"));

        // Act
        let mapped = opt_value.map(|v| v);

        // Assert
        assert_eq!(mapped.unwrap().as_ref(), b"original");
    }

    // ========== Type Conversion Tests ==========
    // Tests for type conversions between Key/Value and Bytes

    #[test]
    fn should_convert_key_to_bytes_slice() {
        // Arrange
        let key = key_from_slice(b"mykey");

        // Act
        let slice: &[u8] = key.as_ref();

        // Assert
        assert_eq!(slice, b"mykey");
    }

    #[test]
    fn should_convert_value_to_bytes_slice() {
        // Arrange
        let value = value_from_vec(vec![1, 2, 3]);

        // Act
        let slice: &[u8] = value.as_ref();

        // Assert
        assert_eq!(slice, &[1, 2, 3]);
    }

    // ========== Edge Cases ==========

    #[test]
    fn should_handle_utf8_in_key() {
        // Arrange
        let utf8_bytes = "日本語".as_bytes();

        // Act
        let key = key_from_slice(utf8_bytes);

        // Assert
        assert_eq!(key.as_ref(), utf8_bytes);
    }

    #[test]
    fn should_handle_utf8_in_value() {
        // Arrange
        let utf8_string = "中文测试";
        let bytes = utf8_string.as_bytes().to_vec();

        // Act
        let value = value_from_vec(bytes.clone());

        // Assert
        assert_eq!(value.as_ref(), bytes.as_slice());
    }

    #[test]
    fn should_handle_all_zeros_in_key() {
        // Arrange
        let slice = vec![0u8; 100];

        // Act
        let key = key_from_slice(&slice);

        // Assert
        assert_eq!(key.len(), 100);
        assert!(key.iter().all(|&b| b == 0));
    }

    #[test]
    fn should_handle_all_ones_in_value() {
        // Arrange
        let slice = vec![0xFFu8; 100];

        // Act
        let value = value_from_vec(slice);

        // Assert
        assert_eq!(value.len(), 100);
        assert!(value.iter().all(|&b| b == 0xFF));
    }
}
