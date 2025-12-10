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
