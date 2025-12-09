//! Testing utilities and mocks
//!
//! Mock implementations for testing

use crate::storage::StorageBackend;
use crate::common::MidgeResult;

/// Mock storage backend for testing
pub struct MockStorage {
    data: std::collections::HashMap<String, Vec<u8>>,
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            data: std::collections::HashMap::new(),
        }
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MockStorage {
    fn read(&self, key: &str) -> MidgeResult<Vec<u8>> {
        self.data
            .get(key)
            .cloned()
            .ok_or(crate::common::MidgeError::NotFound)
    }

    fn write(&mut self, _key: &str, _data: &[u8]) -> MidgeResult<()> {
        todo!()
    }

    fn delete(&mut self, _key: &str) -> MidgeResult<()> {
        todo!()
    }

    fn list(&self, _prefix: &str) -> MidgeResult<Vec<String>> {
        todo!()
    }
}
