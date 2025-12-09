//! Testing utilities and mocks
//!
//! Mock implementations for testing

use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Mock storage backend for testing
pub struct MockStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MockStorage {
    fn submit_read(&self, path: String, callback: StorageCallback) {
        let data = self.data.lock().unwrap();
        let result = data
            .get(&path)
            .cloned()
            .ok_or(crate::common::MidgeError::NotFound);

        let event = StorageEvent::ReadComplete {
            path,
            result: StorageOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_write(&self, path: String, data: Vec<u8>, callback: StorageCallback) {
        let mut storage = self.data.lock().unwrap();
        storage.insert(path.clone(), data);

        let event = StorageEvent::WriteComplete {
            path,
            result: StorageOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, path: String, callback: StorageCallback) {
        let mut storage = self.data.lock().unwrap();
        storage.remove(&path);

        let event = StorageEvent::DeleteComplete {
            path,
            result: StorageOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: String, callback: StorageCallback) {
        let data = self.data.lock().unwrap();
        let results: Vec<_> = data
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        let event = StorageEvent::ListComplete {
            prefix,
            result: StorageOutcome::Ok(results),
        };
        let _ = callback.send(event);
    }
}
