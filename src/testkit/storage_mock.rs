//! Mock storage backend for tests.

use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Mock storage backend for testing.
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
    fn submit_read(&self, key: String, callback: StorageCallback) {
        let data = self.data.lock().expect("storage mutex poisoned");
        let result = data
            .get(&key)
            .cloned()
            .ok_or(crate::common::MidgeError::NotFound);

        let event = StorageEvent::ReadComplete {
            key,
            result: StorageOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_write(&self, key: String, data: Vec<u8>, callback: StorageCallback) {
        let mut storage = self.data.lock().expect("storage mutex poisoned");
        storage.insert(key.clone(), data);

        let event = StorageEvent::WriteComplete {
            key,
            result: StorageOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, key: String, callback: StorageCallback) {
        let mut storage = self.data.lock().expect("storage mutex poisoned");
        storage.remove(&key);

        let event = StorageEvent::DeleteComplete {
            key,
            result: StorageOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: String, callback: StorageCallback) {
        let data = self.data.lock().expect("storage mutex poisoned");
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
