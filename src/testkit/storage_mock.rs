//! Mock storage backend for tests.

use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

/// Mock storage backend for testing.
pub struct MockStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockStorage {
    #[must_use]
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
    fn submit_read(&self, key: &str, callback: StorageCallback) {
        let data = self.data.lock();
        let result = data
            .get(key)
            .cloned()
            .ok_or(crate::common::MidgeError::NotFound);

        let event = StorageEvent::ReadComplete {
            key: key.to_string(),
            result: match result {
                Ok(v) => StorageOutcome::Ok(v),
                Err(e) => StorageOutcome::Err(format!("{e:?}")),
            },
        };
        let _ = callback.send(event);
    }

    fn submit_write(&self, key: &str, data: Vec<u8>, callback: StorageCallback) {
        let mut storage = self.data.lock();
        storage.insert(key.to_string(), data);

        let event = StorageEvent::WriteComplete {
            key: key.to_string(),
            result: StorageOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, key: &str, callback: StorageCallback) {
        let mut storage = self.data.lock();
        storage.remove(key);

        let event = StorageEvent::DeleteComplete {
            key: key.to_string(),
            result: StorageOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: &str, callback: StorageCallback) {
        let data = self.data.lock();
        let results: Vec<_> = data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();

        let event = StorageEvent::ListComplete {
            prefix: prefix.to_string(),
            result: StorageOutcome::Ok(results),
        };
        let _ = callback.send(event);
    }
}
