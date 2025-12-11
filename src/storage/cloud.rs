//! Callback-based cloud storage abstractions.
//!
//! Aligns with the actor runtime model: synchronous submission + async completion.
//! - `CloudBackend` defines submit-only methods (PUT/GET/DELETE/LIST/HEAD).
//! - Backends send results via `CloudCallback` channels (no futures in the engine).
//! - `CloudStorage` is a namespace-aware dispatcher that shields the rest of the engine.
//! - `MockCloudBackend` keeps deterministic testing without async runtimes.

pub mod executor;

use super::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use crate::common::MidgeError;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[cfg(feature = "cloud-common")]
pub use executor::{AwsCredentials, CloudExecutor, CloudRequest, CloudResponse, CloudSigner};

/// Cloud operation outcome – cloneable wrapper around Result
#[derive(Clone, Debug)]
pub enum CloudOutcome<T: Clone> {
    Ok(T),
    Err(String),
}

impl<T: Clone> CloudOutcome<T> {
    pub fn is_ok(&self) -> bool {
        matches!(self, CloudOutcome::Ok(_))
    }

    pub fn is_err(&self) -> bool {
        matches!(self, CloudOutcome::Err(_))
    }

    pub fn from_result(result: Result<T, MidgeError>) -> Self {
        match result {
            Ok(value) => CloudOutcome::Ok(value),
            Err(err) => CloudOutcome::Err(format!("{:?}", err)),
        }
    }
}

/// Cloud operation completion events sent back via callback.
#[derive(Clone, Debug)]
pub enum CloudEvent {
    PutComplete {
        key: String,
        result: CloudOutcome<()>,
    },
    GetComplete {
        key: String,
        result: CloudOutcome<Vec<u8>>,
    },
    GetRangeComplete {
        key: String,
        start: u64,
        end: Option<u64>,
        result: CloudOutcome<Vec<u8>>,
    },
    DeleteComplete {
        key: String,
        result: CloudOutcome<()>,
    },
    ListComplete {
        prefix: String,
        result: CloudOutcome<Vec<String>>,
    },
    HeadComplete {
        key: String,
        result: CloudOutcome<ObjectMetadata>,
    },
}

/// Callback type used to send `CloudEvent`s back to the runtime.
pub type CloudCallback = std::sync::mpsc::Sender<CloudEvent>;

/// Basic metadata emitted by HEAD operations.
#[derive(Clone, Debug)]
pub struct ObjectMetadata {
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
}

impl ObjectMetadata {
    pub fn new(size: u64, etag: String, last_modified: u64) -> Self {
        Self {
            size,
            etag,
            last_modified,
        }
    }
}

/// Non-blocking cloud backend interface used by the engine.
pub trait CloudBackend: Send + Sync + 'static {
    fn submit_put(&self, key: String, data: Vec<u8>, callback: CloudCallback);
    fn submit_get(&self, key: String, callback: CloudCallback);
    fn submit_get_range(&self, key: String, start: u64, end: Option<u64>, callback: CloudCallback);
    fn submit_delete(&self, key: String, callback: CloudCallback);
    fn submit_list(&self, prefix: String, callback: CloudCallback);
    fn submit_head(&self, key: String, callback: CloudCallback);
}

/// Deterministic mock backend for testing (synchronous).
pub struct MockCloudBackend {
    storage: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    uploads: Arc<Mutex<Vec<(String, u64)>>>,
    downloads: Arc<Mutex<Vec<String>>>,
}

impl MockCloudBackend {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            uploads: Arc::new(Mutex::new(Vec::new())),
            downloads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn object_count(&self) -> usize {
        self.storage.lock().expect("storage mutex poisoned").len()
    }

    pub fn get_uploads(&self) -> Vec<(String, u64)> {
        self.uploads.lock().expect("uploads mutex poisoned").clone()
    }

    pub fn get_downloads(&self) -> Vec<String> {
        self.downloads
            .lock()
            .expect("downloads mutex poisoned")
            .clone()
    }

    pub fn clear_history(&self) {
        self.uploads.lock().expect("uploads mutex poisoned").clear();
        self.downloads
            .lock()
            .expect("downloads mutex poisoned")
            .clear();
    }
}

impl Default for MockCloudBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudBackend for MockCloudBackend {
    fn submit_put(&self, key: String, data: Vec<u8>, callback: CloudCallback) {
        self.storage
            .lock()
            .expect("storage mutex poisoned")
            .insert(key.clone(), data.clone());
        self.uploads
            .lock()
            .expect("uploads mutex poisoned")
            .push((key.clone(), data.len() as u64));
        let event = CloudEvent::PutComplete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_get(&self, key: String, callback: CloudCallback) {
        let result = self
            .storage
            .lock()
            .expect("storage mutex poisoned")
            .get(&key)
            .cloned()
            .ok_or(MidgeError::NotFound);
        self.downloads
            .lock()
            .expect("downloads mutex poisoned")
            .push(key.clone());
        let event = CloudEvent::GetComplete {
            key,
            result: CloudOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_get_range(&self, key: String, start: u64, end: Option<u64>, callback: CloudCallback) {
        let result = self
            .storage
            .lock()
            .expect("storage mutex poisoned")
            .get(&key)
            .map(|data| {
                let end_idx = end.unwrap_or(data.len() as u64) as usize;
                let start_idx = start as usize;
                data[start_idx..end_idx].to_vec()
            })
            .ok_or(MidgeError::NotFound);
        let event = CloudEvent::GetRangeComplete {
            key,
            start,
            end,
            result: CloudOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, key: String, callback: CloudCallback) {
        self.storage
            .lock()
            .expect("storage mutex poisoned")
            .remove(&key);
        let event = CloudEvent::DeleteComplete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: String, callback: CloudCallback) {
        let results: Vec<_> = self
            .storage
            .lock()
            .expect("storage mutex poisoned")
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        let event = CloudEvent::ListComplete {
            prefix,
            result: CloudOutcome::Ok(results),
        };
        let _ = callback.send(event);
    }

    fn submit_head(&self, key: String, callback: CloudCallback) {
        let result = self
            .storage
            .lock()
            .expect("storage mutex poisoned")
            .get(&key)
            .map(|data| ObjectMetadata::new(data.len() as u64, format!("mock-{}", data.len()), 0))
            .ok_or(MidgeError::NotFound);
        let event = CloudEvent::HeadComplete {
            key,
            result: CloudOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }
}

/// Namespace-aware dispatcher that forwards calls to the active backend.
pub struct CloudStorage {
    backend: Arc<dyn CloudBackend>,
    namespace: String,
}

impl CloudStorage {
    pub fn new(backend: Arc<dyn CloudBackend>, namespace: String) -> Self {
        Self { backend, namespace }
    }

    pub fn with_mock() -> Self {
        let backend = Arc::new(MockCloudBackend::new());
        Self::new(backend, "midge".to_string())
    }

    fn full_path(&self, suffix: &str) -> String {
        format!("{}/{}", self.namespace, suffix)
    }

    pub fn submit_put(&self, key: String, data: Vec<u8>, callback: CloudCallback) {
        self.backend
            .submit_put(self.full_path(&key), data, callback);
    }

    pub fn submit_get(&self, key: String, callback: CloudCallback) {
        self.backend.submit_get(self.full_path(&key), callback);
    }

    pub fn submit_get_range(
        &self,
        key: String,
        start: u64,
        end: Option<u64>,
        callback: CloudCallback,
    ) {
        self.backend
            .submit_get_range(self.full_path(&key), start, end, callback);
    }

    pub fn submit_delete(&self, key: String, callback: CloudCallback) {
        self.backend.submit_delete(self.full_path(&key), callback);
    }

    pub fn submit_list(&self, prefix: String, callback: CloudCallback) {
        self.backend.submit_list(self.full_path(&prefix), callback);
    }

    pub fn submit_head(&self, key: String, callback: CloudCallback) {
        self.backend.submit_head(self.full_path(&key), callback);
    }

    fn submit_delete_internal(&self, key: String, callback: CloudCallback) {
        self.backend.submit_delete(self.full_path(&key), callback);
    }

    fn submit_list_internal(&self, prefix: String, callback: CloudCallback) {
        self.backend.submit_list(self.full_path(&prefix), callback);
    }
}

impl StorageBackend for CloudStorage {
    fn submit_read(&self, path: String, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_get(path.clone(), tx);
        if let Ok(CloudEvent::GetComplete { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(data) => StorageOutcome::Ok(data),
                CloudOutcome::Err(err) => StorageOutcome::Err(err),
            };
            let event = StorageEvent::ReadComplete {
                path: key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_write(&self, path: String, data: Vec<u8>, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_put(path.clone(), data, tx);
        if let Ok(CloudEvent::PutComplete { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(()) => StorageOutcome::Ok(()),
                CloudOutcome::Err(err) => StorageOutcome::Err(err),
            };
            let event = StorageEvent::WriteComplete {
                path: key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_delete(&self, path: String, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_delete_internal(path.clone(), tx);
        if let Ok(CloudEvent::DeleteComplete { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(()) => StorageOutcome::Ok(()),
                CloudOutcome::Err(err) => StorageOutcome::Err(err),
            };
            let event = StorageEvent::DeleteComplete {
                path: key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_list(&self, prefix: String, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_list_internal(prefix.clone(), tx);
        if let Ok(CloudEvent::ListComplete {
            prefix: key_prefix,
            result,
        }) = rx.recv()
        {
            let outcome = match result {
                CloudOutcome::Ok(items) => StorageOutcome::Ok(items),
                CloudOutcome::Err(err) => StorageOutcome::Err(err),
            };
            let event = StorageEvent::ListComplete {
                prefix: key_prefix,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn should_route_put_to_backend() {
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        storage.submit_put("file".into(), vec![1], tx);
        let event = rx.recv().unwrap();
        if let CloudEvent::PutComplete { key, result } = event {
            assert_eq!(key, "midge/file");
            assert!(result.is_ok());
        } else {
            panic!("unexpected event");
        }
    }
}
