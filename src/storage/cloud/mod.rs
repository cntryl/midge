//! Callback-based cloud storage abstractions.
//!
//! Aligns with the actor runtime model: synchronous submission + async completion.
//! - `CloudBackend` defines submit-only methods (PUT/GET/DELETE/LIST/HEAD).
//! - Backends send results via `CloudCallback` channels (no futures in the engine).
//! - `CloudStorage` is a namespace-aware dispatcher that shields the rest of the engine.
//! - `MockCloudBackend` keeps deterministic testing without async runtimes.
//!
//! ## Architecture
//!
//! ```text
//! CloudStorage (namespace-aware dispatcher)
//!     ↓
//! CloudBackend trait (interface: submit_put, submit_get, etc.)
//!     ↓
//! [Real backends via CloudExecutor]  [MockCloudBackend for testing]
//! ```
//!
//! ## Async Model
//!
//! - `submit_*()` methods return immediately (non-blocking)
//! - Results are sent back via `CloudCallback` channels (mpsc::Sender<CloudEvent>)
//! - Events are received asynchronously but callback processing is synchronous
//! - No futures in the engine: all async work happens in `CloudExecutor` embedded tokio runtime

pub mod aws;
pub mod executor;

use super::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use crate::common::MidgeError;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub use aws::AwsCredentials;
pub use executor::{CloudExecutor, CloudRequest, CloudResponse, CloudSigner};

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
        CloudStorage::submit_delete(self, path.clone(), tx);
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
        CloudStorage::submit_list(self, prefix.clone(), tx);
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

    // =========== CloudOutcome Tests ===========

    #[test]
    fn should_distinguish_ok_and_err_outcomes() {
        // Arrange
        let ok_outcome: CloudOutcome<String> = CloudOutcome::Ok("success".into());
        let err_outcome: CloudOutcome<String> = CloudOutcome::Err("failure".into());

        // Act & Assert
        assert!(ok_outcome.is_ok());
        assert!(!ok_outcome.is_err());
        assert!(!err_outcome.is_ok());
        assert!(err_outcome.is_err());
    }

    #[test]
    fn should_clone_outcomes_with_different_types() {
        // Arrange
        let int_ok = CloudOutcome::Ok(42);
        let int_err: CloudOutcome<i32> = CloudOutcome::Err("error".into());

        // Act
        let int_ok_cloned = int_ok.clone();
        let int_err_cloned = int_err.clone();

        // Assert
        assert!(int_ok_cloned.is_ok());
        assert!(int_err_cloned.is_err());
    }

    #[test]
    fn should_convert_result_to_outcome() {
        // Arrange
        let ok_result: Result<i32, MidgeError> = Ok(100);
        let err_result: Result<i32, MidgeError> = Err(MidgeError::Corruption("test".into()));

        // Act
        let ok_outcome = CloudOutcome::from_result(ok_result);
        let err_outcome = CloudOutcome::from_result(err_result);

        // Assert
        assert!(ok_outcome.is_ok());
        assert!(err_outcome.is_err());
    }

    // =========== ObjectMetadata Tests ===========

    #[test]
    fn should_create_and_clone_object_metadata() {
        // Arrange
        let metadata = ObjectMetadata::new(1024, "etag123".into(), 1000000);

        // Act
        let cloned = metadata.clone();

        // Assert
        assert_eq!(cloned.size, 1024);
        assert_eq!(cloned.etag, "etag123");
        assert_eq!(cloned.last_modified, 1000000);
    }

    #[test]
    fn should_handle_metadata_with_boundary_sizes() {
        // Arrange & Act
        let zero_size = ObjectMetadata::new(0, "zero".into(), 100);
        let max_size = ObjectMetadata::new(u64::MAX, "max".into(), 200);

        // Assert
        assert_eq!(zero_size.size, 0);
        assert_eq!(max_size.size, u64::MAX);
    }

    #[test]
    fn should_handle_metadata_with_empty_etag() {
        // Arrange & Act
        let metadata = ObjectMetadata::new(100, String::new(), 1000);

        // Assert
        assert_eq!(metadata.etag, "");
        assert_eq!(metadata.size, 100);
    }

    // =========== CloudStorage Routing Tests ===========

    #[test]
    fn should_route_and_namespace_put_operation() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        let data = vec![1, 2, 3];

        // Act
        storage.submit_put("file".into(), data, tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::PutComplete { key, result } => {
                assert_eq!(key, "midge/file");
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete"),
        }
    }

    #[test]
    fn should_route_and_namespace_get_operation() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile".into(), vec![1, 2, 3], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_get("testfile".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetComplete { key, result } => {
                assert!(key.starts_with("midge/"));
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_route_delete_and_apply_namespace() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_delete("file".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::DeleteComplete { key, result } => {
                assert_eq!(key, "midge/file");
                assert!(result.is_ok());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_route_list_and_apply_namespace() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put multiple files
        let (tx, rx) = mpsc::channel();
        storage.submit_put("prefix/file1".into(), vec![1], tx);
        let _ = rx.recv();

        let (tx, rx) = mpsc::channel();
        storage.submit_put("prefix/file2".into(), vec![2], tx);
        let _ = rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_list("prefix".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::ListComplete { prefix, result } => {
                assert!(prefix.starts_with("midge/"));
                match result {
                    CloudOutcome::Ok(items) => {
                        assert!(!items.is_empty());
                    }
                    _ => panic!("Expected Ok result"),
                }
            }
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_route_head_and_return_metadata() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile".into(), vec![1, 2, 3], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_head("testfile".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::HeadComplete { key, result } => {
                assert!(key.starts_with("midge/"));
                match result {
                    CloudOutcome::Ok(metadata) => {
                        assert_eq!(metadata.size, 3);
                    }
                    _ => panic!("Expected Ok metadata"),
                }
            }
            _ => panic!("Expected HeadComplete"),
        }
    }

    #[test]
    fn should_route_get_range_with_bounds() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("rangefile".into(), vec![1, 2, 3, 4, 5], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_get_range("rangefile".into(), 1, Some(4), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetRangeComplete {
                key,
                start,
                end,
                result,
            } => {
                assert!(key.starts_with("midge/"));
                assert_eq!(start, 1);
                assert_eq!(end, Some(4));
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetRangeComplete"),
        }
    }

    #[test]
    fn should_handle_get_range_with_none_end_bound() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_get_range("file".into(), 0, None, tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetRangeComplete { end, .. } => {
                assert_eq!(end, None);
            }
            _ => panic!("Expected GetRangeComplete"),
        }
    }

    // =========== CloudEvent Tests ===========

    #[test]
    fn should_send_put_complete_event_via_callback() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        let data = vec![1, 2, 3];

        // Act
        storage.submit_put("file".into(), data, tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::PutComplete { key, result } => {
                assert_eq!(key, "midge/file");
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete"),
        }
    }

    #[test]
    fn should_send_get_complete_event_via_callback() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // First put a file so we can get it
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile".into(), vec![1, 2, 3], put_tx);
        let _ = put_rx.recv();

        // Act
        storage.submit_get("testfile".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetComplete { key, result } => {
                assert_eq!(key, "midge/testfile");
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_send_list_complete_event_via_callback() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("prefix/file1".into(), vec![1], put_tx);
        let _ = put_rx.recv();

        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("prefix/file2".into(), vec![2], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_list("prefix".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::ListComplete { prefix, result } => {
                assert_eq!(prefix, "midge/prefix");
                match result {
                    CloudOutcome::Ok(items) => {
                        assert!(items.len() >= 2);
                        assert!(items.iter().any(|k| k.contains("file1")));
                        assert!(items.iter().any(|k| k.contains("file2")));
                    }
                    _ => panic!("Expected Ok result"),
                }
            }
            _ => panic!("Expected ListComplete"),
        }
    }

    // =========== Data Handling & Integration Tests ===========

    #[test]
    fn should_handle_large_file_operations() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let large_data = vec![42u8; 1_000_000]; // 1 MB
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_put("largefile".into(), large_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::PutComplete { result, .. } => {
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete"),
        }

        // Verify we can retrieve it
        let (tx, rx) = mpsc::channel();
        storage.submit_get("largefile".into(), tx);
        let event = rx.recv().unwrap();

        match event {
            CloudEvent::GetComplete { result, .. } => match result {
                CloudOutcome::Ok(data) => {
                    assert_eq!(data.len(), 1_000_000);
                }
                _ => panic!("Expected Ok"),
            },
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_preserve_binary_data_fidelity() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let binary_data = vec![0u8, 1u8, 255u8, 254u8, 127u8, 128u8];

        // Act - put the binary file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("binaryfile".into(), binary_data.clone(), put_tx);
        let _ = put_rx.recv();

        // Act - get it back
        let (tx, rx) = mpsc::channel();
        storage.submit_get("binaryfile".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetComplete { result, .. } => match result {
                CloudOutcome::Ok(data) => {
                    assert_eq!(data, binary_data, "binary data must be preserved exactly");
                }
                _ => panic!("Expected Ok result"),
            },
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_dispatch_all_cloud_operations_successfully() {
        // Arrange & Act
        let storage = CloudStorage::with_mock();

        // Put operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_put("f1".into(), vec![1, 2], tx);

        // Get operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_get("f2".into(), tx);

        // Delete operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_delete("f3".into(), tx);

        // List operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_list("prefix".into(), tx);

        // Head operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_head("f4".into(), tx);

        // Get range operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_get_range("f5".into(), 0, Some(100), tx);

        // Assert - just verify that all methods can be called without panic
        // The actual event handling is tested elsewhere
    }

    #[test]
    fn should_handle_get_missing_file_gracefully() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_get("nonexistent".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetComplete { result, .. } => {
                assert!(result.is_err());
            }
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_handle_metadata_for_empty_files() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // Put an empty file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("emptyfile".into(), vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_head("emptyfile".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::HeadComplete { result, .. } => match result {
                CloudOutcome::Ok(metadata) => {
                    assert_eq!(metadata.size, 0);
                }
                _ => panic!("Expected Ok metadata"),
            },
            _ => panic!("Expected HeadComplete"),
        }
    }
}
