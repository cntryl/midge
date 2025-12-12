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

    // =========== CloudOutcome Tests ===========

    #[test]
    fn should_create_ok_outcome() {
        // Arrange & Act
        let outcome: CloudOutcome<String> = CloudOutcome::Ok("success".into());

        // Assert
        assert!(outcome.is_ok());
        assert!(!outcome.is_err());
    }

    #[test]
    fn should_create_err_outcome() {
        // Arrange & Act
        let outcome: CloudOutcome<String> = CloudOutcome::Err("error message".into());

        // Assert
        assert!(outcome.is_err());
        assert!(!outcome.is_ok());
    }

    #[test]
    fn should_clone_ok_outcome() {
        // Arrange
        let outcome = CloudOutcome::Ok(42);

        // Act
        let cloned = outcome.clone();

        // Assert
        assert!(cloned.is_ok());
    }

    #[test]
    fn should_clone_err_outcome() {
        // Arrange
        let outcome: CloudOutcome<i32> = CloudOutcome::Err("failure".into());

        // Act
        let cloned = outcome.clone();

        // Assert
        assert!(cloned.is_err());
    }

    #[test]
    fn should_convert_result_to_ok_outcome() {
        // Arrange
        let result: Result<i32, MidgeError> = Ok(100);

        // Act
        let outcome = CloudOutcome::from_result(result);

        // Assert
        assert!(outcome.is_ok());
    }

    #[test]
    fn should_convert_result_to_err_outcome() {
        // Arrange
        let result: Result<i32, MidgeError> = Err(MidgeError::Corruption("test error".into()));

        // Act
        let outcome = CloudOutcome::from_result(result);

        // Assert
        assert!(outcome.is_err());
    }

    // =========== ObjectMetadata Tests ===========

    #[test]
    fn should_create_object_metadata() {
        // Arrange & Act
        let metadata = ObjectMetadata::new(1024, "etag123".into(), 1000000);

        // Assert
        assert_eq!(metadata.size, 1024);
        assert_eq!(metadata.etag, "etag123");
        assert_eq!(metadata.last_modified, 1000000);
    }

    #[test]
    fn should_clone_object_metadata() {
        // Arrange
        let metadata = ObjectMetadata::new(512, "tag".into(), 500);

        // Act
        let cloned = metadata.clone();

        // Assert
        assert_eq!(cloned.size, 512);
        assert_eq!(cloned.etag, "tag");
    }

    #[test]
    fn should_create_metadata_with_zero_size() {
        // Arrange & Act
        let metadata = ObjectMetadata::new(0, "etag".into(), 1000);

        // Assert
        assert_eq!(metadata.size, 0);
    }

    #[test]
    fn should_create_metadata_with_large_size() {
        // Arrange & Act
        let metadata = ObjectMetadata::new(u64::MAX, "etag".into(), 1000);

        // Assert
        assert_eq!(metadata.size, u64::MAX);
    }

    #[test]
    fn should_create_metadata_with_empty_etag() {
        // Arrange & Act
        let metadata = ObjectMetadata::new(100, String::new(), 1000);

        // Assert
        assert_eq!(metadata.etag, "");
    }

    // =========== CloudStorage Routing Tests ===========

    #[test]
    fn should_route_put_to_backend() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        let data = vec![1, 2, 3];

        // Act
        storage.submit_put("file".into(), data.clone(), tx);
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
    fn should_route_get_to_backend() {
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
    fn should_route_delete_to_backend() {
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
    fn should_route_list_to_backend() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_list("prefix".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::ListComplete { prefix, result } => {
                assert_eq!(prefix, "midge/prefix");
                assert!(result.is_ok());
            }
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_route_head_to_backend() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        
        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile".into(), vec![1, 2, 3], put_tx);
        let _ = put_rx.recv();

        // Act
        storage.submit_head("testfile".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::HeadComplete { key, result } => {
                assert_eq!(key, "midge/testfile");
                assert!(result.is_ok());
            }
            _ => panic!("Expected HeadComplete"),
        }
    }

    #[test]
    fn should_route_get_range_to_backend() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        
        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile".into(), vec![1, 2, 3, 4, 5], put_tx);
        let _ = put_rx.recv();

        // Act
        storage.submit_get_range("testfile".into(), 0, Some(5), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetRangeComplete { key, start, end, result } => {
                assert_eq!(key, "midge/testfile");
                assert_eq!(start, 0);
                assert_eq!(end, Some(5));
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetRangeComplete"),
        }
    }

    // =========== CloudStorage Namespace Tests ===========

    #[test]
    fn should_prefix_put_with_namespace() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_put("myfile".into(), vec![], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::PutComplete { key, .. } => {
                assert!(key.starts_with("midge/"));
                assert!(key.ends_with("myfile"));
            }
            _ => panic!("Expected PutComplete"),
        }
    }

    #[test]
    fn should_prefix_get_with_namespace() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_get("sst/data".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetComplete { key, .. } => {
                assert!(key.starts_with("midge/"));
            }
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_handle_empty_namespace_key() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_put("".into(), vec![], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::PutComplete { key, .. } => {
                // Should still add namespace
                assert_eq!(key, "midge/");
            }
            _ => panic!("Expected PutComplete"),
        }
    }

    // =========== CloudEvent Tests ===========

    #[test]
    fn should_create_put_complete_event() {
        // Arrange & Act
        let event = CloudEvent::PutComplete {
            key: "file".into(),
            result: CloudOutcome::Ok(()),
        };

        // Assert
        match event {
            CloudEvent::PutComplete { key, result } => {
                assert_eq!(key, "file");
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete"),
        }
    }

    #[test]
    fn should_create_get_complete_event() {
        // Arrange & Act
        let event = CloudEvent::GetComplete {
            key: "file".into(),
            result: CloudOutcome::Ok(vec![1, 2, 3]),
        };

        // Assert
        match event {
            CloudEvent::GetComplete { key, result } => {
                assert_eq!(key, "file");
                assert!(result.is_ok());
            }
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_create_list_complete_event() {
        // Arrange & Act
        let items = vec!["file1".into(), "file2".into()];
        let event = CloudEvent::ListComplete {
            prefix: "prefix".into(),
            result: CloudOutcome::Ok(items.clone()),
        };

        // Assert
        match event {
            CloudEvent::ListComplete { prefix, result } => {
                assert_eq!(prefix, "prefix");
                match result {
                    CloudOutcome::Ok(returned) => assert_eq!(returned, items),
                    _ => panic!("Expected Ok result"),
                }
            }
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_clone_put_complete_event() {
        // Arrange
        let event = CloudEvent::PutComplete {
            key: "file".into(),
            result: CloudOutcome::Ok(()),
        };

        // Act
        let cloned = event.clone();

        // Assert
        match cloned {
            CloudEvent::PutComplete { key, .. } => assert_eq!(key, "file"),
            _ => panic!("Expected PutComplete"),
        }
    }

    // =========== StorageBackend Trait Tests ===========

    #[test]
    fn should_dispatch_all_event_types() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // Act & Assert - Just verify all methods can be called
        let (tx, _rx) = mpsc::channel();
        storage.submit_put("f1".into(), vec![], tx.clone());
        
        let (tx, _rx) = mpsc::channel();
        storage.submit_get("f2".into(), tx.clone());
        
        let (tx, _rx) = mpsc::channel();
        storage.submit_delete("f3".into(), tx.clone());
        
        let (tx, _rx) = mpsc::channel();
        storage.submit_list("prefix".into(), tx.clone());
        
        let (tx, _rx) = mpsc::channel();
        storage.submit_head("f4".into(), tx.clone());
        
        let (tx, _rx) = mpsc::channel();
        storage.submit_get_range("f5".into(), 0, Some(100), tx);
    }

    #[test]
    fn should_handle_large_data_in_put() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        let large_data = vec![42u8; 1_000_000];

        // Act
        storage.submit_put("largefile".into(), large_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::PutComplete { result, .. } => assert!(result.is_ok()),
            _ => panic!("Expected PutComplete"),
        }
    }

    #[test]
    fn should_handle_binary_data_in_get() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let binary_data = vec![0u8, 1u8, 255u8, 254u8];
        
        // First put the binary file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("binaryfile".into(), binary_data.clone(), put_tx);
        let _ = put_rx.recv();
        
        // Act - now get it
        let (tx, rx) = mpsc::channel();
        storage.submit_get("binaryfile".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetComplete { result, .. } => {
                match result {
                    CloudOutcome::Ok(data) => assert_eq!(data, binary_data),
                    _ => panic!("Expected Ok result"),
                }
            }
            _ => panic!("Expected GetComplete"),
        }
    }

    #[test]
    fn should_handle_get_range_with_partial_bounds() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();

        // Act
        storage.submit_get_range("file".into(), 0, None, tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::GetRangeComplete { end, .. } => assert_eq!(end, None),
            _ => panic!("Expected GetRangeComplete"),
        }
    }

    #[test]
    fn should_handle_head_metadata() {
        // Arrange
        let storage = CloudStorage::with_mock();
        
        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("metadata_file".into(), vec![1, 2, 3, 4, 5], put_tx);
        let _ = put_rx.recv();
        
        // Act
        let (tx, rx) = mpsc::channel();
        storage.submit_head("metadata_file".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            CloudEvent::HeadComplete { result, .. } => {
                match result {
                    CloudOutcome::Ok(metadata) => {
                        assert_eq!(metadata.size, 5u64);
                        assert!(!metadata.etag.is_empty());
                    }
                    _ => panic!("Expected Ok metadata"),
                }
            }
            _ => panic!("Expected HeadComplete"),
        }
    }
}
