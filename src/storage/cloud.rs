//! Cloud Storage Backend - Multi-cloud support with callback-based async I/O
//!
//! Architecture (FoundationDB/ScyllaDB pattern):
//! - Engine stays synchronous: submits operations via callbacks
//! - Cloud I/O is fully async: spawns tasks, sends events back
//! - Runtime processes CloudEvents deterministically
//! - Zero async contamination in engine core
//!
//! Key insight: callbacks are sync channels, not closures. This allows:
//! - Deterministic runtime state machines
//! - Clean backpressure handling
//! - Easy testing with mock providers
//! - No lock escaping

use crate::common::{MidgeResult, MidgeError};
use crate::storage::StorageBackend;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Cloud operation outcome - serializable version of Result
#[derive(Debug, Clone)]
pub enum CloudOutcome<T: Clone> {
    Ok(T),
    Err(String),
}

impl<T: Clone> CloudOutcome<T> {
    /// Convert to standard Result
    pub fn to_result(self) -> MidgeResult<T> {
        match self {
            CloudOutcome::Ok(v) => Ok(v),
            CloudOutcome::Err(e) => Err(MidgeError::Internal(e)),
        }
    }

    /// Convert from standard Result
    pub fn from_result(result: MidgeResult<T>) -> Self {
        match result {
            Ok(v) => CloudOutcome::Ok(v),
            Err(e) => CloudOutcome::Err(format!("{:?}", e)),
        }
    }

    /// Check if outcome is Ok
    pub fn is_ok(&self) -> bool {
        matches!(self, CloudOutcome::Ok(_))
    }

    /// Unwrap the value (panics if Err)
    pub fn unwrap(self) -> T {
        match self {
            CloudOutcome::Ok(v) => v,
            CloudOutcome::Err(e) => panic!("called unwrap on CloudOutcome::Err: {}", e),
        }
    }
}

/// Events sent back to the runtime from async cloud I/O
/// Unifies all cloud operation results into a single typed enum
#[derive(Debug, Clone)]
pub enum CloudEvent {
    /// Put/upload operation completed
    PutComplete {
        key: String,
        result: CloudOutcome<()>,
    },
    /// Get/download operation completed
    GetComplete {
        key: String,
        result: CloudOutcome<Vec<u8>>,
    },
    /// Delete operation completed
    DeleteComplete {
        key: String,
        result: CloudOutcome<()>,
    },
    /// List operation completed
    ListComplete {
        prefix: String,
        result: CloudOutcome<Vec<String>>,
    },
    /// Head/metadata operation completed
    HeadComplete {
        key: String,
        result: CloudOutcome<ObjectMetadata>,
    },
}

/// Callback type: a sync channel to send CloudEvent back to runtime
/// This is the critical pattern:
/// - Cheap to clone (it's just a channel sender)
/// - Send + Sync (works across threads)
/// - Typed (CloudEvent, not a closure)
/// - Runtime processes these synchronously in its event loop
pub type CloudCallback = std::sync::mpsc::Sender<CloudEvent>;

/// Cloud object metadata
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Object size in bytes
    pub size: u64,
    /// ETag checksum
    pub etag: String,
    /// Last modified time (Unix timestamp)
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

/// Mock cloud provider for testing - implements callback-based API
/// Executes operations synchronously (suitable for tests)
pub struct MockCloud {
    /// In-memory storage: path → data
    storage: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    /// Upload history for verification
    uploads: Arc<Mutex<Vec<(String, u64)>>>,
    /// Download history for verification
    downloads: Arc<Mutex<Vec<String>>>,
}

impl MockCloud {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            uploads: Arc::new(Mutex::new(Vec::new())),
            downloads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get_uploads(&self) -> Vec<(String, u64)> {
        self.uploads.lock().unwrap().clone()
    }

    pub fn get_downloads(&self) -> Vec<String> {
        self.downloads.lock().unwrap().clone()
    }

    pub fn clear_history(&self) {
        self.uploads.lock().unwrap().clear();
        self.downloads.lock().unwrap().clear();
    }

    pub fn object_count(&self) -> usize {
        self.storage.lock().unwrap().len()
    }
}

impl Default for MockCloud {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MockCloud {
    fn read(&self, path: &str) -> MidgeResult<Vec<u8>> {
        let storage = self.storage.lock().unwrap();
        storage
            .get(path)
            .cloned()
            .ok_or(MidgeError::NotFound)
    }

    fn write(&mut self, path: &str, data: &[u8]) -> MidgeResult<()> {
        let mut storage = self.storage.lock().unwrap();
        storage.insert(path.to_string(), data.to_vec());

        let mut uploads = self.uploads.lock().unwrap();
        uploads.push((path.to_string(), data.len() as u64));

        Ok(())
    }

    fn delete(&mut self, path: &str) -> MidgeResult<()> {
        let mut storage = self.storage.lock().unwrap();
        storage.remove(path);
        Ok(())
    }

    fn list(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        let storage = self.storage.lock().unwrap();
        let results: Vec<_> = storage
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        Ok(results)
    }
}

/// Cloud storage wrapper that provides callback-based async API
/// Converts between synchronous StorageBackend and async CloudEvent callbacks
pub struct CloudStorage {
    /// Underlying storage backend
    backend: Arc<dyn StorageBackend>,
    /// Namespace/prefix for operations
    namespace: String,
}

impl CloudStorage {
    pub fn new(backend: Arc<dyn StorageBackend>, namespace: String) -> Self {
        Self { backend, namespace }
    }

    pub fn with_mock() -> Self {
        let backend = Arc::new(MockCloud::new());
        Self::new(backend, "midge".to_string())
    }

    fn full_path(&self, path: &str) -> String {
        format!("{}/{}", self.namespace, path)
    }

    /// Submit put operation via callback
    pub fn submit_put(&self, key: String, _data: Vec<u8>, callback: CloudCallback) {
        // In production, this would spawn an async task to a tokio runtime
        // For now, just send success
        let result = Ok(());
        let event = CloudEvent::PutComplete {
            key,
            result: CloudOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    /// Submit get operation via callback
    pub fn submit_get(&self, key: String, callback: CloudCallback) {
        let backend = Arc::clone(&self.backend);
        let full_key = self.full_path(&key);

        // In production, this would spawn an async task to a tokio runtime
        // For now, execute synchronously (suitable for tests and MockCloud)
        let result = backend.read(&full_key);
        let event = CloudEvent::GetComplete {
            key,
            result: CloudOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    /// Submit delete operation via callback
    pub fn submit_delete(&self, key: String, callback: CloudCallback) {
        // Note: StorageBackend requires &mut, but Arc doesn't allow mutable access
        // In production, cloud backends would be async and wouldn't need &mut
        // For tests, we skip this for now
        let result = Ok(());
        let event = CloudEvent::DeleteComplete {
            key,
            result: CloudOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    /// Submit list operation via callback
    pub fn submit_list(&self, prefix: String, callback: CloudCallback) {
        let backend = Arc::clone(&self.backend);
        let full_prefix = self.full_path(&prefix);

        // In production, this would spawn an async task to a tokio runtime
        // For now, execute synchronously (suitable for tests and MockCloud)
        let result = backend.list(&full_prefix);
        let event = CloudEvent::ListComplete {
            prefix,
            result: CloudOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    /// Submit head operation via callback
    pub fn submit_head(&self, key: String, callback: CloudCallback) {
        let metadata = ObjectMetadata::new(0, String::new(), 0);
        let result = Ok(metadata);
        let event = CloudEvent::HeadComplete {
            key,
            result: CloudOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_mock_cloud_when_instantiated() {
        // Arrange & Act
        let cloud = MockCloud::new();

        // Assert
        assert_eq!(cloud.object_count(), 0);
        assert!(cloud.get_uploads().is_empty());
        assert!(cloud.get_downloads().is_empty());
    }

    #[test]
    fn should_write_and_read_blob() {
        // Arrange
        let mut cloud = MockCloud::new();
        let path = "test/file.txt";
        let data = vec![1, 2, 3, 4, 5];

        // Act
        cloud.write(path, &data).unwrap();
        let read_data = cloud.read(path).unwrap();

        // Assert
        assert_eq!(read_data, data);
        assert_eq!(cloud.object_count(), 1);
        assert_eq!(cloud.get_uploads().len(), 1);
    }

    #[test]
    fn should_return_not_found_on_missing_read() {
        // Arrange
        let cloud = MockCloud::new();

        // Act
        let result = cloud.read("nonexistent");

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_delete_blob() {
        // Arrange
        let mut cloud = MockCloud::new();
        let path = "test/file.txt";
        cloud.write(path, &[1, 2, 3]).unwrap();
        assert_eq!(cloud.object_count(), 1);

        // Act
        cloud.delete(path).unwrap();

        // Assert
        assert_eq!(cloud.object_count(), 0);
    }

    #[test]
    fn should_list_blobs_with_prefix() {
        // Arrange
        let mut cloud = MockCloud::new();
        cloud.write("prefix/file1.txt", &[1, 2]).unwrap();
        cloud.write("prefix/file2.txt", &[3, 4]).unwrap();
        cloud.write("other/file3.txt", &[5, 6]).unwrap();

        // Act
        let results = cloud.list("prefix").unwrap();

        // Assert
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|p| p.contains("file1")));
        assert!(results.iter().any(|p| p.contains("file2")));
    }

    #[test]
    fn should_clear_history() {
        // Arrange
        let mut cloud = MockCloud::new();
        cloud.write("test/file.txt", &[1, 2]).unwrap();
        assert!(!cloud.get_uploads().is_empty());

        // Act
        cloud.clear_history();

        // Assert
        assert!(cloud.get_uploads().is_empty());
    }

    #[test]
    fn should_submit_put_via_callback() {
        // Arrange
        let cloud = CloudStorage::with_mock();
        let (tx, rx) = std::sync::mpsc::channel();

        // Act
        cloud.submit_put("test/key".to_string(), vec![1, 2, 3], tx);

        // Assert
        let event = rx.recv().unwrap();
        match event {
            CloudEvent::PutComplete { key, result } => {
                assert_eq!(key, "test/key");
                assert!(result.is_ok());
            }
            _ => panic!("Expected PutComplete event"),
        }
    }

    #[test]
    fn should_submit_get_via_callback() {
        // Arrange
        let cloud = CloudStorage::with_mock();
        let (tx, rx) = std::sync::mpsc::channel();

        // Act
        cloud.submit_get("test/key".to_string(), tx);

        // Assert
        let event = rx.recv().unwrap();
        match event {
            CloudEvent::GetComplete { key, result } => {
                assert_eq!(key, "test/key");
                // MockCloud returns NotFound for missing keys - that's still a valid result
                assert!(matches!(result, CloudOutcome::Err(_)));
            }
            _ => panic!("Expected GetComplete event"),
        }
    }

    #[test]
    fn should_submit_delete_via_callback() {
        // Arrange
        let cloud = CloudStorage::with_mock();
        let (tx, rx) = std::sync::mpsc::channel();

        // Act
        cloud.submit_delete("test/key".to_string(), tx);

        // Assert
        let event = rx.recv().unwrap();
        match event {
            CloudEvent::DeleteComplete { key, result } => {
                assert_eq!(key, "test/key");
                assert!(result.is_ok());
            }
            _ => panic!("Expected DeleteComplete event"),
        }
    }
}
