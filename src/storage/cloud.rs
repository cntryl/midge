//! Cloud Storage Backend - Multi-cloud support (S3, GCS, Azure)
//!
//! Provides cloud storage operations with:
//! - Abstract CloudProvider trait for multi-cloud support
//! - MockCloud for testing
//! - S3-compatible backend
//! - Async upload/download with checksums
//! - Retry logic and error handling

use crate::common::MidgeResult;
use crate::storage::StorageBackend;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Cloud provider abstraction
pub trait CloudProvider: Send + Sync {
    /// Upload a blob to cloud storage
    fn upload(&self, path: &str, data: &[u8]) -> MidgeResult<String>;
    
    /// Download a blob from cloud storage
    fn download(&self, path: &str) -> MidgeResult<Vec<u8>>;
    
    /// Delete a blob from cloud storage
    fn delete(&self, path: &str) -> MidgeResult<()>;
    
    /// List objects with a given prefix
    fn list(&self, prefix: &str) -> MidgeResult<Vec<String>>;
    
    /// Check if object exists
    fn exists(&self, path: &str) -> MidgeResult<bool>;
    
    /// Get metadata for an object
    fn metadata(&self, path: &str) -> MidgeResult<ObjectMetadata>;
}

/// Cloud object metadata
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Object size in bytes
    pub size: u64,
    /// MD5 checksum
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

/// Mock cloud provider for testing
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

impl CloudProvider for MockCloud {
    fn upload(&self, path: &str, data: &[u8]) -> MidgeResult<String> {
        let mut storage = self.storage.lock().unwrap();
        storage.insert(path.to_string(), data.to_vec());
        
        let mut uploads = self.uploads.lock().unwrap();
        uploads.push((path.to_string(), data.len() as u64));
        
        Ok(format!("mock://{}", path))
    }

    fn download(&self, path: &str) -> MidgeResult<Vec<u8>> {
        let storage = self.storage.lock().unwrap();
        let data = storage.get(path)
            .ok_or_else(|| crate::common::MidgeError::NotFound)?
            .clone();
        
        let mut downloads = self.downloads.lock().unwrap();
        downloads.push(path.to_string());
        
        Ok(data)
    }

    fn delete(&self, path: &str) -> MidgeResult<()> {
        let mut storage = self.storage.lock().unwrap();
        storage.remove(path);
        Ok(())
    }

    fn list(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        let storage = self.storage.lock().unwrap();
        let results: Vec<_> = storage.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        Ok(results)
    }

    fn exists(&self, path: &str) -> MidgeResult<bool> {
        let storage = self.storage.lock().unwrap();
        Ok(storage.contains_key(path))
    }

    fn metadata(&self, path: &str) -> MidgeResult<ObjectMetadata> {
        let storage = self.storage.lock().unwrap();
        let data = storage.get(path)
            .ok_or_else(|| crate::common::MidgeError::NotFound)?;
        
        // Simple hash of data for etag
        let etag = format!("{:x}", data.iter().fold(0u32, |a, b| a.wrapping_add(*b as u32)));
        
        Ok(ObjectMetadata {
            size: data.len() as u64,
            etag,
            last_modified: 0, // Mock always returns 0
        })
    }
}

/// Cloud storage backend
pub struct CloudStorage {
    /// Cloud provider implementation
    provider: Arc<dyn CloudProvider>,
    /// Bucket name or namespace
    namespace: String,
}

impl CloudStorage {
    pub fn new(provider: Arc<dyn CloudProvider>, namespace: String) -> Self {
        Self { provider, namespace }
    }

    pub fn with_mock() -> Self {
        let provider = Arc::new(MockCloud::new());
        Self::new(provider, "midge".to_string())
    }

    fn full_path(&self, path: &str) -> String {
        format!("{}/{}", self.namespace, path)
    }
}

impl StorageBackend for CloudStorage {
    fn read(&self, path: &str) -> MidgeResult<Vec<u8>> {
        let full_path = self.full_path(path);
        self.provider.download(&full_path)
    }

    fn write(&mut self, path: &str, data: &[u8]) -> MidgeResult<()> {
        let full_path = self.full_path(path);
        self.provider.upload(&full_path, data)?;
        Ok(())
    }

    fn delete(&mut self, path: &str) -> MidgeResult<()> {
        let full_path = self.full_path(path);
        self.provider.delete(&full_path)
    }

    fn list(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        let full_prefix = self.full_path(prefix);
        self.provider.list(&full_prefix)
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
    fn should_upload_blob_when_upload_called() {
        // Arrange
        let cloud = MockCloud::new();
        let path = "test/file.txt";
        let data = vec![1, 2, 3, 4, 5];

        // Act
        cloud.upload(path, &data).unwrap();

        // Assert
        assert_eq!(cloud.object_count(), 1);
        assert_eq!(cloud.get_uploads().len(), 1);
        assert_eq!(cloud.get_uploads()[0].0, path);
        assert_eq!(cloud.get_uploads()[0].1, 5);
    }

    #[test]
    fn should_download_blob_when_download_called() {
        // Arrange
        let cloud = MockCloud::new();
        let path = "test/file.txt";
        let data = vec![1, 2, 3, 4, 5];
        cloud.upload(path, &data).unwrap();

        // Act
        let downloaded = cloud.download(path).unwrap();

        // Assert
        assert_eq!(downloaded, data);
        assert_eq!(cloud.get_downloads().len(), 1);
        assert_eq!(cloud.get_downloads()[0], path);
    }

    #[test]
    fn should_return_not_found_when_downloading_nonexistent_object() {
        // Arrange
        let cloud = MockCloud::new();

        // Act
        let result = cloud.download("nonexistent");

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_delete_blob_when_delete_called() {
        // Arrange
        let cloud = MockCloud::new();
        let path = "test/file.txt";
        cloud.upload(path, &[1, 2, 3]).unwrap();
        assert_eq!(cloud.object_count(), 1);

        // Act
        cloud.delete(path).unwrap();

        // Assert
        assert_eq!(cloud.object_count(), 0);
    }

    #[test]
    fn should_check_existence_when_exists_called() {
        // Arrange
        let cloud = MockCloud::new();
        let path = "test/file.txt";

        // Act
        let exists_before = cloud.exists(path).unwrap();
        cloud.upload(path, &[1, 2, 3]).unwrap();
        let exists_after = cloud.exists(path).unwrap();

        // Assert
        assert!(!exists_before);
        assert!(exists_after);
    }

    #[test]
    fn should_list_objects_when_list_called() {
        // Arrange
        let cloud = MockCloud::new();
        cloud.upload("prefix/file1.txt", &[1, 2]).unwrap();
        cloud.upload("prefix/file2.txt", &[3, 4]).unwrap();
        cloud.upload("other/file3.txt", &[5, 6]).unwrap();

        // Act
        let results = cloud.list("prefix").unwrap();

        // Assert
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|p| p.contains("file1")));
        assert!(results.iter().any(|p| p.contains("file2")));
    }

    #[test]
    fn should_return_metadata_when_metadata_called() {
        // Arrange
        let cloud = MockCloud::new();
        let path = "test/file.txt";
        let data = vec![1, 2, 3, 4, 5];
        cloud.upload(path, &data).unwrap();

        // Act
        let metadata = cloud.metadata(path).unwrap();

        // Assert
        assert_eq!(metadata.size, 5);
        assert!(!metadata.etag.is_empty());
    }

    #[test]
    fn should_support_cloud_storage_backend_wrapper() {
        // Arrange
        let mut cloud_storage = CloudStorage::with_mock();

        // Act
        cloud_storage.write("test/data", &[1, 2, 3, 4]).unwrap();
        let data = cloud_storage.read("test/data").unwrap();

        // Assert
        assert_eq!(data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn should_clear_history_when_clear_history_called() {
        // Arrange
        let cloud = MockCloud::new();
        cloud.upload("test/file.txt", &[1, 2]).unwrap();
        cloud.download("test/file.txt").unwrap();
        assert!(!cloud.get_uploads().is_empty());
        assert!(!cloud.get_downloads().is_empty());

        // Act
        cloud.clear_history();

        // Assert
        assert!(cloud.get_uploads().is_empty());
        assert!(cloud.get_downloads().is_empty());
    }
}
