use crate::cloud::backend::{BlobMeta, StorageBackend};
use crate::common::timestamp;
use crate::error::{MidgeError, MidgeResult};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

pub struct MockCloudBackend {
    root_dir: PathBuf,
    etags: Mutex<HashMap<String, String>>,
    latency: Option<Duration>,
    /// Counter for total uploads (put_blob calls)
    upload_count: AtomicUsize,
    /// Counter for failed uploads
    upload_failure_count: AtomicUsize,
    /// Counter for SST uploads (blob keys containing "sst")
    sst_upload_count: AtomicUsize,
    /// Counter for SST downloads (get_blob calls for SST keys)
    sst_download_count: AtomicUsize,
    /// If set, fail uploads after this many successful uploads
    fail_upload_after: AtomicUsize,
    /// Simulated cloud manifest for drift testing
    cloud_manifest: Mutex<Option<crate::core::manifest::Manifest>>,
}

static NEXT_ROOT_ID: AtomicU64 = AtomicU64::new(0);

impl MockCloudBackend {
    pub fn new() -> Self {
        let unique = NEXT_ROOT_ID.fetch_add(1, Ordering::Relaxed);
        Self::with_root(std::env::temp_dir().join(format!(
            "midge-mock-{}-{}",
            timestamp::now_millis(),
            unique
        )))
    }

    pub fn with_root(root_dir: PathBuf) -> Self {
        // Attempt to create the specified root directory; when this fails
        // log and fall back to the system temp dir rather than panicking.
        let actual_root = match std::fs::create_dir_all(&root_dir) {
            Ok(_) => root_dir,
            Err(e) => {
                tracing::warn!(
                    "Failed to create mock backend root {}: {}; falling back to system tmpdir",
                    root_dir.display(),
                    e
                );
                std::env::temp_dir()
            }
        };
        Self {
            root_dir: actual_root,
            etags: Mutex::new(HashMap::new()),
            latency: None,
            upload_count: AtomicUsize::new(0),
            upload_failure_count: AtomicUsize::new(0),
            sst_upload_count: AtomicUsize::new(0),
            sst_download_count: AtomicUsize::new(0),
            fail_upload_after: AtomicUsize::new(usize::MAX),
            cloud_manifest: Mutex::new(None),
        }
    }

    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = Some(latency);
        self
    }

    /// Set a simulated cloud manifest for drift testing
    pub fn set_cloud_manifest(&self, manifest: crate::core::manifest::Manifest) {
        *self.cloud_manifest.lock() = Some(manifest);
    }

    /// Get the number of successful uploads
    pub fn upload_count(&self) -> usize {
        self.upload_count.load(Ordering::SeqCst)
    }

    /// Get the number of failed uploads
    pub fn upload_failure_count(&self) -> usize {
        self.upload_failure_count.load(Ordering::SeqCst)
    }

    /// Get the number of SST uploads (blob keys containing "sst")
    pub fn sst_upload_count(&self) -> usize {
        self.sst_upload_count.load(Ordering::SeqCst)
    }

    /// Get the number of SST downloads
    pub fn sst_download_count(&self) -> usize {
        self.sst_download_count.load(Ordering::SeqCst)
    }

    /// Configure to fail uploads after N successful uploads
    pub fn set_fail_upload_after(&self, count: usize) {
        self.fail_upload_after.store(count, Ordering::SeqCst);
    }

    /// Reset all counters
    pub fn reset_counters(&self) {
        self.upload_count.store(0, Ordering::SeqCst);
        self.upload_failure_count.store(0, Ordering::SeqCst);
        self.sst_upload_count.store(0, Ordering::SeqCst);
        self.sst_download_count.store(0, Ordering::SeqCst);
        self.fail_upload_after.store(usize::MAX, Ordering::SeqCst);
    }

    /// Wait for upload count to reach at least the expected value (with timeout)
    /// Returns true if condition met, false if timed out
    pub fn wait_for_uploads(&self, expected: usize, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if self.upload_count() >= expected {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    fn simulate_latency(&self) {
        if let Some(delay) = self.latency {
            std::thread::sleep(delay);
        }
    }

    fn blob_path(&self, key: &str) -> PathBuf {
        let sanitized = key.replace('/', std::path::MAIN_SEPARATOR_STR);
        self.root_dir.join(sanitized)
    }

    fn generate_etag() -> String {
        format!("etag-{}", timestamp::now_millis())
    }

    fn is_sst_key(key: &str) -> bool {
        key.contains("sst") || key.ends_with(".sst")
    }
}

impl Default for MockCloudBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MockCloudBackend {
    fn put_blob(&self, key: &str, data: Bytes) -> MidgeResult<()> {
        self.simulate_latency();
        // Debugging log: print when put_blob is invoked to help identify stuck uploads in tests
        eprintln!("[DEBUG] MockCloudBackend.put_blob invoked for key: {}", key);

        // Check if we should fail this upload
        let current_upload = self.upload_count.load(Ordering::SeqCst);
        let fail_after = self.fail_upload_after.load(Ordering::SeqCst);

        if current_upload >= fail_after {
            self.upload_failure_count.fetch_add(1, Ordering::SeqCst);
            return Err(MidgeError::cloud_error("Simulated upload failure"));
        }

        // Perform the upload
        let path = self.blob_path(key);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, &data)?;
        self.etags
            .lock()
            .insert(key.to_string(), Self::generate_etag());

        // Track successful uploads
        let new_count = self.upload_count.fetch_add(1, Ordering::SeqCst) + 1;
        eprintln!(
            "[DEBUG] MockCloudBackend.put_blob completed for key: {}, total_uploads={}",
            key, new_count
        );
        if Self::is_sst_key(key) {
            self.sst_upload_count.fetch_add(1, Ordering::SeqCst);
        }

        // If this is a manifest upload, store it for drift testing
        if key == "manifest.json" {
            match serde_json::from_slice(&data) {
                Ok(manifest) => {
                    *self.cloud_manifest.lock() = Some(manifest);
                }
                Err(_e) => {
                    // Silently ignore manifest parse errors in mock
                }
            }
        }

        Ok(())
    }

    fn get_blob(&self, key: &str) -> MidgeResult<Bytes> {
        self.simulate_latency();

        // Special handling for manifest.json - return the cloud manifest if set
        if key == "manifest.json" {
            if let Some(ref manifest) = *self.cloud_manifest.lock() {
                let data = serde_json::to_vec_pretty(manifest)?;
                return Ok(Bytes::from(data));
            }
        }

        // Track SST downloads
        if Self::is_sst_key(key) {
            self.sst_download_count.fetch_add(1, Ordering::SeqCst);
        }

        let path = self.blob_path(key);
        if !path.exists() {
            return Err(MidgeError::KeyNotFound {
                key: key.to_string(),
            });
        }
        Ok(Bytes::from(std::fs::read(&path)?))
    }

    fn get_blob_range(&self, key: &str, start: u64, end: Option<u64>) -> MidgeResult<Bytes> {
        self.simulate_latency();
        let path = self.blob_path(key);
        if !path.exists() {
            return Err(MidgeError::KeyNotFound {
                key: key.to_string(),
            });
        }
        let data = std::fs::read(&path)?;
        let len = data.len() as u64;
        let s = std::cmp::min(start, len) as usize;
        let e = end.map(|e| std::cmp::min(e, len)).unwrap_or(len) as usize;
        if s > e || s >= data.len() {
            return Ok(Bytes::new());
        }
        Ok(Bytes::from(data[s..e].to_vec()))
    }

    fn delete_blob(&self, key: &str) -> MidgeResult<()> {
        self.simulate_latency();
        let path = self.blob_path(key);
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        self.etags.lock().remove(key);
        Ok(())
    }

    fn list_blobs(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        self.simulate_latency();
        let mut keys = Vec::new();
        fn visit_dir(
            dir: &std::path::Path,
            root: &std::path::Path,
            prefix: &str,
            keys: &mut Vec<String>,
        ) -> MidgeResult<()> {
            if !dir.exists() {
                return Ok(());
            }
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    visit_dir(&path, root, prefix, keys)?;
                } else if path.is_file() {
                    let relative = path
                        .strip_prefix(root)
                        .map_err(|_| MidgeError::internal("Failed to compute relative path"))?;
                    let key = relative
                        .to_str()
                        .ok_or_else(|| MidgeError::internal("Invalid UTF-8 in path"))?
                        .replace(std::path::MAIN_SEPARATOR, "/");
                    if key.starts_with(prefix) {
                        keys.push(key);
                    }
                }
            }
            Ok(())
        }
        visit_dir(&self.root_dir, &self.root_dir, prefix, &mut keys)?;
        keys.sort();
        Ok(keys)
    }

    fn head_blob(&self, key: &str) -> MidgeResult<Option<BlobMeta>> {
        self.simulate_latency();
        let path = self.blob_path(key);
        if !path.exists() {
            return Ok(None);
        }
        let metadata = std::fs::metadata(&path)?;
        let etags = self.etags.lock();
        Ok(Some(BlobMeta {
            size: metadata.len(),
            last_modified: metadata.modified().ok(),
            etag: etags.get(key).cloned(),
        }))
    }

    fn put_blob_if_not_exists(&self, key: &str, data: Bytes) -> MidgeResult<String> {
        self.simulate_latency();
        let path = self.blob_path(key);
        if path.exists() {
            // Map KeyExists to DatabaseLocked for lock acquisition semantics
            return Err(MidgeError::DatabaseLocked);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, &data)?;
        let etag = Self::generate_etag();
        self.etags.lock().insert(key.to_string(), etag.clone());
        Ok(etag)
    }
}

pub type MockCloud = MockCloudBackend;

#[allow(dead_code)]
pub struct MockCloudBackendHandle(Arc<MockCloudBackend>);

impl Default for MockCloudBackendHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl MockCloudBackendHandle {
    pub fn new() -> Self {
        Self(Arc::new(MockCloudBackend::new()))
    }
}

pub struct MockCloudBackendPublic;
impl MockCloudBackendPublic {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Arc<MockCloudBackend> {
        Arc::new(MockCloudBackend::new())
    }
}

impl MockCloudBackend {
    pub fn into_arc(self) -> Arc<dyn StorageBackend> {
        Arc::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::super::backend::StorageBackend;
    use super::super::mock::MockCloudBackend;
    use bytes::Bytes;
    use std::time::Duration;

    // ===== Basic Operations =====

    #[test]
    fn should_create_backend_with_default_temp_directory() {
        // Arrange
        // Act
        let backend = MockCloudBackend::new();

        // Assert
        assert!(backend.root_dir.to_string_lossy().contains("midge-mock"));
    }

    #[test]
    fn should_create_backend_with_custom_root_directory() {
        // Arrange
        let temp_dir = std::env::temp_dir().join("test-custom-root");

        // Act
        let backend = MockCloudBackend::with_root(temp_dir.clone());

        // Assert
        assert_eq!(backend.root_dir, temp_dir);
        assert!(temp_dir.exists());
    }

    #[test]
    fn should_create_root_directory_if_not_exists() {
        // Arrange
        let temp_dir = std::env::temp_dir().join(format!(
            "test-auto-create-{}",
            crate::common::timestamp::now_millis()
        ));

        // Act
        let _backend = MockCloudBackend::with_root(temp_dir.clone());

        // Assert
        assert!(temp_dir.exists());
    }

    #[test]
    fn should_set_latency_with_builder_pattern() {
        // Arrange
        let latency = Duration::from_millis(10);

        // Act
        let backend = MockCloudBackend::new().with_latency(latency);

        // Assert
        assert_eq!(backend.latency, Some(latency));
    }

    // ===== put_blob Tests =====

    #[test]
    fn should_put_blob_successfully() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "test/blob.dat";
        let data = Bytes::from("test data");

        // Act
        let result = backend.put_blob(key, data.clone());

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_write_blob_to_filesystem() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "test/file.txt";
        let data = Bytes::from("hello world");

        // Act
        backend.put_blob(key, data.clone()).unwrap();

        // Assert
        let path = backend.blob_path(key);
        assert!(path.exists());
        let written = std::fs::read(&path).unwrap();
        assert_eq!(written, data.as_ref());
    }

    #[test]
    fn should_create_parent_directories_when_putting_blob() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "level1/level2/level3/file.dat";
        let data = Bytes::from("nested");

        // Act
        backend.put_blob(key, data).unwrap();

        // Assert
        let path = backend.blob_path(key);
        assert!(path.exists());
        assert!(path.parent().unwrap().exists());
    }

    #[test]
    fn should_generate_etag_when_putting_blob() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "test.dat";
        let data = Bytes::from("test");

        // Act
        backend.put_blob(key, data).unwrap();

        // Assert
        let etags = backend.etags.lock();
        assert!(etags.contains_key(key));
        assert!(etags.get(key).unwrap().starts_with("etag-"));
    }

    #[test]
    fn should_overwrite_existing_blob() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "overwrite.dat";
        backend.put_blob(key, Bytes::from("original")).unwrap();

        // Act
        backend.put_blob(key, Bytes::from("updated")).unwrap();

        // Assert
        let retrieved = backend.get_blob(key).unwrap();
        assert_eq!(retrieved, Bytes::from("updated"));
    }

    // ===== get_blob Tests =====

    #[test]
    fn should_get_blob_successfully() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "test.dat";
        let data = Bytes::from("test data");
        backend.put_blob(key, data.clone()).unwrap();

        // Act
        let result = backend.get_blob(key);

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn should_return_error_when_blob_does_not_exist() {
        // Arrange
        let backend = MockCloudBackend::new();

        // Act
        let result = backend.get_blob("nonexistent.dat");

        // Assert
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::MidgeError::KeyNotFound { .. }
        ));
    }

    #[test]
    fn should_get_exact_blob_content() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "data.bin";
        let data = Bytes::from(vec![0u8, 1, 2, 3, 255, 254, 253]);
        backend.put_blob(key, data.clone()).unwrap();

        // Act
        let retrieved = backend.get_blob(key).unwrap();

        // Assert
        assert_eq!(retrieved, data);
    }

    // ===== get_blob_range Tests =====

    #[test]
    fn should_get_blob_range() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "range.dat";
        let data = Bytes::from("0123456789");
        backend.put_blob(key, data).unwrap();

        // Act
        let result = backend.get_blob_range(key, 2, Some(5)).unwrap();

        // Assert
        assert_eq!(result, Bytes::from("234"));
    }

    #[test]
    fn should_get_blob_range_from_start_to_end_of_file() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "range.dat";
        let data = Bytes::from("0123456789");
        backend.put_blob(key, data).unwrap();

        // Act
        let result = backend.get_blob_range(key, 5, None).unwrap();

        // Assert
        assert_eq!(result, Bytes::from("56789"));
    }

    #[test]
    fn should_return_empty_bytes_when_range_start_exceeds_length() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "range.dat";
        let data = Bytes::from("short");
        backend.put_blob(key, data).unwrap();

        // Act
        let result = backend.get_blob_range(key, 100, Some(200)).unwrap();

        // Assert
        assert_eq!(result, Bytes::new());
    }

    #[test]
    fn should_clamp_range_end_to_file_length() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "range.dat";
        let data = Bytes::from("0123456789");
        backend.put_blob(key, data).unwrap();

        // Act
        let result = backend.get_blob_range(key, 5, Some(1000)).unwrap();

        // Assert
        assert_eq!(result, Bytes::from("56789"));
    }

    #[test]
    fn should_return_error_when_getting_range_from_nonexistent_blob() {
        // Arrange
        let backend = MockCloudBackend::new();

        // Act
        let result = backend.get_blob_range("missing.dat", 0, Some(10));

        // Assert
        assert!(result.is_err());
    }

    // ===== delete_blob Tests =====

    #[test]
    fn should_delete_blob_successfully() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "delete.dat";
        backend.put_blob(key, Bytes::from("data")).unwrap();

        // Act
        let result = backend.delete_blob(key);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_remove_blob_from_filesystem() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "delete.dat";
        backend.put_blob(key, Bytes::from("data")).unwrap();
        let path = backend.blob_path(key);
        assert!(path.exists());

        // Act
        backend.delete_blob(key).unwrap();

        // Assert
        assert!(!path.exists());
    }

    #[test]
    fn should_remove_etag_when_deleting_blob() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "delete.dat";
        backend.put_blob(key, Bytes::from("data")).unwrap();
        assert!(backend.etags.lock().contains_key(key));

        // Act
        backend.delete_blob(key).unwrap();

        // Assert
        assert!(!backend.etags.lock().contains_key(key));
    }

    #[test]
    fn should_succeed_when_deleting_nonexistent_blob() {
        // Arrange
        let backend = MockCloudBackend::new();

        // Act
        let result = backend.delete_blob("nonexistent.dat");

        // Assert
        assert!(result.is_ok());
    }

    // ===== list_blobs Tests =====

    #[test]
    fn should_list_blobs_with_prefix() {
        // Arrange
        let backend = MockCloudBackend::new();
        backend
            .put_blob("logs/2024-01.log", Bytes::from("log1"))
            .unwrap();
        backend
            .put_blob("logs/2024-02.log", Bytes::from("log2"))
            .unwrap();
        backend
            .put_blob("data/file.dat", Bytes::from("data"))
            .unwrap();

        // Act
        let result = backend.list_blobs("logs/").unwrap();

        // Assert
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"logs/2024-01.log".to_string()));
        assert!(result.contains(&"logs/2024-02.log".to_string()));
    }

    #[test]
    fn should_return_empty_list_when_no_blobs_match_prefix() {
        // Arrange
        let backend = MockCloudBackend::new();
        backend
            .put_blob("data/file.dat", Bytes::from("data"))
            .unwrap();

        // Act
        let result = backend.list_blobs("logs/").unwrap();

        // Assert
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn should_list_all_blobs_with_empty_prefix() {
        // Arrange
        let backend = MockCloudBackend::new();
        backend.put_blob("file1.dat", Bytes::from("1")).unwrap();
        backend.put_blob("dir/file2.dat", Bytes::from("2")).unwrap();

        // Act
        let result = backend.list_blobs("").unwrap();

        // Assert
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn should_return_sorted_blob_list() {
        // Arrange
        let backend = MockCloudBackend::new();
        backend.put_blob("c.dat", Bytes::from("c")).unwrap();
        backend.put_blob("a.dat", Bytes::from("a")).unwrap();
        backend.put_blob("b.dat", Bytes::from("b")).unwrap();

        // Act
        let result = backend.list_blobs("").unwrap();

        // Assert
        assert_eq!(result, vec!["a.dat", "b.dat", "c.dat"]);
    }

    #[test]
    fn should_handle_nested_directory_listing() {
        // Arrange
        let backend = MockCloudBackend::new();
        backend
            .put_blob("a/b/c/file1.dat", Bytes::from("1"))
            .unwrap();
        backend.put_blob("a/b/file2.dat", Bytes::from("2")).unwrap();
        backend.put_blob("a/file3.dat", Bytes::from("3")).unwrap();

        // Act
        let result = backend.list_blobs("a/b/").unwrap();

        // Assert
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"a/b/c/file1.dat".to_string()));
        assert!(result.contains(&"a/b/file2.dat".to_string()));
    }

    // ===== head_blob Tests =====

    #[test]
    fn should_return_blob_metadata_when_blob_exists() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "test.dat";
        let data = Bytes::from("test data");
        backend.put_blob(key, data.clone()).unwrap();

        // Act
        let result = backend.head_blob(key).unwrap();

        // Assert
        assert!(result.is_some());
        let meta = result.unwrap();
        assert_eq!(meta.size, data.len() as u64);
        assert!(meta.etag.is_some());
    }

    #[test]
    fn should_return_none_when_blob_does_not_exist() {
        // Arrange
        let backend = MockCloudBackend::new();

        // Act
        let result = backend.head_blob("nonexistent.dat").unwrap();

        // Assert
        assert!(result.is_none());
    }

    #[test]
    fn should_return_correct_blob_size() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "size.dat";
        let data = Bytes::from(vec![0u8; 1024]); // 1KB
        backend.put_blob(key, data).unwrap();

        // Act
        let meta = backend.head_blob(key).unwrap().unwrap();

        // Assert
        assert_eq!(meta.size, 1024);
    }

    #[test]
    fn should_return_etag_in_metadata() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "etag.dat";
        backend.put_blob(key, Bytes::from("data")).unwrap();

        // Act
        let meta = backend.head_blob(key).unwrap().unwrap();

        // Assert
        assert!(meta.etag.is_some());
        assert!(meta.etag.unwrap().starts_with("etag-"));
    }

    #[test]
    fn should_return_last_modified_in_metadata() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "modified.dat";
        backend.put_blob(key, Bytes::from("data")).unwrap();

        // Act
        let meta = backend.head_blob(key).unwrap().unwrap();

        // Assert
        assert!(meta.last_modified.is_some());
    }

    // ===== put_blob_if_not_exists Tests =====

    #[test]
    fn should_put_blob_when_it_does_not_exist() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "new.dat";
        let data = Bytes::from("data");

        // Act
        let result = backend.put_blob_if_not_exists(key, data.clone());

        // Assert
        assert!(result.is_ok());
        let etag = result.unwrap();
        assert!(etag.starts_with("etag-"));
    }

    #[test]
    fn should_return_error_when_blob_already_exists() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "existing.dat";
        backend.put_blob(key, Bytes::from("original")).unwrap();

        // Act
        let result = backend.put_blob_if_not_exists(key, Bytes::from("new"));

        // Assert
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::MidgeError::DatabaseLocked
        ));
    }

    #[test]
    fn should_write_blob_to_filesystem_when_using_if_not_exists() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "conditional.dat";
        let data = Bytes::from("test");

        // Act
        backend.put_blob_if_not_exists(key, data.clone()).unwrap();

        // Assert
        let retrieved = backend.get_blob(key).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn should_not_modify_existing_blob_when_using_if_not_exists() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "protected.dat";
        let original = Bytes::from("original");
        backend.put_blob(key, original.clone()).unwrap();

        // Act
        let _ = backend.put_blob_if_not_exists(key, Bytes::from("attempted"));

        // Assert
        let retrieved = backend.get_blob(key).unwrap();
        assert_eq!(retrieved, original);
    }

    // ===== Latency Simulation Tests =====

    #[test]
    fn should_add_latency_to_operations_when_configured() {
        // Arrange
        let latency = Duration::from_millis(50);
        let backend = MockCloudBackend::new().with_latency(latency);
        let key = "latency.dat";
        let data = Bytes::from("test");

        // Act
        let start = std::time::Instant::now();
        backend.put_blob(key, data).unwrap();
        let elapsed = start.elapsed();

        // Assert
        assert!(elapsed >= latency);
    }

    #[test]
    fn should_not_add_latency_when_not_configured() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "fast.dat";
        let data = Bytes::from("test");

        // Act
        let start = std::time::Instant::now();
        backend.put_blob(key, data).unwrap();
        let elapsed = start.elapsed();

        // Assert
        assert!(elapsed < Duration::from_millis(10)); // Should be very fast
    }

    // ===== Path Handling Tests =====

    #[test]
    fn should_convert_forward_slashes_to_platform_separators() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "dir/subdir/file.dat";

        // Act
        let path = backend.blob_path(key);

        // Assert
        #[cfg(windows)]
        assert!(path.to_string_lossy().contains("dir\\subdir\\file.dat"));
        #[cfg(not(windows))]
        assert!(path.to_string_lossy().contains("dir/subdir/file.dat"));
    }

    #[test]
    fn should_handle_keys_without_slashes() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "simple.dat";

        // Act
        let path = backend.blob_path(key);

        // Assert
        assert!(path.ends_with("simple.dat"));
    }

    // ===== ETag Tests =====

    #[test]
    fn should_generate_unique_etags_for_different_puts() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "etag-test.dat";
        let data = Bytes::from("data");

        // Act
        backend.put_blob(key, data.clone()).unwrap();
        let etag1 = backend.etags.lock().get(key).cloned().unwrap();

        // Advance test clock instead of real sleep so tests remain fast and deterministic
        crate::common::timestamp::add_clock_offset_millis(1);

        backend.put_blob(key, data).unwrap();

        // Revert the clock adjustment to avoid impacting other tests
        crate::common::timestamp::add_clock_offset_millis(-1);
        let etag2 = backend.etags.lock().get(key).cloned().unwrap();

        // Assert
        assert_ne!(etag1, etag2);
    }

    #[test]
    fn should_return_etag_from_put_if_not_exists() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "etag-return.dat";
        let data = Bytes::from("data");

        // Act
        let etag = backend.put_blob_if_not_exists(key, data).unwrap();

        // Assert
        let stored_etag = backend.etags.lock().get(key).cloned().unwrap();
        assert_eq!(etag, stored_etag);
    }

    // ===== Concurrent Access Tests =====

    #[test]
    fn should_handle_concurrent_puts_to_different_keys() {
        // Arrange
        let backend = std::sync::Arc::new(MockCloudBackend::new());
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let backend = backend.clone();
                std::thread::spawn(move || {
                    let key = format!("concurrent-{}.dat", i);
                    backend
                        .put_blob(&key, Bytes::from(format!("data-{}", i)))
                        .unwrap();
                })
            })
            .collect();

        // Act
        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        let keys = backend.list_blobs("concurrent-").unwrap();
        assert_eq!(keys.len(), 10);
    }

    #[test]
    fn should_handle_concurrent_reads() {
        // Arrange
        let backend = std::sync::Arc::new(MockCloudBackend::new());
        let key = "shared.dat";
        let data = Bytes::from("shared data");
        backend.put_blob(key, data.clone()).unwrap();

        // Act
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let backend = backend.clone();
                let expected = data.clone();
                std::thread::spawn(move || {
                    let retrieved = backend.get_blob(key).unwrap();
                    assert_eq!(retrieved, expected);
                })
            })
            .collect();

        // Assert
        for handle in handles {
            handle.join().unwrap();
        }
    }

    // ===== Edge Cases =====

    #[test]
    fn should_handle_empty_blob() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "empty.dat";
        let data = Bytes::new();

        // Act
        backend.put_blob(key, data.clone()).unwrap();

        // Assert
        let retrieved = backend.get_blob(key).unwrap();
        assert_eq!(retrieved, data);
        assert_eq!(retrieved.len(), 0);
    }

    #[test]
    fn should_handle_large_blob() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "large.dat";
        let data = Bytes::from(vec![0u8; 10 * 1024 * 1024]); // 10MB

        // Act
        backend.put_blob(key, data.clone()).unwrap();

        // Assert
        let retrieved = backend.get_blob(key).unwrap();
        assert_eq!(retrieved.len(), data.len());
    }

    #[test]
    fn should_handle_keys_with_special_characters() {
        // Arrange
        let backend = MockCloudBackend::new();
        let key = "file-with_special.chars-123.dat";
        let data = Bytes::from("data");

        // Act
        backend.put_blob(key, data.clone()).unwrap();

        // Assert
        let retrieved = backend.get_blob(key).unwrap();
        assert_eq!(retrieved, data);
    }
}
