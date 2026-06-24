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

pub mod executor;

use super::{StorageBackend, StorageCallback, StorageEvent, StorageObjectMetadata, StorageOutcome};
use crate::common::MidgeError;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub use executor::{CloudExecutor, CloudRequest, CloudResponse, CloudSigner};

/// Cloud operation outcome – cloneable wrapper around Result
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum CloudOutcome<T: Clone> {
    Ok(T),
    Err(String),
}

#[allow(dead_code)]
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
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
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
#[allow(dead_code)]
pub struct ObjectMetadata {
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
    pub generation: Option<String>,
}

#[allow(dead_code)]
impl ObjectMetadata {
    pub fn new(size: u64, etag: String, last_modified: u64) -> Self {
        Self {
            size,
            etag,
            last_modified,
            generation: None,
        }
    }

    pub fn with_generation(
        size: u64,
        etag: String,
        last_modified: u64,
        generation: impl Into<String>,
    ) -> Self {
        Self {
            size,
            etag,
            last_modified,
            generation: Some(generation.into()),
        }
    }
}

/// Non-blocking cloud backend interface used by the engine.
#[allow(dead_code)]
pub trait CloudBackend: Send + Sync + 'static {
    /// Submit a PUT request for `key` with optional HTTP headers. Implementations
    /// MUST honor headers (e.g. `If-None-Match`, `If-Match`) when supported by the
    /// provider to allow conditional writes.
    fn submit_put(
        &self,
        key: String,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    );
    fn submit_get(&self, key: String, callback: CloudCallback);
    fn submit_get_range(&self, key: String, start: u64, end: Option<u64>, callback: CloudCallback);
    fn submit_delete(&self, key: String, headers: Vec<(String, String)>, callback: CloudCallback);
    fn submit_list(&self, prefix: String, callback: CloudCallback);
    fn submit_head(&self, key: String, callback: CloudCallback);
}

/// Deterministic mock backend for testing (synchronous).
#[allow(dead_code)]
pub struct MockCloudBackend {
    storage: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    gens: Arc<Mutex<HashMap<String, u64>>>,
    uploads: Arc<Mutex<Vec<(String, u64)>>>,
    downloads: Arc<Mutex<Vec<String>>>,
}

#[allow(dead_code)]
impl MockCloudBackend {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            gens: Arc::new(Mutex::new(HashMap::new())),
            uploads: Arc::new(Mutex::new(Vec::new())),
            downloads: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn object_count(&self) -> usize {
        self.storage.lock().len()
    }

    pub fn get_uploads(&self) -> Vec<(String, u64)> {
        self.uploads.lock().clone()
    }

    pub fn get_downloads(&self) -> Vec<String> {
        self.downloads.lock().clone()
    }

    pub fn clear_history(&self) {
        self.uploads.lock().clear();
        self.downloads.lock().clear();
    }
}

impl Default for MockCloudBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl CloudBackend for MockCloudBackend {
    fn submit_put(
        &self,
        key: String,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        // Honor `If-None-Match: *` (conditional create).
        let if_none_match = headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("if-none-match") && v == "*");
        if if_none_match && self.storage.lock().contains_key(&key) {
            // Simulate conditional failure (precondition failed)
            let event = CloudEvent::PutComplete {
                key,
                result: CloudOutcome::Err("precondition failed".to_string()),
            };
            let _ = callback.send(event);
            return;
        }

        // Honor `If-Match: <etag>` (conditional update).
        // If object missing → precondition failed. If etag mismatches → precondition failed.
        if let Some((_, expected)) = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("if-match"))
        {
            let exists = self.storage.lock().contains_key(&key);
            if !exists {
                let event = CloudEvent::PutComplete {
                    key,
                    result: CloudOutcome::Err("precondition failed".to_string()),
                };
                let _ = callback.send(event);
                return;
            }

            // compare expected value to stored generation-based etag
            let gens_lock = self.gens.lock();
            let current_gen = gens_lock.get(&key).cloned().unwrap_or(0);
            let current_etag = format!("mock-gen-{}", current_gen);
            if expected != &current_etag {
                let event = CloudEvent::PutComplete {
                    key,
                    result: CloudOutcome::Err("precondition failed".to_string()),
                };
                let _ = callback.send(event);
                return;
            }
        }

        // Perform put: store data and bump generation (etag)
        {
            let mut store = self.storage.lock();
            store.insert(key.clone(), data.clone());
        }
        let mut gens = self.gens.lock();
        let new_gen = gens.get(&key).cloned().unwrap_or(0).saturating_add(1);
        gens.insert(key.clone(), new_gen);

        self.uploads.lock().push((key.clone(), data.len() as u64));
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
            .get(&key)
            .cloned()
            .ok_or(MidgeError::NotFound);
        self.downloads.lock().push(key.clone());
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

    fn submit_delete(&self, key: String, headers: Vec<(String, String)>, callback: CloudCallback) {
        if let Some((_, expected)) = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("if-match"))
        {
            let exists = self.storage.lock().contains_key(&key);
            if !exists {
                let event = CloudEvent::DeleteComplete {
                    key,
                    result: CloudOutcome::Err("precondition failed".to_string()),
                };
                let _ = callback.send(event);
                return;
            }

            let current_gen = self.gens.lock().get(&key).cloned().unwrap_or(0);
            let current_etag = format!("mock-gen-{}", current_gen);
            if expected.trim_matches('"') != current_etag {
                let event = CloudEvent::DeleteComplete {
                    key,
                    result: CloudOutcome::Err("precondition failed".to_string()),
                };
                let _ = callback.send(event);
                return;
            }
        }

        self.storage.lock().remove(&key);
        self.gens.lock().remove(&key);
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
            .get(&key)
            .map(|data| {
                // ETag is generation based and independent from content length.
                let gen = self.gens.lock().get(&key).cloned().unwrap_or(0);
                ObjectMetadata::new(data.len() as u64, format!("mock-gen-{}", gen), 0)
            })
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

pub(crate) const CLOUD_METADATA_FILES: &[&str] = &[
    "FORMAT",
    "manifest.snapshot.json",
    "manifest.json",
    "manifest.journal",
    "intent_log.json",
];

pub(crate) fn cloud_metadata_key(file_name: &str) -> String {
    format!("metadata/{file_name}")
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
        let namespace = self.namespace.trim_matches('/');
        let suffix = suffix.trim_start_matches('/');
        if namespace.is_empty() {
            suffix.to_string()
        } else if suffix.is_empty() {
            namespace.to_string()
        } else {
            format!("{}/{}", namespace, suffix)
        }
    }

    pub(crate) fn strip_namespace<'a>(&self, key: &'a str) -> &'a str {
        let namespace = self.namespace.trim_matches('/');
        if namespace.is_empty() {
            return key;
        }
        key.strip_prefix(namespace)
            .and_then(|rest| rest.strip_prefix('/'))
            .unwrap_or(key)
    }

    pub fn submit_put(
        &self,
        key: String,
        data: Vec<u8>,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        let full_key = self.full_path(&key);
        if let Some(error) = self.precondition_error(&full_key, &headers) {
            let _ = callback.send(CloudEvent::PutComplete {
                key: full_key,
                result: CloudOutcome::Err(error),
            });
            return;
        }
        self.backend.submit_put(full_key, data, headers, callback);
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
        self.submit_delete_with_headers(key, vec![], callback);
    }

    pub fn submit_delete_with_headers(
        &self,
        key: String,
        headers: Vec<(String, String)>,
        callback: CloudCallback,
    ) {
        self.backend
            .submit_delete(self.full_path(&key), headers, callback);
    }

    pub fn submit_list(&self, prefix: String, callback: CloudCallback) {
        self.backend.submit_list(self.full_path(&prefix), callback);
    }

    pub fn submit_head(&self, key: String, callback: CloudCallback) {
        self.backend.submit_head(self.full_path(&key), callback);
    }

    fn precondition_error(&self, full_key: &str, headers: &[(String, String)]) -> Option<String> {
        let if_none_match = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-none-match"))
            .map(|(_, value)| value.trim().to_string());
        let if_match = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("if-match"))
            .map(|(_, value)| value.trim().trim_matches('"').to_string());

        if if_none_match.as_deref() != Some("*") && if_match.is_none() {
            return None;
        }

        let (tx, rx) = std::sync::mpsc::channel();
        self.backend.submit_head(full_key.to_string(), tx);
        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok(CloudEvent::HeadComplete { result, .. }) => match result {
                CloudOutcome::Ok(metadata) => {
                    if if_none_match.as_deref() == Some("*") {
                        Some("precondition failed: object already exists".to_string())
                    } else if let Some(expected) = if_match {
                        let current = metadata.etag.trim_matches('"');
                        if current == expected {
                            None
                        } else {
                            Some("precondition failed: etag mismatch".to_string())
                        }
                    } else {
                        None
                    }
                }
                CloudOutcome::Err(error) => {
                    if is_not_found_error(&error) {
                        if if_match.is_some() {
                            Some("precondition failed: object missing".to_string())
                        } else {
                            None
                        }
                    } else {
                        Some(format!("precondition check failed: {}", error))
                    }
                }
            },
            Ok(other) => Some(format!(
                "unexpected precondition HEAD response: {:?}",
                other
            )),
            Err(error) => Some(format!("precondition HEAD timed out: {}", error)),
        }
    }
}

fn is_not_found_error(error: &str) -> bool {
    let lowered = error.to_ascii_lowercase();
    lowered.contains("not found")
        || lowered.contains("notfound")
        || lowered.contains("404")
        || lowered.contains("nosuchkey")
        || lowered.contains("blobnotfound")
}

impl StorageBackend for CloudStorage {
    fn submit_read(&self, key: String, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_get(key.clone(), tx);
        if let Ok(CloudEvent::GetComplete { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(data) => StorageOutcome::Ok(data),
                CloudOutcome::Err(err) => StorageOutcome::Err(err),
            };
            let event = StorageEvent::ReadComplete {
                key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_write(&self, key: String, data: Vec<u8>, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        self.submit_put(key.clone(), data, vec![], tx);
        if let Ok(CloudEvent::PutComplete { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(()) => StorageOutcome::Ok(()),
                CloudOutcome::Err(err) => StorageOutcome::Err(err),
            };
            let event = StorageEvent::WriteComplete {
                key,
                result: outcome,
            };
            let _ = callback.send(event);
        }
    }

    fn submit_delete(&self, key: String, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_delete(self, key.clone(), tx);
        if let Ok(CloudEvent::DeleteComplete { key, result }) = rx.recv() {
            let outcome = match result {
                CloudOutcome::Ok(()) => StorageOutcome::Ok(()),
                CloudOutcome::Err(err) => StorageOutcome::Err(err),
            };
            let event = StorageEvent::DeleteComplete {
                key,
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

    fn submit_head(&self, key: String, callback: StorageCallback) {
        let (tx, rx) = std::sync::mpsc::channel();
        CloudStorage::submit_head(self, key.clone(), tx);
        let event = match rx.recv() {
            Ok(CloudEvent::HeadComplete { key, result }) => {
                let outcome = match result {
                    CloudOutcome::Ok(metadata) => StorageOutcome::Ok(StorageObjectMetadata {
                        size: metadata.size,
                        etag: metadata.etag,
                        generation: metadata.generation,
                    }),
                    CloudOutcome::Err(err) => StorageOutcome::Err(err),
                };
                StorageEvent::HeadComplete {
                    key,
                    result: outcome,
                }
            }
            Ok(other) => StorageEvent::HeadComplete {
                key,
                result: StorageOutcome::Err(format!("unexpected cloud HEAD response: {other:?}")),
            },
            Err(error) => StorageEvent::HeadComplete {
                key,
                result: StorageOutcome::Err(format!("cloud HEAD channel closed: {error}")),
            },
        };
        let _ = callback.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    // =========== CloudOutcome Tests ===========

    #[test]
    fn should_discriminate_success_from_failure_outcomes() {
        // Arrange
        let ok_outcome: CloudOutcome<String> = CloudOutcome::Ok("success".into());
        let err_outcome: CloudOutcome<String> = CloudOutcome::Err("failure".into());

        // Act
        let ok_is_ok = ok_outcome.is_ok();
        let ok_is_err = ok_outcome.is_err();
        let err_is_ok = err_outcome.is_ok();
        let err_is_err = err_outcome.is_err();

        // Assert
        assert!(ok_is_ok);
        assert!(!ok_is_err);
        assert!(!err_is_ok);
        assert!(err_is_err);
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
    fn should_create_object_metadata_with_correct_fields() {
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
        // Arrange
        let zero_etag = "zero".to_string();
        let max_etag = "max".to_string();

        // Act
        let zero_size = ObjectMetadata::new(0, zero_etag, 100);
        let max_size = ObjectMetadata::new(u64::MAX, max_etag, 200);

        // Assert
        assert_eq!(zero_size.size, 0);
        assert_eq!(max_size.size, u64::MAX);
    }

    #[test]
    fn should_handle_metadata_with_empty_etag() {
        // Arrange
        let empty_etag = String::new();

        // Act
        let metadata = ObjectMetadata::new(100, empty_etag, 1000);

        // Assert
        assert_eq!(metadata.etag, "");
        assert_eq!(metadata.size, 100);
    }

    // =========== CloudStorage Routing Tests ===========

    #[test]
    fn should_route_namespace_put_operation() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (tx, rx) = mpsc::channel();
        let data = vec![1, 2, 3];

        // Act
        storage.submit_put("file".into(), data, vec![], tx);
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
    fn should_route_namespace_get_operation() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile".into(), vec![1, 2, 3], vec![], put_tx);
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
    fn should_route_delete_with_namespace_applied() {
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
    fn should_route_list_with_namespace_applied() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put multiple files
        let (tx, rx) = mpsc::channel();
        storage.submit_put("prefix/file1".into(), vec![1], vec![], tx);
        let _ = rx.recv();

        let (tx, rx) = mpsc::channel();
        storage.submit_put("prefix/file2".into(), vec![2], vec![], tx);
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
    fn should_route_head_return_metadata() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("testfile".into(), vec![1, 2, 3], vec![], put_tx);
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
    fn should_honor_if_match_header_on_put() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file1".into(), vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        // Get current etag via HEAD
        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("file1".into(), head_tx);
        let head_event = head_rx.recv().unwrap();
        let current_etag = match head_event {
            CloudEvent::HeadComplete { result, .. } => match result {
                CloudOutcome::Ok(meta) => meta.etag,
                _ => panic!("expected head ok"),
            },
            _ => panic!("expected head event"),
        };

        // Act - conditional update with matching If-Match
        let headers = vec![("If-Match".into(), current_etag.clone())];
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file1".into(), vec![9, 9, 9], headers, put_tx);
        let put_event = put_rx.recv().unwrap();

        // Assert - success and new etag changed
        match put_event {
            CloudEvent::PutComplete { result, .. } => assert!(result.is_ok()),
            _ => panic!("expected put complete"),
        }

        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("file1".into(), head_tx);
        let head_event = head_rx.recv().unwrap();
        let new_etag = match head_event {
            CloudEvent::HeadComplete { result, .. } => match result {
                CloudOutcome::Ok(meta) => meta.etag,
                _ => panic!("expected head ok"),
            },
            _ => panic!("expected head event"),
        };

        assert_ne!(current_etag, new_etag);
    }

    #[test]
    fn should_fail_put_when_if_match_mismatch() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file2".into(), vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        // Act - conditional update with non-matching If-Match
        let headers = vec![("If-Match".into(), "mock-gen-999".into())];
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file2".into(), vec![2], headers, put_tx);
        let put_event = put_rx.recv().unwrap();

        // Assert - precondition failed
        match put_event {
            CloudEvent::PutComplete { result, .. } => assert!(result.is_err()),
            _ => panic!("expected put complete"),
        }
    }

    #[test]
    fn should_fail_if_match_on_missing_object() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // Act - If-Match on non-existent key should fail
        let headers = vec![("If-Match".into(), "mock-gen-1".into())];
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("no-such".into(), vec![1], headers, put_tx);
        let put_event = put_rx.recv().unwrap();

        // Assert - precondition failed
        match put_event {
            CloudEvent::PutComplete { result, .. } => assert!(result.is_err()),
            _ => panic!("expected put complete"),
        }
    }

    #[test]
    fn should_respect_if_none_match_star_on_existing_object() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file3".into(), vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        // Act - conditional create should fail when object exists
        let headers = vec![("If-None-Match".into(), "*".into())];
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("file3".into(), vec![2], headers, put_tx);
        let put_event = put_rx.recv().unwrap();

        // Assert - precondition failed
        match put_event {
            CloudEvent::PutComplete { result, .. } => assert!(result.is_err()),
            _ => panic!("expected put complete"),
        }
    }

    #[test]
    fn should_honor_if_match_header_on_delete() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("delete-file".into(), vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("delete-file".into(), head_tx);
        let etag = match head_rx.recv().unwrap() {
            CloudEvent::HeadComplete {
                result: CloudOutcome::Ok(metadata),
                ..
            } => metadata.etag,
            other => panic!("expected HEAD ok, got {other:?}"),
        };

        // Act
        let (delete_tx, delete_rx) = mpsc::channel();
        storage.submit_delete_with_headers(
            "delete-file".into(),
            vec![("If-Match".into(), etag)],
            delete_tx,
        );

        // Assert
        match delete_rx.recv().unwrap() {
            CloudEvent::DeleteComplete { result, .. } => assert!(result.is_ok()),
            other => panic!("expected delete complete, got {other:?}"),
        }
        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("delete-file".into(), head_tx);
        match head_rx.recv().unwrap() {
            CloudEvent::HeadComplete { result, .. } => assert!(result.is_err()),
            other => panic!("expected HEAD complete, got {other:?}"),
        }
    }

    #[test]
    fn should_reject_delete_when_if_match_mismatches() {
        // Arrange
        let storage = CloudStorage::with_mock();
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("stale-delete-file".into(), vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        // Act
        let (delete_tx, delete_rx) = mpsc::channel();
        storage.submit_delete_with_headers(
            "stale-delete-file".into(),
            vec![("If-Match".into(), "mock-gen-999".into())],
            delete_tx,
        );

        // Assert
        match delete_rx.recv().unwrap() {
            CloudEvent::DeleteComplete { result, .. } => assert!(result.is_err()),
            other => panic!("expected delete complete, got {other:?}"),
        }
        let (head_tx, head_rx) = mpsc::channel();
        storage.submit_head("stale-delete-file".into(), head_tx);
        match head_rx.recv().unwrap() {
            CloudEvent::HeadComplete {
                result: CloudOutcome::Ok(_),
                ..
            } => {}
            other => panic!("expected object to survive stale delete, got {other:?}"),
        }
    }

    #[test]
    fn should_route_get_range_with_bounds() {
        // Arrange
        let storage = CloudStorage::with_mock();

        // First put a file
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("rangefile".into(), vec![1, 2, 3, 4, 5], vec![], put_tx);
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
        storage.submit_put("file".into(), data, vec![], tx);
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
        storage.submit_put("testfile".into(), vec![1, 2, 3], vec![], put_tx);
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
        storage.submit_put("prefix/file1".into(), vec![1], vec![], put_tx);
        let _ = put_rx.recv();

        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("prefix/file2".into(), vec![2], vec![], put_tx);
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
        storage.submit_put("largefile".into(), large_data.clone(), vec![], tx);
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

        // Act: put and get binary data round-trip
        let (put_tx, put_rx) = mpsc::channel();
        storage.submit_put("binaryfile".into(), binary_data.clone(), vec![], put_tx);
        let _ = put_rx.recv();

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
        // Arrange
        let storage = CloudStorage::with_mock();

        // Act

        // Put operation
        let (tx, _rx) = mpsc::channel();
        storage.submit_put("f1".into(), vec![1, 2], vec![], tx);

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

        // Assert
        // Smoke test: all calls complete without panic and storage remains valid.
        assert_eq!(storage.namespace, "midge");
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
        storage.submit_put("emptyfile".into(), vec![], vec![], put_tx);
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
