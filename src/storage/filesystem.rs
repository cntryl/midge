//! Filesystem storage backend implementation
//!
//! Provides local filesystem storage with callback-based operations.
//! Executes immediately (synchronously) but conforms to the async-compatible trait.

use crate::common::MidgeResult;
use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use std::fs;
use std::path::{Path, PathBuf};

/// Filesystem-based storage backend
///
/// Implements StorageBackend synchronously. Suitable for local file storage.
/// All operations execute immediately and send completion events via callback.
pub struct FileSystem {
    base_path: PathBuf,
}

impl FileSystem {
    /// Create a new filesystem storage backend
    pub fn new<P: AsRef<Path>>(base_path: P) -> MidgeResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;
        Ok(Self { base_path: path })
    }

    /// Get the full path for a given key
    fn full_path(&self, key: &str) -> PathBuf {
        self.base_path.join(key)
    }
}

impl StorageBackend for FileSystem {
    fn submit_read(&self, path: String, callback: StorageCallback) {
        let full_path = self.full_path(&path);
        let result = fs::read(&full_path).map_err(|e| MidgeError::Io(e));

        let event = StorageEvent::ReadComplete {
            path,
            result: StorageOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_write(&self, path: String, data: Vec<u8>, callback: StorageCallback) {
        let full_path = self.full_path(&path);

        // Ensure parent directory exists
        let result = if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .and_then(|_| fs::write(&full_path, data))
                .map_err(|e| MidgeError::Io(e))
        } else {
            Ok(())
        };

        let event = StorageEvent::WriteComplete {
            path,
            result: StorageOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, path: String, callback: StorageCallback) {
        let full_path = self.full_path(&path);
        let result = fs::remove_file(&full_path).map_err(|e| MidgeError::Io(e));

        let event = StorageEvent::DeleteComplete {
            path,
            result: StorageOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: String, callback: StorageCallback) {
        let prefix_path = self.full_path(&prefix);

        let result = if prefix_path.is_dir() {
            fs::read_dir(&prefix_path)
                .and_then(|iter| {
                    Ok(iter
                        .filter_map(|e| e.ok())
                        .filter_map(|e| {
                            e.path()
                                .file_name()
                                .and_then(|n| n.to_str())
                                .map(|s| s.to_string())
                        })
                        .collect())
                })
                .map_err(|e| MidgeError::Io(e))
        } else {
            Ok(Vec::new())
        };

        let event = StorageEvent::ListComplete {
            prefix,
            result: StorageOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }
}

// Use local imports for MidgeError to avoid conflicts
use crate::common::MidgeError;
