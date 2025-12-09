//! Filesystem storage backend implementation
//!
//! Provides local filesystem storage with basic read/write/delete operations.

use crate::common::MidgeResult;
use crate::storage::StorageBackend;
use std::fs;
use std::path::{Path, PathBuf};

/// Filesystem-based storage backend
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
    fn read(&self, path: &str) -> MidgeResult<Vec<u8>> {
        let full_path = self.full_path(path);
        fs::read(&full_path).map_err(|e| {
            crate::common::MidgeError::Io(e)
        })
    }

    fn write(&mut self, path: &str, data: &[u8]) -> MidgeResult<()> {
        let full_path = self.full_path(path);
        // Ensure parent directory exists
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&full_path, data).map_err(|e| {
            crate::common::MidgeError::Io(e)
        })
    }

    fn delete(&mut self, path: &str) -> MidgeResult<()> {
        let full_path = self.full_path(path);
        fs::remove_file(&full_path).map_err(|e| {
            crate::common::MidgeError::Io(e)
        })
    }

    fn list(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        let mut results = Vec::new();
        let prefix_path = self.full_path(prefix);

        // If prefix is a directory, list its contents
        if prefix_path.is_dir() {
            for entry in fs::read_dir(&prefix_path)? {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    results.push(name.to_string());
                }
            }
        } else {
            // If prefix is a file pattern, we could implement glob matching here
            // For now, just return empty list
        }

        Ok(results)
    }
}
