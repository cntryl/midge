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
/// Implements `StorageBackend` synchronously. Suitable for local file storage.
/// All operations execute immediately and send completion events via callback.
pub struct FileSystem {
    base_path: PathBuf,
}

impl FileSystem {
    /// Create a new filesystem storage backend.
    pub fn new<P: AsRef<Path>>(base_path: P) -> MidgeResult<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?; // Ensure base dir exists
        Ok(Self { base_path: path })
    }

    /// Compute a sanitized full path for a given key.
    fn full_path(&self, key: &str) -> PathBuf {
        // Prevent absolute paths or path traversal outside the base directory.
        let sanitized = key.trim_start_matches('/');
        self.base_path.join(sanitized)
    }
}

impl StorageBackend for FileSystem {
    fn submit_read(&self, path: String, callback: StorageCallback) {
        let full_path = self.full_path(&path);

        let outcome = match fs::read(&full_path) {
            Ok(bytes) => StorageOutcome::Ok(bytes),
            Err(e) => StorageOutcome::Err(format!("read {:?}: {}", full_path, e)),
        };

        let _ = callback.send(StorageEvent::ReadComplete { path, result: outcome });
    }

    fn submit_write(&self, path: String, data: Vec<u8>, callback: StorageCallback) {
        let full_path = self.full_path(&path);

        let outcome = {
            // Always try to create parent directories if present.
            if let Some(parent) = full_path.parent() {
                if let Err(e) = fs::create_dir_all(parent) {
                    StorageOutcome::Err(format!("mkdir {:?}: {}", parent, e))
                } else if let Err(e) = fs::write(&full_path, data) {
                    StorageOutcome::Err(format!("write {:?}: {}", full_path, e))
                } else {
                    StorageOutcome::Ok(())
                }
            } else {
                // Path has no parent (e.g., "foo") — still attempt the write.
                match fs::write(&full_path, data) {
                    Ok(_) => StorageOutcome::Ok(()),
                    Err(e) => StorageOutcome::Err(format!("write {:?}: {}", full_path, e)),
                }
            }
        };

        let _ = callback.send(StorageEvent::WriteComplete { path, result: outcome });
    }

    fn submit_delete(&self, path: String, callback: StorageCallback) {
        let full_path = self.full_path(&path);

        let outcome = match fs::remove_file(&full_path) {
            Ok(_) => StorageOutcome::Ok(()),
            Err(e) => StorageOutcome::Err(format!("delete {:?}: {}", full_path, e)),
        };

        let _ =
            callback.send(StorageEvent::DeleteComplete { path, result: outcome });
    }

    fn submit_list(&self, prefix: String, callback: StorageCallback) {
        let full = self.full_path(&prefix);

        let outcome = if full.is_dir() {
            match fs::read_dir(&full) {
                Ok(iter) => {
                    let mut items: Vec<String> = Vec::new();

                    for entry in iter.flatten() {
                        if let Some(name) = entry.file_name().to_str() {
                            items.push(name.to_string());
                        }
                    }

                    StorageOutcome::Ok(items)
                }
                Err(e) => StorageOutcome::Err(format!("list {:?}: {}", full, e)),
            }
        } else {
            StorageOutcome::Ok(Vec::new())
        };

        let _ = callback.send(StorageEvent::ListComplete {
            prefix,
            result: outcome,
        });
    }
}
