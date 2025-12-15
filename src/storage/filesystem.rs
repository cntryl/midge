//! Filesystem storage backend implementation
//!
//! Provides local filesystem storage with callback-based operations.
//! Executes immediately (synchronously) but conforms to the async-compatible trait.

use crate::common::MidgeResult;
use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use std::fs;
use std::path::{Component, Path, PathBuf};

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
        // Treat the key as a relative, forward-slash-friendly path.
        let mut out = self.base_path.clone();
        for component in Path::new(key).components() {
            match component {
                Component::Normal(part) => out.push(part),
                Component::CurDir => {}
                Component::ParentDir => {}
                Component::RootDir => {}
                Component::Prefix(_) => {}
            }
        }
        out
    }
}

impl StorageBackend for FileSystem {
    fn submit_read(&self, key: String, callback: StorageCallback) {
        let full_path = self.full_path(&key);

        let outcome = match fs::read(&full_path) {
            Ok(bytes) => StorageOutcome::Ok(bytes),
            Err(e) => StorageOutcome::Err(format!("read {:?}: {}", full_path, e)),
        };

        let _ = callback.send(StorageEvent::ReadComplete {
            key,
            result: outcome,
        });
    }

    fn submit_write(&self, key: String, data: Vec<u8>, callback: StorageCallback) {
        let full_path = self.full_path(&key);

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

        let _ = callback.send(StorageEvent::WriteComplete {
            key,
            result: outcome,
        });
    }

    fn submit_delete(&self, key: String, callback: StorageCallback) {
        let full_path = self.full_path(&key);

        let outcome = match fs::remove_file(&full_path) {
            Ok(_) => StorageOutcome::Ok(()),
            Err(e) => StorageOutcome::Err(format!("delete {:?}: {}", full_path, e)),
        };

        let _ = callback.send(StorageEvent::DeleteComplete {
            key,
            result: outcome,
        });
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use tempfile::TempDir;

    // =========== Write Tests ===========

    #[test]
    fn should_write_file_successfully() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let data = b"test data".to_vec();

        // Act
        fs.submit_write("test.txt".into(), data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { key, result } => {
                assert_eq!(key, "test.txt");
                assert!(result.is_ok());
                // Verify file was actually written
                let content = std::fs::read(temp_dir.path().join("test.txt")).unwrap();
                assert_eq!(content, data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_empty_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("empty.txt".into(), vec![], tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("empty.txt")).unwrap();
                assert!(content.is_empty());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_large_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let large_data = vec![42u8; 1_000_000];

        // Act
        fs.submit_write("large.bin".into(), large_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("large.bin")).unwrap();
                assert_eq!(content, large_data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_create_parent_directories() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_write("subdir/nested/file.txt".into(), b"data".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("subdir/nested/file.txt").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_overwrite_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let path = temp_dir.path().join("file.txt");
        std::fs::write(&path, b"old").unwrap();

        // Act
        let (tx, rx) = mpsc::channel();
        fs.submit_write("file.txt".into(), b"new".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(&path).unwrap();
                assert_eq!(content, b"new");
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_write_binary_data() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();
        let binary_data = vec![0u8, 1u8, 255u8, 254u8];

        // Act
        fs.submit_write("binary.bin".into(), binary_data.clone(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                let content = std::fs::read(temp_dir.path().join("binary.bin")).unwrap();
                assert_eq!(content, binary_data);
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    // =========== Read Tests ===========

    #[test]
    fn should_read_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = b"hello world";
        std::fs::write(temp_dir.path().join("test.txt"), data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("test.txt".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { key, result } => {
                assert_eq!(key, "test.txt");
                match result {
                    StorageOutcome::Ok(content) => assert_eq!(content, data),
                    StorageOutcome::Err(e) => panic!("Read failed: {}", e),
                }
            }
            _ => panic!("Expected ReadComplete"),
        }
    }

    #[test]
    fn should_fail_reading_nonexistent_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("nonexistent.txt".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { result, .. } => {
                assert!(result.is_err());
            }
            _ => panic!("Expected ReadComplete"),
        }
    }

    #[test]
    fn should_read_large_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = vec![42u8; 1_000_000];
        std::fs::write(temp_dir.path().join("large.bin"), &data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("large.bin".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { result, .. } => match result {
                StorageOutcome::Ok(content) => assert_eq!(content, data),
                StorageOutcome::Err(e) => panic!("Read failed: {}", e),
            },
            _ => panic!("Expected ReadComplete"),
        }
    }

    #[test]
    fn should_read_empty_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("empty.txt"), b"").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("empty.txt".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { result, .. } => match result {
                StorageOutcome::Ok(content) => assert!(content.is_empty()),
                StorageOutcome::Err(e) => panic!("Read failed: {}", e),
            },
            _ => panic!("Expected ReadComplete"),
        }
    }

    #[test]
    fn should_read_binary_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let data = vec![0u8, 1u8, 255u8, 254u8];
        std::fs::write(temp_dir.path().join("binary.bin"), &data).unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_read("binary.bin".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ReadComplete { result, .. } => match result {
                StorageOutcome::Ok(content) => assert_eq!(content, data),
                StorageOutcome::Err(e) => panic!("Read failed: {}", e),
            },
            _ => panic!("Expected ReadComplete"),
        }
    }

    // =========== Delete Tests ===========

    #[test]
    fn should_delete_existing_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("file.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("file.txt".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_fail_deleting_nonexistent_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("nonexistent.txt".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_err());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    #[test]
    fn should_delete_nested_file() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("subdir")).unwrap();
        let path = temp_dir.path().join("subdir/file.txt");
        std::fs::write(&path, b"data").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_delete("subdir/file.txt".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::DeleteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(!path.exists());
            }
            _ => panic!("Expected DeleteComplete"),
        }
    }

    // =========== List Tests ===========

    #[test]
    fn should_list_directory_contents() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        std::fs::write(temp_dir.path().join("file1.txt"), b"data1").unwrap();
        std::fs::write(temp_dir.path().join("file2.txt"), b"data2").unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_list("".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ListComplete { result, .. } => match result {
                StorageOutcome::Ok(items) => {
                    assert_eq!(items.len(), 2);
                    assert!(items.contains(&"file1.txt".to_string()));
                    assert!(items.contains(&"file2.txt".to_string()));
                }
                StorageOutcome::Err(e) => panic!("List failed: {}", e),
            },
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_list_empty_directory() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_list("".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ListComplete { result, .. } => match result {
                StorageOutcome::Ok(items) => assert!(items.is_empty()),
                StorageOutcome::Err(e) => panic!("List failed: {}", e),
            },
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_list_nonexistent_directory() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act
        fs.submit_list("nonexistent".into(), tx);
        let event = rx.recv().unwrap();

        // Assert
        match event {
            StorageEvent::ListComplete { result, .. } => match result {
                StorageOutcome::Ok(items) => assert!(items.is_empty()),
                StorageOutcome::Err(e) => panic!("List failed: {}", e),
            },
            _ => panic!("Expected ListComplete"),
        }
    }

    #[test]
    fn should_sanitize_path_traversal() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act - Try to use path traversal
        fs.submit_write("../escape_dir/evil.txt".into(), b"x".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert - Should succeed but write inside base_path
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("escape_dir/evil.txt").exists());
                assert!(!temp_dir.path().join("../escape_dir/evil.txt").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_handle_absolute_paths() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let fs = FileSystem::new(temp_dir.path()).unwrap();
        let (tx, rx) = mpsc::channel();

        // Act - Try absolute path
        fs.submit_write("/etc/passwd".into(), b"x".to_vec(), tx);
        let event = rx.recv().unwrap();

        // Assert - Should sanitize
        match event {
            StorageEvent::WriteComplete { result, .. } => {
                assert!(result.is_ok());
                assert!(temp_dir.path().join("etc/passwd").exists());
            }
            _ => panic!("Expected WriteComplete"),
        }
    }

    #[test]
    fn should_construct_with_custom_base_path() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();

        // Act
        let fs = FileSystem::new(temp_dir.path());

        // Assert
        assert!(fs.is_ok());
    }

    #[test]
    fn should_create_base_directory_if_missing() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let new_dir = temp_dir.path().join("new_base");
        assert!(!new_dir.exists());

        // Act
        let fs = FileSystem::new(&new_dir);

        // Assert
        assert!(fs.is_ok());
        assert!(new_dir.exists());
    }
}
