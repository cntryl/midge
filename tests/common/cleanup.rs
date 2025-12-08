//! Test cleanup utilities to ensure temporary directories are always removed
//!
//! This module provides:
//! - Automatic cleanup of test directories via RAII
//! - Registration of directories for cleanup on test suite exit
//! - Manual cleanup of leaked test directories

use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::Mutex;

/// Global registry of test directories that need cleanup
static CLEANUP_REGISTRY: Lazy<Mutex<Vec<PathBuf>>> = Lazy::new(|| Mutex::new(Vec::new()));

/// Register a directory for cleanup when the test suite exits
pub fn register_for_cleanup(path: PathBuf) {
    if let Ok(mut registry) = CLEANUP_REGISTRY.lock() {
        registry.push(path);
    }
}

/// Clean up all registered directories
#[allow(dead_code)]
pub fn cleanup_all_registered() {
    if let Ok(mut registry) = CLEANUP_REGISTRY.lock() {
        for path in registry.drain(..) {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

/// Clean up leaked test directories in /tmp
///
/// Searches for directories matching "midge_test_*" and "midge-mem"
/// patterns and removes them. This is useful for cleaning up after
/// tests that panicked or were interrupted.
#[allow(dead_code)]
pub fn cleanup_leaked_test_dirs() -> std::io::Result<usize> {
    let tmp_dir = std::env::temp_dir();
    let mut cleaned = 0;

    if let Ok(entries) = std::fs::read_dir(&tmp_dir) {
        for entry in entries.flatten() {
            if let Ok(file_name) = entry.file_name().into_string() {
                if file_name.starts_with("midge_test_") || file_name == "midge-mem" {
                    let path = entry.path();
                    if path.is_dir() && std::fs::remove_dir_all(&path).is_ok() {
                        cleaned += 1;
                    }
                }
            }
        }
    }

    Ok(cleaned)
}

/// A test directory guard that ensures cleanup on drop
///
/// This wrapper tracks the directory in a global registry and
/// ensures it's cleaned up even if the test panics.
pub struct TestDirGuard {
    path: PathBuf,
    cleanup_on_drop: bool,
}

impl TestDirGuard {
    /// Create a new test directory guard
    pub fn new(path: PathBuf) -> Self {
        register_for_cleanup(path.clone());
        Self {
            path,
            cleanup_on_drop: true,
        }
    }

    /// Get the path to this directory
    #[allow(dead_code)]
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Keep the directory after drop (for debugging)
    #[allow(dead_code)]
    pub fn keep(&mut self) {
        self.cleanup_on_drop = false;
    }
}

impl Drop for TestDirGuard {
    fn drop(&mut self) {
        if self.cleanup_on_drop {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

impl AsRef<std::path::Path> for TestDirGuard {
    fn as_ref(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_cleanup_directory_when_guard_dropped() {
        // Arrange
        let temp_path = std::env::temp_dir().join(format!(
            "midge_test_cleanup_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));

        std::fs::create_dir_all(&temp_path).unwrap();
        assert!(temp_path.exists());

        // Act: Create and drop guard
        {
            let _guard = TestDirGuard::new(temp_path.clone());
            assert!(temp_path.exists());
        }

        // Assert: Directory should be cleaned up after guard is dropped
        assert!(!temp_path.exists());
    }

    #[test]
    fn should_keep_directory_when_requested() {
        // Arrange
        let temp_path = std::env::temp_dir().join(format!(
            "midge_test_keep_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));

        std::fs::create_dir_all(&temp_path).unwrap();

        // Act: Create guard and mark for keeping
        {
            let mut guard = TestDirGuard::new(temp_path.clone());
            guard.keep();
        }

        // Assert: Directory should still exist
        assert!(temp_path.exists());

        // Clean up for the test
        let _ = std::fs::remove_dir_all(&temp_path);
    }
}
