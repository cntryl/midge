//! Test directory cleanup utilities
//!
//! This module provides automatic cleanup of temporary directories created during tests.
//! It's designed to work with both unit tests and integration tests.

use once_cell::sync::Lazy;
use std::path::PathBuf;
use std::sync::Mutex;

/// Global registry of temporary directories that need cleanup
static TEMP_DIR_REGISTRY: Lazy<Mutex<Vec<PathBuf>>> = Lazy::new(|| Mutex::new(Vec::new()));

/// Register a temporary directory for cleanup
///
/// The directory will be removed when cleanup_registered_dirs() is called,
/// typically during test teardown or Drop implementations.
pub fn register_temp_dir(path: PathBuf) {
    if let Ok(mut registry) = TEMP_DIR_REGISTRY.lock() {
        registry.push(path);
    }
}

/// Clean up all registered temporary directories
///
/// Removes all directories that were registered via register_temp_dir().
/// This is safe to call multiple times.
pub fn cleanup_registered_dirs() {
    if let Ok(mut registry) = TEMP_DIR_REGISTRY.lock() {
        for path in registry.drain(..) {
            let _ = std::fs::remove_dir_all(&path);
        }
    }
}

/// A guard that ensures a directory is cleaned up when dropped
pub struct TempDirGuard {
    path: PathBuf,
    cleanup: bool,
}

impl TempDirGuard {
    /// Create a new temporary directory guard
    pub fn new(path: PathBuf) -> Self {
        register_temp_dir(path.clone());
        Self {
            path,
            cleanup: true,
        }
    }

    /// Get the path to this directory
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Prevent automatic cleanup (for debugging)
    #[allow(dead_code)]
    pub fn keep(&mut self) {
        self.cleanup = false;
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

impl AsRef<std::path::Path> for TempDirGuard {
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

        // Act
        {
            let _guard = TempDirGuard::new(temp_path.clone());
            assert!(temp_path.exists());
        }

        // Assert - Directory should be cleaned up after guard is dropped
        assert!(!temp_path.exists());
    }

    #[test]
    fn should_register_directory_for_batch_cleanup() {
        // Arrange
        let temp_path = std::env::temp_dir().join(format!(
            "midge_test_registry_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        ));

        std::fs::create_dir_all(&temp_path).unwrap();
        register_temp_dir(temp_path.clone());

        assert!(temp_path.exists());

        // Act
        cleanup_registered_dirs();

        // Assert - Directory should be cleaned up
        assert!(!temp_path.exists());
    }
}
