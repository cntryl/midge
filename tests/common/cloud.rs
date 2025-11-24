//! Cloud-specific test helpers
//!
//! This module provides utilities for testing cloud storage integration.

use cntryl_midge::cloud::MockCloudBackend;
use super::test_helpers::TEST_CLOUD_TIMEOUT;
use cntryl_midge::manifest::Manifest;
use parking_lot::Mutex;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;

/// Setup for cloud integration tests.
///
/// Creates:
/// - Temporary test directory
/// - MockCloudBackend instance
/// - Default Manifest
///
/// # Returns
///
/// A tuple of (temp_dir, backend, manifest)
///
/// # Examples
///
/// ```rust
/// let (temp_dir, backend, manifest) = setup_cloud_test();
/// // ... perform test ...
/// cleanup_cloud_test(&temp_dir);
/// ```
#[allow(dead_code)]
pub fn setup_cloud_test() -> (PathBuf, Arc<MockCloudBackend>, Arc<Mutex<Manifest>>) {
    let temp_dir = std::env::temp_dir().join(format!("midge_cloud_test_{}", Uuid::new_v4()));
    fs::create_dir_all(&temp_dir).expect("Failed to create cloud test directory");

    let backend = Arc::new(MockCloudBackend::new());
    let manifest = Arc::new(Mutex::new(Manifest::default()));

    (temp_dir, backend, manifest)
}

/// Cleanup cloud test directory.
///
/// Removes the temporary directory created by `setup_cloud_test()`.
/// Ignores errors (best-effort cleanup).
///
/// # Examples
///
/// ```rust
/// let (temp_dir, backend, manifest) = setup_cloud_test();
/// // ... perform test ...
/// cleanup_cloud_test(&temp_dir);
/// ```
#[allow(dead_code)]
pub fn cleanup_cloud_test(temp_dir: &PathBuf) {
    let _ = fs::remove_dir_all(temp_dir);
}

/// Creates a dummy SST file for testing.
///
/// Writes the provided content to a file with the given name
/// in the specified directory.
///
/// # Returns
///
/// The full path to the created SST file.
///
/// # Examples
///
/// ```rust
/// let sst_path = create_test_sst(&temp_dir, "test.sst", b"data");
/// assert!(sst_path.exists());
/// ```
#[allow(dead_code)]
pub fn create_test_sst(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
    let sst_dir = dir.join("sst");
    fs::create_dir_all(&sst_dir).expect("Failed to create SST directory");

    let path = sst_dir.join(name);
    fs::write(&path, content).expect("Failed to write test SST");
    path
}

/// Waits for async cloud operations to complete.
///
/// Provides a brief delay to allow background upload operations
/// to finish. Used in tests that need to verify upload completion.
///
/// # Examples
///
/// ```rust
/// // Trigger upload
/// eng.put(key, value).unwrap();
/// wait_for_cloud_upload();
/// // Verify upload completed
/// assert!(backend.has_file("some_file"));
/// ```
#[allow(dead_code)]
pub fn wait_for_cloud_upload(backend: &MockCloudBackend) -> bool {
    // Prefer the mock backend's structured wait helper which polls with a short sleep
    // interval rather than sleeping blind. Return whether an upload occurred within
    // the provided timeout window.
    backend.wait_for_uploads(1, TEST_CLOUD_TIMEOUT)
}
