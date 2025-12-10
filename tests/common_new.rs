//! Simplified test utilities for the new structure
//!
//! Provides minimal test helpers that work with the refactored codebase.

use bytes::Bytes;
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::path::PathBuf;
use tempfile::TempDir;

/// Create a temporary directory for test isolation
pub fn test_temp_dir() -> TempDir {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    std::fs::create_dir_all(&base).ok();
    tempfile::Builder::new()
        .prefix("midge_test_")
        .tempdir_in(&base)
        .expect("Failed to create test temp directory")
}

/// All available storage modes for integration tests
pub fn all_storage_modes() -> Vec<&'static str> {
    vec!["Memory", "LocalDisk"]
}

/// Create storage mode configuration from mode name
/// Returns (mode_name, StorageMode, optional_temp_dir)
pub fn create_storage_mode(mode: &str) -> (String, StorageMode, Option<TempDir>) {
    match mode {
        "Memory" => (
            "Memory".to_string(),
            StorageMode::Memory,
            None,
        ),
        "LocalDisk" => {
            let dir = test_temp_dir();
            let storage_mode = StorageMode::LocalDisk {
                db_path: dir.path().to_path_buf(),
            };
            ("LocalDisk".to_string(), storage_mode, Some(dir))
        }
        _ => panic!("Unknown storage mode: {}", mode),
    }
}

/// Open a new engine with the given options
pub fn new_engine_with_options(opts: MidgeOptions) -> MidgeResult<MidgeEngine> {
    MidgeEngine::open_with_options(opts)
}

/// Create an engine in a temporary directory with default options
pub fn new_engine() -> (TempDir, MidgeEngine) {
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open_with_options(opts).expect("Failed to open engine");
    (dir, engine)
}

/// Assert that a get returns the expected value
pub fn assert_get_equals(eng: &MidgeEngine, key: &[u8], expected: &[u8]) {
    let result = eng.get(key).expect("get");
    assert_eq!(
        result,
        Some(Bytes::from_static(expected)),
        "Key mismatch for {:?}",
        std::str::from_utf8(key).unwrap_or("<invalid utf8>")
    );
}

/// Assert that a key is absent
pub fn assert_key_absent(eng: &MidgeEngine, key: &[u8]) {
    let result = eng.get(key).expect("get");
    assert_eq!(result, None, "Key {:?} should be absent", key);
}

/// Assert that a get returns Some value
pub fn assert_get_exists(eng: &MidgeEngine, key: &[u8]) {
    let result = eng.get(key).expect("get");
    assert!(result.is_some(), "Key {:?} should exist", key);
}

// Re-export MidgeResult from testkit if needed
pub use cntryl_midge::MidgeResult;
