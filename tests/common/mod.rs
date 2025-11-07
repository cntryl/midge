//! Common test utilities shared across integration tests
//!
//! This module provides reusable test helpers to reduce code duplication
//! and maintain consistent test patterns across the test suite.
//!
//! # Usage
//!
//! In your test file, add:
//! ```rust
//! mod common;
//! use common::*;
//! ```
//!
//! # Examples
//!
//! ## Basic test with engine restart:
//! ```rust,no_run
//! # use midge::*;
//! # use bytes::Bytes;
//! # fn test_persistence() {
//!     let dir = test_temp_dir();
//!     let opts = durability_opts(dir.path().to_path_buf());
//!     
//!     with_engine_restart(
//!         opts,
//!         |eng| eng.put(Bytes::from("key"), Bytes::from("value")).unwrap(),
//!         |eng| assert_get_equals(eng, b"key", b"value"),
//!     );
//! }
//! ```

pub mod cloud;

use bytes::Bytes;
use midge::{MidgeEngine, MidgeOptions, StorageMode};
use std::path::PathBuf;
use tempfile::TempDir;

/// Creates a temporary directory for test isolation.
///
/// The directory is automatically cleaned up when dropped.
///
/// # Examples
///
/// ```rust
/// let dir = test_temp_dir();
/// let opts = default_opts(dir.path().to_path_buf());
/// ```
/// Create a temporary directory for testing that auto-cleans on drop
#[allow(dead_code)]
pub fn test_temp_dir() -> TempDir {
    tempfile::tempdir().expect("Failed to create test temp directory")
}

/// Create MidgeOptions for durability testing with fsync enabled
///
/// Configured with:
/// - LocalDisk storage mode
/// - WAL sync enabled
/// - Default settings for other options
///
/// # Examples
///
/// ```rust
/// let dir = test_temp_dir();
/// let opts = durability_opts(dir.path().to_path_buf());
/// let eng = MidgeEngine::open(opts).unwrap();
/// ```
#[allow(dead_code)]
pub fn durability_opts(db_path: PathBuf) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        wal_sync: true,
        wal_recovery_mode: midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    }
}

/// Create MidgeOptions for flush testing with small memtable
///
/// Configured with:
/// - LocalDisk storage mode
/// - WAL sync enabled
/// - Custom memtable size
///
/// # Examples
///
/// ```rust
/// let dir = test_temp_dir();
/// let opts = flush_test_opts(dir.path().to_path_buf(), 1024);
/// ```
#[allow(dead_code)]
pub fn flush_test_opts(db_path: PathBuf, memtable_size: usize) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        memtable_size,
        wal_sync: true,
        wal_recovery_mode: midge::WalRecoveryMode::TolerateCorruptedTail,
        ..Default::default()
    }
}

/// Opens an engine, executes a closure, ensures clean shutdown.
///
/// The engine is explicitly dropped after the closure completes.
///
/// # Examples
///
/// ```rust
/// let dir = test_temp_dir();
/// let opts = default_opts(dir.path().to_path_buf());
///
/// with_engine(opts, |eng| {
///     eng.put(Bytes::from("key"), Bytes::from("value")).unwrap();
///     assert_get_equals(eng, b"key", b"value");
/// });
/// ```
#[allow(dead_code)]
pub fn with_engine<F>(opts: MidgeOptions, f: F)
where
    F: FnOnce(&MidgeEngine),
{
    let eng = MidgeEngine::open(opts).expect("Failed to open engine");
    f(&eng);
    drop(eng); // Explicit drop for clarity
}

/// Opens engine, runs closure, drops it, reopens with same options, runs second closure.
///
/// This is useful for testing persistence and recovery behavior.
///
/// # Examples
///
/// ```rust
/// let dir = test_temp_dir();
/// let opts = durability_opts(dir.path().to_path_buf());
///
/// with_engine_restart(
///     opts,
///     |eng| {
///         eng.put(Bytes::from("key"), Bytes::from("value")).unwrap();
///     },
///     |eng| {
///         assert_get_equals(eng, b"key", b"value");
///     }
/// );
/// ```
#[allow(dead_code)]
pub fn with_engine_restart<F, G>(opts: MidgeOptions, before_restart: F, after_restart: G)
where
    F: FnOnce(&MidgeEngine),
    G: FnOnce(&MidgeEngine),
{
    {
        let eng = MidgeEngine::open(opts.clone()).expect("Failed to open engine");
        before_restart(&eng);
    } // Engine drops here

    let eng = MidgeEngine::open(opts).expect("Failed to reopen engine");
    after_restart(&eng);
}

/// Asserts that a get operation returns the expected value.
///
/// # Panics
///
/// Panics if:
/// - The get operation fails
/// - The returned value doesn't match expected
///
/// # Examples
///
/// ```rust
/// assert_get_equals(&eng, b"key", b"value");
/// ```
#[allow(dead_code)]
pub fn assert_get_equals(eng: &MidgeEngine, key: &[u8], expected: &[u8]) {
    let result = eng.get(key).expect("Get operation failed");
    let expected_bytes = Bytes::copy_from_slice(expected);
    assert_eq!(
        result,
        Some(expected_bytes),
        "Expected value mismatch for key: {:?}",
        String::from_utf8_lossy(key)
    );
}

/// Asserts that a key is absent from the engine.
///
/// # Panics
///
/// Panics if:
/// - The get operation fails
/// - The key exists when it shouldn't
///
/// # Examples
///
/// ```rust
/// assert_key_absent(&eng, b"deleted_key");
/// ```
#[allow(dead_code)]
pub fn assert_key_absent(eng: &MidgeEngine, key: &[u8]) {
    let result = eng.get(key).expect("Get operation failed");
    assert!(
        result.is_none(),
        "Expected key to be absent: {:?}",
        String::from_utf8_lossy(key)
    );
}

/// Asserts that a key exists in the database (value doesn't matter).
///
/// # Panics
///
/// Panics if the key doesn't exist or get operation fails.
#[allow(dead_code)]
pub fn assert_get_exists(eng: &MidgeEngine, key: &[u8]) {
    let result = eng.get(key).expect("Get operation failed");
    assert!(
        result.is_some(),
        "Expected key to exist: {:?}",
        String::from_utf8_lossy(key)
    );
}

/// Asserts that a key does not exist in the database.
///
/// # Panics
///
/// Panics if the key exists or get operation fails.
#[allow(dead_code)]
pub fn assert_get_not_exists(eng: &MidgeEngine, key: &[u8]) {
    let result = eng.get(key).expect("Get operation failed");
    assert!(
        result.is_none(),
        "Expected key to not exist: {:?}",
        String::from_utf8_lossy(key)
    );
}

/// Asserts that the manifest file exists at the given database path.
///
/// # Panics
///
/// Panics if the manifest.json file doesn't exist.
///
/// # Examples
///
/// ```rust
/// assert_manifest_exists(dir.path());
/// ```
#[allow(dead_code)]
pub fn assert_manifest_exists(db_path: &std::path::Path) {
    let manifest_path = db_path.join("manifest.json");
    assert!(
        manifest_path.exists(),
        "Manifest file should exist at {:?}",
        manifest_path
    );
}

/// Asserts that the WAL directory exists at the given database path.
///
/// # Panics
///
/// Panics if the wal/ directory doesn't exist.
///
/// # Examples
///
/// ```rust
/// assert_wal_exists(dir.path());
/// ```
#[allow(dead_code)]
pub fn assert_wal_exists(db_path: &std::path::Path) {
    let wal_path = db_path.join("wal");
    assert!(
        wal_path.exists() && wal_path.is_dir(),
        "WAL directory should exist at {:?}",
        wal_path
    );
}
