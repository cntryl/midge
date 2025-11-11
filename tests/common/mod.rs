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
//! # use cntryl_midge::*;
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
use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};
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

/// Creates a new MidgeEngine in a fresh temporary directory.
///
/// Returns both the TempDir (to keep it alive) and the opened engine.
/// Uses default MidgeOptions with LocalDisk storage mode.
///
/// # Examples
///
/// ```rust
/// let (dir, engine) = new_engine();
/// let cf = engine.default_column_family();
/// engine.put(&cf, b"key", b"value").unwrap();
/// ```
#[allow(dead_code)]
pub fn new_engine() -> (TempDir, MidgeEngine) {
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("Failed to open engine");
    (dir, engine)
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
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
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
        wal_recovery_mode: cntryl_midge::WalRecoveryMode::TolerateCorruptedTail,
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
    let cf = eng.default_column_family();
    let result = eng.get(&cf, key).expect("Get operation failed");
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
    let cf = eng.default_column_family();
    let result = eng.get(&cf, key).expect("Get operation failed");
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
    let cf = eng.default_column_family();
    let result = eng.get(&cf, key).expect("Get operation failed");
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
    let cf = eng.default_column_family();
    let result = eng.get(&cf, key).expect("Get operation failed");
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

// ============================================================================
// Compaction Test Helpers
// ============================================================================

/// Creates MidgeOptions configured for compaction testing.
///
/// Uses small memtable size and low SST threshold to trigger compaction easily
/// for testing purposes.
///
/// # Returns
///
/// MidgeOptions with:
/// - Memory storage mode
/// - 1KB memtable size (triggers frequent flushes)
/// - SST threshold of 2 (triggers compaction with just 2 files)
///
/// # Examples
///
/// ```rust
/// let opts = compaction_test_opts();
/// let engine = MidgeEngine::open(opts).unwrap();
/// ```
#[allow(dead_code)]
pub fn compaction_test_opts() -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::Memory,
        memtable_size: 1024,         // Small memtable to trigger flushes easily
        compaction_sst_threshold: 2, // Trigger compaction with just 2 SST files
        ..Default::default()
    }
}

/// Populates an engine with overlapping data across multiple L0 files.
///
/// This helper creates a multi-level dataset suitable for testing compaction
/// behavior. It writes three batches of overlapping keys, each flushed to
/// create separate L0 SST files.
///
/// # Data Pattern
///
/// - Batch 1: key000..key049 (50 keys)
/// - Batch 2: key025..key074 (50 keys, overlapping)
/// - Batch 3: key050..key099 (50 keys)
///
/// Each batch is flushed to create a separate L0 file, resulting in
/// overlapping key ranges that require compaction to merge.
///
/// # Arguments
///
/// * `engine` - The MidgeEngine to populate
/// * `cf` - The ColumnFamilyHandle to write to
///
/// # Examples
///
/// ```rust
/// let opts = compaction_test_opts();
/// let engine = MidgeEngine::open(opts).unwrap();
/// let cf = engine.default_column_family();
/// populate_multi_level_data(&engine, &cf);
/// ```
#[allow(dead_code)]
pub fn populate_multi_level_data(
    engine: &MidgeEngine,
    cf: &cntryl_midge::ColumnFamilyHandle,
) {
    // Write batch 1 and flush to L0
    for i in 0..50 {
        let key = format!("key{:03}", i);
        let value = format!("value1_{}", i);
        engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Write batch 2 and flush to L0 (overlapping keys)
    for i in 25..75 {
        let key = format!("key{:03}", i);
        let value = format!("value2_{}", i);
        engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();

    // Write batch 3 and flush to L0
    for i in 50..100 {
        let key = format!("key{:03}", i);
        let value = format!("value3_{}", i);
        engine.put(cf, key.as_bytes(), value.as_bytes()).unwrap();
    }
    engine.flush().unwrap();
}
