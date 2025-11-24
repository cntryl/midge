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
use cntryl_midge::{
    cloud::MockCloudBackend, config::cloud::StorageContext, test_hooks::TestHooks, MidgeEngine,
    MidgeOptions, StorageMode,
};
use std::path::PathBuf;
use std::sync::Arc;
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

/// Asserts that a key in a specific column family equals an expected value.
///
/// # Panics
///
/// Panics if:
/// - The get operation fails
/// - The value doesn't match expected
/// - The key doesn't exist
#[allow(dead_code)]
pub fn assert_get_equals_cf(
    eng: &MidgeEngine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    key: &[u8],
    expected: &[u8],
) {
    let result = eng.get(cf, key).expect("Get operation failed");
    let expected_bytes = Bytes::copy_from_slice(expected);
    assert_eq!(
        result,
        Some(expected_bytes),
        "Expected value mismatch for key: {:?} in CF: {}",
        String::from_utf8_lossy(key),
        cf.name()
    );
}

/// Asserts that a key is absent from a specific column family.
///
/// # Panics
///
/// Panics if:
/// - The get operation fails
/// - The key exists when it shouldn't
#[allow(dead_code)]
pub fn assert_key_absent_cf(eng: &MidgeEngine, cf: &cntryl_midge::ColumnFamilyHandle, key: &[u8]) {
    let result = eng.get(cf, key).expect("Get operation failed");
    assert!(
        result.is_none(),
        "Expected key to be absent: {:?} in CF: {}",
        String::from_utf8_lossy(key),
        cf.name()
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
pub fn compaction_test_opts(storage_mode: StorageMode) -> MidgeOptions {
    MidgeOptions {
        storage_mode,
        memtable_size: 1024,         // Small memtable to trigger flushes easily
        enable_compaction: true,    // Enable background compaction for compaction workloads in tests
        compaction_sst_threshold: 2, // Not used when background compaction is disabled
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
pub fn populate_multi_level_data(engine: &MidgeEngine, cf: &cntryl_midge::ColumnFamilyHandle) {
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

// ============================================================================
// Engine Creation Helpers
// ============================================================================

/// Creates a new MidgeEngine with custom options.
///
/// Returns both the TempDir (to keep it alive) and the opened engine.
///
/// # Arguments
///
/// * `memtable_size` - Size of memtable in bytes
/// * `enable_compaction` - Whether to enable background compaction
///
/// # Examples
///
/// ```rust
/// let (dir, engine) = new_engine_with_opts(512, true);
/// let cf = engine.default_column_family();
/// ```
#[allow(dead_code)]
pub fn new_engine_with_opts(
    memtable_size: usize,
    enable_compaction: bool,
) -> (TempDir, MidgeEngine) {
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size,
        enable_compaction,
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("Failed to open engine");
    (dir, engine)
}

/// Creates a new `MidgeEngine` configured with custom test hooks.
///
/// Useful for deterministic coordination in concurrency tests.
#[allow(dead_code)]
pub fn new_engine_with_test_hooks(
    memtable_size: usize,
    enable_compaction: bool,
    hooks: TestHooks,
) -> (TempDir, MidgeEngine) {
    let dir = test_temp_dir();
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: dir.path().to_path_buf(),
        },
        memtable_size,
        enable_compaction,
        test_hooks: Some(hooks),
        ..Default::default()
    };
    let engine = MidgeEngine::open(opts).expect("Failed to open engine");
    (dir, engine)
}

/// Creates MidgeOptions configured for compaction testing with custom settings.
///
/// # Arguments
///
/// * `db_path` - Path to database directory
/// * `memtable_size` - Size of memtable in bytes
///
/// # Examples
///
/// ```rust
/// let dir = test_temp_dir();
/// let opts = compaction_opts(dir.path().to_path_buf(), 512);
/// ```
#[allow(dead_code)]
pub fn compaction_opts(db_path: PathBuf, memtable_size: usize) -> MidgeOptions {
    MidgeOptions {
        storage_mode: StorageMode::LocalDisk { db_path },
        memtable_size,
        enable_compaction: true,
        ..Default::default()
    }
}

/// Creates an Arc-wrapped MidgeEngine for concurrent tests.
///
/// Returns both the TempDir (to keep it alive) and the Arc-wrapped engine.
///
/// # Examples
///
/// ```rust
/// let (dir, engine) = new_shared_engine();
/// let eng_clone = engine.clone();
/// // Use in multiple threads
/// ```
#[allow(dead_code)]
pub fn new_shared_engine() -> (TempDir, Arc<MidgeEngine>) {
    let (dir, eng) = new_engine();
    (dir, Arc::new(eng))
}

// ============================================================================
// Bulk Write Helpers
// ============================================================================

/// Bulk insert keys with a common prefix and value.
///
/// Generates keys in format: `{prefix}{i:03}` where i is 0-padded to 3 digits.
///
/// # Arguments
///
/// * `eng` - The MidgeEngine to write to
/// * `cf` - The ColumnFamilyHandle to write to
/// * `prefix` - Key prefix string (e.g., "key_")
/// * `count` - Number of keys to insert
/// * `value` - Value bytes to use for all keys
///
/// # Examples
///
/// ```rust
/// let (dir, eng) = new_engine();
/// let cf = eng.default_column_family();
/// bulk_put(&eng, &cf, "key_", 100, b"value");
/// // Creates: key_000, key_001, ..., key_099
/// ```
#[allow(dead_code)]
pub fn bulk_put(
    eng: &MidgeEngine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    prefix: &str,
    count: usize,
    value: &[u8],
) {
    for i in 0..count {
        let key = format!("{}{:03}", prefix, i);
        eng.put(cf, key.as_bytes(), value).expect("bulk_put failed");
    }
}

/// Bulk insert keys with custom value generation function.
///
/// Generates keys in format: `{prefix}{i:03}` and calls `value_fn(i)` for each value.
///
/// # Arguments
///
/// * `eng` - The MidgeEngine to write to
/// * `cf` - The ColumnFamilyHandle to write to
/// * `prefix` - Key prefix string
/// * `count` - Number of keys to insert
/// * `value_fn` - Function that takes index and returns value bytes
///
/// # Examples
///
/// ```rust
/// let (dir, eng) = new_engine();
/// let cf = eng.default_column_family();
/// bulk_put_fn(&eng, &cf, "key_", 100, |i| format!("value_{}", i).into_bytes());
/// ```
#[allow(dead_code)]
pub fn bulk_put_fn<F>(
    eng: &MidgeEngine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    prefix: &str,
    count: usize,
    value_fn: F,
) where
    F: Fn(usize) -> Vec<u8>,
{
    for i in 0..count {
        let key = format!("{}{:03}", prefix, i);
        let value = value_fn(i);
        eng.put(cf, key.as_bytes(), &value)
            .expect("bulk_put_fn failed");
    }
}

// ============================================================================
// Assertion Helpers
// ============================================================================

/// Asserts that a key exists (without checking value) with custom message.
///
/// # Arguments
///
/// * `eng` - The MidgeEngine to query
/// * `cf` - The ColumnFamilyHandle to query
/// * `key` - The key to check
/// * `msg` - Custom failure message
///
/// # Panics
///
/// Panics if the key doesn't exist or get operation fails.
///
/// # Examples
///
/// ```rust
/// assert_key_present(&eng, &cf, b"key", "Key should exist after write");
/// ```
#[allow(dead_code)]
pub fn assert_key_present(
    eng: &MidgeEngine,
    cf: &cntryl_midge::ColumnFamilyHandle,
    key: &[u8],
    msg: &str,
) {
    let result = eng.get(cf, key).expect("get failed");
    assert!(result.is_some(), "{}", msg);
}

// ============================================================================
// Storage Mode Testing Helpers
// ============================================================================

/// Creates a storage mode configuration for testing.
///
/// Returns a tuple of (mode_name, storage_mode, optional_temp_dir).
/// The TempDir must be kept alive for the duration of the test to ensure
/// directories exist for LocalDisk and CloudBacked modes.
///
/// # Supported Modes
///
/// - `"Memory"` - Pure in-memory storage (no disk writes)
/// - `"LocalDisk"` - Local filesystem storage with temp directory
/// - `"CloudBacked"` - Mock cloud storage with local cache
///
/// # Note on Memory Mode
///
/// Memory mode does not write SST files to disk, so tests that require
/// flush/compaction operations should exclude Memory mode and only test
/// LocalDisk and CloudBacked modes.
///
/// # Examples
///
/// ```rust
/// // Test with all three modes
/// for mode in &["Memory", "LocalDisk", "CloudBacked"] {
///     let (name, storage_mode, _temp_dir) = create_storage_mode(mode);
///     let opts = MidgeOptions { storage_mode, ..Default::default() };
///     let engine = MidgeEngine::open(opts).unwrap();
///     // ... test code ...
/// }
/// ```
///
/// ```rust
/// // Test only modes that support SST files
/// for mode in &["LocalDisk", "CloudBacked"] {
///     let (name, storage_mode, _temp_dir) = create_storage_mode(mode);
///     // ... test compaction behavior ...
/// }
/// ```
#[allow(dead_code)]
pub fn create_storage_mode(mode: &str) -> (String, StorageMode, Option<TempDir>) {
    match mode {
        "Memory" => ("Memory".to_string(), StorageMode::Memory, None),
        "LocalDisk" => {
            let temp_dir = test_temp_dir();
            let storage_mode = StorageMode::LocalDisk {
                db_path: temp_dir.path().to_path_buf(),
            };
            ("LocalDisk".to_string(), storage_mode, Some(temp_dir))
        }
        "CloudBacked" => {
            let temp_dir = test_temp_dir();
            let storage_mode = StorageMode::CloudBacked {
                local_cache_path: temp_dir.path().to_path_buf(),
                cloud_backend: Arc::new(MockCloudBackend::new()),
                storage_context: StorageContext::default(),
                local_wal_sync: false,
                wal_batch_size: 4 * 1024 * 1024,
                sst_cache_capacity: 16,
            };
            ("CloudBacked".to_string(), storage_mode, Some(temp_dir))
        }
        _ => panic!("Unknown storage mode: {}", mode),
    }
}

/// Returns all three storage modes for comprehensive testing.
///
/// Use this for tests that should work with any storage backend,
/// including pure in-memory mode.
///
/// # Examples
///
/// ```rust
/// for mode in all_storage_modes() {
///     let (name, storage_mode, _temp_dir) = create_storage_mode(mode);
///     // ... test code ...
/// }
/// ```
#[allow(dead_code)]
pub fn all_storage_modes() -> &'static [&'static str] {
    &["Memory", "LocalDisk", "CloudBacked"]
}

/// Returns only storage modes that write SST files to disk.
///
/// Use this for tests that require flush() or compaction operations,
/// as Memory mode does not persist SST files.
///
/// # Examples
///
/// ```rust
/// for mode in disk_storage_modes() {
///     let (name, storage_mode, _temp_dir) = create_storage_mode(mode);
///     engine.flush().unwrap();  // This works for LocalDisk and CloudBacked
///     // ... test compaction ...
/// }
/// ```
#[allow(dead_code)]
pub fn disk_storage_modes() -> &'static [&'static str] {
    &["LocalDisk", "CloudBacked"]
}

/// Wait for a condition to become true within a timeout.
///
/// Polls `cond()` every `interval` until it returns true or `timeout` elapses.
/// Returns true if condition became true, false if timed out.
#[allow(dead_code)]
pub fn wait_for_condition<F>(
    timeout: std::time::Duration,
    _interval: std::time::Duration,
    cond: F,
) -> bool
where
    F: Fn() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        // Use yields to allow scheduler to run other threads instead
        // of fixed sleeps which introduce timing dependencies in tests.
        std::thread::yield_now();
    }
    cond()
}
