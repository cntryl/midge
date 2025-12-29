//! Compatibility helpers for tests.
//!
//! This module exists to support the older integration-test calling style while
//! the suite continues to migrate toward the newer API surface.

use crate::engine::ColumnFamilyHandle;
use bytes::Bytes;

use super::{MidgeOptions, StorageMode};

/// Create storage mode configuration from mode name (uppercase: backward-compatible).
/// Returns (mode_name, StorageMode, unused_placeholder).
pub fn create_storage_mode(mode: &str) -> (String, StorageMode, ()) {
    match mode {
        "Memory" => ("Memory".to_string(), StorageMode::Memory, ()),
        "LocalDisk" => {
            let storage_mode = StorageMode::LocalDisk {
                db_path: std::path::PathBuf::from("target/tmp/midge_test"),
            };
            ("LocalDisk".to_string(), storage_mode, ())
        }
        _ => panic!("Unknown storage mode: {}", mode),
    }
}

/// Open engine from `MidgeOptions`.
pub fn open_engine(opts: MidgeOptions) -> crate::MidgeResult<crate::MidgeEngine> {
    crate::MidgeEngine::open_with_options(opts)
}

/// Helper: unwrap engine open with consistent error context.
///
/// Panics on error with a message that includes the storage mode name.
pub fn open_with_mode(opts: MidgeOptions, mode: &str) -> crate::MidgeEngine {
    open_engine(opts).unwrap_or_else(|e| panic!("open_engine failed in mode {}: {}", mode, e))
}

/// Create an engine in memory with default options.
pub fn new_engine() -> crate::MidgeResult<crate::MidgeEngine> {
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    crate::MidgeEngine::open_with_options(opts)
}

// ============================================================================
// OLD-STYLE API ADAPTER TRAIT
// ============================================================================

/// Extension trait for old-style test API compatibility.
pub trait MidgeEngineTestExt {
    /// Old-style API: open with `MidgeOptions`.
    fn open(opts: MidgeOptions) -> crate::MidgeResult<crate::MidgeEngine>;

    /// Old-style API: put with explicit column family.
    fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> crate::MidgeResult<()>;

    /// Old-style API: get with explicit column family, returns Option<Bytes>.
    fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> crate::MidgeResult<Option<Bytes>>;

    /// Old-style API: delete with explicit column family.
    fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> crate::MidgeResult<()>;
}

impl MidgeEngineTestExt for crate::MidgeEngine {
    fn open(opts: MidgeOptions) -> crate::MidgeResult<crate::MidgeEngine> {
        crate::MidgeEngine::open_with_options(opts)
    }

    fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> crate::MidgeResult<()> {
        crate::MidgeEngine::put(self, cf, key, value)
    }

    fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> crate::MidgeResult<Option<Bytes>> {
        crate::MidgeEngine::get(self, cf, key)
    }

    fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> crate::MidgeResult<()> {
        crate::MidgeEngine::delete(self, cf, key)
    }
}
