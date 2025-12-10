//! Testing utilities and mocks
//!
//! Mock implementations for testing and configuration for integration tests

use crate::storage::{StorageBackend, StorageCallback, StorageEvent, StorageOutcome};
use crate::engine::ColumnFamilyHandle;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Mock storage backend for testing
pub struct MockStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for MockStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MockStorage {
    fn submit_read(&self, path: String, callback: StorageCallback) {
        let data = self.data.lock().expect("storage mutex poisoned");
        let result = data
            .get(&path)
            .cloned()
            .ok_or(crate::common::MidgeError::NotFound);

        let event = StorageEvent::ReadComplete {
            path,
            result: StorageOutcome::from_result(result),
        };
        let _ = callback.send(event);
    }

    fn submit_write(&self, path: String, data: Vec<u8>, callback: StorageCallback) {
        let mut storage = self.data.lock().expect("storage mutex poisoned");
        storage.insert(path.clone(), data);

        let event = StorageEvent::WriteComplete {
            path,
            result: StorageOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_delete(&self, path: String, callback: StorageCallback) {
        let mut storage = self.data.lock().expect("storage mutex poisoned");
        storage.remove(&path);

        let event = StorageEvent::DeleteComplete {
            path,
            result: StorageOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    fn submit_list(&self, prefix: String, callback: StorageCallback) {
        let data = self.data.lock().expect("storage mutex poisoned");
        let results: Vec<_> = data
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        let event = StorageEvent::ListComplete {
            prefix,
            result: StorageOutcome::Ok(results),
        };
        let _ = callback.send(event);
    }
}

// ===== Configuration Types for Tests =====

/// Storage mode configuration for the engine
#[derive(Clone)]
pub enum StorageMode {
    /// In-memory storage (no persistence)
    Memory,
    /// Local filesystem storage
    LocalDisk { db_path: std::path::PathBuf },
    /// Cloud-backed storage with local cache
    CloudBacked {
        local_cache_path: std::path::PathBuf,
    },
}

/// Configuration options for opening a MidgeEngine
#[derive(Clone)]
pub struct MidgeOptions {
    /// Storage mode
    pub storage_mode: StorageMode,
    /// WAL sync enabled
    pub wal_sync: bool,
    /// Maximum memtable size before flush
    pub memtable_size: usize,
    /// Compression enabled
    pub compression: bool,
}

impl Default for MidgeOptions {
    fn default() -> Self {
        Self {
            storage_mode: StorageMode::Memory,
            wal_sync: false,
            memtable_size: 64 * 1024 * 1024, // 64 MB
            compression: false,
        }
    }
}

// ===== Test Helper Functions =====

/// All available storage modes for integration tests
pub fn all_storage_modes() -> Vec<&'static str> {
    vec!["Memory", "LocalDisk"]
}

/// Create storage mode configuration from mode name
/// Returns (mode_name, StorageMode, unused_placeholder)
pub fn create_storage_mode(mode: &str) -> (String, StorageMode, ()) {
    match mode {
        "Memory" => (
            "Memory".to_string(),
            StorageMode::Memory,
            (),
        ),
        "LocalDisk" => {
            let storage_mode = StorageMode::LocalDisk {
                db_path: PathBuf::from("target/tmp/midge_test"),
            };
            ("LocalDisk".to_string(), storage_mode, ())
        }
        _ => panic!("Unknown storage mode: {}", mode),
    }
}

/// Open engine from MidgeOptions (test helper to adapt old API to new)
pub fn open_engine(opts: MidgeOptions) -> crate::MidgeResult<crate::MidgeEngine> {
    crate::MidgeEngine::open_with_options(opts)
}

/// Create an engine in memory with default options
pub fn new_engine() -> crate::MidgeResult<crate::MidgeEngine> {
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    crate::MidgeEngine::open_with_options(opts)
}

/// Disk storage modes for testing
pub fn disk_storage_modes() -> Vec<&'static str> {
    vec!["LocalDisk"]
}

// ============================================================================
// OLD-STYLE API ADAPTER TRAIT
// ============================================================================
// This trait allows old tests to use the old calling convention:
//   engine.put(&cf, key, val)        => engine.put_cf(&cf, key, val)
//   engine.get(&cf, key)             => engine.get_cf(&cf, key) but returns Option<Bytes>
//   engine.delete(&cf, key)          => engine.delete_cf(&cf, key)
//   MidgeEngine::open(opts)          => MidgeEngine::open_with_options(opts)
// This is a temporary bridge while we migrate the test suite.

/// Extension trait for old-style test API compatibility
pub trait MidgeEngineTestExt {
    /// Old-style API: open with MidgeOptions
    fn open(opts: MidgeOptions) -> crate::MidgeResult<crate::MidgeEngine>;
    
    /// Old-style API: put with explicit column family
    fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> crate::MidgeResult<()>;
    
    /// Old-style API: get with explicit column family, returns Option<Bytes>
    fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> crate::MidgeResult<Option<Bytes>>;
    
    /// Old-style API: delete with explicit column family
    fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> crate::MidgeResult<()>;
}

impl MidgeEngineTestExt for crate::MidgeEngine {
    fn open(opts: MidgeOptions) -> crate::MidgeResult<crate::MidgeEngine> {
        crate::MidgeEngine::open_with_options(opts)
    }
    
    fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> crate::MidgeResult<()> {
        self.put_cf(cf, key, value)
    }
    
    fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> crate::MidgeResult<Option<Bytes>> {
        let result = self.get_cf(cf, key)?;
        Ok(result.map(|v| Bytes::from(v)))
    }
    
    fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> crate::MidgeResult<()> {
        self.delete_cf(cf, key)
    }
}

