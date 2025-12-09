//! Main KV store engine
//!
//! Public API for database operations

use crate::common::MidgeResult;
use crate::sst::KvPair;
use crate::metadata::Version;
use crate::storage::StorageBackend;
use std::sync::{Arc, Mutex};

pub mod open;
pub mod context;
pub mod api;

pub use open::open_engine;
pub use context::Context;
pub use api::*;

/// The main Midge KV store
pub struct MidgeEngine {
    memtable: Arc<Mutex<Vec<KvPair>>>,
    manifest_version: Version,
    storage: Arc<dyn StorageBackend>,
}

impl MidgeEngine {
    /// Create a new engine with given storage backend
    pub fn with_storage(storage: Arc<dyn StorageBackend>) -> MidgeResult<Self> {
        Ok(Self {
            memtable: Arc::new(Mutex::new(Vec::new())),
            manifest_version: Version(1),
            storage,
        })
    }

    /// Put a key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> MidgeResult<()> {
        let mut memtable = self.memtable.lock()
            .map_err(|_| crate::common::MidgeError::Internal("Failed to lock memtable".to_string()))?;
        memtable.push(KvPair {
            key,
            value: Some(value),
            sequence: 0,
        });
        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>> {
        let memtable = self.memtable.lock()
            .map_err(|_| crate::common::MidgeError::Internal("Failed to lock memtable".to_string()))?;
        for pair in memtable.iter() {
            if pair.key.as_slice() == key {
                return Ok(pair.value.clone());
            }
        }
        Ok(None)
    }

    /// Delete a key
    pub fn delete(&self, key: Vec<u8>) -> MidgeResult<()> {
        let mut memtable = self.memtable.lock()
            .map_err(|_| crate::common::MidgeError::Internal("Failed to lock memtable".to_string()))?;
        memtable.retain(|pair| pair.key != key);
        Ok(())
    }

    /// Range scan
    pub fn range(&self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<KvPair>> {
        let memtable = self.memtable.lock()
            .map_err(|_| crate::common::MidgeError::Internal("Failed to lock memtable".to_string()))?;
        let pairs: Vec<KvPair> = memtable
            .iter()
            .filter(|p| p.key.as_slice() >= start && p.key.as_slice() < end)
            .cloned()
            .collect();
        Ok(pairs)
    }

    /// Get current memtable size in bytes
    pub fn memtable_size(&self) -> usize {
        self.memtable.lock()
            .map(|memtable| memtable.len() * 100)
            .unwrap_or(0)
    }
}

