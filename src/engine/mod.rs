//! Main KV store engine
//!
//! Public API for database operations.
//!
//! The engine is a thin façade that delegates all work to the runtime.
//! It provides ergonomic APIs for:
//! - Key-value operations (put, get, delete, range)
//! - Column families
//! - Write batches
//! - Transactions
//! - Snapshots

use crate::common::MidgeResult;
use crate::runtime::{Runtime, RuntimeHandle, RuntimeMsg, RuntimeState};
use crate::sst::{Memtable, SkipListMemtable};
use std::path::PathBuf;
use std::sync::Arc;

pub mod engine;
pub mod open;
pub mod context;
pub mod api;

pub use open::open_engine;
pub use context::Context;
pub use api::*;

/// Column family identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnFamilyId(pub u32);

impl ColumnFamilyId {
    pub const DEFAULT: Self = Self(0);

    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

impl Default for ColumnFamilyId {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Column family handle for API operations
#[derive(Debug, Clone)]
pub struct ColumnFamilyHandle {
    id: ColumnFamilyId,
    name: String,
}

impl ColumnFamilyHandle {
    pub fn new(id: ColumnFamilyId, name: String) -> Self {
        Self { id, name }
    }

    pub fn id(&self) -> ColumnFamilyId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// The main Midge KV store
///
/// This is a thin façade over the runtime. All state and background work
/// is managed by the runtime actors.
pub struct MidgeEngine {
    /// Handle to submit work to the runtime
    runtime_handle: RuntimeHandle,
    /// Database path
    db_path: PathBuf,
    /// Default column family for convenience
    default_cf: ColumnFamilyHandle,
    /// Local memtable reference for fast reads
    /// (Runtime owns the authoritative copy)
    memtable: Arc<SkipListMemtable>,
    /// Current sequence number (for local tracking)
    sequence: std::sync::atomic::AtomicU64,
}

impl MidgeEngine {
    /// Open a database at the given path
    pub fn open(db_path: PathBuf) -> MidgeResult<Self> {
        // Create runtime state
        let state = RuntimeState::new(db_path.clone());

        // Start runtime
        let (runtime, _) = Runtime::new()?;
        let runtime_handle = runtime.start(state)?;

        Ok(Self {
            runtime_handle,
            db_path,
            default_cf: ColumnFamilyHandle::new(ColumnFamilyId::DEFAULT, "default".to_string()),
            memtable: Arc::new(SkipListMemtable::new()),
            sequence: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Get the default column family
    pub fn default_column_family(&self) -> &ColumnFamilyHandle {
        &self.default_cf
    }

    /// Put a key-value pair into the default column family
    pub fn put(&self, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        self.put_cf(&self.default_cf, key, value)
    }

    /// Put a key-value pair into a specific column family
    pub fn put_cf(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        let seq = self.next_sequence();

        // Write to local memtable
        self.memtable.put(key.to_vec(), value.to_vec())?;

        // Send WAL append to runtime
        self.runtime_handle.send(RuntimeMsg::WalAppend {
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: Some(value.to_vec()),
            sequence: seq,
        })?;

        Ok(())
    }

    /// Get a value from the default column family
    pub fn get(&self, key: &[u8]) -> MidgeResult<Option<Vec<u8>>> {
        self.get_cf(&self.default_cf, key)
    }

    /// Get a value from a specific column family
    pub fn get_cf(&self, _cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<Vec<u8>>> {
        // First check local memtable
        if let Some(value) = self.memtable.get(key)? {
            return Ok(Some(value));
        }

        // TODO: Check immutable memtables and SST files via runtime
        Ok(None)
    }

    /// Delete a key from the default column family
    pub fn delete(&self, key: &[u8]) -> MidgeResult<()> {
        self.delete_cf(&self.default_cf, key)
    }

    /// Delete a key from a specific column family
    pub fn delete_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        let seq = self.next_sequence();

        // Write tombstone to local memtable
        self.memtable.delete(key.to_vec())?;

        // Send WAL append to runtime (value=None indicates delete)
        self.runtime_handle.send(RuntimeMsg::WalAppend {
            cf_id: cf.id.0,
            key: key.to_vec(),
            value: None,
            sequence: seq,
        })?;

        Ok(())
    }

    /// Range scan in the default column family
    pub fn range(&self, start: &[u8], end: &[u8]) -> MidgeResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.range_cf(&self.default_cf, start, end)
    }

    /// Range scan in a specific column family
    pub fn range_cf(
        &self,
        _cf: &ColumnFamilyHandle,
        _start: &[u8],
        _end: &[u8],
    ) -> MidgeResult<Vec<(Vec<u8>, Vec<u8>)>> {
        // TODO: Implement range scan via memtable + SST merge iterator
        Ok(Vec::new())
    }

    /// Sync all pending writes to disk
    pub fn sync(&self) -> MidgeResult<()> {
        self.runtime_handle.send(RuntimeMsg::WalSync)
    }

    /// Force a flush of the default column family
    pub fn flush(&self) -> MidgeResult<()> {
        self.flush_cf(&self.default_cf)
    }

    /// Force a flush of a specific column family
    pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> MidgeResult<()> {
        self.runtime_handle.send(RuntimeMsg::FlushMemtable { cf_id: cf.id.0 })
    }

    /// Get current memtable size in bytes
    pub fn memtable_size(&self) -> usize {
        self.memtable.size_bytes()
    }

    /// Shutdown the engine gracefully
    pub fn shutdown(self) -> MidgeResult<()> {
        self.runtime_handle.shutdown()
    }

    // === Internal helpers ===

    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

