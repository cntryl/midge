//! Core engine components.
//!
//! This module contains the core LSM-tree storage engine implementation,
//! including WAL, memtables, SSTables, compaction, and related subsystems.
//!
//! ## Module Organization
//!
//! - **engine** - Main MidgeEngine and operations
//! - **compaction** - Background compaction subsystem
//! - **transaction** - MVCC transaction support
//! - **manifest** - Manifest management and versioning
//! - **memtable** - In-memory write buffer
//! - **persistence** - WAL and flushing layer
//!   - flush - Async memtable flushing
//!   - flush_coordinator - Background worker management
//!   - wal_replay - WAL recovery during startup
//! - **data_structures** - Utility structures
//!   - skiplist - Lock-free concurrent skiplist
//!   - merge_iterator - Streaming merge iterator
//! - **storage** - Storage configuration
//!   - storage_mode - Memory/disk/cloud mode selection
//! - **locking** - Distributed locking
//! - **backup** - Backup and restore
//!
//! ## Note on Metrics
//!
//! Metrics have been moved to the top-level `metrics` module to avoid circular dependencies.
//! Use `crate::metrics::*` instead of `crate::core::metrics::*`.

pub mod backup;
pub mod compaction;
pub mod data_structures;
pub mod engine;
pub mod locking;
pub mod manifest;
pub mod memtable;
pub mod persistence;
pub mod transaction;

/// Common entry metadata used across memtable/flush paths.
///
/// Represents a single database entry with all its associated metadata.
/// This is an internal type used by the core persistence and memtable subsystems.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryMeta {
    /// The entry's key
    pub key: Vec<u8>,
    /// The entry's value (None for tombstones)
    pub value: Option<Vec<u8>>,
    /// Sequence number for MVCC
    pub sequence: u64,
    /// Whether this entry is a delete tombstone
    pub is_tombstone: bool,
    /// Optional TTL expiration timestamp in milliseconds since epoch
    pub expiration_millis: Option<u64>,
}

impl EntryMeta {
    /// Create a new entry metadata
    pub fn new(
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        is_tombstone: bool,
        expiration_millis: Option<u64>,
    ) -> Self {
        Self {
            key,
            value,
            sequence,
            is_tombstone,
            expiration_millis,
        }
    }

    /// Convert from legacy tuple format
    pub fn from_tuple(tuple: (Vec<u8>, Option<Vec<u8>>, u64, bool, Option<u64>)) -> Self {
        Self {
            key: tuple.0,
            value: tuple.1,
            sequence: tuple.2,
            is_tombstone: tuple.3,
            expiration_millis: tuple.4,
        }
    }

    /// Convert to legacy tuple format for backward compatibility
    pub fn to_tuple(self) -> (Vec<u8>, Option<Vec<u8>>, u64, bool, Option<u64>) {
        (
            self.key,
            self.value,
            self.sequence,
            self.is_tombstone,
            self.expiration_millis,
        )
    }
}

// Re-export commonly used types for convenience
pub use compaction::CompactionCoordinator;
pub use data_structures::{MergingIterator, SkipList};
pub use engine::*;
pub use locking::{create_cloud_lock, create_local_lock, DbLock, LockMeta};
pub use manifest::*;
pub use persistence::{FlushCoordinator, FlushJob, FlushWorkerConfig};
pub use transaction::{Key, TransactionManager};

// Re-export configuration types from config module
pub use crate::config::{CloudStorageBuilder, StorageMode};

// Re-export data structure internals for direct access
pub use data_structures::merge_iterator;
pub use data_structures::skiplist;
