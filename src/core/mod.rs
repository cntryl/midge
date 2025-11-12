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
//! - **metrics** - Performance metrics collection

pub mod backup;
pub mod compaction;
pub mod data_structures;
pub mod engine;
pub mod locking;
pub mod manifest;
pub mod memtable;
pub mod metrics;
pub mod persistence;
pub mod transaction;

// Re-export commonly used types for convenience
pub use compaction::CompactionCoordinator;
pub use data_structures::{MergingIterator, SkipList};
pub use engine::*;
pub use locking::{CloudLeaseLock, DbLock, LocalFileLock, LockMeta};
pub use manifest::*;
pub use persistence::{FlushCoordinator, FlushJob, FlushWorkerConfig};
pub use transaction::{Key, TransactionManager};

// Re-export configuration types from config module
pub use crate::config::{CloudStorageBuilder, StorageMode};

// Backward compatibility: re-export from new locations
pub use persistence::flush as flush_module;
pub use persistence::flush_coordinator as flush_coordinator_module;
pub use persistence::wal_replay as wal_replay_module;

pub use data_structures::skiplist;
pub use data_structures::merge_iterator;
