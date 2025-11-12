//! Core engine components.
//!
//! This module contains the core LSM-tree storage engine implementation,
//! including WAL, memtables, SSTables, compaction, and related subsystems.

pub mod backup;

pub mod compaction;
pub mod engine;
pub mod flush;
pub mod flush_coordinator;
pub mod locking;
pub mod manifest;

pub mod memtable;
pub mod merge_iterator;
pub mod metrics;
pub mod skiplist;

pub mod storage_mode;
pub mod transaction;
pub mod wal_replay;

// Re-export commonly used types for convenience
pub use compaction::CompactionCoordinator;
pub use engine::*;
pub use flush_coordinator::FlushCoordinator;
pub use locking::{CloudLeaseLock, DbLock, LocalFileLock, LockMeta};
pub use manifest::*;
pub use storage_mode::*;
pub use transaction::{Key, TransactionManager};
