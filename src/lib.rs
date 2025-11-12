//! Midge - A high-performance embedded key-value database
//!
//! # Architecture
//!
//! Midge is organized into layers:
//!
//! - [`api`]: Public API layer - traits and types for users
//! - [`core`]: Engine implementation - LSM-tree, compaction, transactions, manifests
//! - [`sst`]: SSTable storage format and bloom filters
//! - [`wal`]: Write-ahead logging for durability
//! - [`cloud`]: Cloud-backed storage implementations
//! - [`compaction`]: User-extensible compaction filter API

// Lint enforcement for production code quality
// Note: unwrap is allowed in test modules via #![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::unwrap_used))]

// Foundational modules with no internal dependencies
pub mod common;
pub mod metrics;

// Re-export error types at crate root for convenience
pub use common::error;
pub use common::{MidgeError, MidgeResult};
pub use metrics::PerformanceMetrics;

// Re-export test hooks for testing
pub use common::test_hooks;

// Core modules
pub mod config;
pub mod core;
pub mod fs;

// Public API layer
pub mod api;

// Feature modules (implementation details, but exposed for extensibility)
pub mod cloud;
pub mod health;
pub mod sst;
pub mod wal;

// Convenience re-exports from core (commonly needed internal types)
pub use crate::core::backup;
pub use crate::core::locking;
pub use crate::core::manifest;

// Compaction filter API for user-provided custom logic
pub mod compaction {
    //! Compaction filter API for custom compaction logic
    pub use crate::core::compaction::executor::CompactionVersion;
    pub use crate::core::compaction::filter::{CompactionFilter, FilterDecision};
}

// Re-export commonly used types at crate root for convenience
pub use crate::api::{
    BytesAppendOperator, ColumnFamilyConfig, ColumnFamilyHandle, ColumnFamilyId, DynKvStore,
    DynMergeOperator, IntegerAddOperator, KvStore, KvTransaction, MergeOperator, Mutation,
    MutationOp, Query, Snapshot, StringAppendOperator, WriteBatch, WriteOptions, DEFAULT_CF_ID,
    DEFAULT_CF_NAME,
};
pub use crate::config::{CompactionStyle, CompressionType, MidgeOptions, WalRecoveryMode, StorageMode, CloudStorageBuilder};
// Export EngineTransaction as the public Transaction type
pub use crate::core::transaction::EngineTransaction as Transaction;
// Re-export the engine API from the new `core` location
pub use crate::core::engine::{CasResult, InsertResult, MidgeEngine};
pub use crate::sst::bloom::Filter;
