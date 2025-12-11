//! Midge - High-performance embedded LSM-tree database
//!
//! Clean architectural layers:
//!   - `common`      - foundational types with zero dependencies
//!   - `engine`      - main KV store and public API surface
//!   - `runtime`     - background actors (compaction, flush, metrics)
//!   - `metadata`    - manifest + version mgmt
//!   - `wal`         - write-ahead log abstraction
//!   - `sst`         - sorted-string table formats + readers/writers
//!   - `storage`     - storage backend abstraction (fs, cloud, hybrid)
//!   - `compaction`  - compaction planning + execution
//!   - `iterators`   - iterator implementations
//!   - `metrics`     - performance instrumentation
//!   - `testkit`     - testing utilities
//!
//! # Public API Surface
//!
//! Only modules re-exported at the bottom of this file are intended to be
//! stable for external consumption. Internal modules are free to evolve.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::unwrap_used))]

// Foundation - no dependencies
pub mod common;

// Storage abstraction
pub mod storage;

// Data structures
pub mod iterators;

// Logging
pub mod wal;

// SSTs and memtables
pub mod sst;

// Metadata management
pub mod metadata;

// Main engine
pub mod engine;

// Background processing
pub mod runtime;

// Data organization
pub mod compaction;

// Observability
pub mod metrics;

// Testing
pub mod testkit;

// ---------------------------------------------------------------------------
// Public Export Surface
// ---------------------------------------------------------------------------

// Common types
pub use common::{MidgeError, MidgeResult};

// Main engine API
pub use engine::{open_engine, ColumnFamilyHandle, ColumnFamilyId, MidgeEngine};

// High-level API types
pub use engine::api::{
    // Errors
    ApiError,
    ApiResult,
    // Results
    CasResult,
    // Column families
    ColumnFamily,

    Direction,

    Durability,
    Goal,
    InsertResult,

    IsolationLevel,

    Iterator,
    // KV types
    Key,
    KvPair,

    KvTransaction,
    MemoryBudget,
    // Engine configuration
    OpenOptions,
    // Query + scans
    Query,
    // Snapshots
    Snapshot,

    // Transactions
    Transaction,
    Value,
    WorkloadProfile,

    // Writes
    WriteBatch,
    WriteOptions,
};

// Observability
pub use metrics::PerformanceMetrics;

// Testing utilities
pub use testkit::{MidgeOptions, MockStorage, StorageMode};

// ---------------------------------------------------------------------------
// Ergonomic Prelude
// ---------------------------------------------------------------------------

/// Prelude for ergonomic wildcard imports.
///
/// # Example
///
/// ```no_run
/// use midge::prelude::*;
///
/// let engine = MidgeEngine::new(OpenOptions::default());
/// let mut batch = WriteBatch::new();
/// batch.put(b"key", b"value");
/// ```
pub mod prelude {
    pub use crate::{
        ApiError,
        ApiResult,
        ColumnFamily,

        ColumnFamilyHandle,
        Direction,

        Iterator,
        // Types
        Key,
        KvPair,

        // Engine
        MidgeEngine,
        // Errors
        MidgeError,
        MidgeResult,
        // Configuration
        OpenOptions,

        // Query + iteration
        Query,
        Snapshot,

        // Transactions + snapshots
        Transaction,
        Value,
        // Writes
        WriteBatch,
        WriteOptions,
    };
}
