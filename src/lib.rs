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
pub use engine::{
    MidgeEngine,
    ColumnFamilyHandle,
    ColumnFamilyId,
    open_engine,
};

// High-level API types
pub use engine::api::{
    // Query + scans
    Query,
    Iterator,
    Direction,

    // Writes
    WriteBatch,
    WriteOptions,

    // Transactions
    Transaction,
    KvTransaction,
    IsolationLevel,

    // Results
    CasResult,
    InsertResult,

    // Snapshots
    Snapshot,

    // Column families
    ColumnFamily,

    // Engine configuration
    OpenOptions,
    Goal,
    Durability,
    MemoryBudget,
    WorkloadProfile,

    // KV types
    Key,
    Value,
    KvPair,

    // Errors
    ApiError,
    ApiResult,
};

// Observability
pub use metrics::PerformanceMetrics;

// Testing utilities
pub use testkit::{MidgeOptions, StorageMode, MockStorage};

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
        // Types
        Key,
        Value,
        KvPair,

        // Query + iteration
        Query,
        Iterator,
        Direction,

        // Writes
        WriteBatch,
        WriteOptions,

        // Transactions + snapshots
        Transaction,
        Snapshot,

        // Engine
        MidgeEngine,
        ColumnFamilyHandle,
        ColumnFamily,

        // Configuration
        OpenOptions,

        // Errors
        MidgeError,
        MidgeResult,
        ApiError,
        ApiResult,
    };
}
