//! Midge - High-performance embedded LSM-tree database
//!
//! # Architecture
//!
//! Internal modules (implementation details):
//!   - `common`      - foundational types with zero dependencies
//!   - `io`          - base filesystem abstraction
//!   - `engine`      - main KV store and public API surface
//!   - `runtime`     - background actors (compaction, flush, metrics)
//!   - `metadata`    - manifest + version mgmt
//!   - `wal`         - write-ahead log
//!   - `sst`         - sorted-string table
//!   - `storage`     - storage orchestration layer
//!   - `compaction`  - compaction planning + execution
//!   - `iterators`   - iterator implementations
//!   - `metrics`     - performance instrumentation
//!   - `testkit`     - testing utilities
//!
//! # Public API Surface
//!
//! Only types re-exported at the bottom of this file are intended to be
//! stable for external consumption. Internal modules are hidden by default.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::unwrap_used))]

// Foundation - no dependencies
pub mod common;

// Internal modules - implementation details (pub for internal use)
pub(crate) mod compaction;
pub(crate) mod io;
pub(crate) mod iterators;
pub(crate) mod metadata;
pub(crate) mod metrics;
pub(crate) mod runtime;
pub(crate) mod sst;
pub(crate) mod storage;
pub(crate) mod telemetry;
pub(crate) mod wal;

// Main engine (public)
pub mod engine;

// Testing utilities (public for integration tests)
pub mod testkit;

// ---------------------------------------------------------------------------
// Public Export Surface
// ---------------------------------------------------------------------------

// Core error types
pub use common::{AckPolicy, MidgeError, MidgeResult};

// Main engine
pub use engine::{open_engine, ColumnFamilyHandle, ColumnFamilyId, MidgeEngine};

// Transaction API
pub use engine::api::{
    // Errors
    ApiError,
    ApiResult,
    Direction,

    Durability,

    // Write options
    DurabilityPolicy,
    Goal,
    // Transaction types
    IsolationLevel,
    // Core data types
    Key,
    KvPair,

    MemoryBudget,
    // Configuration
    OpenOptions,
    // Query/Scan
    Query,
    Transaction,
    TransactionMode,
    TransactionState,

    Value,
    WorkloadProfile,
    WriteOptions,
};

// Merge operators (stable API)
pub use engine::api::MergeOperator;

// Legacy/Internal APIs (hidden from documentation)
#[doc(hidden)]
pub use engine::api::{
    CasResult,    // Internal
    ColumnFamily, // Internal
    InsertResult, // Internal
    Snapshot,     // Internal: not part of public API
    WriteBatch,   // Internal: not part of public API
    WriteIntent,  // Internal
};

// Observability
pub use metrics::EngineMetrics;

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
/// use cntryl_midge::prelude::*;
///
/// // Open engine
/// let engine = MidgeEngine::open("./db")?;
/// let cf = engine.default_column_family();
///
/// // All operations require explicit transactions
/// let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
/// tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
///
/// // Commit with explicit durability
/// let opts = WriteOptions::default().with_sync(true);
/// engine.commit(tx, opts)?;
/// # Ok::<(), cntryl_midge::MidgeError>(())
/// ```
pub mod prelude {
    pub use crate::{
        // API
        ApiError,
        ApiResult,
        // Column families
        ColumnFamilyHandle,
        ColumnFamilyId,

        Direction,

        Durability,
        DurabilityPolicy,

        Goal,
        IsolationLevel,

        // Data types
        Key,
        KvPair,

        MemoryBudget,

        // Core types
        MidgeEngine,
        MidgeError,
        MidgeResult,

        // Configuration
        OpenOptions,
        // Query
        Query,
        // Transactions
        Transaction,
        TransactionMode,
        Value,
        WorkloadProfile,
        // Write options
        WriteOptions,
    };
}
