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

// Internal modules - implementation details
// Exposed for benchmarks but hidden from public documentation
#[doc(hidden)]
pub mod compaction;
#[doc(hidden)]
pub mod io;
#[doc(hidden)]
pub mod iterators;
#[doc(hidden)]
pub mod metadata;
#[doc(hidden)]
pub mod metrics;
#[doc(hidden)]
pub mod runtime;
#[doc(hidden)]
pub mod sst;
#[doc(hidden)]
pub mod storage;
#[doc(hidden)]
pub mod telemetry;
#[doc(hidden)]
pub mod wal;

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

// Internal APIs (hidden from documentation)
#[doc(hidden)]
pub use engine::api::{
    CasResult,   // Return type for compare_and_swap
    InsertResult, // Return type for insert
    WriteIntent, // Internal transaction state
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
/// use std::path::PathBuf;
///
/// // Open engine
/// let engine = MidgeEngine::open(PathBuf::from("./db"))?;
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
