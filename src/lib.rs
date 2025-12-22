//! Midge - High-performance embedded LSM-tree database
//!
//! Clean architectural layers:
//!   - `common`      - foundational types with zero dependencies
//!   - `io`          - base filesystem abstraction (Fs, File, RealFs, MockFs, ChaosFs)
//!   - `engine`      - main KV store and public API surface
//!   - `runtime`     - background actors (compaction, flush, metrics)
//!   - `metadata`    - manifest + version mgmt
//!   - `wal`         - write-ahead log (uses io:: abstraction)
//!   - `sst`         - sorted-string table (uses io:: abstraction)
//!   - `storage`     - storage orchestration layer (fs, cloud, hybrid)
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

// Base I/O abstraction - domain-agnostic filesystem
pub mod io;

// Telemetry (observability)
pub mod telemetry;

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

// Stress harnesses and long-running workloads live under `stress/` (binary crate),
// not in the library public API. See `stress/` for the harness and workloads.


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
    // Merge operators
    MergeOperator,
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
pub use metrics::{EngineMetrics, PerformanceMetrics};

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
/// let engine = MidgeEngine::open(PathBuf::from("./db"))?;
/// let mut batch = WriteBatch::new();
/// batch.put(b"key".to_vec().into(), b"value".to_vec().into());
/// engine.write_batch(&batch)?;
/// # Ok::<(), cntryl_midge::MidgeError>(())
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

        // Merge operators
        MergeOperator,
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
