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
pub(crate) mod io;
pub(crate) mod telemetry;
pub(crate) mod storage;
pub(crate) mod iterators;
pub(crate) mod wal;
pub(crate) mod sst;
pub(crate) mod metadata;
pub(crate) mod runtime;
pub(crate) mod compaction;
pub(crate) mod metrics;

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
    // Transaction types
    IsolationLevel,
    Transaction,
    TransactionMode,
    TransactionState,
    
    // Write options
    DurabilityPolicy,
    WriteOptions,
    
    // Core data types
    Key,
    Value,
    KvPair,
    
    // Configuration
    OpenOptions,
    Goal,
    MemoryBudget,
    WorkloadProfile,
    Durability,
    
    // Query/Scan
    Query,
    Direction,
    
    // Errors
    ApiError,
    ApiResult,
};

// Merge operators (stable API)
pub use engine::api::MergeOperator;

// Legacy/Internal APIs (hidden from documentation)
#[doc(hidden)]
pub use engine::api::{
    WriteBatch,   // Internal: not part of public API
    Snapshot,     // Internal: not part of public API
    CasResult,    // Internal
    InsertResult, // Internal
    ColumnFamily, // Internal
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
/// let engine = MidgeEngine::open(OpenOptions::new().path("./db").build())?;
/// let cf = engine.default_column_family();
///
/// // All operations require explicit transactions
/// let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
/// tx.put(cf.id(), b"key".to_vec(), b"value".to_vec(), None)?;
/// 
/// // Commit with explicit durability
/// let opts = WriteOptions::default().with_sync(true);
/// engine.commit(tx, opts)?;
/// # Ok::<(), cntryl_midge::MidgeError>(())
/// ```
pub mod prelude {
    pub use crate::{
        // Core types
        MidgeEngine,
        MidgeError,
        MidgeResult,
        
        // Column families
        ColumnFamilyHandle,
        ColumnFamilyId,
        
        // Transactions
        Transaction,
        TransactionMode,
        IsolationLevel,
        
        // Write options
        WriteOptions,
        DurabilityPolicy,
        
        // Data types
        Key,
        Value,
        KvPair,
        
        // Configuration
        OpenOptions,
        Goal,
        WorkloadProfile,
        Durability,
        MemoryBudget,
        
        // Query
        Query,
        Direction,
        
        // API
        ApiError,
        ApiResult,
    };
}
