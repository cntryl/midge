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
    Storage,
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
    CasResult,    // Return type for compare_and_swap
    InsertResult, // Return type for insert
    WriteIntent,  // Internal transaction state
};

// Observability
pub use metrics::EngineMetrics;

// Testing utilities
pub use testkit::{MidgeOptions, MockStorage, StorageMode};

// ---------------------------------------------------------------------------
// Canonical Prelude
// ---------------------------------------------------------------------------

/// Canonical prelude - the ONE correct way to use Midge.
///
/// This module re-exports only the essential, AI-safe API surface required
/// for the canonical usage pattern. It contains no convenience methods,
/// no legacy APIs, and no alternative entry points.
///
/// **Design principle:** If it's in the prelude, it's required for almost
/// every real program. If it's optional, advanced, or dangerous, it must
/// be imported explicitly.
///
/// # Canonical Usage Pattern
///
/// ```no_run
/// use cntryl_midge::prelude::*;
/// use std::path::PathBuf;
///
/// // Open engine
/// let engine = MidgeEngine::open(PathBuf::from("./db"))?;
/// let cf = engine.default_column_family();
///
/// // Write: explicit transaction, explicit commit, explicit durability
/// let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
/// tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
/// engine.commit(tx, WriteOptions::recommended())?;
///
/// // Read: explicit transaction
/// let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
/// let value = tx.get(b"key")?;
/// # Ok::<(), MidgeError>(())
/// ```
///
/// Everything needed for this pattern is in the prelude.
/// Nothing else is.
pub mod prelude {
    /// Canonical API surface for Midge.
    ///
    /// Use `use midge::prelude::*;` to import the essential types needed
    /// for the standard transaction-based workflow.
    // Engine
    pub use crate::engine::{open_engine, ColumnFamilyHandle, ColumnFamilyId, MidgeEngine};

    // Transactions
    pub use crate::engine::api::{Transaction, TransactionMode, WriteOptions};

    // Core data types
    pub use crate::engine::api::{Key, Value};

    // Errors
    pub use crate::common::{MidgeError, MidgeResult};
}
