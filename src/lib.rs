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
mod common;

// Internal modules - implementation details (NOT part of public API)
mod compaction;
mod io;
mod iterators;
mod metadata;
mod metrics;
mod runtime;
mod sst;
mod storage;
mod telemetry;
mod wal;

// Main engine (canonical public API)
mod engine;

// Test support (public for testkit when testing, otherwise acts as private module)
pub mod testkit;

// ---------------------------------------------------------------------------
// Public Export Surface
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Canonical Public Export Surface (1.0)
// ---------------------------------------------------------------------------

// Errors
pub use common::{MidgeError, MidgeResult};

// Engine / Transactions
pub use engine::{ColumnFamilyHandle, ColumnFamilyId, Engine, Transaction, TransactionMode};

// Backward-compatible alias
pub type MidgeEngine = Engine;

// Scan API
pub use engine::{Direction, Query, ScanIterator};

// Configuration
pub use engine::{Goal, MemoryBudget, OpenOptions, Storage, WriteOptions, WorkloadProfile};

// Key/value types
pub use engine::{Key, Value};

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
    pub use crate::{
        ColumnFamilyId, Direction, Engine, Key, MidgeError, MidgeResult, OpenOptions, Query,
        ScanIterator, Storage, Transaction, TransactionMode, Value, WriteOptions,
    };
}
