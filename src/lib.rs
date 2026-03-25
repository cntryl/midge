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

// Internal modules — used by engine/runtime; the compiler reports "dead" because
// no external crate references them. They are exercised by unit tests within each module.
#[allow(dead_code)]
mod compaction;
pub mod handler;
#[allow(dead_code)]
mod io;
pub mod iterators;
#[allow(dead_code)]
mod lease;
pub mod message;
#[allow(dead_code)]
mod metadata;
#[allow(dead_code)]
mod runtime;
pub mod sst;
#[allow(dead_code)]
mod storage;
#[allow(dead_code)]
mod telemetry;
pub mod wal;

// Main engine (canonical public API — re-exported below)
#[allow(dead_code)]
mod engine;

// Test support (shared by tests, benches, and optional downstream utilities).
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
pub use engine::{
    ColumnFamilyHandle, ColumnFamilyId, Engine, IsolationLevel, Transaction, TransactionMode,
};

// Backward-compatible alias
pub type MidgeEngine = Engine;

// Scan API
pub use engine::{Direction, Query, ScanIterator};

// Observability and diagnostics
pub use engine::{
    EngineHealth, ReadAmpMetricsSnapshot, RecoveryMetricsSnapshot, RuntimeMetricsSnapshot,
    SnapshotPinSnapshot, StorageFileLayout, StorageLayoutLevel, StorageLayoutSnapshot,
    StorageVerificationReport,
};

// Configuration
pub use engine::{
    Goal, MemoryBudget, OpenOptions, RecoveryPolicy, Storage, WorkloadProfile, WriteOptions,
};

// Key/value types
pub use engine::{Key, Value};

// Re-export Bytes and BytesMut at crate root so external consumers (including
// generated stress binaries) can refer to `cntryl_midge::Bytes` without needing
// to depend on the `bytes` crate directly.

// `engine::Key` is a public alias to `bytes::Bytes`; re-export it as `Bytes`.
pub use engine::Key as Bytes;
// Re-export BytesMut directly from the `bytes` crate.
pub use bytes::BytesMut;

/// Test support types re-exported for benches/tests and compatibility helpers.
pub use testkit::{MidgeOptions, StorageMode};

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
/// let engine = Engine::open(OpenOptions::local("./db").build())?;
/// let cf = engine.create_column_family("cf1")?;
///
/// // Write: explicit transaction, explicit commit, explicit durability
/// let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
/// tx.put(b"key".to_vec(), b"value".to_vec(), None)?;
/// tx.commit(WriteOptions::sync())?;
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
        ColumnFamilyId, Direction, Engine, IsolationLevel, Key, MidgeError, MidgeResult,
        OpenOptions, Query, ScanIterator, Storage, Transaction, TransactionMode, Value,
        WriteOptions,
    };
}
