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
//!
//! # Public API Surface
//!
//! Only types re-exported at the bottom of this file are intended to be
//! stable for external consumption. Some implementation modules remain public
//! during the 0.x series for integration tests and diagnostics, but they are
//! not stable API and may change without compatibility guarantees.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]
// Foundation - no dependencies
mod cloud_layout;
#[doc(hidden)]
pub mod common;
#[doc(hidden)]
pub mod config;
#[doc(hidden)]
pub mod types;

// Internal modules used by engine/runtime.
mod compaction;
#[doc(hidden)]
pub mod diagnostics;
mod failpoints;
#[cfg(test)]
pub mod io;
#[cfg(not(test))]
mod io;
#[doc(hidden)]
pub mod iterators;
mod lease;
mod memtable;
mod metadata;
mod runtime;
#[doc(hidden)]
pub mod sst;
mod storage;
mod telemetry;
#[doc(hidden)]
pub mod wal;

// Main engine (canonical public API — re-exported below)
mod engine;

// ---------------------------------------------------------------------------
// Public Export Surface
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Canonical Public Export Surface (1.0)
// ---------------------------------------------------------------------------

// Errors
pub use cloud_layout::CloudObjectLayout;
pub use common::{MidgeError, MidgeResult};

// Engine / Transactions
pub use engine::{
    ColumnFamilyHandle, ConflictPolicy, Engine, EngineMetrics, StorageVerifier, Transaction,
    TransactionMode,
};
pub use types::ColumnFamilyId;

// Backward-compatible alias
pub type MidgeEngine = Engine;

// Scan API
pub use engine::{Direction, IteratorState, Query, ScanIterator};

// Observability and diagnostics
pub use config::{
    AzureCredentialSource, CloudCredentialSource, CloudProviderConfig, CloudStorageLocation,
    CloudStorageTopology, EngineHealth, GcsApiStyle, GcsCredentialSource, S3CredentialSource,
};
pub use types::{
    ReadAmpMetricsSnapshot, RecoveryMetricsSnapshot, RuntimeMetricsSnapshot, SnapshotPinSnapshot,
    StorageFileLayout, StorageLayoutLevel, StorageLayoutSnapshot, StorageVerificationReport,
};

// Configuration
pub use engine::{
    BlockCachePolicy, CloudWritePolicy, DurabilityPolicy, Goal, MemoryBudget, OpenOptions,
    OpenOptionsBuilder, RecoveryPolicy, Storage, WorkloadProfile, WriteOptions,
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

#[doc(hidden)]
pub fn init_benchmark_telemetry() -> MidgeResult<()> {
    let mut config = telemetry::TelemetryConfig::new()
        .with_enabled(true)
        .with_service_name("midge-bench".to_string());
    config.features.enable_logging = false;
    config.features.enable_tracing = false;
    config.features.enable_metrics = true;

    match telemetry::Telemetry::init(&config) {
        Ok(()) => Ok(()),
        Err(MidgeError::Internal(message))
            if message == "Telemetry already initialized"
                && telemetry::Telemetry::global().is_some() =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

// Low-level filesystem abstraction exports for advanced/testing use.
pub use io::{
    Durability as FsDurability, Fs, FsPath, OpenMode as FsOpenMode, OpenOptions as FsOpenOptions,
};

#[cfg(test)]
mod internal_path_guards {
    fn assert_type_exists<T>() {}

    #[test]
    fn should_keep_key_internal_paths_compilable() {
        // Arrange
        // Act
        assert_type_exists::<crate::runtime::actors::WalActor>();
        assert_type_exists::<crate::sst::fs::SstFileIo>();
        assert_type_exists::<crate::storage::HybridStorage>();
        assert_type_exists::<crate::storage::hybrid::backend::HybridStorage>();
        // Assert
    }
}

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
/// let engine = Engine::open(OpenOptions::local("./db").build()?)?;
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
        ColumnFamilyId, ConflictPolicy, Direction, Engine, IteratorState, Key, MidgeError,
        MidgeResult, OpenOptions, Query, ScanIterator, Storage, Transaction, TransactionMode,
        Value, WriteOptions,
    };
}
