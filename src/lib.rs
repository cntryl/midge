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
//! Only the types re-exported at the bottom of this file and the [`prelude`]
//! are public API. Every implementation module is private. This crate's own
//! tests, benches and fuzz targets reach internals through `__internal`,
//! which is compiled only when the non-default `internal-testing` feature is
//! enabled, and which carries no compatibility guarantee.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]
// Implementation modules are private. The ones re-exported through
// `__internal` carry `#[doc(hidden)]` so rustdoc and the pedantic
// documentation lints treat them as the internals they are, and they relax
// `dead_code`/`unused_imports` when `internal-testing` is off, because their
// only remaining callers (tests, benches, fuzz targets) are then unreachable.
// Both lints stay active in every build that enables the feature, which is
// every build CI and developers run.

// Foundation - no dependencies
mod cloud_layout;
#[doc(hidden)]
#[cfg_attr(not(feature = "internal-testing"), allow(dead_code, unused_imports))]
mod common;
#[doc(hidden)]
#[cfg_attr(not(feature = "internal-testing"), allow(dead_code, unused_imports))]
mod config;
#[doc(hidden)]
#[cfg_attr(not(feature = "internal-testing"), allow(dead_code, unused_imports))]
mod types;

// Internal modules used by engine/runtime.
mod compaction;
#[doc(hidden)]
#[cfg_attr(not(feature = "internal-testing"), allow(dead_code, unused_imports))]
mod diagnostics;
mod failpoints;
mod io;
#[doc(hidden)]
#[cfg(feature = "internal-testing")]
mod iterators;
mod lease;
#[doc(hidden)]
#[cfg_attr(not(feature = "internal-testing"), allow(dead_code, unused_imports))]
mod memtable;
mod metadata;
mod runtime;
#[doc(hidden)]
#[cfg_attr(not(feature = "internal-testing"), allow(dead_code, unused_imports))]
mod sst;
mod storage;
mod telemetry;
#[doc(hidden)]
#[cfg_attr(not(feature = "internal-testing"), allow(dead_code, unused_imports))]
mod wal;

// Main engine (canonical public API — re-exported below)
mod engine;

// ---------------------------------------------------------------------------
// Internal Test Surface (NOT public API)
// ---------------------------------------------------------------------------

/// Implementation internals exposed for this crate's own tests, benches and
/// fuzz targets.
///
/// Gated behind the non-default `internal-testing` feature. Nothing here is
/// public API: names, shapes and behaviour may change in any release without
/// a compatibility guarantee. Downstream consumers must use the canonical
/// re-exports (or [`prelude`]) instead.
#[cfg(feature = "internal-testing")]
#[doc(hidden)]
pub mod __internal {
    pub mod common {
        pub use crate::common::*;
    }
    pub mod config {
        pub use crate::config::*;
    }
    pub mod diagnostics {
        pub use crate::diagnostics::*;
    }
    pub mod iterators {
        pub use crate::iterators::*;
    }
    pub mod sst {
        pub use crate::sst::*;
    }
    pub mod types {
        pub use crate::types::*;
    }
    pub mod wal {
        pub use crate::wal::*;
    }
}

// ---------------------------------------------------------------------------
// Public Export Surface
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Canonical Public Export Surface (1.0)
// ---------------------------------------------------------------------------

// Errors
pub use cloud_layout::CloudObjectLayout;
pub use common::{MidgeError, MidgeResult, Severity};

#[cfg(feature = "cloud-common")]
pub(crate) mod cloud_preflight_backend {
    #[cfg(test)]
    pub(crate) use crate::storage::cloud::MockCloudBackend;
    pub(crate) use crate::storage::cloud::{CloudBackend, CloudEvent};

    pub(crate) fn build(
        provider: &crate::config::CloudProviderConfig,
    ) -> crate::common::MidgeResult<std::sync::Arc<dyn CloudBackend>> {
        crate::storage::providers::build_cloud_backend(provider)
    }
}

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
    AwsS3Config, AzureBlobConfig, AzureCredentialSource, CloudCheckCode, CloudCheckOutcome,
    CloudPreflightOptions, CloudProviderConfig, CloudProviderKind, CloudStorageLocation,
    CloudStorageRole, CloudStorageTopology, CloudValidationFinding, CloudValidationMode,
    CloudValidationReport, EngineHealth, GcsApiStyle, GcsConfig, GcsCredentialSource,
    OciCredentialSource, OciObjectStorageConfig, S3CompatibleConfig, S3CredentialSource,
};
pub use storage::hybrid::{
    backend::HybridStorageBudgetSnapshot,
    pressure::{StorageAdmissionBlock, StorageAdmissionKind, StorageAdmissionReason},
    state::LocalStorageUsage,
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
    traits::ReadObserver, Durability as FsDurability, Fs, FsPath, OpenMode as FsOpenMode,
    OpenOptions as FsOpenOptions,
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
