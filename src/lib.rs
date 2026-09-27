//! Midge - High-performance embedded LSM-tree database
//!
//! # Architecture
//!
//! Internal modules (implementation details), roughly lowest layer first:
//!   - `common`      - errors, deadlines, clocks and budgets; imports no
//!     other midge subsystem
//!   - `types`, `config` - shared DTOs and configuration; no storage or
//!     runtime imports
//!   - `io`          - base filesystem abstraction
//!   - `telemetry`   - process-global metrics and tracing export
//!   - `memtable`    - in-memory skiplist tables
//!   - `sst`         - sorted-string table format and readers; reports read
//!     activity through `sst::read_path_metrics::SstReadObserver`
//!   - `wal`         - write-ahead log; records to `telemetry`, and recovery
//!     replays into a `memtable::SkipListMemtable`
//!   - `diagnostics` - per-engine read-path and operational counters; sits
//!     above `sst` and implements its observer
//!   - `metadata`    - manifest and version management
//!   - `storage`     - local, cloud and hybrid object storage
//!   - `compaction`  - compaction planning and execution
//!   - `runtime`     - event loop, actors and durability coordination
//!   - `engine`      - the public `Engine` API
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
// documentation lints treat them as the internals they are. `dead_code` stays
// active in every build: `__internal` lists individual items rather than glob
// re-exporting modules, and helpers used only by tests or benches carry
// `#[cfg(any(test, feature = "internal-testing"))]`.

// Foundation - no dependencies
mod cloud_layout;
#[doc(hidden)]
mod codec;
#[doc(hidden)]
mod common;
#[doc(hidden)]
mod config;
#[doc(hidden)]
mod types;

// Internal modules used by engine/runtime.
mod compaction;
#[doc(hidden)]
mod diagnostics;
mod failpoints;
mod io;
mod lease;
#[doc(hidden)]
mod memtable;
mod metadata;
mod runtime;
#[doc(hidden)]
mod sst;
mod storage;
// Only `init_benchmark_telemetry`, which `internal-testing` gates, initializes
// telemetry today, so its initialization path carries the same gate; #355
// tracks a supported initialization API.
mod telemetry;
#[doc(hidden)]
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
    // Each module lists the items its tests, benches and fuzz targets use.
    // Glob re-exports would make every `pub` item reachable from outside the
    // crate and so hide dead code from the compiler.
    pub mod codec {
        pub use crate::codec::{
            compress_block, compress_block_with_trailer, compress_wal_value, decompress_block,
            decompress_block_with_trailer, decompress_wal_value, CompressionAlgo,
            CompressionPolicy, BLOCK_TRAILER_SIZE,
        };
    }
    pub mod diagnostics {
        pub use crate::diagnostics::{
            disable_transaction_commit_timing_for_benchmarks,
            drain_transaction_commit_timings_for_benchmarks,
            enable_transaction_commit_timing_for_benchmarks, TransactionCommitTimingSample,
        };
    }
    pub mod memtable {
        pub use crate::memtable::{bench, SkipListMemtable};
    }
    pub mod runtime {
        pub use crate::runtime::keyed_group_commit::KeyedGroupCommit;
    }
    pub mod sst {
        pub mod bloom {
            pub use crate::sst::bloom::{BloomReader, BloomWriter};
            pub mod writer {
                pub use crate::sst::bloom::writer::BloomFilterOps;
            }
        }
        pub mod cache {
            pub use crate::sst::cache::{BlockCache, CacheKey, CachePolicyType};
        }
        pub mod encoding {
            pub use crate::sst::encoding::{decode, encode};
        }
        pub mod trie {
            pub use crate::sst::trie::{TrieBuilder, TrieReader};
        }
        pub mod types {
            pub use crate::sst::types::{decode_range_tombstones, Footer};
        }
    }
    /// Cloud-boundary types exposed only to this crate's compile-contract
    /// tests. They remain outside Midge's supported public API.
    pub mod storage {
        pub mod cloud {
            #[doc(inline)]
            pub use crate::storage::cloud::{CloudBackend, CloudCallback};
        }
    }
    pub mod types {
        pub use crate::types::{EntryType, KeyState};
    }
    pub mod wal {
        pub use crate::wal::{
            cloud_segment_object_key, parse_segment_id, segment_file_name, WalOpKind, WalRecord,
        };
        pub mod encoding {
            pub use crate::wal::encoding::{decode, decode_view, encode, encode_into};
        }
        pub mod frame {
            pub use crate::wal::frame::{
                append_frame, decode_frame_header, verify_frame_crc, WAL_FRAME_HEADER_LEN,
            };
        }
        pub mod policy {
            pub use crate::wal::policy::BatchConfig;
        }
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

// Engine / Transactions
pub use engine::{
    BackupManifest, BackupObject, BackupStorageKind, ColumnFamilyHandle, ConflictPolicy, Engine,
    EngineMetrics, StorageVerifier, Transaction, TransactionMode,
};
pub use types::ColumnFamilyId;

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
pub use types::{
    HybridStorageBudgetSnapshot, LocalStorageUsage, StorageAdmissionBlock, StorageAdmissionKind,
    StorageAdmissionReason,
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

// TTL clock injection (`OpenOptionsBuilder::ttl_clock`)
pub use common::time::{Clock, SystemClock};

// Re-export Bytes and BytesMut at crate root so external consumers (including
// generated stress binaries) can refer to `cntryl_midge::Bytes` without needing
// to depend on the `bytes` crate directly.

// `engine::Key` is a public alias to `bytes::Bytes`; re-export it as `Bytes`.
pub use engine::Key as Bytes;
// Re-export BytesMut directly from the `bytes` crate.
pub use bytes::BytesMut;

/// Enable metrics-only telemetry for this crate's benches and tests. Succeeds
/// when telemetry is already enabled.
///
/// # Errors
///
/// Returns an error when telemetry cannot be initialized, or was already
/// initialized with telemetry disabled.
#[cfg(feature = "internal-testing")]
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
        Err(telemetry::TelemetryInitError::AlreadyInitialized)
            if telemetry::Telemetry::global().is_some() =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

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
