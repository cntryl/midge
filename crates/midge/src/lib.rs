//! Midge - High-performance embedded LSM-tree database
//!
//! # Architecture
//!
//! The engine owns transactions, compaction, and the runtime decisions that
//! acknowledge writes, publish state, and reclaim files. Four workspace crates
//! provide the underlying mechanisms:
//! - Core owns filesystem I/O, errors, clocks, budgets, codecs, and telemetry.
//! - Table owns memtables, SSTs, and their block caches.
//! - Storage owns provider configuration and object transport.
//! - Durability owns WAL, leases, and persisted metadata.
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

#[doc(hidden)]
mod config;

// Internal modules used by engine/runtime.
mod compaction;
#[doc(hidden)]
mod diagnostics;
mod runtime;

// Main engine (canonical public API — re-exported below)
mod engine;

// The history oracle spans the engine and its simulators.
#[cfg(any(test, feature = "internal-testing"))]
mod simulation;

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
    pub mod checkpoint {
        pub use cntryl_midge_durability::metadata::accounting::{
            Bucket, Counters, LatencyHistogram, Medium, MetricsHandle, Origin, Snapshot,
        };
        pub use cntryl_midge_durability::metadata::checkpoint_policy_probe::run_checkpoint_policy_probe;

        #[must_use]
        pub fn metrics_handle(engine: &crate::Engine) -> MetricsHandle {
            engine.checkpoint_metrics()
        }
    }
    pub mod codec {
        pub use cntryl_midge_core::codec::{
            compress_block, compress_block_with_trailer, compress_wal_value, decompress_block,
            decompress_block_with_trailer, decompress_wal_value, CompressionAlgo,
            CompressionPolicy, BLOCK_TRAILER_SIZE,
        };
    }
    pub mod diagnostics {
        pub use crate::diagnostics::WriteAdmissionSnapshot;

        /// Capture this engine's transaction admission counters after joining callers.
        #[must_use]
        pub fn write_admission_snapshot(engine: &crate::Engine) -> WriteAdmissionSnapshot {
            engine.write_admission_snapshot()
        }

        #[must_use]
        pub fn compaction_grandparent_overlap_roll_count(engine: &crate::Engine) -> u64 {
            engine.compaction_grandparent_overlap_roll_count_for_benchmarks()
        }

        pub use crate::diagnostics::{
            disable_transaction_commit_timing_for_benchmarks,
            drain_transaction_commit_timings_for_benchmarks,
            enable_transaction_commit_timing_for_benchmarks, TransactionCommitTimingSample,
        };
    }
    /// Read clients can open and inspect files without receiving mutators.
    ///
    /// ```compile_fail,E0599
    /// use cntryl_midge::__internal::io::{FsPath, ReadFs};
    ///
    /// fn truncate_read(fs: &dyn ReadFs, path: &FsPath) {
    ///     fs.open_read(path).unwrap().truncate(0).unwrap();
    /// }
    /// ```
    ///
    /// ```
    /// use bytes::Bytes;
    /// use cntryl_midge::__internal::io::{Fs, FsPath, MockFs, OpenMode, OpenOptions, ReadFs};
    ///
    /// let fs = MockFs::new();
    /// let path = FsPath::new("read-control");
    /// let mut writable = fs.open(&path, OpenOptions {
    ///     mode: OpenMode::ReadWrite, create: true, create_new: false, truncate: false,
    /// }).unwrap();
    /// writable.append(Bytes::from_static(b"readable")).unwrap();
    /// drop(writable);
    /// let reader: &dyn ReadFs = &fs;
    /// let file = reader.open_read(&path).unwrap();
    /// assert_eq!(file.len().unwrap(), 8);
    /// assert_eq!(file.read_at(0, 8).unwrap(), Bytes::from_static(b"readable"));
    /// ```
    pub mod io {
        pub use cntryl_midge_core::io::traits::{DirEntry, Metadata};
        pub use cntryl_midge_core::io::{
            open_persistent_handle, Durability, File, Fs, FsError, FsPath, FsResult,
            HostAddressing, MockFs, OpenMode, OpenOptions, PersistentHandleFs, ReadFile, ReadFs,
            RealFs,
        };
    }
    pub mod memtable {
        pub use cntryl_midge_table::memtable::{bench, SkipListMemtable};
    }
    pub mod runtime {
        pub use crate::runtime::keyed_group_commit::KeyedGroupCommit;
        pub use crate::runtime::read_snapshot::point_read::audit::set_point_read_level_order_audit;
    }
    pub mod startup {
        #[cfg(feature = "failpoints")]
        pub use crate::engine::{install_manual_caller_expiry, OwnerExit};
        pub use crate::runtime::{StartupEvent, StartupObserver};
    }
    pub mod maintenance {
        #[doc(hidden)]
        pub fn flush_cf_with_timeout(
            engine: &crate::Engine,
            cf: &crate::ColumnFamilyHandle,
            timeout: std::time::Duration,
        ) -> crate::MidgeResult<()> {
            engine.flush_cf_with_timeout(cf, timeout)
        }
    }
    pub mod recovery {
        pub use crate::runtime::cloud_startup::recovery_progress_fixture::{
            prepare_inventory_progress_fixture, prepare_recovery_progress_fixture,
            PreparedRecoveryProgressFixture, RecoveryProgressFixtureMode,
            RecoveryProgressFixtureResult,
        };
        pub use crate::runtime::{IntentLogEntry, IntentPersistence};
        pub use cntryl_midge_durability::metadata::recovery_progress_fixture::{
            run_journal_recovery_progress_fixture, JournalRecoveryProgressFixtureResult,
        };
        pub use cntryl_midge_durability::metadata::ManifestPersistence;
    }
    pub mod sst {
        /// Read-only SST point reader exposing exact decoder work to benchmarks.
        #[doc(hidden)]
        pub struct PointLookupReaderForBenchmarks(cntryl_midge_table::sst::fs::SstFileIo);

        impl PointLookupReaderForBenchmarks {
            /// Open one immutable SST using its database root and manifest name.
            ///
            /// # Errors
            /// Returns an error if the root or SST cannot be opened or validated.
            pub fn open(
                database_root: &std::path::Path,
                sst_name: &str,
            ) -> crate::MidgeResult<Self> {
                let fs = std::sync::Arc::new(
                    cntryl_midge_core::io::RealFs::open_existing(database_root)
                        .map_err(cntryl_midge_core::io::FsError::into_midge)?,
                );
                let reader =
                    cntryl_midge_table::sst::fs::SstFileIo::open(&format!("sst/{sst_name}"), fs)?;
                Ok(Self(reader))
            }

            /// Return the exact number of encoded entries decoded by one point lookup.
            ///
            /// # Errors
            /// Returns a decoder or filesystem error from the production SST reader.
            pub fn entries_decoded(
                &self,
                key: &[u8],
                snapshot_seq: u64,
            ) -> crate::MidgeResult<u64> {
                let (_, stats) = self.0.get_raw_state_at_with_stats(key, snapshot_seq)?;
                Ok(stats.entries_decoded)
            }
        }

        pub mod decoder {
            pub use cntryl_midge_table::sst::fs::reader_io::{
                validate_data_block, validate_index_block,
            };
        }
        pub mod bloom {
            pub use cntryl_midge_table::sst::bloom::{BlockBloomFilter, BloomReader, BloomWriter};
            pub mod writer {
                pub use cntryl_midge_table::sst::bloom::writer::BloomFilterOps;
            }
        }
        pub mod cache {
            pub use cntryl_midge_table::sst::cache::{BlockCache, CacheKey, CachePolicyType};
        }
        pub mod encoding {
            pub use cntryl_midge_table::sst::encoding::{decode, encode};
        }
        pub mod trie {
            pub use cntryl_midge_table::sst::trie::{TrieBuilder, TrieReader};
        }
        pub mod types {
            pub use cntryl_midge_table::sst::types::{decode_range_tombstones, Footer};
        }
    }
    /// Cloud-boundary types exposed only to this crate's compile-contract and
    /// storage-injection tests. They remain outside Midge's supported public
    /// API.
    pub mod storage {
        pub mod cloud {
            /// In-memory object store for injection tests.
            pub use cntryl_midge_storage::cloud::MockCloudBackend;
            #[doc(inline)]
            pub use cntryl_midge_storage::cloud::{
                CloudBackend, CloudCallback, CloudDeleteCapability, CloudHeadCapability,
                CloudListCapability, CloudReadCapability, CloudReadOptions, CloudRequestOptions,
                CloudWriteCapability,
            };
        }
    }
    /// Simulated storage for crash and fault campaigns.
    pub mod simulation {
        pub use crate::simulation::oracle::{
            Commit, CommitId, Histories, History, Key, Logging, Mismatch, Recovery, Verdict,
            Violation,
        };
        pub use cntryl_midge_core::simulation::clock::SimulatedClock;
        pub use cntryl_midge_core::simulation::crash_fs::{
            CrashDevice, Decision, Fault, FaultEffect, FaultError, FaultId, FaultOp, Subject,
        };
        pub use cntryl_midge_storage::simulation::{
            Failure, Fate, Operation, RequestId, Schedule, SimulatedObjectStore, StoreImage,
            FAILURE_MESSAGE,
        };
    }
    pub mod types {
        pub use cntryl_midge_core::types::{EntryType, KeyState};
    }
    pub mod wal {
        pub use cntryl_midge_durability::wal::{
            parse_segment_file_name, parse_segment_object_key, segment_file_name,
            segment_object_key, WalOpKind, WalRecord,
        };
        pub mod encoding {
            pub use cntryl_midge_durability::wal::encoding::{
                decode, decode_view, encode, encode_into,
            };
        }
        /// WAL segment format v2 (#843), for integration fixtures.
        pub mod segment {
            pub use cntryl_midge_durability::wal::format::link::ChainLink;
            pub use cntryl_midge_durability::wal::format::segment::{
                footer_link, header_epoch, is_footer_payload, records, SegmentBuilder,
                SegmentHeader, HEADER_LEN,
            };
        }
        /// The cloud WAL publication catalog (v2, #844): the only reader
        /// and editor of catalog JSON for integration fixtures.
        pub mod catalog {
            pub use cntryl_midge_durability::wal::cloud_catalog::{
                CatalogDecodeError, CatalogEntry, WalPublicationCatalog,
            };
        }
        pub mod frame {
            pub use cntryl_midge_durability::wal::frame::{
                append_frame, decode_frame_header, verify_frame_crc, WAL_FRAME_HEADER_LEN,
            };
        }
        pub mod policy {
            pub use cntryl_midge_durability::wal::policy::BatchConfig;
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
pub use cntryl_midge_core::cloud_layout::CloudObjectLayout;
pub use cntryl_midge_core::common::{LatencySummary, MidgeError, MidgeResult, Severity};

// Engine / Transactions
pub use cntryl_midge_core::types::ColumnFamilyId;
pub use engine::{
    BackupManifest, BackupObject, BackupStorageKind, ColumnFamilyHandle, ConflictPolicy, Engine,
    EngineMetrics, StorageVerifier, Transaction, TransactionMode,
};

// Scan API
pub use cntryl_midge_table::sst::cache::CacheFill;
pub use engine::{Direction, IteratorState, Query, ScanIterator};

// Observability and diagnostics
pub use cntryl_midge_core::types::{
    DurabilityFrontiersSnapshot, ReadAmpMetricsSnapshot, RecoveryMetricsSnapshot,
    RuntimeLatencySummaries, RuntimeMetricsSnapshot, SnapshotPinSnapshot, StorageFileLayout,
    StorageLayoutLevel, StorageLayoutSnapshot, StorageVerificationReport,
    WalSyncPhaseMetricsSnapshot,
};
pub use cntryl_midge_core::types::{
    EngineHealth, HybridStorageBudgetSnapshot, LocalStorageUsage, StorageAdmissionBlock,
    StorageAdmissionKind, StorageAdmissionReason,
};
pub use cntryl_midge_storage::config::{
    AwsS3Config, AzureBlobConfig, AzureCredentialSource, CloudCheckCode, CloudCheckOutcome,
    CloudPreflightOptions, CloudProviderConfig, CloudProviderKind, CloudStorageLocation,
    CloudStorageRole, CloudStorageTopology, CloudValidationFinding, CloudValidationMode,
    CloudValidationReport, GcsApiStyle, GcsConfig, GcsCredentialSource, OciCredentialSource,
    OciObjectStorageConfig, S3CompatibleConfig, S3CredentialSource,
};
pub use config::AmortizedSyncFloor;
pub use config::CompactionLevelConfig;
pub use config::WriteBackpressureConfig;

// Configuration
pub use engine::{
    BlockCachePolicy, CloudWritePolicy, DurabilityPolicy, Goal, MemoryBudget, OpenOptions,
    OpenOptionsBuilder, RecoveryPolicy, Storage, WorkloadProfile, WriteOptions,
};

// Key/value types
pub use engine::{Key, Value};

// TTL clock injection (`OpenOptionsBuilder::ttl_clock`)
pub use cntryl_midge_core::common::time::{Clock, SystemClock};

// Re-export Bytes and BytesMut at crate root so external consumers (including
// generated stress binaries) can refer to `cntryl_midge::Bytes` without needing
// to depend on the `bytes` crate directly.

// `engine::Key` is a public alias to `bytes::Bytes`; re-export it as `Bytes`.
pub use cntryl_midge_storage::cloud::CloudRetryPolicy;
pub use engine::Key as Bytes;
// Re-export BytesMut directly from the `bytes` crate.
pub use bytes::BytesMut;

#[cfg(feature = "internal-testing")]
#[doc(hidden)]
pub use cntryl_midge_core::telemetry::init_benchmark_telemetry;

#[cfg(test)]
mod internal_path_guards {
    fn assert_type_exists<T>() {}

    #[test]
    fn should_keep_key_internal_paths_compilable() {
        // Arrange
        // Act
        assert_type_exists::<crate::runtime::actors::WalActor>();
        assert_type_exists::<cntryl_midge_table::sst::fs::SstFileIo>();
        assert_type_exists::<cntryl_midge_storage::HybridStorage>();
        assert_type_exists::<cntryl_midge_storage::hybrid::backend::HybridStorage>();
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
