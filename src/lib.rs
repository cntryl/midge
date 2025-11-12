//! Midge - A high-performance embedded key-value database
//!
//! # Architecture
//!
//! Midge is organized into layers:
//!
//! - [`api`]: Public API layer - traits and types for users
//! - [`core`]: Engine implementation - LSM-tree, compaction, transactions, manifests
//! - [`sst`]: SSTable storage format and bloom filters
//! - [`wal`]: Write-ahead logging for durability
//! - [`cloud`]: Cloud-backed storage implementations
//! - [`compaction`]: User-extensible compaction filter API

// Lint enforcement for production code quality
// Note: unwrap is allowed in test modules via #![cfg_attr(test, allow(clippy::unwrap_used))]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
#![cfg_attr(test, allow(clippy::unwrap_used))]

// Foundational modules with no internal dependencies
pub mod common;

// Re-export error types at crate root for convenience and backwards compatibility
pub use common::error;
pub use common::{MidgeError, MidgeResult};
// Backwards-compat module re-exports (legacy paths used widely across the codebase)
// These preserve old `crate::...` paths after internal reorganization.
// Note: legacy crate-root re-exports removed to prefer explicit module paths.

// Re-export test hooks for testing
pub use common::test_hooks;

// Core modules
pub mod config;
pub mod core;
pub mod fs;

// Public API layer
pub mod api;

// Feature modules (implementation details, but exposed for extensibility)
pub mod cloud;
pub mod health;
pub mod sst;
pub mod wal;

// Convenience re-exports from core (commonly needed internal types)
pub use crate::core::backup;
pub use crate::core::locking;
pub use crate::core::manifest;
pub use crate::core::storage_mode;
// TransactionManager is available via `core::transaction`; no legacy module re-export.

// Compaction filter API for user-provided custom logic
pub mod compaction {
    //! Compaction filter API for custom compaction logic
    pub use crate::core::compaction::executor::CompactionVersion;
    pub use crate::core::compaction::filter::{CompactionFilter, FilterDecision};
}

// Re-export commonly used types at crate root for convenience
pub use crate::api::{
    BytesAppendOperator, ColumnFamilyConfig, ColumnFamilyHandle, ColumnFamilyId,
    DynKvStore, DynMergeOperator, IntegerAddOperator, KvStore, KvTransaction,
    MergeOperator, Mutation, MutationOp, Query, Snapshot, StringAppendOperator, WriteBatch,
    WriteOptions, DEFAULT_CF_ID, DEFAULT_CF_NAME,
};
pub use crate::config::{CompactionStyle, CompressionType};
// Export EngineTransaction as the public Transaction type
pub use crate::core::transaction::EngineTransaction as Transaction;
// Re-export the engine API from the new `core` location for backwards compatibility
pub use crate::core::engine::{CasResult, InsertResult, MidgeEngine};
pub use crate::core::metrics::PerformanceMetrics;
pub use crate::sst::bloom::Filter;
pub use crate::storage_mode::{CloudStorageBuilder, StorageMode};

/// Common entry metadata used across memtable/flush paths.
///
/// Represents a single database entry with all its associated metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryMeta {
    /// The entry's key
    pub key: Vec<u8>,
    /// The entry's value (None for tombstones)
    pub value: Option<Vec<u8>>,
    /// Sequence number for MVCC
    pub sequence: u64,
    /// Whether this entry is a delete tombstone
    pub is_tombstone: bool,
    /// Optional TTL expiration timestamp in milliseconds since epoch
    pub expiration_millis: Option<u64>,
}

impl EntryMeta {
    /// Create a new entry metadata
    pub fn new(
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        sequence: u64,
        is_tombstone: bool,
        expiration_millis: Option<u64>,
    ) -> Self {
        Self {
            key,
            value,
            sequence,
            is_tombstone,
            expiration_millis,
        }
    }

    /// Convert from legacy tuple format
    pub fn from_tuple(tuple: (Vec<u8>, Option<Vec<u8>>, u64, bool, Option<u64>)) -> Self {
        Self {
            key: tuple.0,
            value: tuple.1,
            sequence: tuple.2,
            is_tombstone: tuple.3,
            expiration_millis: tuple.4,
        }
    }

    /// Convert to legacy tuple format for backward compatibility
    pub fn to_tuple(self) -> (Vec<u8>, Option<Vec<u8>>, u64, bool, Option<u64>) {
        (
            self.key,
            self.value,
            self.sequence,
            self.is_tombstone,
            self.expiration_millis,
        )
    }
}

/// WAL recovery mode determines how Midge handles corrupted WAL files
///
/// Different modes offer different tradeoffs between safety and availability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalRecoveryMode {
    /// Refuse to open database if ANY corruption is detected (default)
    ///
    /// Most conservative mode - provides absolute consistency guarantee.
    /// If any WAL record is corrupted (checksum mismatch, truncation, etc.),
    /// the database refuses to open.
    ///
    /// **Use for:** Financial systems, critical infrastructure, audit trails
    ///
    /// # Example
    /// ```rust,no_run
    /// use cntryl_midge::{MidgeOptions, WalRecoveryMode};
    ///
    /// let opts = MidgeOptions {
    ///     wal_recovery_mode: WalRecoveryMode::AbsoluteConsistency,
    ///     ..Default::default()
    /// };
    /// ```
    #[default]
    AbsoluteConsistency,

    /// Tolerate corrupted records at the END of WAL files
    ///
    /// Recovers all complete records before the corruption point. Common in
    /// power-loss scenarios where the last write is truncated mid-operation.
    ///
    /// **Safety:** Corruption in the middle of the file still fails recovery
    /// (only tail truncation is tolerated).
    ///
    /// **Use for:** Consumer applications, high-availability systems
    ///
    /// # Example
    /// ```rust,no_run
    /// use cntryl_midge::{MidgeOptions, WalRecoveryMode};
    ///
    /// let opts = MidgeOptions {
    ///     wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
    ///     ..Default::default()
    /// };
    /// ```
    TolerateCorruptedTail,
}

/// Midge database configuration options
#[derive(Debug, Clone)]
pub struct MidgeOptions {
    /// Storage mode configuration (memory, local disk, or cloud-backed)
    ///
    /// This determines where data is persisted and how durability works.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use cntryl_midge::{MidgeOptions, StorageMode};
    ///
    /// // Pure in-memory
    /// let opts = MidgeOptions {
    ///     storage_mode: StorageMode::Memory,
    ///     ..Default::default()
    /// };
    ///
    /// // Local disk
    /// let opts = MidgeOptions {
    ///     storage_mode: StorageMode::LocalDisk {
    ///         db_path: "./my_db".into(),
    ///     },
    ///     ..Default::default()
    /// };
    /// ```
    pub storage_mode: StorageMode,

    /// Maximum size of a memtable before flush (bytes)
    pub memtable_size: usize,

    /// Number of levels in the LSM tree
    pub max_levels: usize,

    /// Size multiplier between levels
    pub level_multiplier: usize,

    /// Block size for SST files (bytes)
    pub block_size: usize,

    /// Compression algorithm to use
    pub compression: crate::common::codec::CompressionType,

    /// Enable background compaction
    pub enable_compaction: bool,

    /// Trigger background compaction when SST count exceeds this threshold
    pub compaction_sst_threshold: usize,

    /// Background compaction check interval in milliseconds
    pub compaction_check_interval_ms: u64,

    /// Open the database in read-only mode; disallows writes and background threads
    pub read_only: bool,

    /// Bloom filter false positive rate
    pub bloom_filter_fp_rate: f64,

    /// Write buffer size for WAL
    pub wal_buffer_size: usize,

    /// If true, call fsync after each WAL append for durability (slower writes).
    pub wal_sync: bool,

    /// Block cache size in MB (0 = disabled)
    pub cache_size_mb: usize,

    /// Maximum number of open SST tables to cache (0 = disabled)
    pub table_cache_size: usize,

    /// Maximum number of open files
    pub max_open_files: usize,

    /// Transaction spill threshold in bytes (staged in-memory writes beyond this spill to disk)
    pub txn_spill_threshold_bytes: usize,

    /// Time-to-live in seconds for automatic key expiration.
    ///
    /// When set to a non-zero value, background compaction will be configured to
    /// remove expired keys based on TTL. Keys written with `put_with_ttl()` include
    /// expiration timestamps in the WAL.
    ///
    /// **Current Status:** TTL infrastructure is in place but automatic compaction
    /// cleanup requires expiration timestamps to be persisted in SST files (planned
    /// enhancement). For now, set to 0 (disabled).
    ///
    /// Set to 0 to disable automatic TTL cleanup (default).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use cntryl_midge::MidgeOptions;
    ///
    /// // Configure 1-hour TTL (will be active once SST expiration metadata is added)
    /// let opts = MidgeOptions {
    ///     ttl_seconds: 3600,
    ///     ..Default::default()
    /// };
    /// ```
    pub ttl_seconds: u64,

    /// Tombstone density threshold percentage for triggering compaction.
    ///
    /// When an SST file has a tombstone density (percentage of tombstones
    /// vs total entries) above this threshold, it becomes a candidate for
    /// tombstone-triggered compaction to reclaim space and improve read performance.
    ///
    /// - `0.0` = Disabled (no tombstone-triggered compaction)
    /// - `50.0` = Trigger when >=50% of entries are tombstones
    /// - `100.0` = Only compact SSTs that are 100% tombstones
    ///
    /// Recommended range: 30.0 - 60.0 for balanced space/performance tradeoff.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use cntryl_midge::MidgeOptions;
    ///
    /// // Trigger compaction when SSTs have >=40% tombstones
    /// let opts = MidgeOptions {
    ///     tombstone_density_threshold: 40.0,
    ///     ..Default::default()
    /// };
    /// ```
    pub tombstone_density_threshold: f64,

    /// Maximum number of high-density SSTs to compact in one tombstone compaction.
    ///
    /// Limits the scope of tombstone-triggered compaction to avoid overwhelming
    /// the system with a massive compaction job.
    ///
    /// Default: 3 files per compaction cycle
    pub max_tombstone_compaction_files: usize,

    /// WAL recovery mode - determines how to handle corrupted WAL files
    ///
    /// Controls behavior when corruption is detected during recovery:
    /// - `AbsoluteConsistency`: Fail on ANY corruption (default, safest)
    /// - `TolerateCorruptedTail`: Recover up to tail corruption (more flexible)
    ///
    /// See [`WalRecoveryMode`] for detailed documentation.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use cntryl_midge::{MidgeOptions, WalRecoveryMode};
    ///
    /// // Strict mode (default) - fail on any corruption
    /// let opts = MidgeOptions {
    ///     wal_recovery_mode: WalRecoveryMode::AbsoluteConsistency,
    ///     ..Default::default()
    /// };
    ///
    /// // Tolerant mode - recover partial data from power-loss scenarios
    /// let opts = MidgeOptions {
    ///     wal_recovery_mode: WalRecoveryMode::TolerateCorruptedTail,
    ///     ..Default::default()
    /// };
    /// ```
    pub wal_recovery_mode: WalRecoveryMode,
    /// Optional global upload rate limit (bytes per second). 0 = unlimited.
    ///
    /// When set, the engine will configure a global rate limiter that
    /// throttles cloud upload paths (WAL/SST uploads). This provides a
    /// simple, centralized way to cap outgoing bandwidth.
    pub cloud_upload_bytes_per_sec: u64,

    /// Maximum burst size for the global cloud upload limiter (bytes).
    ///
    /// Controls how much burst traffic is allowed before throttling kicks in.
    pub cloud_upload_max_burst_bytes: u64,

    /// Test hooks for fault injection and instrumentation (test builds only).
    ///
    /// Allows tests to intercept operations, inject failures, and verify
    /// internal behavior. Set to `None` for normal operation (default).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use cntryl_midge::{MidgeOptions, test_hooks::{TestHooks, FsyncBehavior}};
    ///
    /// let hooks = TestHooks::new()
    ///     .with_fsync_behavior(FsyncBehavior::Skip);
    ///
    /// let opts = MidgeOptions {
    ///     test_hooks: Some(hooks),
    ///     ..Default::default()
    /// };
    /// ```
    pub test_hooks: Option<crate::common::test_hooks::TestHooks>,

    /// Enable paranoid checksum verification on every SST block read.
    ///
    /// When enabled, checksums are verified on every SST block read, not just
    /// during decompression. This provides stronger data integrity guarantees
    /// at the cost of read performance (typically 5-10% overhead).
    ///
    /// Recommended for:
    /// - Systems with unreliable storage (e.g., network filesystems)
    /// - Compliance scenarios requiring end-to-end data integrity
    /// - Debugging data corruption issues
    ///
    /// Default: `false` (verify checksums only during decompression)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cntryl_midge::MidgeOptions;
    ///
    /// let opts = MidgeOptions {
    ///     paranoid_checksums: true,
    ///     ..Default::default()
    /// };
    /// ```
    pub paranoid_checksums: bool,
}

impl Default for MidgeOptions {
    fn default() -> Self {
        Self {
            storage_mode: StorageMode::default(),
            memtable_size: 64 * 1024 * 1024, // 64MB
            max_levels: 7,
            level_multiplier: 10,
            block_size: 4 * 1024, // 4KB
            compression: crate::common::codec::CompressionType::Lz4,
            enable_compaction: true,
            bloom_filter_fp_rate: 0.01,
            wal_buffer_size: 1024 * 1024, // 1MB
            wal_sync: false,
            cache_size_mb: 128,    // 128MB default
            table_cache_size: 100, // 100 tables
            max_open_files: 1000,
            txn_spill_threshold_bytes: 8 * 1024 * 1024, // 8MB
            compaction_sst_threshold: 4,
            compaction_check_interval_ms: 200,
            read_only: false,
            ttl_seconds: 0,                                // Disabled by default
            tombstone_density_threshold: 50.0,             // 50% density threshold
            max_tombstone_compaction_files: 3,             // Compact up to 3 files at once
            wal_recovery_mode: WalRecoveryMode::default(), // Strict consistency
            cloud_upload_bytes_per_sec: 0,
            cloud_upload_max_burst_bytes: 0,
            test_hooks: None,          // No test hooks by default
            paranoid_checksums: false, // Disabled by default for performance
        }
    }
}

impl MidgeOptions {
    /// Validate configuration options and return an error if any are invalid.
    ///
    /// This checks for:
    /// - Unreasonably large memory allocations
    /// - Invalid bloom filter rates
    /// - Invalid level configuration
    /// - Unreasonable threshold values
    ///
    /// # Examples
    ///
    /// ```rust
    /// use cntryl_midge::{MidgeOptions, StorageMode};
    ///
    /// let mut opts = MidgeOptions::default();
    /// opts.memtable_size = usize::MAX; // Invalid
    ///
    /// assert!(opts.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), String> {
        // Validate memtable size (max 4GB to prevent unreasonable allocations)
        const MAX_MEMTABLE_SIZE: usize = 4 * 1024 * 1024 * 1024; // 4GB
        if self.memtable_size == 0 {
            return Err("memtable_size must be greater than 0".to_string());
        }
        if self.memtable_size > MAX_MEMTABLE_SIZE {
            return Err(format!(
                "memtable_size ({}) exceeds maximum of {} bytes (4GB)",
                self.memtable_size, MAX_MEMTABLE_SIZE
            ));
        }

        // Validate max_levels
        if self.max_levels == 0 {
            return Err("max_levels must be greater than 0".to_string());
        }
        if self.max_levels > 20 {
            return Err(format!(
                "max_levels ({}) exceeds reasonable maximum of 20",
                self.max_levels
            ));
        }

        // Validate level_multiplier
        if self.level_multiplier < 2 {
            return Err("level_multiplier must be at least 2".to_string());
        }
        if self.level_multiplier > 100 {
            return Err(format!(
                "level_multiplier ({}) exceeds reasonable maximum of 100",
                self.level_multiplier
            ));
        }

        // Validate block_size
        if self.block_size == 0 {
            return Err("block_size must be greater than 0".to_string());
        }
        if self.block_size < 1024 {
            return Err("block_size must be at least 1024 bytes (1KB)".to_string());
        }
        if self.block_size > 16 * 1024 * 1024 {
            return Err(format!(
                "block_size ({}) exceeds reasonable maximum of 16MB",
                self.block_size
            ));
        }

        // Validate bloom filter false positive rate
        if self.bloom_filter_fp_rate <= 0.0 || self.bloom_filter_fp_rate >= 1.0 {
            return Err(format!(
                "bloom_filter_fp_rate ({}) must be between 0.0 and 1.0 (exclusive)",
                self.bloom_filter_fp_rate
            ));
        }

        // Validate WAL buffer size
        if self.wal_buffer_size == 0 {
            return Err("wal_buffer_size must be greater than 0".to_string());
        }
        if self.wal_buffer_size > 1024 * 1024 * 1024 {
            return Err(format!(
                "wal_buffer_size ({}) exceeds reasonable maximum of 1GB",
                self.wal_buffer_size
            ));
        }

        // Validate cache_size_mb
        const MAX_CACHE_MB: usize = 100 * 1024; // 100GB
        if self.cache_size_mb > MAX_CACHE_MB {
            return Err(format!(
                "cache_size_mb ({}) exceeds reasonable maximum of {} MB (100GB)",
                self.cache_size_mb, MAX_CACHE_MB
            ));
        }

        // Validate transaction spill threshold
        if self.txn_spill_threshold_bytes == 0 {
            return Err("txn_spill_threshold_bytes must be greater than 0".to_string());
        }
        if self.txn_spill_threshold_bytes < 1024 * 1024 {
            return Err("txn_spill_threshold_bytes should be at least 1MB".to_string());
        }

        // Validate compaction threshold
        if self.compaction_sst_threshold == 0 {
            return Err("compaction_sst_threshold must be greater than 0".to_string());
        }

        // Validate tombstone density threshold
        if self.tombstone_density_threshold < 0.0 || self.tombstone_density_threshold > 100.0 {
            return Err(format!(
                "tombstone_density_threshold ({}) must be between 0.0 and 100.0",
                self.tombstone_density_threshold
            ));
        }

        // Validate max_tombstone_compaction_files
        if self.max_tombstone_compaction_files == 0 {
            return Err("max_tombstone_compaction_files must be greater than 0".to_string());
        }

        Ok(())
    }
}
