use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tracing::{debug, warn};

use crate::api::column_family::{
    ColumnFamilyConfig, ColumnFamilyHandle, ColumnFamilyId, DEFAULT_CF_ID,
};
use crate::core::compaction::{
    apply_compaction_filter, collect_compaction_versions, deduplicate_versions,
    filter_safe_tombstones, sort_versions_for_output, write_compacted_sst,
};
use crate::error::{MidgeError, MidgeResult};

use crate::api::mutation::Mutation;
pub use crate::api::query::Query;
pub use crate::api::snapshot::Snapshot;
use crate::api::transaction::Transaction;
use crate::common::timestamp;
use crate::core::memtable::MemTable;
use crate::core::metrics::Metrics;
use crate::core::wal_replay::{replay_wal_to_memtables, wal_record_encoded_len};
use crate::manifest::Manifest;
use crate::wal::WalOpKind;

// Import from sibling modules
use super::column_family::{ColumnFamily, ColumnFamilySet};
pub use super::types::{CasResult, InsertResult};

/// Core LSM-tree storage engine with WAL, memtables, SSTs, and background compaction.
///
/// Supports column families, snapshot isolation, and configurable compression/caching.
pub struct MidgeEngine {
    /// WAL coordinator managing write-ahead log operations
    wal_coordinator: crate::wal::WalCoordinator,
    cf_set: ColumnFamilySet,
    seq: AtomicU64,
    txn_id: AtomicU64,
    db_path: PathBuf,
    #[allow(dead_code)]
    mem_mode: bool,
    read_only: bool,
    memtable_size: usize,
    sst_dir: PathBuf,
    block_size: usize,
    compression: crate::codec::CompressionType,
    sst_factory: Arc<dyn crate::sst::SstFactory>,
    sst_reader_factory: Arc<dyn crate::sst::SstReaderFactory>,
    wal_buffer_size: usize,
    wal_sync: bool,
    snapshot_registry: Arc<crate::api::snapshot::SnapshotRegistry>,
    block_cache: Option<Arc<crate::cache::BlockCache>>,
    table_cache: Option<Arc<crate::sst::table_cache::TableCache>>,
    metrics: Arc<Metrics>,
    /// Performance metrics for real-time monitoring and optimization
    performance_metrics: Arc<crate::core::metrics::PerformanceMetrics>,
    /// Background flush coordinator
    flush_coordinator: crate::core::FlushCoordinator,
    /// Background compaction coordinator (optional - may be disabled)
    compaction_coordinator: Option<crate::core::CompactionCoordinator>,
    merge_operators: RwLock<HashMap<u32, crate::api::DynMergeOperator>>,
    cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
    /// Database lock to prevent concurrent writers. Held for RAII - released on drop.
    #[allow(dead_code)]
    db_lock: Option<Box<dyn crate::lock::DbLock>>,
    /// Dynamic read-only flag that can be set during runtime (e.g., when lock renewal fails)
    is_read_only: AtomicBool,
    /// Transaction manager for ACID guarantees
    txn_manager: crate::transaction_manager::TransactionManager,
    /// Flush mutex to serialize concurrent flush operations and prevent file conflicts
    flush_mutex: Mutex<()>,
    /// Cached manifest for fast read access without disk I/O
    /// OPTIMIZATION: Eliminates manifest load on every get() - 75% performance improvement
    manifest_cache: crate::sst::manifest_cache::ManifestCache,
    /// Bloom filter cache for fast SST pre-checks
    /// OPTIMIZATION: Avoids SST opens when bloom says key is absent
    bloom_cache: crate::sst::bloom_cache::BloomCache,
    /// Sparse index cache for fast block lookups
    /// OPTIMIZATION: Avoids SST metadata reads and index deserialization overhead
    sparse_index_cache: crate::sst::sparse_index_cache::SparseIndexCache,
}

impl MidgeEngine {
    /// Open or create a database with high-level configuration.
    ///
    /// This is the **recommended** way to open a Midge database using the new
    /// configuration system. It provides a high-level API for specifying performance
    /// goals, durability requirements, and workload profiles while automatically
    /// deriving all low-level parameters.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use cntryl_midge::MidgeEngine;
    /// use cntryl_midge::config::{ConfigBuilder, Goal, Durability};
    ///
    /// // Simple latency-optimized configuration
    /// let config = ConfigBuilder::new("./my_db")
    ///     .goal(Goal::Latency)
    ///     .durability(Durability::Steady)
    ///     .build()
    ///     .expect("valid configuration");
    ///
    /// let engine = MidgeEngine::open_with_config(config)
    ///     .expect("failed to open database");
    /// ```
    ///
    /// # Configuration
    ///
    /// Use [`crate::config::ConfigBuilder`] to create a configuration:
    ///
    /// - **Goal**: Optimize for `Latency`, `Throughput`, or `Cost`
    /// - **Durability**: Choose `Strict`, `Steady`, or `CloudReplicated`
    /// - **Memory Budget**: Use `Auto` or specify bytes explicitly
    /// - **Workload Profile**: Optional tuning for read/write patterns
    /// - **Cloud Mode**: Optional cloud storage integration
    ///
    /// All other parameters (block size, cache allocation, compaction threads, etc.)
    /// are automatically derived from these high-level knobs.
    ///
    /// # Autotuning
    ///
    /// If autotuning is enabled in the config, the engine will adaptively adjust
    /// parameters at runtime based on observed metrics. See [`crate::config::Autotuner`]
    /// for details.
    ///
    /// # Backward Compatibility
    ///
    /// For existing code using [`MidgeOptions`], continue using [`MidgeEngine::open()`].
    /// Both APIs are fully supported and can be used interchangeably.
    pub fn open_with_config(config: crate::config::Config) -> MidgeResult<Self> {
        // Convert high-level Config to low-level MidgeOptions
        let opts = config.to_options();

        // TODO: Initialize autotuner if enabled
        // let autotuner = if config.autotune_enabled() {
        //     Some(crate::config::Autotuner::new())
        // } else {
        //     None
        // };

        // Open engine with derived options
        Self::open(opts)
    }

    /// Wait for the compaction coordinator to become idle.
    ///
    /// If compaction is disabled or not configured, this returns Ok(()) immediately.
    pub fn wait_for_compaction_idle(&self, timeout: Duration) -> MidgeResult<()> {
        // First, wait for any outstanding background flushes to complete. This
        // ensures SST files and manifest updates from flushes are finished
        // before we consider compaction idle.
        self.flush_coordinator.wait_until_idle(timeout)?;

        if let Some(ref coord) = self.compaction_coordinator {
            coord.wait_until_idle(timeout)
        } else {
            Ok(())
        }
    }

    /// Open or create a database with the specified storage mode.
    ///
    /// Supports in-memory, local disk, and cloud-backed storage modes.
    ///
    /// **Note:** Consider using [`MidgeEngine::open_with_config()`] for the
    /// new high-level configuration API with automatic parameter derivation.
    pub fn open(opts: crate::MidgeOptions) -> MidgeResult<Self> {
        let mem_mode = matches!(opts.storage_mode, crate::StorageMode::Memory);

        // Precompute db path and sst dir so we can create an FS-backed writer factory
        let db_path = opts.storage_mode.local_path();
        let sst_dir = db_path.join("sst");

        // Ensure sst directory exists before constructing filesystem-backed factories.
        // Some environments (tests/ephemeral dirs) may not have the parent path yet,
        // and creating the FsSstFactory before the directory exists can cause
        // subsequent file creation to fail with NotFound. Try to create it here and
        // log a warning on failure.
        if !mem_mode {
            if let Err(e) = std::fs::create_dir_all(&sst_dir) {
                tracing::warn!("failed to create sst dir {}: {}", sst_dir.display(), e);
            }
        } else {
            // For in-memory mode, best-effort create (no error propagation)
            let _ = std::fs::create_dir_all(&sst_dir);
        }

        // Choose SST writer factory based on storage mode
        let sst_factory: Box<dyn crate::sst::SstFactory> =
            if let Some(cloud_backend) = opts.storage_mode.cloud_backend() {
                // Use CloudSstFactory for cloud-backed mode
                let prefix = opts
                    .storage_mode
                    .cloud_prefix()
                    .unwrap_or_else(|| "midge".to_string());
                Box::new(crate::sst::cloud::CloudSstFactory::new(
                    cloud_backend.clone(),
                    prefix,
                ))
            } else if mem_mode {
                // In-memory mode uses MemSstFactory
                Box::new(crate::sst::mem::MemSstFactory {})
            } else {
                // Default to filesystem-backed streaming SST writer factory
                Box::new(crate::sst::fs::FsSstFactory::new(sst_dir.clone()))
            };

        let (sst_reader_factory, wal_factory): (
            Box<dyn crate::sst::SstReaderFactory>,
            Box<dyn crate::wal::WalFactory>,
        ) = if mem_mode {
            (
                Box::new(crate::sst::mem::MemSstReaderFactory),
                Box::new(crate::wal::MemWalFactory),
            )
        } else if let Some(cloud_backend) = opts.storage_mode.cloud_backend() {
            // Use CloudSstReaderFactory for cloud-backed mode
            (
                Box::new(crate::sst::cloud::CloudSstReaderFactory::new(cloud_backend)),
                Box::new(crate::wal::FsWalFactory::new()),
            )
        } else {
            (
                Box::new(crate::sst::fs::FsSstReaderFactory),
                Box::new(crate::wal::FsWalFactory::new()),
            )
        };

        Self::open_with_factories(opts, sst_factory, sst_reader_factory, wal_factory, mem_mode)
    }

    /// Open with a provided `SstFactory` implementation.
    pub fn open_with_factories(
        opts: crate::MidgeOptions,
        sst_factory: Box<dyn crate::sst::SstFactory>,
        sst_reader_factory: Box<dyn crate::sst::SstReaderFactory>,
        wal_factory: Box<dyn crate::wal::WalFactory>,
        mem_mode: bool,
    ) -> MidgeResult<Self> {
        let db_path = opts.storage_mode.local_path();
        let wal_dir = db_path.join("wal");
        if !mem_mode {
            std::fs::create_dir_all(&wal_dir)?;
        }

        // Configure global cloud upload rate limiter if requested in options.
        // A zero value means "unlimited" and leaves the default unlimited limiter.
        if opts.cloud_upload_bytes_per_sec > 0 {
            let burst = if opts.cloud_upload_max_burst_bytes > 0 {
                opts.cloud_upload_max_burst_bytes
            } else {
                opts.cloud_upload_bytes_per_sec
            };
            let limiter = Arc::new(crate::common::rate_limiter::RateLimiter::new(
                opts.cloud_upload_bytes_per_sec,
                burst,
            ));
            crate::common::rate_limiter::set_global_rate_limiter(limiter);
        }

        // Delegate construction to factory module
        let db_lock = super::factory::acquire_db_lock(&db_path, opts.read_only, mem_mode)?;
        let (manifest, max_cf_id) = super::factory::init_manifest(&db_path, opts.read_only)?;
        let cf_set = ColumnFamilySet::new();
        super::factory::init_column_families(&manifest, &cf_set, max_cf_id)?;

        // Replay WAL and setup WAL writer
        let max_replay_seq = super::factory::replay_local_wal_segments(
            &wal_dir,
            &cf_set,
            manifest.last_persisted_sequence,
            opts.wal_recovery_mode,
            mem_mode,
        )?;
        let (wal_writer_box, max_replay_seq) = super::factory::setup_wal_writer(
            &opts,
            &wal_dir,
            &db_path,
            &cf_set,
            &manifest,
            max_replay_seq,
        )?;

        // Setup directories and factories
        let sst_dir = db_path.join("sst");
        if !mem_mode {
            std::fs::create_dir_all(&sst_dir)?;
        } else {
            std::fs::create_dir_all(&sst_dir).ok();
        }

        let metrics_arc = Arc::new(Metrics::new());
        let sst_factory_arc: Arc<dyn crate::sst::SstFactory> = Arc::from(sst_factory);
        let wal_factory_arc: Arc<dyn crate::wal::WalFactory> = Arc::from(wal_factory);
        let sst_reader_factory_arc: Arc<dyn crate::sst::SstReaderFactory> =
            Arc::from(sst_reader_factory);
        let snapshot_registry_arc: Arc<crate::api::snapshot::SnapshotRegistry> = Arc::new(
            crate::api::snapshot::SnapshotRegistry::with_metrics(metrics_arc.clone()),
        );

        // Create CloudSstManager if in cloud-backed mode
        let cloud_sst_manager = if let Some(cloud_backend) = opts.storage_mode.cloud_backend() {
            let prefix = opts
                .storage_mode
                .cloud_prefix()
                .unwrap_or_else(|| "midge".to_string());
            let config = crate::sst::cloud::CloudSstManagerConfig {
                bucket: String::new(), // Not used with direct backend
                prefix: Some(prefix),
                cache_dir: Some(sst_dir.clone()),
            };
            Some(Arc::new(crate::sst::cloud::CloudSstManager::new(
                config,
                cloud_backend,
            )?))
        } else {
            None
        };

        // Delegate flush and compaction coordinator setup to factory module
        let flush_coordinator = super::factory::setup_flush_coordinator(
            &opts,
            sst_factory_arc.clone(),
            sst_dir.clone(),
            &db_path,
            metrics_arc.clone(),
            cloud_sst_manager.clone(),
            mem_mode,
        )?;

        let compaction_coordinator = super::factory::setup_compaction_coordinator(
            &opts,
            &db_path,
            sst_dir.clone(),
            sst_factory_arc.clone(),
            sst_reader_factory_arc.clone(),
            snapshot_registry_arc.clone(),
            metrics_arc.clone(),
        )?;

        // Initialize manifest cache for fast read access
        let manifest_cache = crate::sst::ManifestCache::new(db_path.clone())?;
        let manifest = manifest_cache.get();

        // Initialize bloom filter cache and populate from existing SSTs
        let bloom_cache = crate::sst::BloomCache::new(sst_dir.clone());
        bloom_cache.populate_from_manifest(&manifest);

        // Initialize sparse index cache and populate from existing SSTs
        let sparse_index_cache = crate::sst::SparseIndexCache::new(sst_dir.clone());
        sparse_index_cache.populate_from_manifest(&manifest);

        // If running in cloud-backed mode and a CloudSstManager was created,
        // ensure any missing SSTs in the local cache are downloaded from cloud.
        if let Some(cloud_mgr) = &cloud_sst_manager {
            for file_meta in &manifest.files {
                let local_path = sst_dir.join(&file_meta.name);
                if !local_path.exists() {
                    // Derive sst id from filename by stripping known extension
                    let sst_id = file_meta
                        .name
                        .strip_suffix(".sst")
                        .unwrap_or(&file_meta.name);
                    match cloud_mgr.download_sst(sst_id) {
                        Ok(bytes) => {
                            // Write out the downloaded SST into local cache
                            if let Err(e) = std::fs::write(&local_path, &bytes) {
                                tracing::error!(
                                    "Failed to write downloaded SST to {}: {}",
                                    local_path.display(),
                                    e
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to download SST {} from cloud: {}", sst_id, e);
                        }
                    }
                }
            }
        }

        // Create WAL coordinator
        let wal_coordinator = crate::wal::WalCoordinator::new(wal_writer_box, wal_factory_arc);

        Ok(Self {
            wal_coordinator,
            cf_set,
            seq: AtomicU64::new(max_replay_seq),
            txn_id: AtomicU64::new(0),
            db_path,
            mem_mode,
            read_only: opts.read_only,
            memtable_size: opts.memtable_size,
            sst_dir,
            block_size: opts.block_size,
            compression: opts.compression,
            sst_factory: sst_factory_arc,
            sst_reader_factory: sst_reader_factory_arc,
            wal_buffer_size: opts.wal_buffer_size,
            wal_sync: opts.wal_sync,
            snapshot_registry: snapshot_registry_arc,
            block_cache: if opts.cache_size_mb > 0 {
                Some(Arc::new(crate::cache::BlockCache::new(
                    opts.cache_size_mb * 1024 * 1024,
                )))
            } else {
                None
            },
            table_cache: if opts.table_cache_size > 0 {
                Some(Arc::new(crate::sst::table_cache::TableCache::new(
                    opts.table_cache_size,
                )))
            } else {
                None
            },
            metrics: metrics_arc,
            performance_metrics: Arc::new(crate::core::metrics::PerformanceMetrics::new()),
            flush_coordinator,
            compaction_coordinator,
            merge_operators: RwLock::new(HashMap::new()),
            cloud_sst_manager,
            db_lock,
            is_read_only: AtomicBool::new(opts.read_only),
            txn_manager: crate::transaction_manager::TransactionManager::new(),
            flush_mutex: Mutex::new(()),
            manifest_cache,
            bloom_cache,
            sparse_index_cache,
        })
    }

    /// Transition the engine to read-only mode.
    /// Called when lock renewal fails or other critical errors occur.
    /// Once set, all mutation operations will be rejected.
    pub fn transition_to_read_only(&self) {
        self.is_read_only.store(true, Ordering::SeqCst);
        warn!("Database transitioned to read-only mode");
    }

    /// Check if the engine is in read-only mode (either from startup or runtime transition)
    fn check_read_only(&self) -> MidgeResult<()> {
        if self.read_only || self.is_read_only.load(Ordering::SeqCst) {
            return Err(MidgeError::ReadOnly);
        }
        Ok(())
    }

    /// Get a read-only snapshot of the cached manifest
    /// OPTIMIZATION: Avoids disk I/O on every read operation
    /// Delegates to ManifestCache which clones to avoid holding RwLock during SST iteration
    #[inline]
    fn get_manifest(&self) -> Manifest {
        self.manifest_cache.get()
    }

    /// Update the cached manifest (called after flush/compaction)
    fn update_manifest_cache(&self, manifest: Manifest) {
        self.manifest_cache.update(manifest);
    }

    /// Update caches for a newly created SST file
    /// Called after flush or compaction to cache bloom filters and sparse indexes
    fn update_caches_for_new_sst(&self, sst_name: &str) {
        let sst_path = self.sst_dir.join(sst_name);

        // Try to load and cache the bloom filter
        if let Ok(bytes) = std::fs::read(&sst_path) {
            if let Ok(sst_reader) = crate::sst::mem::SstMemReader::from_bytes(bytes.clone()) {
                // Cache bloom filter if present
                if let Some(bloom_bytes) = sst_reader.get_bloom_filter_bytes() {
                    if let Ok(bloom) = crate::sst::bloom::BloomFilter::decode_block(&bloom_bytes) {
                        self.bloom_cache.insert(sst_name.to_string(), bloom);
                    }
                }
            }

            // Cache sparse index
            if let Ok(metadata) = crate::sst::reader_common::SstMetadata::from_bytes(&bytes) {
                self.sparse_index_cache
                    .insert(sst_name.to_string(), metadata.sparse_index);
            }
        }
    }

    /// Remove caches for deleted SST files
    /// Called after compaction to clean up caches for old SST files
    fn remove_caches_for_sst(&self, sst_name: &str) {
        self.bloom_cache.remove(sst_name);
        self.sparse_index_cache.remove(sst_name);
    }

    // Helper methods for accessing default CF MemTable (now lock-free!)
    fn with_default_memtable<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&MemTable) -> R,
    {
        let cf = self.cf_set.default_cf();
        let mt = cf.memtable.read();
        f(&mt)
    }

    fn with_default_memtable_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&MemTable) -> R,
    {
        // MemTable uses interior mutability (lock-free skiplist)
        // No need for write lock for reads, just read lock
        let cf = self.cf_set.default_cf();
        let mt = cf.memtable.read();
        f(&mt)
    }

    /// Replay WAL records into column families. Ignores records for dropped CFs.
    /// Returns the maximum sequence number seen in the records.
    pub(super) fn replay_wal_to_cfs(
        cf_set: &ColumnFamilySet,
        records: &[crate::wal::WalRecord],
    ) -> u64 {
        // Build a map of cf_id -> Arc<ColumnFamily> for replay
        // We clone the Arcs so they live long enough for the replay
        let cf_refs: Vec<Arc<ColumnFamily>> = cf_set
            .cfs
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        // Acquire read locks on all memtables and build the cf_map
        // Note: We hold all locks for the duration of replay for consistency
        let guards: Vec<_> = cf_refs.iter().map(|cf| cf.memtable.read()).collect();
        let mut cf_map: HashMap<u32, &MemTable> = HashMap::new();
        for (cf, guard) in cf_refs.iter().zip(guards.iter()) {
            cf_map.insert(cf.id.as_u32(), &**guard);
        }

        replay_wal_to_memtables(&mut cf_map, records)
    }
    /// Rotate WAL: close current writer and create a new one using wal_factory.
    fn rollover_and_queue_flush(&self) -> MidgeResult<u64> {
        crate::core::flush::rollover_and_queue_flush(
            DEFAULT_CF_ID, // TODO Phase 4: Make this per-CF flush
            &self.seq,
            self.wal_coordinator.writer_lock(),
            self.wal_coordinator.factory(),
            &self.db_path.join("wal"),
            || {
                let entries = self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
                let range_tombstones =
                    self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
                (entries, range_tombstones)
            },
            &self.flush_coordinator,
        )
    }

    fn flush_memtable_to_sst(&self) -> MidgeResult<(PathBuf, crate::manifest::FileMeta)> {
        // Resolve any pending merge operations before flushing
        self.resolve_memtable_merges()?;

        // Get CF config for the default CF (TODO: extend for multi-CF support)
        let cf_config = self.cf_set.get_cf_config(DEFAULT_CF_ID).unwrap_or_default();

        crate::core::flush::flush_memtable_to_sst(
            DEFAULT_CF_ID,
            || {
                let entries = self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
                let range_tombstones =
                    self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
                (entries, range_tombstones)
            },
            crate::core::flush::FlushConfig {
                sst_factory: &self.sst_factory,
                compression: cf_config.compression.into(),
                block_size: self.block_size,
                bloom_bits_per_key: cf_config.bloom_bits_per_key,
                sst_dir: &self.sst_dir,
                metrics: &self.metrics,
                cloud_sst_mgr: self.cloud_sst_manager.as_ref().map(|m| m.as_ref()),
            },
        )
    }

    /// Resolve all pending merge operations in the memtable before flushing.
    /// This combines all merge operands for each key into a single resolved value.
    fn resolve_memtable_merges(&self) -> MidgeResult<()> {
        // Get all keys from memtable
        let all_keys = self.with_default_memtable(|mt| mt.get_all_keys());

        // For each key, check if it has merge operands and resolve them
        for key in all_keys.iter() {
            let versions =
                self.with_default_memtable(|mt| mt.get_versions_for_merge(key, u64::MAX));

            if versions.is_empty() {
                continue;
            }

            // Check if the latest operation is a Delete or Put - if so, don't resolve
            // (only resolve if we have Merge operations)
            if let Some((_value, _exp, op_type)) = versions.first() {
                if *op_type == crate::core::skiplist::OpType::Delete
                    || *op_type == crate::core::skiplist::OpType::Put
                {
                    continue; // Skip non-merge operations
                }
            }

            // Check if there are any merge operations
            let has_merges = versions
                .iter()
                .any(|(_, _, op)| *op == crate::core::skiplist::OpType::Merge);
            if !has_merges {
                continue; // Skip keys without merges
            }

            // Resolve the merges
            if let Some(resolved_value) = self.resolve_merges(key, versions)? {
                // Replace all versions with a single Put containing the resolved value
                let seq = self.seq.load(Ordering::SeqCst);
                self.with_default_memtable_mut(|mt| {
                    mt.put_with_seq_and_exp(key, &resolved_value, seq, None);
                });
            }
        }

        Ok(())
    }

    /// Flush MemTable to SST and update manifest. No-op if MemTable is empty or read-only.
    pub fn flush(&self) -> MidgeResult<()> {
        // Serialize flush operations to prevent concurrent file conflicts
        let _flush_guard = self.flush_mutex.lock();

        if self.read_only {
            return Ok(());
        }
        if self.with_default_memtable(|mt| mt.is_empty()) {
            return Ok(());
        }
        let (file_path, file_meta) = self.flush_memtable_to_sst()?;
        let mut m =
            Manifest::load_with_retry(&self.db_path, 10, std::time::Duration::from_millis(10))?;
        let name = file_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        if !name.is_empty() {
            let size_bytes = std::fs::metadata(&file_path)
                .map(|md| md.len())
                .unwrap_or(0);

            let sublevel =
                if let (Some(sk), Some(lk)) = (&file_meta.smallest_key, &file_meta.largest_key) {
                    m.assign_l0_sublevel(sk, lk)
                } else {
                    0
                };

            // preserve metadata computed by flush_memtable_to_sst
            m.files.push(crate::manifest::FileMeta {
                name: file_meta.name.clone(),
                level: file_meta.level,
                size_bytes,
                cf_id: 0, // Default CF
                smallest_key: file_meta.smallest_key,
                largest_key: file_meta.largest_key,
                smallest_seq: file_meta.smallest_seq,
                largest_seq: file_meta.largest_seq,
                sublevel,
                cloud_location: file_meta.cloud_location,
                cloud_checksum: file_meta.cloud_checksum,
                cloud_uploaded_at: file_meta.cloud_uploaded_at,
                cloud_state: file_meta.cloud_state,
                point_tombstone_count: file_meta.point_tombstone_count,
                range_tombstone_count: file_meta.range_tombstone_count,
                total_entries: file_meta.total_entries,
            });
            m.ssts.push(name.clone());
        }
        m.last_persisted_sequence = self.seq.load(Ordering::SeqCst);
        m.save_atomic(&self.db_path)?;

        // Update cached manifest after successful save
        self.update_manifest_cache(m);

        // Update bloom and sparse index caches for the new SST
        if !name.is_empty() {
            self.update_caches_for_new_sst(&name);
        }

        Ok(())
    }

    /// Trigger manual compaction for a specific level in a column family.
    ///
    /// This compacts all files at the specified level to the next level.
    /// The compaction runs asynchronously in the background compaction thread.
    ///
    /// # Arguments
    /// * `cf` - Column family handle
    /// * `level` - Level to compact (0-based)
    ///
    /// # Errors
    /// Returns an error if compaction is disabled or the channel is disconnected.
    pub fn compact_level(&self, cf: &ColumnFamilyHandle, level: u32) -> MidgeResult<()> {
        if let Some(ref coordinator) = self.compaction_coordinator {
            coordinator.compact_level(cf.id.as_u32(), level)
        } else {
            Err(MidgeError::invalid_config(
                "Manual compaction requested but compaction is disabled",
            ))
        }
    }

    /// Trigger manual compaction for a key range in a column family.
    ///
    /// This compacts all files overlapping the specified key range across all levels.
    /// The compaction runs asynchronously in the background compaction thread.
    ///
    /// # Arguments
    /// * `cf` - Column family handle
    /// * `start_key` - Start of key range (inclusive), None means from beginning
    /// * `end_key` - End of key range (exclusive), None means to end
    ///
    /// # Errors
    /// Returns an error if compaction is disabled or the channel is disconnected.
    pub fn compact_range(
        &self,
        cf: &ColumnFamilyHandle,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> MidgeResult<()> {
        if let Some(ref coordinator) = self.compaction_coordinator {
            coordinator.compact_range(
                cf.id.as_u32(),
                start_key.map(|k| k.to_vec()),
                end_key.map(|k| k.to_vec()),
            )
        } else {
            Err(MidgeError::invalid_config(
                "Manual compaction requested but compaction is disabled",
            ))
        }
    }

    /// Close the engine: flush MemTable and stop background workers.
    pub fn close(self) -> MidgeResult<()> {
        let _ = self.flush();
        Ok(())
    }

    // ==================== Column Family Management ====================

    /// Create a new column family.
    ///
    /// # Errors
    /// Returns an error if:
    /// - A column family with the same name already exists
    /// - The database is in read-only mode
    pub fn create_column_family(
        &self,
        name: &str,
        config: ColumnFamilyConfig,
    ) -> MidgeResult<ColumnFamilyHandle> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot create column family in read-only mode",
            ));
        }

        let cf_id = ColumnFamilyId::new(self.cf_set.next_cf_id.fetch_add(1, Ordering::SeqCst));
        let handle = self
            .cf_set
            .create_cf(cf_id, name.to_string(), config.clone())?;

        let mut manifest = Manifest::load(&self.db_path).unwrap_or_default();
        manifest.add_cf(cf_id, name.to_string(), Some(config));

        // Persist manifest. If persistence fails, roll back the in-memory CF registration
        if let Err(e) = manifest.save_atomic(&self.db_path) {
            // Best-effort rollback of in-memory state inserted by create_cf
            let id_u32 = cf_id.as_u32();
            let _ = self.cf_set.cfs.remove(&id_u32);
            let _ = self.cf_set.name_to_id.remove(handle.name());
            return Err(e);
        }

        // Update cached manifest after successful save
        self.update_manifest_cache(manifest);

        Ok(handle)
    }

    /// Drop a column family and delete all its data.
    ///
    /// # Warning
    /// This operation is irreversible. All data in the column family will be permanently deleted.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The column family does not exist
    /// - Attempting to drop the default column family
    /// - The database is in read-only mode
    pub fn drop_column_family(&self, handle: &ColumnFamilyHandle) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot drop column family in read-only mode",
            ));
        }

        let cf_id = handle.id();

        if cf_id == DEFAULT_CF_ID {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot drop the default column family",
            ));
        }

        // Check for unflushed data - refuse to drop if memtable or immutables are non-empty
        let cf_id_u32 = cf_id.as_u32();
        if let Some(cf) = self.cf_set.cfs.get(&cf_id_u32) {
            // Check if active memtable has any data
            let memtable = cf.memtable.read();
            let is_empty = memtable.is_empty();
            drop(memtable);

            if !is_empty {
                return Err(crate::error::MidgeError::invalid_config(format!(
                    "Cannot drop column family '{}' with unflushed data in active memtable. \
                     Please flush the column family first.",
                    handle.name()
                )));
            }

            // Check if there are any immutable memtables
            let immutable_count = cf.immutable_count();
            if immutable_count > 0 {
                return Err(crate::error::MidgeError::invalid_config(format!(
                    "Cannot drop column family '{}' with {} unflushed immutable memtable(s). \
                     Please flush the column family first.",
                    handle.name(),
                    immutable_count
                )));
            }
        }

        // Remove from manifest. Collect SST file names first so we can delete them
        // after the manifest is updated.
        let mut manifest = Manifest::load(&self.db_path).unwrap_or_default();

        let cf_id_u32 = cf_id.as_u32();
        let files_to_delete: Vec<String> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id_u32)
            .map(|f| f.name.clone())
            .collect();

        manifest.remove_cf(cf_id);
        manifest.save_atomic(&self.db_path)?;

        // Update cached manifest after successful save
        self.update_manifest_cache(manifest.clone());

        // Delete SST files for this CF (best-effort)
        for name in files_to_delete {
            let path = self.sst_dir.join(&name);
            let _ = std::fs::remove_file(path);
        }

        // Remove in-memory CF metadata
        self.cf_set.cfs.remove(&cf_id_u32);
        self.cf_set.name_to_id.remove(handle.name());

        Ok(())
    }

    /// List all column families.
    pub fn list_column_families(&self) -> Vec<ColumnFamilyHandle> {
        self.cf_set
            .cfs
            .iter()
            .map(|entry| entry.value().handle())
            .collect()
    }

    /// Get the default column family handle.
    pub fn default_column_family(&self) -> ColumnFamilyHandle {
        self.cf_set.default_cf().handle()
    }

    /// Get a column family handle by name.
    pub fn get_column_family(&self, name: &str) -> MidgeResult<ColumnFamilyHandle> {
        self.cf_set
            .get_cf_by_name(name)
            .map(|cf| cf.handle())
            .ok_or_else(|| {
                crate::error::MidgeError::invalid_config(format!(
                    "Column family '{}' does not exist",
                    name
                ))
            })
    }

    // ==================== Column Family Operations ====================

    /// Get a value from a column family.
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        self.metrics.record_get();

        let cf_id = cf.id();
        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // Check active memtable first
        {
            let mt = column_family.memtable.read();
            if let Some(v) = mt.get(key) {
                return Ok(Some(v));
            }
        }

        // Check immutable memtables (newest to oldest)
        {
            let immutables = column_family.immutable_memtables.lock();
            // Iterate in reverse order (newest to oldest)
            for immutable_mt in immutables.iter().rev() {
                if let Some(v) = immutable_mt.get(key) {
                    return Ok(Some(v));
                }
            }
        }

        let manifest = self.get_manifest();
        let cf_files: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id.as_u32())
            .collect();

        for file in cf_files.iter().rev() {
            let p = self.sst_dir.join(&file.name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                match sst.get_state(key) {
                    Ok(crate::sst::KeyState::Value(v, _, expiration)) => {
                        // Check if key is expired
                        if let Some(exp_ts) = expiration {
                            let now_millis = timestamp::now_millis();
                            if exp_ts <= now_millis {
                                // Key is expired, treat as deleted
                                return Ok(None);
                            }
                        }
                        return Ok(Some(v));
                    }
                    Ok(crate::sst::KeyState::Tombstone(_)) => return Ok(None),
                    Ok(crate::sst::KeyState::Absent) => continue,
                    Err(_) => continue,
                }
            }
        }
        Ok(None)
    }

    /// Put a key-value pair into a specific column family.
    pub fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot write in read-only mode",
            ));
        }

        let cf_id = cf.id();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Write to WAL
        let rec = crate::wal::WalRecord::new_cf(
            cf_id,
            crate::wal::WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            seq,
        );

        self.wal_coordinator.append_record(&rec)?;
        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        // Write to MemTable
        let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // MemTable uses interior mutability - acquire read lock and write
        {
            let mt = column_family.memtable.read();
            mt.put_with_seq(key, value, seq);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            // Try to freeze the active memtable
            let frozen = column_family.try_freeze_memtable();

            if frozen {
                // Successfully froze memtable, trigger flush
                // TODO Phase 4: Enqueue per-CF FlushJob instead of legacy flush
                // For now, only the default CF flush is fully implemented
                if cf_id == DEFAULT_CF_ID {
                    let _ = self.flush();
                }
            } else {
                // Immutable queue is full - implement write stall
                if column_family.should_stall_writes() {
                    // TODO: Implement proper write stall mechanism
                    // For now, we'll return an error
                    return Err(crate::error::MidgeError::invalid_config(
                        "Write stall: too many immutable memtables pending flush",
                    ));
                }
            }
        }

        Ok(())
    }

    /// Delete a key from a column family.
    pub fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot write in read-only mode",
            ));
        }

        let cf_id = cf.id();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Write to WAL
        let rec = crate::wal::WalRecord::new_cf(
            cf_id,
            crate::wal::WalOpKind::Delete,
            Bytes::copy_from_slice(key),
            None,
            seq,
        );

        self.wal_coordinator.append_record(&rec)?;
        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // MemTable uses interior mutability - acquire read lock and delete
        {
            let mt = column_family.memtable.read();
            mt.delete_with_seq(key, seq);
        }

        Ok(())
    }

    /// Scan a range in a column family.
    pub fn scan(&self, cf: &ColumnFamilyHandle, query: Query) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let cf_id = cf.id();
        let start = query.start.as_ref().map(|b| b.as_ref());
        let end_ref = query.end.as_ref().map(|b| b.as_ref());

        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // Scan active memtable
        let (mem_items, mem_tombs) = {
            let mt = column_family.memtable.read();
            let items = mt
                .scan_range(start, end_ref)
                .into_iter()
                .map(|(k, v)| (k, Some(v), 0u64))
                .collect();
            let tombs = mt
                .tombstones_range(start, end_ref)
                .into_iter()
                .map(|k| (k, None, 0u64))
                .collect();
            (items, tombs)
        };

        // Build sources for merging iterator
        let mut sources: Vec<Box<dyn crate::core::merge_iterator::IteratorSource>> = vec![];
        sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
            mem_items,
        )));
        sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
            mem_tombs,
        )));

        // Scan immutable memtables (newest to oldest)
        {
            let immutables = column_family.immutable_memtables.lock();
            for immutable_mt in immutables.iter().rev() {
                let immut_items: Vec<(Bytes, Option<Bytes>, u64)> = immutable_mt
                    .scan_range(start, end_ref)
                    .into_iter()
                    .map(|(k, v)| (k, Some(v), 0u64))
                    .collect();
                let immut_tombs: Vec<(Bytes, Option<Bytes>, u64)> = immutable_mt
                    .tombstones_range(start, end_ref)
                    .into_iter()
                    .map(|k| (k, None, 0u64))
                    .collect();

                if !immut_items.is_empty() {
                    sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
                        immut_items,
                    )));
                }
                if !immut_tombs.is_empty() {
                    sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
                        immut_tombs,
                    )));
                }
            }
        }

        // Add SST sources for this CF
        let manifest = self.get_manifest();
        let cf_files: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id.as_u32())
            .collect();

        for file in &cf_files {
            let p = self.sst_dir.join(&file.name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                if let Ok(rows) = sst.scan_range_state(start, end_ref) {
                    let now_millis = timestamp::now_millis();

                    let items: Vec<(Bytes, Option<Bytes>, u64)> = rows
                        .into_iter()
                        .map(|(k, st)| {
                            use crate::sst::KeyState;
                            match st {
                                KeyState::Value(v, _, expiration) => {
                                    // Check if key is expired
                                    if let Some(exp_ts) = expiration {
                                        if exp_ts <= now_millis {
                                            // Key is expired, treat as tombstone
                                            return (k, None, 0);
                                        }
                                    }
                                    (k, Some(v), 0)
                                }
                                KeyState::Tombstone(_) => (k, None, 0),
                                KeyState::Absent => (k, None, 0),
                            }
                        })
                        .collect();
                    if !items.is_empty() {
                        sources.push(Box::new(crate::core::merge_iterator::VecSource::new(items)));
                    }
                }
            }
        }

        // Merge and collect
        let iter = crate::core::merge_iterator::MergingIterator::new(sources, query.limit);
        let results: Vec<(Bytes, Bytes)> = iter.collect();

        if query.reverse {
            Ok(results.into_iter().rev().collect())
        } else {
            Ok(results)
        }
    }

    /// Delete a range of keys in a column family where `start <= key < end`.
    pub fn delete_range(
        &self,
        cf: &ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()> {
        self.check_read_only()?;
        self.metrics.record_delete();
        self.metrics.record_memtable_write();
        self.metrics.record_range_tombstone_created();

        // Validate range
        if start >= end {
            return Ok(()); // Empty range, no-op
        }

        let cf_id = cf.id();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Write to WAL first for durability
        self.metrics.record_wal_write();
        let record = crate::wal::WalRecord::new_delete_range(
            cf_id,
            Bytes::copy_from_slice(start),
            Bytes::copy_from_slice(end),
            seq,
        );
        self.wal_coordinator.append_record(&record)?;

        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        // Apply to column family's memtable
        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        {
            let mt = column_family.memtable.read();
            mt.delete_range_with_seq(start, end, seq);
        }

        Ok(())
    }

    /// Resolve merge operations given a list of versions from newest to oldest.
    /// Returns None if the chain ends with Delete or all values are expired.
    fn resolve_merges(
        &self,
        key: &[u8],
        versions: Vec<(Option<Bytes>, Option<u64>, crate::core::skiplist::OpType)>,
    ) -> MidgeResult<Option<Bytes>> {
        use crate::core::skiplist::OpType;

        let now = timestamp::now_millis();

        let mut merge_operands: Vec<Bytes> = Vec::new();
        let mut base_value: Option<Bytes> = None;

        // Scan versions from newest to oldest
        for (value_opt, expiration_opt, op_type) in versions {
            // Check expiration FIRST - expired values act as barriers
            if let Some(exp_millis) = expiration_opt {
                if now >= exp_millis {
                    // Expired value - this is a tombstone barrier!
                    // NO RESURRECTION: Don't scan older versions
                    // Return current merge state or None
                    if merge_operands.is_empty() {
                        return Ok(None);
                    } else {
                        // We have merge operands accumulated - resolve them with no base
                        break;
                    }
                }
            }

            match op_type {
                OpType::Merge => {
                    // Accumulate merge operand
                    if let Some(v) = value_opt {
                        merge_operands.push(v);
                    }
                }
                OpType::Put => {
                    // Put acts as base value and stops the scan
                    base_value = value_opt;
                    break;
                }
                OpType::Delete => {
                    // Delete stops the scan with no base
                    break;
                }
            }
        }

        // If no merge operands, just return the base (Put) or None (Delete/not found)
        if merge_operands.is_empty() {
            return Ok(base_value);
        }

        // We have merge operands - need to resolve them
        // Reverse to get oldest-to-newest order for merging
        merge_operands.reverse();

        // Get the merge operator for CF 0 (default)
        let ops = self.merge_operators.read();
        let op = ops.get(&0).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(
                "No merge operator registered for column family 0",
            )
        })?;

        // Apply merges
        let result = if merge_operands.len() == 1 {
            // Single merge operand
            op.merge(key, base_value.as_deref(), &merge_operands[0])?
        } else {
            // Multiple merge operands - use merge_many for efficiency
            let operand_refs: Vec<&[u8]> = merge_operands.iter().map(|b| b.as_ref()).collect();
            op.merge_many(key, base_value.as_deref(), &operand_refs)?
        };

        Ok(Some(Bytes::from(result)))
    }

    /// Range scan using merging iterator for memory efficiency.
    pub fn scan_streaming(&self, q: Query) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        use crate::core::merge_iterator::{IteratorSource, MergingIterator, VecSource};

        // Compute effective range based on forward/reverse direction
        let start = q.effective_start();
        let end = q.effective_end();
        let end_ref = end.as_deref();

        let mut sources: Vec<Box<dyn IteratorSource>> = Vec::new();

        let mem_items = self
            .with_default_memtable(|mt| mt.scan_range(start, end_ref))
            .into_iter()
            .map(|(k, v)| (k, Some(v), 0u64))
            .collect();
        if q.reverse {
            sources.push(Box::new(VecSource::new_reverse(mem_items)));
        } else {
            sources.push(Box::new(VecSource::new(mem_items)));
        }

        // Add MemTable tombstones
        let mem_tombs = self
            .with_default_memtable(|mt| mt.tombstones_range(start, end_ref))
            .into_iter()
            .map(|k| (k, None, 0u64))
            .collect();
        if q.reverse {
            sources.push(Box::new(VecSource::new_reverse(mem_tombs)));
        } else {
            sources.push(Box::new(VecSource::new(mem_tombs)));
        }

        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        for name in manifest.ssts.iter().rev() {
            let p = self.sst_dir.join(name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                if let Ok(rows) = sst.scan_range_state(start, end_ref) {
                    let now_millis = timestamp::now_millis();

                    let items: Vec<(Bytes, Option<Bytes>, u64)> = rows
                        .into_iter()
                        .map(|(k, st)| {
                            let user_key = crate::internal_key::decode_internal_key(k.as_ref())
                                .map(|(u, s, _t)| (Bytes::from(u), s))
                                .unwrap_or_else(|| (k, 0));
                            match st {
                                crate::sst::KeyState::Value(v, seq, expiration) => {
                                    // Check if key is expired
                                    if let Some(exp_ts) = expiration {
                                        if exp_ts <= now_millis {
                                            // Key is expired, treat as tombstone
                                            return (user_key.0, None, seq);
                                        }
                                    }
                                    (user_key.0, Some(v), seq)
                                }
                                crate::sst::KeyState::Tombstone(seq) => (user_key.0, None, seq),
                                crate::sst::KeyState::Absent => (user_key.0, None, 0),
                            }
                        })
                        .filter(|(_, val, _)| val.is_some() || val.is_none()) // Keep all (values and tombstones)
                        .collect();
                    if q.reverse {
                        sources.push(Box::new(VecSource::new_reverse(items)));
                    } else {
                        sources.push(Box::new(VecSource::new(items)));
                    }
                }
            }
        }

        // Create merging iterator and collect results
        let iter = MergingIterator::with_reverse(sources, q.limit, q.reverse);
        Ok(iter.collect())
    }

    /// Put a key/value with a time-to-live (TTL) in seconds.
    ///
    /// The key will automatically expire after the specified duration.
    /// A TTL of 0 means no expiration.
    ///
    /// BREAKING CHANGE: Now consumes key and value for zero-copy performance.
    ///
    /// Put a key-value pair with TTL into a specific column family.
    ///
    /// # Examples
    /// ```no_run
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # use bytes::Bytes;
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    /// // Key expires after 60 seconds
    /// engine.put_with_ttl_cf(&cf, b"session:123", b"data", 60).unwrap();
    /// ```
    pub fn put_with_ttl(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot write in read-only mode",
            ));
        }

        let cf_id = cf.id();
        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Compute expiration time in milliseconds if TTL > 0
        let expiration = if ttl_seconds > 0 {
            let now_millis = timestamp::now_millis();
            Some(now_millis + (ttl_seconds * 1000))
        } else {
            None
        };

        // Write to WAL with TTL
        let rec = crate::wal::WalRecord::new_with_ttl(
            cf_id,
            crate::wal::WalOpKind::Put,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            seq,
            ttl_seconds,
        );

        self.wal_coordinator.append_record(&rec)?;
        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        // Write to MemTable with expiration
        let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        {
            let mt = column_family.memtable.read();
            mt.put_with_seq_and_exp(key, value, seq, expiration);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            let frozen = column_family.try_freeze_memtable();

            if frozen && cf_id == DEFAULT_CF_ID {
                let _ = self.flush();
            } else if column_family.should_stall_writes() {
                return Err(crate::error::MidgeError::invalid_config(
                    "Write stall: too many immutable memtables pending flush",
                ));
            }
        }

        Ok(())
    }

    /// Write a batch of operations atomically.
    ///
    /// All operations in the batch are written to the WAL in a single write,
    /// then applied to the memtable. This provides better throughput than
    /// individual puts by reducing WAL overhead.
    ///
    /// Each operation in the batch can target a different column family.
    pub fn write_batch(&self, batch: &crate::api::WriteBatch) -> MidgeResult<()> {
        if batch.is_empty() {
            return Ok(());
        }

        self.check_read_only()?;

        // Check if we need to rotate WAL before writing the batch
        let mut total_size = 0;
        for op in batch.operations() {
            let predicted = wal_record_encoded_len(
                op.kind(),
                op.key().len(),
                op.value().map(|v| v.len()),
                None,
            );
            total_size += predicted;
        }

        if self
            .wal_coordinator
            .current_pos()
            .saturating_add(total_size)
            > self.wal_buffer_size as u64
        {
            let _ = self.rollover_and_queue_flush();
        }

        // Build WAL records for batch
        let mut wal_records = Vec::with_capacity(batch.operations().size_hint().0);
        let mut sequences = Vec::with_capacity(batch.operations().size_hint().0);

        // OPTIMIZATION: Compute timestamp once for entire batch to avoid redundant system calls
        let now_millis = timestamp::now_millis();

        for op in batch.operations() {
            let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;

            let expiration = if op.ttl_seconds() > 0 {
                Some(now_millis + (op.ttl_seconds() * 1000))
            } else {
                None
            };

            sequences.push((op, seq, expiration));

            let record = crate::wal::WalRecord {
                cf_id: op.cf_id().as_u32(),
                op: op.kind(),
                key: op.key().clone(),
                value: op.value().cloned(),
                seq,
                expiration,
                range_end: None,
                txn_id: None,
                compression: None,
            };
            wal_records.push(record);
        }

        // Write all records in one batch
        self.metrics.record_wal_write();
        self.wal_coordinator.append_batch(&wal_records)?;

        // Apply to memtable (using pre-computed expirations from WAL record creation)
        for (op, seq, expiration) in sequences {
            let cf_id = op.cf_id();
            let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
                crate::error::MidgeError::invalid_config(format!(
                    "Column family with id {} does not exist",
                    cf_id.as_u32()
                ))
            })?;

            match op.kind() {
                WalOpKind::Put => {
                    self.metrics.record_put();
                    self.metrics.record_memtable_write();

                    if let Some(value) = op.value() {
                        let mt = column_family.memtable.read();
                        mt.put_with_seq_and_exp(op.key(), value, seq, expiration);
                    }
                }
                WalOpKind::Delete => {
                    self.metrics.record_delete();
                    self.metrics.record_memtable_write();
                    self.metrics.record_point_tombstone_created();
                    let mt = column_family.memtable.read();
                    mt.delete_with_seq(op.key(), seq);
                }
                _ => {}
            }
        }

        // Single sync for entire batch if configured
        if self.wal_sync {
            self.metrics.record_wal_sync();
            self.wal_coordinator.sync()?;
        }

        // Check if any memtables are full after batch
        // TODO Phase 4: Implement per-CF flush triggering
        if self.with_default_memtable(|mt| mt.is_full(self.memtable_size)) {
            let _ = self.flush();
        }

        Ok(())
    }

    /// Register a merge operator for a column family.
    ///
    /// Merge operators define how to combine multiple values for the same key,
    /// enabling efficient patterns like counters, append-only logs, and document updates.
    ///
    /// # Arguments
    ///
    /// * `cf_id` - Column family ID (use 0 for default CF)
    /// * `operator` - The merge operator implementation
    ///
    /// # Examples
    ///
    /// ```
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # use cntryl_midge::merge_operator::IntegerAddOperator;
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// // Register counter operator for default CF
    /// engine.register_merge_operator(0, Box::new(IntegerAddOperator)).unwrap();
    /// ```
    pub fn register_merge_operator(
        &self,
        cf_id: u32,
        operator: Box<dyn crate::api::MergeOperator>,
    ) -> MidgeResult<()> {
        let mut ops = self.merge_operators.write();
        ops.insert(cf_id, Arc::from(operator));
        Ok(())
    }

    /// Apply a merge operation to a key in a specific column family.
    ///
    /// Merge operations are deferred - they don't require reading the current value.
    /// Multiple merge operands are combined during compaction or on read.
    ///
    /// A merge operator must be registered for the column family before calling merge.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # use cntryl_midge::merge_operator::IntegerAddOperator;
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    /// engine.register_merge_operator(cf.id().as_u32(), Box::new(IntegerAddOperator)).unwrap();
    /// // Increment counter without reading current value
    /// engine.merge_cf(&cf, b"page_views", b"1").unwrap();
    /// engine.merge_cf(&cf, b"page_views", b"5").unwrap();
    /// ```
    pub fn merge_cf(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        self.merge_with_ttl_cf(cf, key, value, 0)
    }

    /// Apply a merge operation with TTL to a key in a specific column family.
    ///
    /// Like `merge_cf`, but the resulting value will expire after the specified duration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # use cntryl_midge::merge_operator::IntegerAddOperator;
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    /// engine.register_merge_operator(cf.id().as_u32(), Box::new(IntegerAddOperator)).unwrap();
    /// // Temporary counter expires after 60 seconds
    /// engine.merge_with_ttl_cf(&cf, b"temp_counter", b"1", 60).unwrap();
    /// ```
    pub fn merge_with_ttl_cf(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::invalid_config(
                "Cannot write in read-only mode",
            ));
        }

        let cf_id = cf.id();

        // Check that a merge operator is registered for this CF
        {
            let ops = self.merge_operators.read();
            if !ops.contains_key(&cf_id.as_u32()) {
                return Err(crate::error::MidgeError::invalid_config(format!(
                    "No merge operator registered for column family '{}'",
                    cf.name()
                )));
            }
        }

        let seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Compute expiration time in milliseconds if TTL > 0
        let expiration = if ttl_seconds > 0 {
            let now_millis = timestamp::now_millis();
            Some(now_millis + (ttl_seconds * 1000))
        } else {
            None
        };

        // Write to WAL
        let rec = crate::wal::WalRecord::new_with_ttl(
            cf_id,
            crate::wal::WalOpKind::Merge,
            Bytes::copy_from_slice(key),
            Some(Bytes::copy_from_slice(value)),
            seq,
            ttl_seconds,
        );

        self.wal_coordinator.append_record(&rec)?;
        if self.wal_sync {
            self.wal_coordinator.sync()?;
        }

        // Write to MemTable as merge operand
        let column_family = self.cf_set.cfs.get(&cf_id.as_u32()).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        {
            let mt = column_family.memtable.read();
            mt.merge_with_seq_and_exp(key, value, seq, expiration);
        }

        // Check if memtable is full and trigger freeze + flush
        let memtable_full = column_family.is_full();

        if memtable_full {
            let frozen = column_family.try_freeze_memtable();

            if frozen && cf_id == DEFAULT_CF_ID {
                let _ = self.flush();
            } else if column_family.should_stall_writes() {
                return Err(crate::error::MidgeError::invalid_config(
                    "Write stall: too many immutable memtables pending flush",
                ));
            }
        }

        Ok(())
    }

    /// Insert only if the key does not exist (atomic check-and-set).
    ///
    /// Uses snapshot isolation for consistency.
    ///
    /// # Examples
    /// ```
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine, StorageMode};
    /// # use bytes::Bytes;
    /// # let mut opts = MidgeOptions::default();
    /// # opts.storage_mode = StorageMode::Memory;
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    ///
    /// // First insert succeeds
    /// assert!(engine.insert(&cf, b"user:123", b"Alice").unwrap());
    ///
    /// // Second insert fails (key exists)
    /// assert!(!engine.insert(&cf, b"user:123", b"Alice").unwrap());
    /// ```
    pub fn insert(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> MidgeResult<bool> {
        self.insert_with_ttl(cf, key, value, 0)
    }

    /// Insert a key-value pair only if the key does not exist, with TTL.
    ///
    /// Returns true if inserted, false if key already exists.
    /// TTL is specified in seconds; 0 means no expiration.
    ///
    /// # Examples
    /// ```no_run
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine};
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    /// // Insert with 300 second TTL
    /// let inserted = engine.insert_with_ttl(
    ///     &cf,
    ///     b"lock:resource",
    ///     b"held",
    ///     300
    /// ).unwrap();
    /// ```
    pub fn insert_with_ttl(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
    ) -> MidgeResult<bool> {
        self.check_read_only()?;

        // Check if key exists
        // TODO: Use snapshot isolation for consistent read across CFs
        let exists = self.get(cf, key)?.is_some();

        if exists {
            return Ok(false);
        }

        // Key doesn't exist, perform the put with TTL
        self.put_with_ttl(cf, key, value, ttl_seconds)?;
        Ok(true)
    }

    /// Insert a key-value pair only if the key does not exist, returning the result.
    ///
    /// Similar to [`insert`](MidgeEngine::insert), but returns the existing value
    /// if the key is already present.
    ///
    /// # Returns
    /// - `Ok(InsertResult::Inserted)` if the key was newly inserted
    /// - `Ok(InsertResult::AlreadyExists(value))` if the key existed, with its current value
    ///
    /// # Examples
    /// ```
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine, InsertResult};
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    ///
    /// match engine.insert_with_value(&cf, b"counter", b"1").unwrap() {
    ///     InsertResult::Inserted => println!("Created new counter"),
    ///     InsertResult::AlreadyExists(v) => println!("Counter exists: {:?}", v),
    /// }
    /// ```
    pub fn insert_with_value(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<InsertResult> {
        if self.read_only {
            return Err(crate::error::MidgeError::ReadOnly);
        }

        // Check if key exists
        // TODO: Use snapshot isolation for consistent read across CFs
        if let Some(existing) = self.get(cf, key)? {
            return Ok(InsertResult::AlreadyExists(existing));
        }

        // Key doesn't exist, perform the put
        self.put_with_ttl(cf, key, value, 0)?;
        Ok(InsertResult::Inserted)
    }

    /// Compare-and-swap: atomically update a key's value only if it matches expected.
    ///
    /// This operation provides atomic test-and-set semantics:
    /// 1. Reads the current value using snapshot isolation
    /// 2. Compares it to the expected value
    /// 3. If they match, writes the new value
    ///
    /// # Arguments
    /// * `key` - The key to update
    /// * `expected` - The expected current value (None means key should not exist)
    /// * `new_value` - The new value to write if the comparison succeeds
    ///
    /// # Returns
    /// - `Ok(CasResult::Swapped)` if the value matched and was updated
    /// - `Ok(CasResult::Mismatch(actual))` if the current value differs from expected
    ///
    /// # Examples
    /// ```
    /// # use cntryl_midge::{MidgeOptions, MidgeEngine, CasResult};
    /// # let opts = MidgeOptions::default();
    /// # let engine = MidgeEngine::open(opts).unwrap();
    /// let cf = engine.default_column_family();
    ///
    /// // Initialize counter (expect it to not exist)
    /// match engine.compare_and_swap(
    ///     &cf,
    ///     b"counter",
    ///     None,
    ///     b"0"
    /// ).unwrap() {
    ///     CasResult::Swapped => println!("Initialized"),
    ///     CasResult::Mismatch(_) => println!("Already exists"),
    /// }
    ///
    /// // Increment counter (expect current value to be "0")
    /// match engine.compare_and_swap(
    ///     &cf,
    ///     b"counter",
    ///     Some(Bytes::from("0")),
    ///     b"1"
    /// ).unwrap() {
    ///     CasResult::Swapped => println!("Incremented"),
    ///     CasResult::Mismatch(actual) => println!("Race detected: {:?}", actual),
    /// }
    /// ```
    pub fn compare_and_swap(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        expected: Option<Bytes>,
        new_value: &[u8],
    ) -> MidgeResult<CasResult> {
        self.check_read_only()?;

        // Check current value
        // TODO: Use snapshot isolation for consistent read across CFs
        let current = self.get(cf, key)?;

        // Compare current value with expected
        if current != expected {
            return Ok(CasResult::Mismatch(current));
        }

        // Match succeeded, perform the write
        self.put_with_ttl(cf, key, new_value, 0)?;
        Ok(CasResult::Swapped)
    }

    /// Internal batch implementation with explicit sync control.
    ///
    /// Used by `batch()` (with database-level sync) and `commit_transaction()`
    /// (with per-transaction sync).
    fn batch_internal(&self, mutations: Vec<Mutation>, sync: bool) -> MidgeResult<()> {
        self.check_read_only()?;

        if mutations.is_empty() {
            return Ok(());
        }

        // Allocate a transaction ID for this batch
        let txn_id = self.txn_id.fetch_add(1, Ordering::SeqCst);

        // Write TxnBegin marker
        let begin_seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let begin_rec = crate::wal::WalRecord::new_txn_begin(txn_id, begin_seq);
        self.wal_coordinator.append_record(&begin_rec)?;

        // Pre-compute a sequence per mutation to keep ordering stable for MemTable apply
        let mut seqs: Vec<u64> = Vec::with_capacity(mutations.len());
        for m in &mutations {
            let (kind, vlen, rend_len) = match m.op {
                crate::api::mutation::MutationOp::Put
                | crate::api::mutation::MutationOp::Insert => {
                    (WalOpKind::Put, m.value.as_ref().map(|v| v.len()), None)
                }
                crate::api::mutation::MutationOp::Delete => (WalOpKind::Delete, None, None),
                crate::api::mutation::MutationOp::DeleteRange => (
                    WalOpKind::DeleteRange,
                    None,
                    m.range_end.as_ref().map(|r| r.len()),
                ),
            };
            let predicted = wal_record_encoded_len(kind, m.key.len(), vlen, rend_len);
            if self.wal_coordinator.current_pos().saturating_add(predicted)
                > self.wal_buffer_size as u64
            {
                // rotate before appending this record
                let _ = self.rollover_and_queue_flush();
            }
            let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
            seqs.push(seq);
            let ttl_seconds = m.ttl.map(|d| d.as_secs()).unwrap_or(0);

            // Build record with txn_id
            let mut record = match m.op {
                crate::api::mutation::MutationOp::Put
                | crate::api::mutation::MutationOp::Insert => {
                    let expiration = if ttl_seconds > 0 {
                        let now = timestamp::now_millis();
                        Some(now + (ttl_seconds * 1000))
                    } else {
                        None
                    };
                    let mut rec = crate::wal::WalRecord::new_cf(
                        crate::api::column_family::ColumnFamilyId::new(0),
                        WalOpKind::Put,
                        m.key.clone(),
                        m.value.clone(),
                        seq,
                    );
                    rec.expiration = expiration;
                    rec
                }
                crate::api::mutation::MutationOp::Delete => crate::wal::WalRecord::new_cf(
                    crate::api::column_family::ColumnFamilyId::new(0),
                    WalOpKind::Delete,
                    m.key.clone(),
                    None,
                    seq,
                ),
                crate::api::mutation::MutationOp::DeleteRange => {
                    if let Some(end) = m.range_end.as_ref() {
                        crate::wal::WalRecord::new_delete_range(
                            crate::api::column_family::ColumnFamilyId::new(0),
                            m.key.clone(),
                            end.clone(),
                            seq,
                        )
                    } else {
                        // Skip if no end provided
                        continue;
                    }
                }
            };

            // Set txn_id
            record.txn_id = Some(txn_id);
            self.wal_coordinator.append_record(&record)?;
        }

        // Write TxnCommit marker
        let commit_seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let commit_rec = crate::wal::WalRecord::new_txn_commit(txn_id, commit_seq);
        self.wal_coordinator.append_record(&commit_rec)?;

        // Apply to MemTable (with per-mutation seqs preserved)
        for (i, m) in mutations.into_iter().enumerate() {
            let s = seqs[i];
            match m.op {
                crate::api::mutation::MutationOp::Put
                | crate::api::mutation::MutationOp::Insert => {
                    if let Some(v) = m.value {
                        self.with_default_memtable_mut(|mt| mt.put_with_seq(&m.key, &v, s));
                    }
                }
                crate::api::mutation::MutationOp::Delete => {
                    self.with_default_memtable_mut(|mt| mt.delete_with_seq(&m.key, s));
                }
                crate::api::mutation::MutationOp::DeleteRange => {
                    if let Some(end) = m.range_end.as_ref() {
                        self.with_default_memtable_mut(|mt| {
                            mt.delete_range_with_seq(&m.key, end.as_ref(), s)
                        });
                    } else {
                        // If no end provided, treat as no-op for safety
                    }
                }
            }
        }
        // Durability for the batch
        if sync {
            let _ = self.wal_coordinator.sync();
        }
        // OPTIMIZATION: When wal_sync=false, don't flush on every write.
        if self.with_default_memtable(|mt| mt.is_full(self.memtable_size)) {
            let _ = self.flush();
        }
        // No post-append rotation; we rotated before to avoid splitting

        Ok(())
    }

    /// Commit a Transaction by applying its staged mutations to WAL and MemTable.
    ///
    /// The `opts` parameter allows per-transaction control over durability:
    /// - `WriteOptions::sync()` - fsync immediately for strict durability
    /// - `WriteOptions::no_sync()` - defer sync for better performance
    /// - `WriteOptions::default()` - use database-level `wal_sync` setting
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions, WriteOptions};
    /// # use bytes::Bytes;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// // Critical transaction - sync immediately
    /// let mut txn = engine.begin_transaction();
    /// txn.put(Bytes::from("account:1"), Bytes::from("balance:1000"), None);
    /// engine.commit_transaction(txn, WriteOptions::sync()).unwrap();
    ///
    /// // Non-critical transaction - amortize sync cost
    /// let mut txn2 = engine.begin_transaction();
    /// txn2.put(Bytes::from("cache:key"), Bytes::from("value"), None);
    /// engine.commit_transaction(txn2, WriteOptions::no_sync()).unwrap();
    /// ```
    pub fn commit_transaction(
        &self,
        txn: Transaction,
        opts: crate::api::WriteOptions,
    ) -> MidgeResult<()> {
        self.check_read_only()?;

        // Check if transaction is expired (timeout)
        if txn.is_expired() {
            return Err(MidgeError::transaction_conflict("transaction timed out"));
        }

        // Register transaction with manager (tracks read/write sets)
        if let Err(e) = self.txn_manager.begin(
            txn.txn_id(),
            txn.begin_sequence(),
            txn.write_set().clone(),
            txn.read_set().clone(),
            txn.read_versions().clone(),
        ) {
            return Err(MidgeError::transaction_conflict(e));
        }

        // Update wait-for graph and check for deadlocks before commit
        if let Err(e) = self.txn_manager.update_wait_for_graph(txn.txn_id()) {
            self.txn_manager.abort(txn.txn_id());
            return Err(MidgeError::transaction_conflict(e));
        }

        // Check for deadlocks in wait-for graph
        if let Some((victim_id, cycle)) = self.txn_manager.check_for_deadlock() {
            // If this transaction is the victim, abort it
            if victim_id == txn.txn_id() {
                self.txn_manager.abort(txn.txn_id());
                return Err(MidgeError::deadlock(victim_id, cycle));
            }
            // Otherwise, abort the victim transaction (it will fail when it tries to commit)
        }

        // Allocate commit sequence for conflict detection
        // This ensures each committing transaction has a unique sequence number
        let commit_seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Check for conflicts using transaction manager
        match self.txn_manager.try_commit(txn.txn_id(), commit_seq) {
            Ok(()) => {
                // No conflicts, proceed with commit
                let muts = txn.commit()?;
                self.batch_internal(muts, opts.sync)
            }
            Err(e) => {
                // Conflict detected, abort transaction
                self.txn_manager.abort(txn.txn_id());
                Err(MidgeError::transaction_conflict(e))
            }
        }
    }

    /// Get a value within a transaction's snapshot isolation.
    ///
    /// This method reads the value as it existed at the transaction's begin_sequence,
    /// enforcing snapshot isolation. The read is automatically tracked for conflict detection.
    ///
    /// # Arguments
    ///
    /// * `txn` - Mutable reference to the transaction
    /// * `key` - The key to read
    ///
    /// # Returns
    ///
    /// Returns the value at the transaction's snapshot, or None if the key doesn't exist
    /// or was deleted before the transaction began.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use cntryl_midge::{MidgeEngine, MidgeOptions};
    /// # use bytes::Bytes;
    /// # let engine = MidgeEngine::open(MidgeOptions::default()).unwrap();
    /// let mut txn = engine.begin_transaction();
    ///
    /// // Read with snapshot isolation
    /// if let Some(value) = engine.transaction_get(&mut txn, b"key").unwrap() {
    ///     println!("Value: {:?}", value);
    /// }
    ///
    /// // Reads are tracked for conflict detection
    /// txn.put(Bytes::from("other_key"), Bytes::from("value"), None).unwrap();
    /// engine.commit_transaction(txn, cntryl_midge::WriteOptions::default()).unwrap();
    /// ```
    pub fn transaction_get(&self, txn: &mut Transaction, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        // First check transaction's local staged mutations
        if let Some(local_value) = txn.get_local(key) {
            // Track the read at current sequence (local reads see latest)
            let current_seq = self.seq.load(Ordering::SeqCst);
            txn.track_read(Bytes::copy_from_slice(key), current_seq);
            return Ok(local_value);
        }

        // Read from engine at transaction's begin_sequence
        let begin_seq = txn.begin_sequence();

        // 1) Check MemTable visible value at snapshot
        if let Some(v) = self.with_default_memtable(|mt| mt.get_at(key, begin_seq)) {
            txn.track_read(Bytes::copy_from_slice(key), begin_seq);
            return Ok(Some(v));
        }

        // 2) If MemTable has a visible tombstone at snapshot, it's deleted
        let end_key = {
            let mut v = key.to_vec();
            v.push(0);
            v
        };
        let tombs = self.with_default_memtable(|mt| {
            mt.tombstones_range_at(Some(key), Some(end_key.as_slice()), begin_seq)
        });
        if !tombs.is_empty() {
            txn.track_read(Bytes::copy_from_slice(key), begin_seq);
            return Ok(None);
        }

        // 3) Probe SSTs newest->oldest using snapshot-aware state
        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        let now_millis = timestamp::now_millis();

        for name in manifest.ssts.iter().rev() {
            let p = self.sst_dir.join(name);
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                match sst.get_state_at(key, begin_seq) {
                    Ok(crate::sst::KeyState::Value(v, seq, exp)) => {
                        // Check if value is expired
                        if let Some(exp_millis) = exp {
                            if now_millis >= exp_millis {
                                txn.track_read(Bytes::copy_from_slice(key), begin_seq);
                                return Ok(None);
                            }
                        }
                        txn.track_read(Bytes::copy_from_slice(key), seq);
                        return Ok(Some(v));
                    }
                    Ok(crate::sst::KeyState::Tombstone(seq)) => {
                        txn.track_read(Bytes::copy_from_slice(key), seq);
                        return Ok(None);
                    }
                    Ok(_) => continue,
                    Err(_) => continue,
                }
            }
        }

        // Not found anywhere
        txn.track_read(Bytes::copy_from_slice(key), begin_seq);
        Ok(None)
    }

    /// Check if a key exists within a transaction's snapshot isolation.
    ///
    /// This is equivalent to `transaction_get()` but only returns whether the key exists.
    pub fn transaction_exists(&self, txn: &mut Transaction, key: &[u8]) -> MidgeResult<bool> {
        self.transaction_get(txn, key).map(|opt| opt.is_some())
    }

    /// Create a snapshot capturing the current sequence number for consistent reads.
    pub fn snapshot(&self) -> Snapshot {
        self.metrics.snapshot_created();
        let seq = self.seq.load(Ordering::SeqCst);
        self.snapshot_registry.register(seq)
    }

    /// Get at a specific snapshot sequence. Currently identical to `get` until
    /// per-entry sequence visibility is added in the MemTable and SST readers.
    pub fn get_at(&self, key: &[u8], snap: &Snapshot) -> MidgeResult<Option<Bytes>> {
        // 1) Check MemTable visible value at snapshot
        if let Some(v) = self.with_default_memtable(|mt| mt.get_at(key, snap.seq)) {
            return Ok(Some(v));
        }
        // 2) If MemTable has a visible tombstone at snapshot, it's deleted
        let end_key = {
            let mut v = key.to_vec();
            v.push(0);
            v
        };
        let tombs = self.with_default_memtable(|mt| {
            mt.tombstones_range_at(Some(key), Some(end_key.as_slice()), snap.seq)
        });
        if !tombs.is_empty() {
            return Ok(None);
        }
        // 3) Probe SSTs newest->oldest using snapshot-aware state
        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        let now_millis = timestamp::now_millis();

        for name in manifest.ssts.iter().rev() {
            let p = self.sst_dir.join(name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                match sst.get_state_at(key, snap.seq) {
                    Ok(crate::sst::KeyState::Value(v, _seq, exp)) => {
                        // Check if value is expired
                        if let Some(exp_millis) = exp {
                            if now_millis >= exp_millis {
                                // Expired, return None (enforcing no-resurrection)
                                return Ok(None);
                            }
                        }
                        return Ok(Some(v));
                    }
                    Ok(crate::sst::KeyState::Tombstone(_seq)) => return Ok(None),
                    Ok(state) => {
                        tracing::debug!(
                            key = ?key,
                            seq = snap.seq,
                            state = ?state,
                            "unexpected key state in get_at"
                        );
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "get_state_at error in get_at");
                    }
                }
            }
        }
        Ok(None)
    }

    /// Scan at a specific snapshot. Currently identical to `scan` until sequence
    /// visibility is implemented.
    pub fn scan_at(&self, q: Query, snap: &Snapshot) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let start = q
            .start
            .as_ref()
            .map(|b| b.as_ref())
            .or_else(|| q.prefix.as_ref().map(|p| p.as_ref()));
        let end_from_prefix: Option<Vec<u8>> = q.prefix.as_ref().map(|p| {
            let mut v = p.to_vec();
            v.push(0xFF);
            v
        });
        let end = match (
            q.end.as_ref().map(|b| b.as_ref()),
            end_from_prefix.as_deref(),
        ) {
            (Some(e), _) => Some(e),
            (None, Some(ep)) => Some(ep),
            (None, None) => None,
        };
        // Pre-compute MemTable tombstones visible at snapshot
        let mem_tombs: std::collections::BTreeSet<Vec<u8>> = self
            .with_default_memtable(|mt| mt.tombstones_range_at(start, end, snap.seq))
            .into_iter()
            .map(|b| b.to_vec())
            .collect();
        // 1) Merge from SSTs newest->oldest with snapshot-aware state and coverage set
        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        let mut map: std::collections::BTreeMap<Vec<u8>, Bytes> = std::collections::BTreeMap::new();
        let mut covered: std::collections::BTreeSet<Vec<u8>> = mem_tombs.clone();
        for name in manifest.ssts.iter().rev() {
            let p = self.sst_dir.join(name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                if let Ok(rows) = sst.scan_range_state_at(start, end, snap.seq) {
                    let now_millis = timestamp::now_millis();

                    for (k, st) in rows {
                        let user_key = crate::internal_key::decode_internal_key(k.as_ref())
                            .map(|(u, _s, _t)| u)
                            .unwrap_or_else(|| k.to_vec());
                        if covered.contains(&user_key) {
                            continue;
                        }
                        match st {
                            crate::sst::KeyState::Value(v, _seq, expiration) => {
                                // Check if key is expired
                                let is_expired = if let Some(exp_ts) = expiration {
                                    exp_ts <= now_millis
                                } else {
                                    false
                                };

                                if is_expired {
                                    // Key is expired, treat as tombstone
                                    map.remove(&user_key);
                                    covered.insert(user_key);
                                } else {
                                    covered.insert(user_key.clone());
                                    map.entry(user_key).or_insert(v);
                                }
                            }
                            crate::sst::KeyState::Tombstone(_seq) => {
                                map.remove(&user_key);
                                covered.insert(user_key);
                            }
                            crate::sst::KeyState::Absent => {}
                        }
                    }
                }
            }
        }
        // 2) Overlay MemTable live values visible at snapshot
        for (k, v) in self.with_default_memtable(|mt| mt.scan_range_at(start, end, snap.seq)) {
            map.insert(k.to_vec(), v);
        }
        // 3) Apply MemTable tombstones defensively
        for k in mem_tombs.iter() {
            map.remove(k);
        }
        let mut out: Vec<(Bytes, Bytes)> =
            map.into_iter().map(|(k, v)| (Bytes::from(k), v)).collect();
        if let Some(n) = q.limit {
            if out.len() > n {
                out.truncate(n);
            }
        }
        Ok(out)
    }

    /// Create a filesystem checkpoint at `dst_dir` containing a consistent snapshot of the DB.
    /// This writes a manifest copy and links/copies all referenced SST files. CURRENT is written
    /// in the checkpoint directory to point at the manifest.
    pub fn create_checkpoint(&self, dst_dir: &std::path::Path) -> MidgeResult<()> {
        // Ensure current MemTable contents are persisted
        if !self.read_only {
            let _ = self.flush();
        }
        // Load manifest snapshot
        let m = Manifest::load(&self.db_path).unwrap_or_default();
        // Prepare checkpoint directories
        std::fs::create_dir_all(dst_dir)?;
        let dst_sst = dst_dir.join("sst");
        std::fs::create_dir_all(&dst_sst)?;
        // Link or copy each SST into checkpoint/sst
        for name in &m.ssts {
            let src = self.sst_dir.join(name);
            let dst = dst_sst.join(name);
            if !src.exists() {
                continue;
            }
            // Try hard link, fallback to copy
            match std::fs::hard_link(&src, &dst) {
                Ok(_) => {}
                Err(_) => {
                    std::fs::copy(&src, &dst)?;
                }
            }
        }
        // Debug: list checkpoint SST files
        if let Ok(entries) = std::fs::read_dir(&dst_sst) {
            debug!("listing checkpoint sst entries");
            for e in entries.flatten() {
                let p = e.path();
                if let Ok(md) = std::fs::metadata(&p) {
                    debug!(path = %p.display(), size_bytes = md.len(), "checkpoint sst file");
                } else {
                    debug!(path = %p.display(), "checkpoint sst file (no metadata)");
                }
            }
        }
        // Try opening SSTs in the checkpoint to validate format
        if let Ok(entries) = std::fs::read_dir(&dst_sst) {
            for e in entries.flatten() {
                let p = e.path();
                match crate::sst::fs::SstFile::open(&p) {
                    Ok(sst) => match crate::sst::SstStateReader::scan_range_state(&sst, None, None)
                    {
                        Ok(rows) => {
                            debug!(path = %p.display(), rows = ?rows, "checkpoint sst scan succeeded")
                        }
                        Err(err) => {
                            warn!(path = %p.display(), error = ?err, "checkpoint sst scan failed")
                        }
                    },
                    Err(err) => warn!(
                        path = %p.display(),
                        error = ?err,
                        "failed to open checkpoint sst"
                    ),
                }
            }
        }
        // Write manifest.json verbatim into checkpoint
        let manifest_path = dst_dir.join("manifest.json");
        let data = serde_json::to_vec_pretty(&m)?;
        std::fs::write(&manifest_path, &data)?;
        // Write CURRENT pointer
        std::fs::write(dst_dir.join("CURRENT"), b"manifest.json")?;
        Ok(())
    }

    /// Minimal compaction: merge all existing SSTs into a single SST.
    /// Preserves per-entry sequence metadata so snapshot visibility remains correct.
    pub fn compact_all(&self) -> MidgeResult<()> {
        if self.read_only {
            return Err(crate::error::MidgeError::ReadOnly);
        }
        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        if manifest.ssts.len() <= 1 {
            return Ok(());
        }
        let mut versions =
            collect_compaction_versions(&self.sst_reader_factory, &self.sst_dir, &manifest.ssts);
        if versions.is_empty() {
            return Ok(());
        }
        sort_versions_for_output(&mut versions);

        // Apply tombstone GC safety: only drop tombstones that are shadowed
        // and not visible to any active snapshot
        let min_snapshot_seq = self.snapshot_registry.min_active_seq();
        let (versions, removed_tombstones) = filter_safe_tombstones(&versions, min_snapshot_seq);

        // Track tombstone removal metrics
        if removed_tombstones > 0 {
            self.metrics
                .record_tombstones_removed(removed_tombstones as u64);
        }

        // Apply compaction filter (currently uses NoOp filter)
        let filter = crate::compaction_filter::NoOpFilter;
        let versions = apply_compaction_filter(&versions, &filter, 0);

        // Deduplicate to ensure only one version per key in output SST
        let versions = deduplicate_versions(&versions);

        // Debug: log versions that will be written during compact_all
        for v in &versions {
            tracing::debug!(
                "compact_all: output version key={} seq={} tombstone={}",
                String::from_utf8_lossy(&v.user_key),
                v.seq,
                v.tombstone
            );
        }

        let Some((_path, meta)) = write_compacted_sst(
            &self.sst_factory,
            self.compression,
            self.block_size,
            &self.sst_dir,
            &versions,
            0,    // Default CF (compact_all is legacy method)
            None, // compact_all doesn't support cloud upload yet (could be added with engine field)
            None, // Manifest will be updated separately after this call
        )?
        else {
            return Ok(());
        };
        // Remove old SSTs
        for name in &manifest.ssts {
            let p = self.sst_dir.join(name);
            let _ = std::fs::remove_file(&p);

            // Remove from caches
            self.remove_caches_for_sst(name);
        }
        // Update manifest to only reference the new SST and record FileMeta
        let mut m2 = manifest.clone();
        m2.ssts = vec![meta.name.clone()];
        m2.files = vec![meta.clone()];
        m2.last_persisted_sequence = meta
            .largest_seq
            .unwrap_or_else(|| self.seq.load(Ordering::SeqCst));
        m2.save_atomic(&self.db_path)?;
        // Update in-memory manifest cache so subsequent reads see the compacted SST
        self.update_manifest_cache(m2.clone());

        // Update caches for the new compacted SST
        self.update_caches_for_new_sst(&meta.name);

        Ok(())
    }

    /// Get a reference to the block cache
    pub fn block_cache(&self) -> Option<&Arc<crate::cache::BlockCache>> {
        self.block_cache.as_ref()
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> Option<crate::cache::CacheStats> {
        self.block_cache.as_ref().map(|c| c.stats())
    }

    /// Get a reference to the table cache
    pub fn table_cache(&self) -> Option<&Arc<crate::sst::table_cache::TableCache>> {
        self.table_cache.as_ref()
    }

    /// Get table cache statistics
    pub fn table_cache_stats(&self) -> Option<crate::sst::table_cache::TableCacheStats> {
        self.table_cache.as_ref().map(|c| c.stats())
    }

    /// Get a reference to the metrics collector
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// Get a reference to the performance metrics
    ///
    /// Provides real-time performance monitoring with:
    /// - WAL throughput and fsync latency
    /// - Memtable operation counters
    /// - SST read metrics and bloom filter effectiveness
    /// - Compaction throughput and write amplification
    /// - Block cache hit rates
    ///
    /// Use this for performance tuning, regression detection, and production monitoring.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use cntryl_midge::MidgeEngine;
    /// # let engine = MidgeEngine::open(Default::default()).unwrap();
    /// let metrics = engine.performance_metrics();
    /// println!("Cache hit rate: {:.2}%", metrics.cache.hit_rate() * 100.0);
    /// println!("WAL ops/sec: {}", metrics.wal.total_operations());
    /// ```
    pub fn performance_metrics(&self) -> &Arc<crate::core::metrics::PerformanceMetrics> {
        &self.performance_metrics
    }

    /// Get the current sequence number
    ///
    /// Returns the latest sequence number allocated by the engine.
    /// Useful for testing and debugging.
    pub fn current_sequence(&self) -> u64 {
        self.seq.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the total memory usage across all column families
    ///
    /// Returns the sum of all memtable sizes in bytes.
    /// Useful for monitoring and testing memory pressure scenarios.
    pub fn total_memory_usage(&self) -> usize {
        let mut total = 0usize;

        for entry in self.cf_set.cfs.iter() {
            let mt = entry.value().memtable.read();
            total += mt.size_bytes();
        }

        total
    }

    /// Get memory usage per column family
    ///
    /// Returns a HashMap mapping CF IDs to their memory usage in bytes.
    /// Useful for testing memory budget distribution across CFs.
    pub fn memory_usage_by_cf(&self) -> std::collections::HashMap<u32, usize> {
        let mut result = std::collections::HashMap::new();

        for entry in self.cf_set.cfs.iter() {
            let mt = entry.value().memtable.read();
            result.insert(*entry.key(), mt.size_bytes());
        }

        result
    }
}

// Implement the external KvStore trait for Arc<MidgeEngine> so external callers
// can use the engine via the `DynKvStore = Arc<dyn KvStore>` abstraction.
// Using Arc allows transactions to hold a reference to the engine for reads.
impl crate::api::kv_store::KvStore for Arc<MidgeEngine> {
    // ==================== Column Family Management ====================

    fn create_column_family(
        &self,
        name: &str,
        config: crate::api::column_family::ColumnFamilyConfig,
    ) -> MidgeResult<crate::api::column_family::ColumnFamilyHandle> {
        self.as_ref().create_column_family(name, config)
    }

    fn column_family(
        &self,
        name: &str,
    ) -> MidgeResult<crate::api::column_family::ColumnFamilyHandle> {
        self.as_ref().get_column_family(name)
    }

    fn default_column_family(&self) -> crate::api::column_family::ColumnFamilyHandle {
        self.as_ref().default_column_family()
    }

    fn list_column_families(&self) -> Vec<crate::api::column_family::ColumnFamilyHandle> {
        self.as_ref().list_column_families()
    }

    fn drop_column_family(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
    ) -> MidgeResult<()> {
        self.as_ref().drop_column_family(cf)
    }

    // ==================== Data Operations (CF-Scoped) ====================

    fn put(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        self.as_ref().put(cf, key, value)
    }

    fn get(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<Option<Bytes>> {
        self.as_ref().get(cf, key)
    }

    fn delete(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
    ) -> MidgeResult<()> {
        self.as_ref().delete(cf, key)
    }

    fn scan(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let q = crate::api::query::Query::new()
            .start_key(Bytes::copy_from_slice(start))
            .end_key(Bytes::copy_from_slice(end));
        self.as_ref().scan(cf, q)
    }

    fn delete_range(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        start: &[u8],
        end: &[u8],
    ) -> MidgeResult<()> {
        self.as_ref().delete_range(cf, start, end)
    }

    fn insert(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        // TODO: Implement proper insert_cf that checks for existence
        // For now, delegate to put_cf
        self.as_ref().put(cf, key, value)
    }

    // ==================== Batch Operations ====================

    fn batch(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        operations: Vec<crate::api::kv_store::BatchOperation>,
    ) -> MidgeResult<()> {
        // TODO: Implement proper batch_cf that applies all operations to a specific CF
        // For now, we'll apply each operation individually
        for op in operations {
            match op {
                crate::api::kv_store::BatchOperation::Insert { key, value } => {
                    self.as_ref().put(cf, &key, &value)?;
                }
                crate::api::kv_store::BatchOperation::Put { key, value } => {
                    self.as_ref().put(cf, &key, &value)?;
                }
                crate::api::kv_store::BatchOperation::Delete { key } => {
                    self.as_ref().delete(cf, &key)?;
                }
                crate::api::kv_store::BatchOperation::DeleteRange { start, end } => {
                    self.as_ref().delete_range(cf, &start, &end)?;
                }
            }
        }
        Ok(())
    }

    // ==================== Transactions ====================

    fn begin_transaction(
        &self,
        _cf: &crate::api::column_family::ColumnFamilyHandle,
    ) -> MidgeResult<Box<dyn crate::api::kv_store::KvTransaction>> {
        // TODO: Implement proper CF-scoped transactions
        // For now, create a transaction that will use the default CF
        // The _cf parameter is ignored but required by the trait
        let txn_id = self.txn_id.fetch_add(1, Ordering::SeqCst);
        let begin_sequence = self.seq.load(Ordering::SeqCst);
        let txn = crate::api::Transaction::new(txn_id, begin_sequence);
        let engine_txn = crate::api::transaction::EngineTransaction::new(txn, Arc::clone(self));
        Ok(Box::new(engine_txn))
    }

    fn commit_transaction(
        &self,
        txn: Box<dyn crate::api::kv_store::KvTransaction>,
        opts: crate::api::WriteOptions,
    ) -> MidgeResult<()> {
        self.check_read_only()?;

        // Downcast to EngineTransaction to extract the Transaction
        let engine_txn = (txn as Box<dyn std::any::Any>)
            .downcast::<crate::api::transaction::EngineTransaction>()
            .map_err(|_| MidgeError::internal("Failed to downcast transaction"))?;

        let txn = engine_txn.into_inner();

        // Check if transaction is expired (timeout)
        if txn.is_expired() {
            return Err(MidgeError::transaction_conflict("transaction timed out"));
        }

        // Register transaction with manager (tracks read/write sets)
        if let Err(e) = self.txn_manager.begin(
            txn.txn_id(),
            txn.begin_sequence(),
            txn.write_set().clone(),
            txn.read_set().clone(),
            txn.read_versions().clone(),
        ) {
            return Err(MidgeError::transaction_conflict(e));
        }

        // Update wait-for graph and check for deadlocks before commit
        if let Err(e) = self.txn_manager.update_wait_for_graph(txn.txn_id()) {
            self.txn_manager.abort(txn.txn_id());
            return Err(MidgeError::transaction_conflict(e));
        }

        // Check for deadlocks in wait-for graph
        if let Some((victim_id, cycle)) = self.txn_manager.check_for_deadlock() {
            // If this transaction is the victim, abort it
            if victim_id == txn.txn_id() {
                self.txn_manager.abort(txn.txn_id());
                return Err(MidgeError::deadlock(victim_id, cycle));
            }
            // Otherwise, abort the victim transaction (it will fail when it tries to commit)
        }

        // Allocate commit sequence for conflict detection
        let commit_seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;

        // Check for conflicts using transaction manager
        let txn_id = txn.txn_id();
        match self.txn_manager.try_commit(txn_id, commit_seq) {
            Ok(()) => {
                // No conflicts, proceed with commit
                let muts = txn.commit()?;
                self.batch_internal(muts, opts.sync)
            }
            Err(e) => {
                // Conflict detected, abort transaction
                self.txn_manager.abort(txn_id);
                Err(MidgeError::transaction_conflict(e))
            }
        }
    }

    fn rollback_transaction(
        &self,
        txn: Box<dyn crate::api::kv_store::KvTransaction>,
    ) -> MidgeResult<()> {
        // Downcast to EngineTransaction to extract the Transaction
        let engine_txn = (txn as Box<dyn std::any::Any>)
            .downcast::<crate::api::transaction::EngineTransaction>()
            .map_err(|_| MidgeError::internal("Failed to downcast transaction"))?;

        let txn = engine_txn.into_inner();

        // Abort the transaction in the transaction manager
        self.txn_manager.abort(txn.txn_id());

        // Transaction is dropped here, releasing its resources
        Ok(())
    }
}

impl Drop for MidgeEngine {
    fn drop(&mut self) {
        // Flush WAL to ensure all writes are persisted
        let _ = self.wal_coordinator.flush();

        // FlushCoordinator will be automatically dropped and shutdown gracefully

        // Background compaction thread is an infinite loop; rely on process exit
        // to terminate it for now.
    }
}
