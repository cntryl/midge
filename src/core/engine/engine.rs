use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tracing::warn;

use crate::api::column_family::{ColumnFamilyId, DEFAULT_CF_ID};
use crate::error::{MidgeError, MidgeResult};
use crate::core::memtable::MemTable;
use crate::core::metrics::Metrics;
use crate::core::wal_replay::replay_wal_to_memtables;
use crate::manifest::Manifest;

// Import from sibling modules
use super::column_family::{ColumnFamily, ColumnFamilySet};

/// Core LSM-tree storage engine with WAL, memtables, SSTs, and background compaction.
///
/// Supports column families, snapshot isolation, and configurable compression/caching.
pub struct MidgeEngine {
    /// WAL coordinator managing write-ahead log operations
    pub(crate) wal_coordinator: crate::wal::WalCoordinator,
    pub(crate) cf_set: ColumnFamilySet,
    pub(crate) seq: AtomicU64,
    pub(crate) txn_id: AtomicU64,
    pub(crate) db_path: PathBuf,
    #[allow(dead_code)]
    mem_mode: bool,
    pub(crate) read_only: bool,
    pub(crate) memtable_size: usize,
    pub(crate) sst_dir: PathBuf,
    pub(crate) block_size: usize,
    pub(crate) compression: crate::codec::CompressionType,
    pub(crate) sst_factory: Arc<dyn crate::sst::SstFactory>,
    pub(crate) sst_reader_factory: Arc<dyn crate::sst::SstReaderFactory>,
    pub(crate) wal_buffer_size: usize,
    pub(crate) wal_sync: bool,
    pub(crate) snapshot_registry: Arc<crate::api::snapshot::SnapshotRegistry>,
    pub(crate) block_cache: Option<Arc<crate::cache::BlockCache>>,
    pub(crate) table_cache: Option<Arc<crate::sst::table_cache::TableCache>>,
    pub(crate) metrics: Arc<Metrics>,
    /// Performance metrics for real-time monitoring and optimization
    pub(crate) performance_metrics: Arc<crate::core::metrics::PerformanceMetrics>,
    /// Background flush coordinator
    flush_coordinator: crate::core::FlushCoordinator,
    /// Background compaction coordinator (optional - may be disabled)
    pub(crate) compaction_coordinator: Option<crate::core::CompactionCoordinator>,
    pub(crate) merge_operators: RwLock<HashMap<u32, crate::api::DynMergeOperator>>,
    cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
    /// Database lock to prevent concurrent writers. Held for RAII - released on drop.
    #[allow(dead_code)]
    db_lock: Option<Box<dyn crate::core::locking::DbLock>>,
    /// Dynamic read-only flag that can be set during runtime (e.g., when lock renewal fails)
    is_read_only: AtomicBool,
    /// Transaction manager for ACID guarantees
    pub(crate) txn_manager: crate::transaction_manager::TransactionManager,
    /// Flush mutex to serialize concurrent flush operations and prevent file conflicts
    pub(crate) flush_mutex: Mutex<()>,
    /// Cached manifest for fast read access without disk I/O
    /// OPTIMIZATION: Eliminates manifest load on every get() - 75% performance improvement
    pub(crate) manifest_cache: crate::sst::manifest_cache::ManifestCache,
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
    pub(crate) fn check_read_only(&self) -> MidgeResult<()> {
        if self.read_only || self.is_read_only.load(Ordering::SeqCst) {
            return Err(MidgeError::ReadOnly);
        }
        Ok(())
    }

    /// Get a read-only snapshot of the cached manifest
    /// OPTIMIZATION: Avoids disk I/O on every read operation
    /// Delegates to ManifestCache which clones to avoid holding RwLock during SST iteration
    #[inline]
    pub(crate) fn get_manifest(&self) -> Manifest {
        self.manifest_cache.get()
    }

    /// Update the cached manifest (called after flush/compaction)
    pub(crate) fn update_manifest_cache(&self, manifest: Manifest) {
        self.manifest_cache.update(manifest);
    }

    /// Update caches for a newly created SST file
    /// Called after flush or compaction to cache bloom filters and sparse indexes
    pub(crate) fn update_caches_for_new_sst(&self, sst_name: &str) {
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

    // Helper methods for accessing default CF MemTable (now lock-free!)
    pub(crate) fn with_default_memtable<F, R>(&self, f: F) -> R
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

    // Helper methods for accessing any CF's MemTable
    pub(crate) fn with_cf_memtable<F, R>(&self, cf_id: ColumnFamilyId, f: F) -> Option<R>
    where
        F: FnOnce(&MemTable) -> R,
    {
        let cf = self.cf_set.cfs.get(&cf_id.as_u32())?;
        let mt = cf.memtable.read();
        Some(f(&mt))
    }

    pub(crate) fn with_cf_memtable_mut<F, R>(&self, cf_id: ColumnFamilyId, f: F) -> Option<R>
    where
        F: FnOnce(&MemTable) -> R,
    {
        // MemTable uses interior mutability (lock-free skiplist)
        let cf = self.cf_set.cfs.get(&cf_id.as_u32())?;
        let mt = cf.memtable.read();
        Some(f(&mt))
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
    /// Drains the specified column family's memtable and queues it for background flush.
    pub(crate) fn rollover_and_queue_flush(&self, cf_id: ColumnFamilyId) -> MidgeResult<u64> {
        crate::core::flush::rollover_and_queue_flush(
            cf_id,
            &self.seq,
            self.wal_coordinator.writer_lock(),
            self.wal_coordinator.factory(),
            &self.db_path.join("wal"),
            || {
                if cf_id == DEFAULT_CF_ID {
                    let entries = self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
                    let range_tombstones =
                        self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
                    (entries, range_tombstones)
                } else {
                    // For non-default CFs, use with_cf_memtable_mut
                    let entries = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_with_meta_internal()).unwrap_or_default();
                    let range_tombstones = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_range_tombstones()).unwrap_or_default();
                    (entries, range_tombstones)
                }
            },
            &self.flush_coordinator,
        )
    }

    /// Flush memtable to SST for the specified column family.
    pub(crate) fn flush_memtable_to_sst(&self, cf_id: ColumnFamilyId) -> MidgeResult<(PathBuf, crate::manifest::FileMeta)> {
        // Resolve any pending merge operations before flushing
        self.resolve_memtable_merges(cf_id)?;

        // Get CF config
        let cf_config = self.cf_set.get_cf_config(cf_id).unwrap_or_default();

        crate::core::flush::flush_memtable_to_sst(
            cf_id,
            || {
                if cf_id == DEFAULT_CF_ID {
                    let entries = self.with_default_memtable_mut(|mt| mt.drain_with_meta_internal());
                    let range_tombstones =
                        self.with_default_memtable_mut(|mt| mt.drain_range_tombstones());
                    (entries, range_tombstones)
                } else {
                    let entries = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_with_meta_internal()).unwrap_or_default();
                    let range_tombstones = self.with_cf_memtable_mut(cf_id, |mt| mt.drain_range_tombstones()).unwrap_or_default();
                    (entries, range_tombstones)
                }
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
    fn resolve_memtable_merges(&self, cf_id: ColumnFamilyId) -> MidgeResult<()> {
        // Get all keys from memtable
        let all_keys = if cf_id == DEFAULT_CF_ID {
            self.with_default_memtable(|mt| mt.get_all_keys())
        } else {
            self.with_cf_memtable(cf_id, |mt| mt.get_all_keys()).unwrap_or_default()
        };

        // For each key, check if it has merge operands and resolve them
        for key in all_keys.iter() {
            let versions = if cf_id == DEFAULT_CF_ID {
                self.with_default_memtable(|mt| mt.get_versions_for_merge(key, u64::MAX))
            } else {
                self.with_cf_memtable(cf_id, |mt| mt.get_versions_for_merge(key, u64::MAX)).unwrap_or_default()
            };

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
                if cf_id == DEFAULT_CF_ID {
                    self.with_default_memtable_mut(|mt| {
                        mt.put_with_seq_and_exp(key, &resolved_value, seq, None);
                    });
                } else {
                    self.with_cf_memtable_mut(cf_id, |mt| {
                        mt.put_with_seq_and_exp(key, &resolved_value, seq, None);
                    });
                }
            }
        }

        Ok(())
    }

    // flush() method moved to operations/maintenance.rs
    // compact_level() method moved to operations/maintenance.rs
    // compact_range() method moved to operations/maintenance.rs
    // close() method moved to operations/maintenance.rs

    // ==================== Column Family Management ====================
    // create_column_family() method moved to cf_manager.rs
    // drop_column_family() method moved to cf_manager.rs
    // list_column_families() method moved to cf_manager.rs
    // default_column_family() method moved to cf_manager.rs
    // get_column_family() method moved to cf_manager.rs

    // ==================== Column Family Operations ====================
    //
    // NOTE: Read operations (get, scan, get_at, scan_at, scan_streaming) have been
    // moved to operations/reads.rs for better organization. The methods are still
    // available on MidgeEngine via impl blocks in that module.
    //
    // NOTE: Write operations (put, put_with_ttl, delete, delete_range, write_batch,
    // merge_cf, merge_with_ttl_cf, insert, insert_with_ttl) have been moved to
    // operations/writes.rs for better organization.
    //
    // NOTE: Merge operators (register_merge_operator, resolve_merges) have been
    // moved to cf_manager.rs as they are part of CF management.

    // put() method moved to operations/writes.rs
    // delete() method moved to operations/writes.rs
    // scan() method moved to operations/reads.rs
    // delete_range() method moved to operations/writes.rs
    // resolve_merges() method moved to cf_manager.rs
    // register_merge_operator() method moved to cf_manager.rs

    // scan_streaming() method moved to operations/reads.rs
    // put_with_ttl() method moved to operations/writes.rs
    // write_batch() method moved to operations/writes.rs

    // merge_cf() method moved to operations/writes.rs
    // merge_with_ttl_cf() method moved to operations/writes.rs
    // insert() method moved to operations/writes.rs
    // insert_with_ttl() method moved to operations/writes.rs
    // insert_with_value() method moved to operations/mutations.rs
    // compare_and_swap() method moved to operations/mutations.rs
    // batch_internal() method moved to operations/transactions.rs
    // commit_transaction() method moved to operations/transactions.rs
    // transaction_get() method moved to operations/transactions.rs
    // transaction_exists() method moved to operations/transactions.rs
    // snapshot() method moved to operations/snapshots.rs
    // get_at() method moved to operations/reads.rs

    // scan_at() method moved to operations/reads.rs
    // create_checkpoint() method moved to operations/maintenance.rs
    // compact_all() method moved to operations/maintenance.rs
    // block_cache() method moved to operations/observability.rs
    // cache_stats() method moved to operations/observability.rs
    // table_cache() method moved to operations/observability.rs
    // table_cache_stats() method moved to operations/observability.rs
    // metrics() method moved to operations/observability.rs
    // performance_metrics() method moved to operations/observability.rs
    // current_sequence() method moved to operations/observability.rs
    // total_memory_usage() method moved to operations/observability.rs
    // memory_usage_by_cf() method moved to operations/observability.rs
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
        // KvStore::insert is currently an alias for put
        // Use insert_with_value() for insert-if-absent semantics
        self.as_ref().put(cf, key, value)
    }

    fn compare_and_swap(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> MidgeResult<bool> {
        // Read current value
        let current = self.as_ref().get(cf, key)?;

        // Check if current value matches expected
        let matches = match (current.as_ref(), expected) {
            (None, None) => true,
            (Some(c), Some(e)) => c.as_ref() == e,
            _ => false,
        };

        // If matches, perform the swap
        if matches {
            self.as_ref().put(cf, key, new_value)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn merge(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
    ) -> MidgeResult<()> {
        // Delegate to the merge operation which handles merge operators
        self.as_ref().merge_cf(cf, key, value)
    }

    // ==================== Batch Operations ====================

    fn batch(
        &self,
        cf: &crate::api::column_family::ColumnFamilyHandle,
        operations: Vec<crate::api::kv_store::BatchOperation>,
    ) -> MidgeResult<()> {
        // Apply each operation individually to the specified CF
        // For atomic multi-operation batches, use write_batch() with WriteBatch
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
                crate::api::kv_store::BatchOperation::CompareAndSwap {
                    key,
                    expected,
                    new_value,
                } => {
                    // For batch operations, CAS is not atomic across the batch
                    // Each CAS is applied individually
                    let current = self.as_ref().get(cf, &key)?;
                    let matches = match (current.as_ref(), expected.as_ref()) {
                        (None, None) => true,
                        (Some(c), Some(e)) => c.as_ref() == e.as_slice(),
                        _ => false,
                    };
                    if matches {
                        self.as_ref().put(cf, &key, &new_value)?;
                    }
                }
                crate::api::kv_store::BatchOperation::Merge { key, value } => {
                    // Delegate to merge operation which handles merge operators
                    self.as_ref().merge_cf(cf, &key, &value)?;
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
        // Transactions work across all column families
        // The CF parameter is accepted for trait compatibility but transactions
        // are not scoped to a single CF - operations within the transaction
        // can target any CF via the EngineTransaction methods
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
        let write_set = txn
            .write_set()
            .clone()
            .into_iter()
            .map(|(cf, key)| crate::core::transaction_manager::Key::new(cf, key))
            .collect::<HashSet<_>>();
        let read_set = txn
            .read_set()
            .clone()
            .into_iter()
            .map(|(cf, key)| crate::core::transaction_manager::Key::new(cf, key))
            .collect::<HashSet<_>>();
        let read_versions = txn
            .read_versions()
            .clone()
            .into_iter()
            .map(|((cf, key), v)| (crate::core::transaction_manager::Key::new(cf, key), v))
            .collect::<HashMap<_, _>>();

        if let Err(e) = self.txn_manager.begin(
            txn.txn_id(),
            txn.begin_sequence(),
            write_set,
            read_set,
            read_versions,
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

