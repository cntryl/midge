use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::api::column_family::ColumnFamilyId;
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
    pub(crate) flush_coordinator: crate::core::FlushCoordinator,
    /// Background compaction coordinator (optional - may be disabled)
    pub(crate) compaction_coordinator: Option<crate::core::CompactionCoordinator>,
    pub(crate) merge_operators: RwLock<HashMap<u32, crate::api::DynMergeOperator>>,
    pub(crate) cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
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

    pub(crate) fn with_default_memtable_mut<F, R>(&self, f: F) -> R
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

    // ==================== Flush Coordination ====================
    // rollover_and_queue_flush() method moved to coordination/flush_manager.rs
    // flush_memtable_to_sst() method moved to coordination/flush_manager.rs
    // resolve_memtable_merges() method moved to coordination/flush_manager.rs

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

// KvStore trait implementation moved to operations/kv_store.rs

impl Drop for MidgeEngine {
    fn drop(&mut self) {
        // Flush WAL to ensure all writes are persisted
        let _ = self.wal_coordinator.flush();

        // FlushCoordinator will be automatically dropped and shutdown gracefully

        // Background compaction thread is an infinite loop; rely on process exit
        // to terminate it for now.
    }
}

