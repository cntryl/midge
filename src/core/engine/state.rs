//! Engine initialization functions.
//!
//! Contains the main entry points for opening a `MidgeEngine` with various
//! configuration and factory options.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::core::engine::column_family::ColumnFamilySet;
use crate::core::engine::MidgeEngine;
use crate::error::MidgeResult;
use crate::metrics::Metrics;

/// Open or create a database using the high-level `Config` API.
///
/// This is the recommended way to configure a `MidgeEngine`. The `Config` API
/// provides automatic parameter derivation based on workload profiles and
/// simplifies database configuration.
///
/// # Examples
///
/// ```ignore
/// use midge::config::{Config, WorkloadProfile};
///
/// let config = Config::default()
///     .with_workload_profile(WorkloadProfile::ReadHeavy)
///     .with_target_throughput_mb_per_sec(100.0);
///
/// let engine = MidgeEngine::open_with_config(config)?;
/// ```
///
/// # Compatibility
///
/// Both `open_with_config()` and `open()` are first-class APIs:
/// - **`open_with_config()`** - High-level, automatic tuning (recommended)
/// - **`open()`** - Low-level, explicit control
///
/// Both APIs are fully supported and can be used interchangeably.
pub fn open_with_config(config: crate::config::Config) -> MidgeResult<MidgeEngine> {
    // Convert high-level Config to low-level MidgeOptions
    let opts = config.to_options();

    // Open engine with derived options
    open(opts)
}

/// Open or create a database with the specified storage mode.
///
/// Supports in-memory, local disk, and cloud-backed storage modes.
///
/// **Note:** Consider using [`open_with_config()`] for the
/// new high-level configuration API with automatic parameter derivation.
pub fn open(opts: crate::MidgeOptions) -> MidgeResult<MidgeEngine> {
    // Validate configuration before opening
    opts.validate()
        .map_err(crate::error::MidgeError::invalid_config)?;

    let mem_mode = matches!(opts.storage_mode, crate::StorageMode::Memory);

    // Precompute db path and sst dir so we can create an FS-backed writer factory
    let db_path = opts.storage_mode.local_path();
    let sst_dir = db_path.join("sst");

    tracing::debug!("opening engine at db_path={}", db_path.display());

    // Ensure sst directory exists before constructing filesystem-backed factories.
    // Some environments (tests/ephemeral dirs) may not have the parent path yet,
    // and creating the FsSstFactory before the directory exists can cause
    // subsequent file creation to fail with NotFound. Try to create it here and
    // log a warning on failure.
    // Skip directory creation entirely in memory mode.
    if !mem_mode {
        if let Err(e) = std::fs::create_dir_all(&sst_dir) {
            tracing::warn!("failed to create sst dir {}: {}", sst_dir.display(), e);
        }
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
            Box::new(crate::sst::fs::FsSstFactory::new_with_hooks(
                sst_dir.clone(),
                opts.test_hooks.clone(),
            ))
        };

    let (sst_reader_factory, wal_factory): (
        Box<dyn crate::sst::SstReaderFactory>,
        Box<dyn crate::wal::WalFactory>,
    ) = if mem_mode {
        (
            Box::new(crate::sst::mem::MemSstReaderFactory::new(
                opts.paranoid_checksums,
            )),
            Box::new(crate::wal::MemWalFactory),
        )
    } else if let Some(cloud_backend) = opts.storage_mode.cloud_backend() {
        // Use CloudSstReaderFactory for cloud-backed mode
        (
            Box::new(crate::sst::cloud::CloudSstReaderFactory::new_with_paranoid(
                cloud_backend,
                opts.paranoid_checksums,
            )),
            Box::new(crate::wal::FsWalFactory::new()),
        )
    } else {
        (
            Box::new(crate::sst::fs::FsSstReaderFactory::new(
                opts.paranoid_checksums,
            )),
            Box::new(crate::wal::FsWalFactory::new()),
        )
    };

    open_with_factories(opts, sst_factory, sst_reader_factory, wal_factory, mem_mode)
}

/// Open with a provided `SstFactory` implementation.
pub fn open_with_factories(
    opts: crate::MidgeOptions,
    sst_factory: Box<dyn crate::sst::SstFactory>,
    sst_reader_factory: Box<dyn crate::sst::SstReaderFactory>,
    wal_factory: Box<dyn crate::wal::WalFactory>,
    mem_mode: bool,
) -> MidgeResult<MidgeEngine> {
    let db_path = opts.storage_mode.local_path();
    let wal_dir = db_path.join("wal");
    tracing::debug!(
        "open_with_factories db_path={} wal_dir={}",
        db_path.display(),
        wal_dir.display()
    );
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
    tracing::debug!("acquiring db lock...");
    let db_lock =
        crate::core::engine::factory::acquire_db_lock(&db_path, opts.read_only, mem_mode)?;
    tracing::debug!("acquired db lock (or running read-only/mem)");
    tracing::debug!("initializing manifest...");
    let (manifest, max_cf_id) = crate::core::engine::factory::init_manifest(
        &db_path,
        opts.read_only,
        opts.memtable_size,
        mem_mode,
    )?;
    tracing::debug!(
        "manifest initialized: last_seq={}",
        manifest.last_persisted_sequence
    );
    tracing::debug!("replaying WAL segments (if any)...");
    let cf_set_arc = Arc::new(ColumnFamilySet::new());
    crate::core::engine::factory::init_column_families(&manifest, &cf_set_arc, max_cf_id)?;

    // Replay WAL and setup WAL writer
    tracing::debug!(
        "replay_local_wal_segments start (wal_dir={})",
        wal_dir.display()
    );
    let max_replay_seq = crate::core::engine::factory::replay_local_wal_segments(
        &wal_dir,
        &cf_set_arc,
        manifest.last_persisted_sequence,
        opts.wal_recovery_mode,
        mem_mode,
    )?;
    tracing::debug!(
        "replay_local_wal_segments done, max_replay_seq={}",
        max_replay_seq
    );
    let (wal_writer_box, max_replay_seq) = crate::core::engine::factory::setup_wal_writer(
        &opts,
        &wal_dir,
        &db_path,
        &cf_set_arc,
        &manifest,
        max_replay_seq,
    )?;

    // Setup directories and factories
    let sst_dir = db_path.join("sst");
    tracing::debug!("creating sst dir: {}", sst_dir.display());
    if !mem_mode {
        std::fs::create_dir_all(&sst_dir)?;
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

    // Initialize manifest cache for fast read access (with test hooks if provided)
    let manifest_cache = Arc::new(crate::sst::ManifestCache::new_with_hooks(
        db_path.clone(),
        opts.test_hooks.clone(),
    )?);

    // Create callback to update manifest cache after flush completes
    let manifest_cache_for_flush = Arc::clone(&manifest_cache);
    let manifest_update_callback = Arc::new(move |manifest: crate::core::manifest::Manifest| {
        manifest_cache_for_flush.update(manifest);
    });

    // Initialize VersionSet and VersionManager for lock-free manifest access
    // Must be created before compaction coordinator so it can be passed to workers
    let current_manifest_for_version = manifest_cache.get();
    let version_set = crate::core::manifest::VersionSet::new(current_manifest_for_version);
    let version_set_atomic = crate::core::manifest::AtomicVersionSet::new(version_set);
    let version_manager = Arc::new(crate::core::manifest::VersionManager::new(
        version_set_atomic.clone(),
        db_path.clone(),
        opts.test_hooks.clone(),
        mem_mode,
    ));

    // Delegate flush and compaction coordinator setup to factory module
    // Shared background error container used by background workers to report errors
    let background_error = Arc::new(parking_lot::RwLock::new(None));

    let (flush_coordinator, flush_handle) = crate::core::engine::factory::setup_flush_coordinator(
        &opts,
        sst_factory_arc.clone(),
        sst_dir.clone(),
        &db_path,
        metrics_arc.clone(),
        cloud_sst_manager.clone(),
        mem_mode,
        Some(manifest_update_callback),
        Some(background_error.clone()),
    )?;

    let (compaction_coordinator, compaction_handle) =
        match crate::core::engine::factory::setup_compaction_coordinator(
            &opts,
            &db_path,
            sst_dir.clone(),
            sst_factory_arc.clone(),
            sst_reader_factory_arc.clone(),
            snapshot_registry_arc.clone(),
            metrics_arc.clone(),
            cf_set_arc.clone(),
            version_manager.clone(),
            Some(background_error.clone()),
        )? {
            Some((coord, handle)) => (Some(coord), Some(handle)),
            None => (None, None),
        };
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
    tracing::debug!("creating wal coordinator");
    let wal_coordinator = crate::wal::WalController::new(wal_writer_box, wal_factory_arc);

    // Create centralized runtime for background workers
    let (shutdown_tx, shutdown_rx) = crossbeam::channel::unbounded();
    let mut runtime = crate::core::runtime::EngineRuntime::new(shutdown_tx, shutdown_rx);

    // Register flush coordinator with runtime
    runtime.set_flush_coordinator(flush_handle);

    // Register compaction coordinator with runtime if enabled
    if let Some(handle) = compaction_handle {
        runtime.set_compaction(handle);
    }

    let runtime = Arc::new(runtime);

    Ok(MidgeEngine {
        wal_coordinator,
        cf_set: cf_set_arc,
        seq: AtomicU64::new(max_replay_seq),
        txn_id: AtomicU64::new(0),
        txn_manager: crate::core::transaction::TransactionController::new(),
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
        wait_for_cloud_wal_uploads_on_sync: match &opts.storage_mode {
            crate::StorageMode::CloudBacked { local_wal_sync, .. } => !(*local_wal_sync),
            _ => true,
        },
        snapshot_registry: snapshot_registry_arc,
        block_cache: if opts.cache_size_mb > 0 {
            Some(crate::sst::create_basic_cache(
                opts.cache_size_mb * 1024 * 1024,
            ))
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
        performance_metrics: Arc::new(crate::metrics::PerformanceMetrics::new()),
        flush_coordinator,
        compaction_coordinator,
        merge_operators: RwLock::new(HashMap::new()),
        cloud_sst_manager,
        db_lock,
        is_read_only: AtomicBool::new(opts.read_only),
        flush_mutex: Mutex::new(()),
        manifest_cache,
        bloom_cache,
        sparse_index_cache,
        autotuner: opts.autotuner.clone(),
        test_hooks: opts.test_hooks.clone(),
        engine_flags: opts.engine_flags,
        version_set: version_set_atomic,
        version_manager,
        background_error: background_error.clone(),
        runtime,
    })
}
