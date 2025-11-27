//! Factory module for constructing MidgeEngine instances.
//!
//! This module encapsulates all the complex initialization logic required to build
//! a MidgeEngine, including:
//! - Database lock acquisition
//! - Manifest initialization and CF setup
//! - WAL replay and writer setup
//! - Flush and compaction coordinator setup
//! - Bloom filter and manifest cache initialization
//!
//! By separating construction from core engine operations, this module makes the
//! codebase more maintainable and easier to test.

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::column_family::{
    ColumnFamilyConfig, ColumnFamilyId, DEFAULT_CF_ID, DEFAULT_CF_NAME,
};
use crate::common::error::{MidgeError, MidgeResult};
use crate::core::engine::column_family::ColumnFamilySet;
use crate::core::engine::core::MidgeEngine;
use crate::core::manifest::Manifest;
use crate::core::persistence::flush::FlushWorkerConfig;
use crate::metrics::Metrics;
use crate::wal::WalFile;

/// Acquire database lock to prevent concurrent writers.
/// Returns None if running in read-only or memory mode (no lock needed).
pub(crate) fn acquire_db_lock(
    db_path: &Path,
    read_only: bool,
    mem_mode: bool,
) -> MidgeResult<Option<Box<dyn crate::core::locking::DbLock>>> {
    if !read_only && !mem_mode {
        let ttl_ms = 5000u32; // 5 second TTL
        let mut lock = crate::core::locking::create_local_lock(db_path, ttl_ms);
        lock.try_acquire(std::time::Duration::from_secs(10))
            .map_err(|e| MidgeError::InvalidConfig {
                message: format!("Failed to acquire database lock: {}", e),
            })?;
        Ok(Some(lock))
    } else {
        Ok(None)
    }
}

/// Initialize manifest, ensuring default CF exists.
/// Returns the manifest and the maximum CF ID for next_cf_id tracking.
pub(crate) fn init_manifest(
    db_path: &Path,
    read_only: bool,
    memtable_size: usize,
    mem_mode: bool,
) -> MidgeResult<(Manifest, u32)> {
    let mut manifest = Manifest::load(db_path).unwrap_or_default();

    // Initialize sequence allocators from manifest
    crate::core::naming::initialize_sequences(&manifest);

    // Ensure default CF is in manifest (for new DBs)
    if !manifest.has_cf(DEFAULT_CF_ID) {
        let default_cf_config = ColumnFamilyConfig {
            memtable_max_bytes: memtable_size,
            ..ColumnFamilyConfig::default()
        };
        manifest.add_cf(
            DEFAULT_CF_ID,
            DEFAULT_CF_NAME.to_string(),
            Some(default_cf_config),
        );
        // Save manifest with default CF for new DBs (skip in memory mode)
        if !read_only && !mem_mode {
            tracing::debug!("saving manifest to {}", db_path.display());
            match manifest.save_atomic(db_path) {
                Ok(_) => tracing::debug!("manifest.save_atomic succeeded"),
                Err(e) => {
                    tracing::debug!("manifest.save_atomic failed: {:?}", e);
                    return Err(e);
                }
            }
        }
    }

    let max_cf_id = manifest
        .column_families
        .iter()
        .map(|cf| cf.id)
        .max()
        .unwrap_or(0);

    Ok((manifest, max_cf_id))
}

/// Initialize column families from manifest.
/// Populates cf_set with all CFs from manifest and sets next_cf_id.
pub(super) fn init_column_families(
    manifest: &Manifest,
    cf_set: &ColumnFamilySet,
    max_cf_id: u32,
) -> MidgeResult<()> {
    for cf_meta in &manifest.column_families {
        let cf_id = ColumnFamilyId::new(cf_meta.id);
        let config = cf_meta.config.clone().unwrap_or_default();
        if cf_set.cfs.contains_key(&cf_id.as_u32()) {
            // CF already exists (e.g., default CF created in new())
            // Remove and recreate with correct config from manifest
            cf_set.cfs.remove(&cf_id.as_u32());
            cf_set.name_to_id.remove(&cf_meta.name);
        }
        cf_set.create_cf(cf_id, cf_meta.name.clone(), config)?;
    }

    cf_set.next_cf_id.store(max_cf_id + 1, Ordering::SeqCst);
    Ok(())
}

/// Replay local WAL segments newer than the last persisted sequence.
/// Returns the maximum sequence number seen during replay.
pub(super) fn replay_local_wal_segments(
    wal_dir: &Path,
    cf_set: &ColumnFamilySet,
    last_persisted_sequence: u64,
    wal_recovery_mode: crate::WalRecoveryMode,
    mem_mode: bool,
) -> MidgeResult<u64> {
    let mut max_replay_seq = last_persisted_sequence;

    if !mem_mode {
        let mut wal_segments: Vec<(u64, PathBuf)> = Vec::new();
        if wal_dir.exists() {
            for entry in std::fs::read_dir(wal_dir)? {
                let entry = entry?;
                let p = entry.path();
                if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                    if let Some(num_str) = name.strip_suffix(".wal") {
                        if let Ok(seq) = num_str.parse::<u64>() {
                            if seq > last_persisted_sequence {
                                wal_segments.push((seq, p.clone()));
                            }
                        }
                    }
                }
            }
            wal_segments.sort_by_key(|(seq, _)| *seq);
            for (_seq, path) in &wal_segments {
                if let Ok(recs) = crate::wal::fs::replay_wal_file_with_mode(path, wal_recovery_mode)
                {
                    // Skip records that were already flushed to SST
                    let replay_max = MidgeEngine::replay_wal_to_cfs_after_seq(
                        cf_set,
                        &recs,
                        last_persisted_sequence,
                    );
                    max_replay_seq = max_replay_seq.max(replay_max);
                }
            }
        }
    }

    Ok(max_replay_seq)
}

/// Setup WAL writer based on storage mode (read-only, cloud, or local).
/// Returns the WAL writer and the maximum sequence number seen during replay.
#[allow(clippy::too_many_arguments)]
pub(super) fn setup_wal_writer(
    opts: &crate::MidgeOptions,
    wal_dir: &Path,
    db_path: &Path,
    cf_set: &ColumnFamilySet,
    manifest: &Manifest,
    max_replay_seq_in: u64,
) -> MidgeResult<(Box<dyn crate::wal::WalWriter>, u64)> {
    let mut max_replay_seq = max_replay_seq_in;

    let wal_writer_box: Box<dyn crate::wal::WalWriter> = if opts.read_only {
        // In read-only mode, attempt to replay any existing WAL file
        if let Ok(latest_num) = crate::fs::find_latest_numbered_file(wal_dir, "wal") {
            if latest_num > 0 {
                let wal_path = crate::fs::numbered_file_path(wal_dir, latest_num, "wal");
                if let Ok(records) =
                    crate::wal::fs::replay_wal_file_with_mode(&wal_path, opts.wal_recovery_mode)
                {
                    let replay_max = MidgeEngine::replay_wal_to_cfs(cf_set, &records);
                    max_replay_seq = max_replay_seq.max(replay_max);
                }
            }
        }
        // Read-only mode uses in-memory WAL (no test hooks needed for memory)
        Box::new(crate::wal::WalMem::new())
    } else if let Some(cloud_backend) = opts.storage_mode.cloud_backend() {
        // Cloud-backed mode: first attempt to replay WAL segments from cloud
        let reader = crate::wal::cloud::CloudWalReader::new(Arc::clone(&cloud_backend));
        if let Ok(segment_ids) = reader.list_segments() {
            for seg_id in segment_ids {
                if let Ok(seg) = reader.read_segment(seg_id) {
                    if seg.sequence > manifest.last_persisted_sequence {
                        for rec in &seg.records {
                            let replay_max =
                                MidgeEngine::replay_wal_to_cfs(cf_set, std::slice::from_ref(rec));
                            max_replay_seq = max_replay_seq.max(replay_max);
                        }
                    }
                }
            }
        }

        let wal_batch_size = match &opts.storage_mode {
            crate::StorageMode::CloudBacked { wal_batch_size, .. } => *wal_batch_size,
            _ => 2_000_000,
        };

        let manifest_arc = Arc::new(parking_lot::Mutex::new(manifest.clone()));
        let cloud_wal = crate::wal::cloud::CloudWalWriter::with_test_hooks(
            cloud_backend,
            wal_batch_size,
            Some(manifest_arc),
            Some(db_path.to_path_buf()),
            opts.test_hooks.clone(),
        );

        // Replay any local WAL files that haven't been uploaded yet
        if wal_dir.exists() {
            // First replay numbered WAL files
            if let Ok(latest_num) = crate::fs::find_latest_numbered_file(wal_dir, "wal") {
                if latest_num > 0 {
                    let wal_path = crate::fs::numbered_file_path(wal_dir, latest_num, "wal");
                    if let Ok(records) =
                        crate::wal::fs::replay_wal_file_with_mode(&wal_path, opts.wal_recovery_mode)
                    {
                        let replay_max = MidgeEngine::replay_wal_to_cfs(cf_set, &records);
                        max_replay_seq = max_replay_seq.max(replay_max);
                    }
                }
            }

            // Also replay the active WAL file if it exists
            let active_wal_path = wal_dir.join("wal.log");
            if active_wal_path.exists() {
                if let Ok(records) = crate::wal::fs::replay_wal_file_with_mode(
                    &active_wal_path,
                    opts.wal_recovery_mode,
                ) {
                    let replay_max = MidgeEngine::replay_wal_to_cfs(cf_set, &records);
                    max_replay_seq = max_replay_seq.max(replay_max);
                }
            }
        }

        let async_cloud_wal = cloud_wal;
        Box::new(async_cloud_wal)
    } else {
        // Replay existing WAL files before opening a new one
        // Skip records that were already flushed to SST (based on manifest.last_persisted_sequence)
        let last_persisted = manifest.last_persisted_sequence;
        if wal_dir.exists() {
            if let Ok(latest_num) = crate::fs::find_latest_numbered_file(wal_dir, "wal") {
                if latest_num > 0 {
                    let wal_path = crate::fs::numbered_file_path(wal_dir, latest_num, "wal");
                    if let Ok(records) =
                        crate::wal::fs::replay_wal_file_with_mode(&wal_path, opts.wal_recovery_mode)
                    {
                        let replay_max = MidgeEngine::replay_wal_to_cfs_after_seq(
                            cf_set,
                            &records,
                            last_persisted,
                        );
                        max_replay_seq = max_replay_seq.max(replay_max);
                    }
                }
            }

            // Also replay the active WAL file if it exists
            let active_wal_path = wal_dir.join("wal.log");
            if active_wal_path.exists() {
                if let Ok(records) = crate::wal::fs::replay_wal_file_with_mode(
                    &active_wal_path,
                    opts.wal_recovery_mode,
                ) {
                    let replay_max =
                        MidgeEngine::replay_wal_to_cfs_after_seq(cf_set, &records, last_persisted);
                    max_replay_seq = max_replay_seq.max(replay_max);
                }
            }
        }

        // Local filesystem WAL with test hooks if configured
        let mut wal = WalFile::open(wal_dir)?;
        wal.test_hooks = opts.test_hooks.clone();
        Box::new(wal)
    };

    Ok((wal_writer_box, max_replay_seq))
}

/// Setup flush coordinator.
/// Even in read-only mode, creates a coordinator (though it won't process jobs).
/// Returns (coordinator, worker_handle) tuple for runtime registration.
#[allow(clippy::too_many_arguments)]
pub(crate) fn setup_flush_coordinator(
    opts: &crate::MidgeOptions,
    sst_factory_arc: Arc<dyn crate::sst::SstFactory>,
    sst_dir: PathBuf,
    db_path: &Path,
    metrics_arc: Arc<Metrics>,
    cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
    mem_mode: bool,
    manifest_update_callback: Option<Arc<dyn Fn(crate::core::manifest::Manifest) + Send + Sync>>,
    background_error: Option<Arc<parking_lot::RwLock<Option<crate::error::MidgeError>>>>,
) -> MidgeResult<(
    crate::core::FlushCoordinator,
    crate::core::runtime::WorkerHandle,
)> {
    let config = FlushWorkerConfig {
        sst_factory: sst_factory_arc,
        sst_dir,
        wal_dir: db_path.join("wal"),
        db_path: db_path.to_path_buf(),
        compression: opts.compression,
        block_size: opts.block_size,
        mem_mode,
        cloud_sst_manager,
        metrics: metrics_arc,
        test_hooks: opts.test_hooks.clone(),
        manifest_update_callback,
        background_error,
    };
    crate::core::FlushCoordinator::spawn(config)
}

/// Setup compaction coordinator if compaction is enabled and not in read-only mode.
/// Returns Option<(coordinator, worker_handle)> tuple for runtime registration.
#[allow(clippy::too_many_arguments)]
pub(crate) fn setup_compaction_coordinator(
    opts: &crate::MidgeOptions,
    db_path: &Path,
    sst_dir: PathBuf,
    sst_factory_arc: Arc<dyn crate::sst::SstFactory>,
    sst_reader_factory_arc: Arc<dyn crate::sst::SstReaderFactory>,
    snapshot_registry_arc: Arc<crate::api::snapshot::SnapshotRegistry>,
    metrics_arc: Arc<Metrics>,
    cf_set_arc: Arc<super::column_family::ColumnFamilySet>,
    version_manager: Arc<crate::core::manifest::VersionManager>,
    background_error: Option<Arc<parking_lot::RwLock<Option<crate::error::MidgeError>>>>,
) -> MidgeResult<
    Option<(
        crate::core::CompactionController,
        crate::core::runtime::WorkerHandle,
    )>,
> {
    if opts.enable_compaction && !opts.read_only {
        // Create CloudSstManager if in cloud-backed mode
        let cloud_sst_manager_c = if let Some(cloud_backend) = opts.storage_mode.cloud_backend() {
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

        let compactor = crate::core::compaction::Compactor::with_config(
            crate::core::compaction::LeveledCompactionConfig {
                l0_compaction_threshold: opts.compaction_sst_threshold,
                level_multiplier: opts.level_multiplier,
                l1_target_size: 10 * 1024 * 1024,
                max_levels: opts.max_levels,
            },
        );

        let config = crate::core::compaction::CompactionWorkerConfig {
            db_path: db_path.to_path_buf(),
            sst_dir,
            sst_factory: sst_factory_arc,
            sst_reader_factory: sst_reader_factory_arc,
            snapshot_registry: snapshot_registry_arc,
            metrics: metrics_arc,
            compression: opts.compression,
            block_size: opts.block_size,
            ttl_seconds: if opts.ttl_seconds > 0 {
                Some(opts.ttl_seconds)
            } else {
                None
            },
            tombstone_density_threshold: opts.tombstone_density_threshold,
            max_tombstone_compaction_files: opts.max_tombstone_compaction_files,
            check_interval_ms: opts.compaction_check_interval_ms,
            cloud_sst_manager: cloud_sst_manager_c,
            compactor,
            cf_set: cf_set_arc,
            test_hooks: opts.test_hooks.clone(),
            version_manager,
            background_error,
        };

        Ok(Some(crate::core::CompactionController::spawn(config)?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn should_acquire_lock_in_writable_mode() {
        // Arrange

        // Act
        let temp_dir = TempDir::new().unwrap();
        let lock = acquire_db_lock(temp_dir.path(), false, false);

        // Assert
        assert!(lock.is_ok());
        assert!(lock.unwrap().is_some());
    }

    #[test]
    fn should_not_acquire_lock_in_read_only_mode() {
        // Arrange

        // Act
        let temp_dir = TempDir::new().unwrap();
        let lock = acquire_db_lock(temp_dir.path(), true, false);

        // Assert
        assert!(lock.is_ok());
        assert!(lock.unwrap().is_none());
    }

    #[test]
    fn should_not_acquire_lock_in_memory_mode() {
        // Arrange

        // Act
        let temp_dir = TempDir::new().unwrap();
        let lock = acquire_db_lock(temp_dir.path(), false, true);

        // Assert
        assert!(lock.is_ok());
        assert!(lock.unwrap().is_none());
    }

    #[test]
    fn should_initialize_manifest_with_default_cf() {
        // Arrange

        // Act
        let temp_dir = TempDir::new().unwrap();
        let (manifest, max_cf_id) =
            init_manifest(temp_dir.path(), false, 64 * 1024 * 1024, false).unwrap();

        // Assert
        assert!(manifest.has_cf(DEFAULT_CF_ID));
        assert_eq!(max_cf_id, 0);
    }

    #[test]
    fn should_load_existing_manifest() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();

        // Create and save a manifest
        let mut manifest = Manifest::default();
        manifest.add_cf(
            DEFAULT_CF_ID,
            DEFAULT_CF_NAME.to_string(),
            Some(ColumnFamilyConfig::default()),
        );
        manifest.add_cf(
            ColumnFamilyId::new(1),
            "custom_cf".to_string(),
            Some(ColumnFamilyConfig::default()),
        );
        manifest.save_atomic(temp_dir.path()).unwrap();

        // Act
        // Load it back
        let (loaded_manifest, max_cf_id) =
            init_manifest(temp_dir.path(), false, 64 * 1024 * 1024, false).unwrap();

        // Assert
        assert!(loaded_manifest.has_cf(DEFAULT_CF_ID));
        assert!(loaded_manifest.has_cf(ColumnFamilyId::new(1)));
        assert_eq!(max_cf_id, 1);
    }

    #[test]
    fn should_initialize_column_families_from_manifest() {
        // Arrange
        let _temp_dir = TempDir::new().unwrap();
        let mut manifest = Manifest::default();
        manifest.add_cf(
            DEFAULT_CF_ID,
            DEFAULT_CF_NAME.to_string(),
            Some(ColumnFamilyConfig::default()),
        );
        manifest.add_cf(
            ColumnFamilyId::new(1),
            "custom_cf".to_string(),
            Some(ColumnFamilyConfig::default()),
        );

        let cf_set = ColumnFamilySet::new();

        // Act
        let result = init_column_families(&manifest, &cf_set, 1);

        // Assert
        assert!(result.is_ok());
        assert!(cf_set.cfs.contains_key(&DEFAULT_CF_ID.as_u32()));
        assert!(cf_set.cfs.contains_key(&1));
        assert_eq!(cf_set.next_cf_id.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn should_return_last_persisted_seq_when_no_wal_files() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let cf_set = ColumnFamilySet::new();

        // Act
        let max_seq = replay_local_wal_segments(
            &wal_dir,
            &cf_set,
            100,
            crate::WalRecoveryMode::TolerateCorruptedTail,
            false,
        )
        .unwrap();

        // Assert
        assert_eq!(max_seq, 100);
    }

    #[test]
    fn should_skip_replay_in_memory_mode() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");

        let cf_set = ColumnFamilySet::new();

        // Act
        let max_seq = replay_local_wal_segments(
            &wal_dir,
            &cf_set,
            100,
            crate::WalRecoveryMode::TolerateCorruptedTail,
            true, // mem_mode = true
        )
        .unwrap();

        // Assert
        assert_eq!(max_seq, 100);
    }

    #[test]
    fn should_not_write_manifest_in_memory_mode() {
        // Arrange
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path();

        // Act
        let (manifest, max_cf_id) = init_manifest(db_path, false, 64 * 1024 * 1024, true).unwrap();

        // Assert
        assert!(manifest.has_cf(DEFAULT_CF_ID)); // Manifest initialized in memory
        assert_eq!(max_cf_id, 0);
        assert!(!db_path.join("manifest.json").exists()); // No file written
    }
}
