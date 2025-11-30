//! Flush job processing and WAL pruning.
//!
//! Contains the core logic for processing flush jobs:
//! - Writing entries to SST files
//! - Updating the manifest
//! - Pruning old WAL files
//! - Cloud upload coordination

use std::path::Path;

use crate::common::test_hooks::FlushGatePoint;
use crate::core::manifest::Manifest;
use crate::error::MidgeResult;

use super::bounds::compute_bounds;
use super::stats::FlushStats;
use super::worker::{FlushJob, FlushWorkerConfig};

/// Process a single flush job: write entries to SST, update manifest, clean up WAL.
pub(crate) fn process_flush_job(config: &FlushWorkerConfig, job: FlushJob) -> MidgeResult<()> {
    // Start timing for throughput measurement
    let flush_start = std::time::Instant::now();

    // Record memtable flush metric for background-flush path as well
    config.metrics.record_memtable_flush();

    let cf_id = job.cf_id;
    let seq_for_prune = job.seq;

    // Compute flush statistics using extracted helper
    let stats = FlushStats::compute(&job.entries, &job.range_tombstones);

    let entries = job.entries;
    let range_tombstones = job.range_tombstones;

    // Compute file metadata (bounds and seq range) from drained entries
    let (smallest_key, largest_key, smallest_seq, largest_seq) =
        compute_bounds(&entries, &range_tombstones);

    // Allocate SST sequence number for this CF FIRST (before creating writer)
    let sst_seq = crate::core::naming::allocate_sst_seq(cf_id);

    // Create SST writer with sequence for deterministic temp file naming
    let mut writer =
        config
            .sst_factory
            .create_with_seq(config.compression, config.block_size, true, sst_seq)?;

    // Add entries with metadata (sequence, tombstone, and expiration)
    for entry in entries {
        writer.add_with_meta(
            &entry.key,
            entry.value.as_deref(),
            entry.sequence,
            entry.op_type.as_u8(),
            entry.expiration_millis,
        )?;
    }

    // Add range tombstones captured during flush
    for (start, end, seq) in range_tombstones {
        writer.add_range_tombstone(&start, &end, seq)?;
    }

    // Finish and persist (streaming writer will write directly to disk)
    // Format: dbpath/sst/{cf_id}/{sst_seq}.sst
    let sst_name = format!(
        "{}/{}",
        crate::core::naming::pad_cf_id(cf_id.as_u32()),
        crate::core::naming::sst_filename(sst_seq)
    );
    let sst_path = config.sst_dir.join(&sst_name);
    if let Some(parent) = sst_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Prefer streaming finish_to_path; default implementation will write bytes
    let boxed_writer = Box::new(writer);
    boxed_writer.finish_to_path(&sst_path)?;

    // Update manifest FIRST to establish the safe deletion point
    if let Some(ref hooks) = config.test_hooks {
        hooks.maybe_pause_flush(FlushGatePoint::BeforeManifestUpdate);
    }

    // Load manifest (or use default in memory mode since no disk persistence)
    let mut m = if config.mem_mode {
        Manifest::default()
    } else {
        Manifest::load_with_retry(&config.db_path, 10, std::time::Duration::from_millis(10))?
    };
    tracing::debug!(target:"midge.instrument", action="bg_flush_before_manifest_update", fname = %sst_name, seq_for_prune, largest_seq = largest_seq, smallest_seq = smallest_seq, current_manifest_seq = m.last_persisted_sequence);
    // Use largest_seq from entries (which includes resolved merge operations)
    // instead of seq_for_prune (which is the WAL rotation sequence)
    // Fall back to seq_for_prune if no entries were flushed
    m.last_persisted_sequence = largest_seq.unwrap_or(seq_for_prune);
    tracing::debug!(target:"midge.instrument", action="bg_flush_after_manifest_seq_set", new_manifest_seq = m.last_persisted_sequence, file = %sst_name);
    // Get file size (skip filesystem access in memory mode)
    let size_bytes = if config.mem_mode {
        0 // Size not relevant for in-memory SSTs
    } else {
        std::fs::metadata(&sst_path).map(|md| md.len()).unwrap_or(0)
    };

    // Assign sublevel based on overlap with existing L0 files
    let sublevel = if let (Some(sk), Some(lk)) = (&smallest_key, &largest_key) {
        m.assign_l0_sublevel(sk, lk)
    } else {
        0 // If no key bounds, assign to oldest sublevel
    };

    // Clone key bounds for cloud upload (before moving into FileMeta)
    let key_range_for_upload = (smallest_key.clone(), largest_key.clone());
    let seq_range_for_upload = (smallest_seq, largest_seq);

    let sst_name = format!(
        "{}/{}",
        crate::core::naming::pad_cf_id(cf_id.as_u32()),
        crate::core::naming::sst_filename(sst_seq)
    );
    m.ssts.push(sst_name.clone());
    m.files.push(crate::core::manifest::FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes,
        cf_id: cf_id.as_u32(),
        sst_seq,
        smallest_key,
        largest_key,
        smallest_seq,
        largest_seq,
        sublevel,
        cloud_location: None,
        cloud_checksum: None,
        cloud_uploaded_at: None,
        cloud_state: None,
        point_tombstone_count: stats.point_tombstone_count,
        range_tombstone_count: stats.range_tombstone_count,
        total_entries: stats.total_entries,
    });

    // Save manifest (skip in memory mode)
    // Update manifest with current sequence allocators before saving
    m.next_wal_seq = crate::core::naming::current_next_wal_seq();
    m.next_sst_seqs = crate::core::naming::current_next_sst_seqs();

    if !config.mem_mode {
        tracing::info!(
            "persisting manifest after creating SST {}",
            sst_path.display()
        );
        // Use test hooks if provided so background flush can inject failures
        if let Some(ref hooks) = config.test_hooks {
            m.save_atomic_with_hooks(&config.db_path, Some(hooks))?;
        } else {
            m.save_atomic(&config.db_path)?;
        }
        tracing::debug!(target:"midge.instrument", action="bg_flush_after_manifest_persist", file = %sst_name, manifest_seq = m.last_persisted_sequence, file_count = m.files.len());
        tracing::info!("manifest persisted successfully");
    }

    // Update engine's cached manifest so reads can immediately see the new SST
    if let Some(ref callback) = config.manifest_update_callback {
        tracing::info!(
            "invoking manifest update callback with {} files",
            m.files.len()
        );
        callback(m.clone());
        tracing::info!("manifest update callback completed");
    } else {
        tracing::warn!("no manifest update callback configured!");
    }

    // Upload SST to cloud if cloud manager is configured
    if let Some(cloud_manager) = &config.cloud_sst_manager {
        spawn_cloud_upload(
            cloud_manager.clone(),
            sst_name.clone(),
            sst_path.clone(),
            seq_range_for_upload,
            key_range_for_upload,
            config.test_hooks.clone(),
        );
    }

    // Prune old WAL files AFTER manifest is updated (fs mode only)
    // In cloud mode, WAL pruning is coordinated via cloud_checkpoint
    // to ensure WAL is only deleted after SSTs are uploaded to cloud
    if !config.mem_mode {
        let safe_seq = determine_safe_prune_sequence(&m);
        tracing::info!("pruning WAL files up to sequence {}", safe_seq);
        prune_old_wal_files(&config.wal_dir, safe_seq)?;
        tracing::info!("prune_old_wal_files completed");
    }

    // Record flush throughput metrics
    let flush_duration_us = flush_start.elapsed().as_micros() as u64;
    config
        .metrics
        .record_flush_throughput(stats.total_bytes, flush_duration_us);

    Ok(())
}

/// Spawn cloud upload in a separate guarded thread.
///
/// This is extracted from process_flush_job to:
/// - Keep the core correctness path clean
/// - Isolate cloud upload concerns
/// - Enable easier testing of upload behavior
fn spawn_cloud_upload(
    cloud_manager: std::sync::Arc<crate::sst::cloud::CloudSstManager>,
    sst_id: String,
    sst_path: std::path::PathBuf,
    seq_range: (Option<u64>, Option<u64>),
    key_range: (Option<Vec<u8>>, Option<Vec<u8>>),
    test_hooks: Option<crate::common::test_hooks::TestHooks>,
) {
    let sequence_range = (seq_range.0.unwrap_or(0), seq_range.1.unwrap_or(0));
    let key_range_vec = (key_range.0.map(|k| k.to_vec()), key_range.1.map(|k| k.to_vec()));

    // Use the centralized guarded spawn helper so panics are converted to
    // TestHooks notifications instead of unwinding into the test harness.
    let _handle = crate::common::worker::spawn_guarded(
        "cloud-upload",
        test_hooks,
        move || {
            if let Err(e) = cloud_manager.upload_sst_async(
                sst_id,
                sst_path,
                sequence_range,
                key_range_vec,
                None,
            ) {
                tracing::error!("Failed to upload SST to cloud: {}", e);
            }
        },
        None::<fn(Box<dyn std::any::Any + Send>)>,
    );
}

/// Determine the safe sequence number for WAL pruning.
///
/// In local disk mode: Uses `last_persisted_sequence` (SST written to disk)
/// In cloud mode: Uses `cloud_checkpoint.checkpoint_sequence` (SST uploaded to cloud)
///
/// This ensures WAL files are only deleted after their data is durably persisted
/// in the appropriate storage tier.
pub fn determine_safe_prune_sequence(manifest: &Manifest) -> u64 {
    // If cloud checkpoint exists, use it (cloud mode)
    // This means we only prune WAL after SSTs are uploaded to cloud
    if let Some(checkpoint) = &manifest.cloud_checkpoint {
        checkpoint.checkpoint_sequence
    } else {
        // No cloud checkpoint: use local persistence (disk mode)
        manifest.last_persisted_sequence
    }
}

/// Prune WAL files that have been safely persisted to SSTs.
///
/// Scans the WAL directory for all WAL files matching the pattern
/// `NNNNNNNNNNNNNNNNNNNN.wal` (20-digit zero-padded) and deletes any where
/// the sequence number is less than or equal to the safe sequence.
/// This ensures we don't accumulate WAL files indefinitely.
///
/// **Cloud Mode Safety**: In cloud-backed storage, WAL files are only pruned
/// after their corresponding SSTs are uploaded to cloud storage. This is
/// coordinated via the manifest's `cloud_checkpoint` field.
///
/// # Arguments
/// * `wal_dir` - Directory containing WAL files
/// * `safe_sequence` - Highest sequence number safely persisted (to disk or cloud)
///
/// # Returns
/// * `Ok(count)` - Number of WAL files deleted
/// * `Err(_)` - If directory scan fails
pub fn prune_old_wal_files(wal_dir: &Path, safe_sequence: u64) -> MidgeResult<usize> {
    let mut pruned_count = 0;

    // Scan directory for WAL files
    let entries = match std::fs::read_dir(wal_dir) {
        Ok(e) => e,
        Err(_) => return Ok(0), // Directory doesn't exist or can't be read
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => continue,
        };

        // Format: {seq}.wal (e.g., 1.wal, 42.wal)
        if filename.ends_with(".wal") && filename != "wal.log" {
            if let Some(seq_str) = filename.strip_suffix(".wal") {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    if seq <= safe_sequence {
                        match std::fs::remove_file(&path) {
                            Ok(_) => {
                                tracing::info!("pruned WAL file: {}", path.display());
                                pruned_count += 1;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "failed to prune WAL file {}: {}",
                                    path.display(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(pruned_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_determine_safe_prune_sequence_without_cloud_checkpoint() {
        // Arrange
        let manifest = Manifest {
            last_persisted_sequence: 100,
            cloud_checkpoint: None,
            ..Default::default()
        };

        // Act
        let safe_seq = determine_safe_prune_sequence(&manifest);

        // Assert
        assert_eq!(safe_seq, 100);
    }

    #[test]
    fn should_determine_safe_prune_sequence_with_cloud_checkpoint() {
        // Arrange
        let manifest = Manifest {
            last_persisted_sequence: 100,
            cloud_checkpoint: Some(crate::core::manifest::CloudCheckpoint {
                checkpoint_sequence: 50,
                covering_ssts: vec![],
                checkpoint_time: std::time::SystemTime::UNIX_EPOCH,
            }),
            ..Default::default()
        };

        // Act
        let safe_seq = determine_safe_prune_sequence(&manifest);

        // Assert
        assert_eq!(safe_seq, 50);
    }

    #[test]
    fn should_prune_old_wal_files_successfully() {
        // Arrange
        let temp_dir = tempfile::TempDir::new().unwrap();
        let wal_dir = temp_dir.path();
        std::fs::write(wal_dir.join("00000000000000000010.wal"), b"data").unwrap();
        std::fs::write(wal_dir.join("00000000000000000050.wal"), b"data").unwrap();
        std::fs::write(wal_dir.join("00000000000000000100.wal"), b"data").unwrap();

        // Act
        let result = prune_old_wal_files(wal_dir, 50);

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
        assert!(!wal_dir.join("00000000000000000010.wal").exists());
        assert!(!wal_dir.join("00000000000000000050.wal").exists());
        assert!(wal_dir.join("00000000000000000100.wal").exists());
    }

    #[test]
    fn should_return_zero_when_wal_dir_does_not_exist() {
        // Arrange
        let temp_dir = tempfile::TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("nonexistent");

        // Act
        let result = prune_old_wal_files(&wal_dir, 100);

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn should_not_prune_wal_files_above_safe_sequence() {
        // Arrange
        let temp_dir = tempfile::TempDir::new().unwrap();
        let wal_dir = temp_dir.path();
        std::fs::write(wal_dir.join("00000000000000000100.wal"), b"data").unwrap();
        std::fs::write(wal_dir.join("00000000000000000200.wal"), b"data").unwrap();

        // Act
        let result = prune_old_wal_files(wal_dir, 50);

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
        assert!(wal_dir.join("00000000000000000100.wal").exists());
        assert!(wal_dir.join("00000000000000000200.wal").exists());
    }
}
