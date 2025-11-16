//! Memtable flushing subsystem.
//!
//! This module handles the asynchronous flushing of memtable contents to on-disk SST files.
//! The flush process works in two phases:
//!
//! 1. **Memtable Rollover**: When the active memtable reaches capacity, the WAL is rotated
//!    and memtable entries are drained and sent to a background flush worker thread.
//!
//! 2. **Background Flush**: The worker thread writes the entries to a new SST file,
//!    updates the manifest, and removes the corresponding WAL file.
//!
//! This asynchronous design allows writes to continue with minimal latency while flushing
//! happens in the background.

use crossbeam::channel;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// Type alias for key bounds and sequence range tuple returned by compute_bounds.
/// (smallest_key, largest_key, smallest_seq, largest_seq)
type KeyBounds = (Option<Vec<u8>>, Option<Vec<u8>>, Option<u64>, Option<u64>);

use crate::common::codec::CompressionType;
use crate::common::test_hooks::FlushGatePoint;
use crate::core::manifest::Manifest;
use crate::error::{MidgeError, MidgeResult};
use crate::metrics::Metrics;

/// A batch of memtable entries to be flushed to an SST file.
///
/// Created during WAL rotation when the memtable is drained. Contains all the
/// key-value pairs and range tombstones that need to be persisted.
pub struct FlushJob {
    /// Column family ID that owns this flush job
    pub cf_id: crate::api::column_family::ColumnFamilyId,
    /// Sequence number of the rotated WAL segment
    pub seq: u64,
    /// Drained memtable entries: (key, value, sequence, is_tombstone)
    pub entries: Vec<crate::core::EntryMeta>,
    /// Range tombstones drained from the memtable
    pub range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)>,
}

/// Messages sent to the background flush worker thread.
pub(crate) enum FlushMsg {
    /// Request to flush a batch of entries
    Entries(FlushJob),
    /// Signal to gracefully shut down the worker
    Shutdown,
    /// Barrier: requester wants to be notified when all prior flush jobs are processed
    Barrier { reply: channel::Sender<()> },
}

/// Configuration for creating a flush worker thread.
pub struct FlushWorkerConfig {
    pub sst_factory: Arc<dyn crate::sst::SstFactory>,
    pub sst_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub db_path: PathBuf,
    pub compression: CompressionType,
    pub block_size: usize,
    pub mem_mode: bool,
    /// Optional cloud SST manager for uploading SSTs to cloud storage
    pub cloud_sst_manager: Option<Arc<crate::sst::cloud::CloudSstManager>>,
    /// Metrics collector to record memtable flushes from background worker
    pub metrics: Arc<Metrics>,
    /// Optional test hooks for deterministic coordination
    pub test_hooks: Option<crate::common::test_hooks::TestHooks>,
}

/// Spawn a background thread that processes flush jobs.
///
/// The worker thread listens for `FlushMsg::Entries` messages and writes them
/// to SST files, updating the manifest and cleaning up WAL files.
///
/// # Returns
/// * `rx` - Receiver channel for sending flush messages
/// * `handle` - Join handle for the background thread
pub(crate) fn spawn_flush_worker(
    config: FlushWorkerConfig,
) -> MidgeResult<(channel::Sender<FlushMsg>, JoinHandle<()>)> {
    let (tx, rx) = channel::unbounded::<FlushMsg>();

    let handle = thread::Builder::new()
        .name("midge-flush-worker".to_string())
        .spawn(move || {
            for msg in rx.iter() {
                match msg {
                    FlushMsg::Entries(job) => {
                        if job.entries.is_empty() {
                            continue;
                        }

                        // Process the flush job
                        let _ = process_flush_job(&config, job);
                    }
                    FlushMsg::Shutdown => break,
                    FlushMsg::Barrier { reply } => {
                        // Since messages are processed in-order, receiving the Barrier
                        // means all prior Entries have been processed. Acknowledge.
                        let _ = reply.send(());
                    }
                }
            }
        })
        .map_err(|e| MidgeError::internal(format!("Failed to spawn flush worker thread: {}", e)))?;

    Ok((tx, handle))
}

/// Struct for managing background flush worker.
pub struct FlushWorker;

/// Process a single flush job: write entries to SST, update manifest, clean up WAL.
fn process_flush_job(config: &FlushWorkerConfig, job: FlushJob) -> MidgeResult<()> {
    // Record memtable flush metric for background-flush path as well
    config.metrics.record_memtable_flush();

    let cf_id = job.cf_id;
    let seq_for_prune = job.seq;
    let entries = job.entries;
    let range_tombstones = job.range_tombstones;

    // Compute file metadata (bounds and seq range) from drained entries
    let (smallest_key, largest_key, smallest_seq, largest_seq) =
        compute_bounds(&entries, &range_tombstones);

    // Calculate tombstone counts for metrics
    let point_tombstone_count = entries.iter().filter(|e| e.is_tombstone).count() as u64;
    let range_tombstone_count = range_tombstones.len() as u64;
    let total_entries = entries.len() as u64;

    // Create SST writer and add all entries
    let mut writer = config
        .sst_factory
        .create(config.compression, config.block_size, true);

    // Add entries with metadata (sequence, tombstone, and expiration)
    for entry in entries {
        writer.add_with_meta(
            &entry.key,
            entry.value.as_deref(),
            entry.sequence,
            entry.is_tombstone,
            entry.expiration_millis,
        )?;
    }

    // Add range tombstones captured during flush
    for (start, end, seq) in range_tombstones {
        writer.add_range_tombstone(&start, &end, seq)?;
    }

    // Finish and persist (streaming writer will write directly to disk)
    // Format: {seq:020}_{cf_id:04}.sst for per-CF identification
    let fname = format!("{:020}_{:04}.sst", seq_for_prune, cf_id.as_u32());
    let sst_path = config.sst_dir.join(&fname);
    // Prefer streaming finish_to_path; default implementation will write bytes
    let boxed_writer = Box::new(writer);
    boxed_writer.finish_to_path(&sst_path)?;

    // Update manifest FIRST to establish the safe deletion point
    if let Some(ref hooks) = config.test_hooks {
        hooks.maybe_pause_flush(FlushGatePoint::BeforeManifestUpdate);
    }

    let mut m =
        Manifest::load_with_retry(&config.db_path, 10, std::time::Duration::from_millis(10))?;
    // Use largest_seq from entries (which includes resolved merge operations)
    // instead of seq_for_prune (which is the WAL rotation sequence)
    // Fall back to seq_for_prune if no entries were flushed
    m.last_persisted_sequence = largest_seq.unwrap_or(seq_for_prune);
    let size_bytes = std::fs::metadata(&sst_path).map(|md| md.len()).unwrap_or(0);

    // Assign sublevel based on overlap with existing L0 files
    let sublevel = if let (Some(sk), Some(lk)) = (&smallest_key, &largest_key) {
        m.assign_l0_sublevel(sk, lk)
    } else {
        0 // If no key bounds, assign to oldest sublevel
    };

    // Clone key bounds for cloud upload (before moving into FileMeta)
    let key_range_for_upload = (smallest_key.clone(), largest_key.clone());
    let seq_range_for_upload = (smallest_seq, largest_seq);

    m.ssts.push(fname.clone());
    m.files.push(crate::core::manifest::FileMeta {
        name: fname.clone(),
        level: 0,
        size_bytes,
        cf_id: cf_id.as_u32(),
        smallest_key,
        largest_key,
        smallest_seq,
        largest_seq,
        sublevel,
        cloud_location: None,
        cloud_checksum: None,
        cloud_uploaded_at: None,
        cloud_state: None,
        point_tombstone_count,
        range_tombstone_count,
        total_entries,
    });

    tracing::info!(
        "persisting manifest after creating SST {}",
        sst_path.display()
    );
    m.save_atomic(&config.db_path)?;
    tracing::info!("manifest persisted successfully");

    // Upload SST to cloud if cloud manager is configured
    if let Some(cloud_manager) = &config.cloud_sst_manager {
        let sst_id = fname.clone();
        let sequence_range = (
            seq_range_for_upload.0.unwrap_or(0),
            seq_range_for_upload.1.unwrap_or(0),
        );
        let key_range = (
            key_range_for_upload.0.map(|k| k.to_vec()),
            key_range_for_upload.1.map(|k| k.to_vec()),
        );

        // Spawn async upload (non-blocking)
        let cloud_manager_clone = cloud_manager.clone();
        let sst_path_clone = sst_path.clone();
        let sst_id_clone = sst_id.clone();

        std::thread::spawn(move || {
            if let Err(e) = cloud_manager_clone.upload_sst_async(
                sst_id_clone,
                sst_path_clone,
                sequence_range,
                key_range,
                None,
            ) {
                tracing::error!("Failed to upload SST to cloud: {}", e);
            }
        });
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

    Ok(())
}

/// Determine the safe sequence number for WAL pruning.
///
/// In local disk mode: Uses `last_persisted_sequence` (SST written to disk)
/// In cloud mode: Uses `cloud_checkpoint.checkpoint_sequence` (SST uploaded to cloud)
///
/// This ensures WAL files are only deleted after their data is durably persisted
/// in the appropriate storage tier.
fn determine_safe_prune_sequence(manifest: &Manifest) -> u64 {
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
fn prune_old_wal_files(wal_dir: &Path, safe_sequence: u64) -> MidgeResult<usize> {
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

        // Format: NNNNNNNNNNNNNNNNNNNN.wal (20-digit, e.g., 00000000000000000001.wal)
        if filename.ends_with(".wal") && filename.len() == 24 {
            if let Ok(seq) = filename[..20].parse::<u64>() {
                if seq <= safe_sequence {
                    match std::fs::remove_file(&path) {
                        Ok(_) => {
                            tracing::info!("pruned WAL file: {}", path.display());
                            pruned_count += 1;
                        }
                        Err(e) => {
                            tracing::warn!("failed to prune WAL file {}: {}", path.display(), e);
                        }
                    }
                }
            }
        }
    }

    Ok(pruned_count)
}

/// Compute key bounds and sequence range from entries and range tombstones.
#[inline]
pub(crate) fn compute_bounds(
    entries: &[crate::core::EntryMeta],
    range_tombstones: &[(Vec<u8>, Vec<u8>, u64)],
) -> KeyBounds {
    let mut smallest_key: Option<Vec<u8>> = None;
    let mut largest_key: Option<Vec<u8>> = None;
    let mut smallest_seq: Option<u64> = None;
    let mut largest_seq: Option<u64> = None;

    // Process entries (expiration is ignored for bounds computation)
    for entry in entries {
        // Entries may contain internal-key encoded keys; decode to user key
        let user_key = if let Some((u, _s, _t)) =
            crate::common::internal_key::decode_internal_key(&entry.key)
        {
            u
        } else {
            entry.key.clone()
        };

        if smallest_key.is_none() {
            smallest_key = Some(user_key.clone());
        }
        if largest_key.is_none() {
            largest_key = Some(user_key.clone());
        }

        if let Some(sk) = smallest_key.as_mut() {
            if user_key.as_slice() < sk.as_slice() {
                *sk = user_key.clone();
            }
        }
        if let Some(lk) = largest_key.as_mut() {
            if user_key.as_slice() > lk.as_slice() {
                *lk = user_key.clone();
            }
        }

        smallest_seq = Some(match smallest_seq {
            None => entry.sequence,
            Some(s) => std::cmp::min(s, entry.sequence),
        });
        largest_seq = Some(match largest_seq {
            None => entry.sequence,
            Some(s) => std::cmp::max(s, entry.sequence),
        });
    }

    // Process range tombstones
    for (start, end, seq) in range_tombstones {
        // Expand key bounds conservatively to include range endpoints
        if smallest_key.is_none()
            || start.as_slice()
                < smallest_key
                    .as_ref()
                    .expect("smallest_key checked above")
                    .as_slice()
        {
            smallest_key = Some(start.clone());
        }
        if largest_key.is_none()
            || end.as_slice()
                > largest_key
                    .as_ref()
                    .expect("largest_key checked above")
                    .as_slice()
        {
            largest_key = Some(end.clone());
        }

        smallest_seq = Some(match smallest_seq {
            None => *seq,
            Some(s) => std::cmp::min(s, *seq),
        });
        largest_seq = Some(match largest_seq {
            None => *seq,
            Some(s) => std::cmp::max(s, *seq),
        });
    }

    (smallest_key, largest_key, smallest_seq, largest_seq)
}

/// Configuration for flushing memtable to SST.
pub(crate) struct FlushConfig<'a> {
    pub sst_factory: &'a Arc<dyn crate::sst::SstFactory>,
    pub compression: CompressionType,
    pub block_size: usize,
    pub bloom_bits_per_key: u32,
    pub sst_dir: &'a Path,
    pub metrics: &'a Arc<Metrics>,
    pub cloud_sst_mgr: Option<&'a crate::sst::cloud::CloudSstManager>,
}

/// Synchronously flush memtable to an SST file.
///
/// This is used for explicit flush operations (e.g., `MidgeEngine::flush()`).
/// Drains the memtable, computes bounds, writes to SST, but does NOT update
/// the manifest - caller is responsible for manifest updates.
///
/// Optionally uploads the SST to cloud storage if a CloudSstManager is provided.
///
/// # Returns
/// * `Ok((path, metadata))` - Path to created SST and its metadata
/// * `Err(_)` - If memtable is empty or write fails
pub(crate) fn flush_memtable_to_sst<F>(
    cf_id: crate::api::column_family::ColumnFamilyId,
    memtable_drain: F,
    config: FlushConfig,
) -> MidgeResult<(PathBuf, crate::core::manifest::FileMeta)>
where
    F: FnOnce() -> (Vec<crate::core::EntryMeta>, Vec<(Vec<u8>, Vec<u8>, u64)>),
{
    config.metrics.record_memtable_flush();

    // Drain memtable with metadata and persist to disk under sst dir
    let (entries, range_tombstones) = memtable_drain();

    if entries.is_empty() {
        return Err(crate::error::MidgeError::internal(
            "memtable empty on flush",
        ));
    }

    let mut dyn_writer = config.sst_factory.create_with_bloom(
        config.compression,
        config.block_size,
        true,
        config.bloom_bits_per_key,
    );

    // Compute bounds and seq range
    let (smallest_key, largest_key, smallest_seq, largest_seq) =
        compute_bounds(&entries, &range_tombstones);

    // Calculate tombstone counts for metrics
    let point_tombstone_count = entries.iter().filter(|e| e.is_tombstone).count() as u64;
    let range_tombstone_count = range_tombstones.len() as u64;
    let total_entries = entries.len() as u64;

    // Add entries with expiration metadata
    for entry in entries {
        let v_ref = entry.value.as_deref();
        dyn_writer.add_with_meta(
            &entry.key,
            v_ref,
            entry.sequence,
            entry.is_tombstone,
            entry.expiration_millis,
        )?;
    }

    // Add range tombstones
    for (start, end, seq) in range_tombstones {
        dyn_writer.add_range_tombstone(&start, &end, seq)?;
    }

    // Persist to file (streaming writer should write directly to disk)
    // Format: {seq:020}_{cf_id:04}.sst for per-CF identification
    let seq = smallest_seq.unwrap_or(0);
    let fname = format!("{:020}_{:04}.sst", seq, cf_id.as_u32());
    let file_path = config.sst_dir.join(&fname);
    let boxed = Box::new(dyn_writer);
    boxed.finish_to_path(&file_path)?;

    // Build FileMeta (size to be filled by caller)
    let fm = crate::core::manifest::FileMeta {
        name: fname.clone(),
        level: 0,
        size_bytes: 0,
        cf_id: cf_id.as_u32(),
        smallest_key: smallest_key.clone(),
        largest_key: largest_key.clone(),
        smallest_seq,
        largest_seq,
        sublevel: 0, // Will be assigned when added to manifest
        cloud_location: None,
        cloud_checksum: None,
        cloud_uploaded_at: None,
        cloud_state: None,
        point_tombstone_count,
        range_tombstone_count,
        total_entries,
    };

    // Upload to cloud if manager is provided
    if let Some(mgr) = config.cloud_sst_mgr {
        let sequence_range = (smallest_seq.unwrap_or(0), largest_seq.unwrap_or(0));

        // Queue async upload (no callback needed - manifest updated by worker)
        mgr.upload_sst_async(
            fname.clone(),
            file_path.clone(),
            sequence_range,
            (smallest_key, largest_key),
            None, // No callback - manifest updated automatically by worker
        )?;

        // Note: FileMeta will be updated by the background worker
        // when upload completes and manifest is updated
    }

    Ok((file_path, fm))
}

/// Rollover WAL and queue flush job for background processing.
///
/// This function:
/// 1. Increments the sequence counter
/// 2. Closes the current WAL and creates a new one
/// 3. Drains the memtable
/// 4. Sends a flush job to the background worker
///
/// Returns the sequence number of the rotated WAL segment.
pub(crate) fn rollover_and_queue_flush<F>(
    cf_id: crate::api::column_family::ColumnFamilyId,
    seq_counter: &std::sync::atomic::AtomicU64,
    wal: &parking_lot::RwLock<Box<dyn crate::wal::WalWriter>>,
    wal_factory: &Arc<dyn crate::wal::WalFactory>,
    wal_dir: &Path,
    memtable_drain: F,
    flush_coordinator: &crate::core::FlushCoordinator,
) -> MidgeResult<u64>
where
    F: FnOnce() -> (Vec<crate::core::EntryMeta>, Vec<(Vec<u8>, Vec<u8>, u64)>),
{
    // Increment sequence
    let seq = seq_counter.fetch_add(1, Ordering::SeqCst) + 1;

    // Rotate writer (this will rename wal.log -> wal-<seq>.log for FS)
    let mut w = wal.write();
    let _ = w.close();
    let new_w = wal_factory.rotate_writer(wal_dir, seq)?;
    *w = new_w;
    drop(w); // Release lock

    // Drain memtable entries (with metadata) to flush asynchronously
    let (entries, range_tombstones) = memtable_drain();

    let job = FlushJob {
        cf_id,
        seq,
        entries,
        range_tombstones,
    };

    // Best-effort send
    let _ = flush_coordinator.request_flush(job);

    Ok(seq)
}
