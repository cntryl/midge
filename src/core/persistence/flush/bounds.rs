//! Key bounds computation and memtable-to-SST flushing.
//!
//! Contains the pure functions for computing key/sequence bounds and
//! the synchronous flush path used by explicit flush operations.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common::codec::CompressionType;
use crate::error::MidgeResult;
use crate::metrics::Metrics;

use super::stats::FlushStats;

/// Type alias for key bounds and sequence range tuple returned by compute_bounds.
/// (smallest_key, largest_key, smallest_seq, largest_seq)
pub type KeyBounds = (Option<Vec<u8>>, Option<Vec<u8>>, Option<u64>, Option<u64>);

/// Compute key bounds and sequence range from entries and range tombstones.
///
/// Optimized to minimize allocations:
/// - Only clones keys when they become new bounds
/// - Tracks sequence min/max without Option overhead after first entry
#[inline]
pub fn compute_bounds(
    entries: &[crate::core::EntryMeta],
    range_tombstones: &[(Vec<u8>, Vec<u8>, u64)],
) -> KeyBounds {
    // Early return for empty inputs
    if entries.is_empty() && range_tombstones.is_empty() {
        return (None, None, None, None);
    }

    let mut smallest_key: Option<Vec<u8>> = None;
    let mut largest_key: Option<Vec<u8>> = None;
    let mut smallest_seq: u64 = u64::MAX;
    let mut largest_seq: u64 = 0;
    let mut has_entries = false;

    // Helper to extract user key without allocation when possible
    #[inline]
    fn extract_user_key(key: &[u8]) -> &[u8] {
        // Internal keys are: userkey || seq (8 bytes BE) || kind (1 byte)
        // If key is long enough, strip the suffix; otherwise use as-is
        if key.len() > 9 {
            &key[..key.len() - 9]
        } else {
            key
        }
    }

    // Process entries - extract user key without cloning until needed
    for entry in entries {
        has_entries = true;
        let user_key = extract_user_key(&entry.key);

        // Update smallest key
        match smallest_key.as_ref() {
            None => smallest_key = Some(user_key.to_vec()),
            Some(sk) if user_key < sk.as_slice() => smallest_key = Some(user_key.to_vec()),
            _ => {}
        }

        // Update largest key
        match largest_key.as_ref() {
            None => largest_key = Some(user_key.to_vec()),
            Some(lk) if user_key > lk.as_slice() => largest_key = Some(user_key.to_vec()),
            _ => {}
        }

        // Track sequence bounds (branchless min/max)
        smallest_seq = smallest_seq.min(entry.sequence);
        largest_seq = largest_seq.max(entry.sequence);
    }

    // Process range tombstones
    for (start, end, seq) in range_tombstones {
        has_entries = true;

        // Update smallest key
        match smallest_key.as_ref() {
            None => smallest_key = Some(start.clone()),
            Some(sk) if start.as_slice() < sk.as_slice() => smallest_key = Some(start.clone()),
            _ => {}
        }

        // Update largest key
        match largest_key.as_ref() {
            None => largest_key = Some(end.clone()),
            Some(lk) if end.as_slice() > lk.as_slice() => largest_key = Some(end.clone()),
            _ => {}
        }

        smallest_seq = smallest_seq.min(*seq);
        largest_seq = largest_seq.max(*seq);
    }

    // Convert sequence bounds to Option (only if we processed entries)
    let (smallest_seq_opt, largest_seq_opt) = if has_entries {
        (Some(smallest_seq), Some(largest_seq))
    } else {
        (None, None)
    };

    (smallest_key, largest_key, smallest_seq_opt, largest_seq_opt)
}

/// Configuration for flushing memtable to SST.
pub struct FlushConfig<'a> {
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
pub fn flush_memtable_to_sst<F>(
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

    if entries.is_empty() && range_tombstones.is_empty() {
        return Err(crate::error::MidgeError::internal(
            "memtable empty on flush",
        ));
    }

    let mut dyn_writer = config.sst_factory.create_with_bloom(
        config.compression,
        config.block_size,
        true,
        config.bloom_bits_per_key,
    )?;

    // Compute bounds and seq range
    let (smallest_key, largest_key, smallest_seq, largest_seq) =
        compute_bounds(&entries, &range_tombstones);

    // Compute flush statistics
    let stats = FlushStats::compute(&entries, &range_tombstones);

    // Add entries with expiration metadata
    for entry in entries.iter() {
        let v_ref = entry.value.as_deref();
        dyn_writer.add_with_meta(
            &entry.key,
            v_ref,
            entry.sequence,
            entry.op_type.as_u8(),
            entry.expiration_millis,
        )?;
    }

    // Add range tombstones
    for (start, end, seq) in range_tombstones {
        dyn_writer.add_range_tombstone(&start, &end, seq)?;
    }

    // Allocate SST sequence number for this CF
    let sst_seq = crate::core::naming::allocate_sst_seq(cf_id);
    let sst_name = format!(
        "{}/{}",
        crate::core::naming::pad_cf_id(cf_id.as_u32()),
        crate::core::naming::sst_filename(sst_seq)
    );

    // Persist to file (streaming writer should write directly to disk)
    // Format: dbpath/{cf_id}/{sst_seq}.sst
    let file_path = crate::core::naming::sst_path(config.sst_dir, cf_id, sst_seq);
    if let Some(parent) = file_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    dyn_writer.finish_to_path(&file_path)?;

    // Build FileMeta (size to be filled by caller)
    let fm = crate::core::manifest::FileMeta {
        name: sst_name.clone(),
        level: 0,
        size_bytes: 0,
        cf_id: cf_id.as_u32(),
        sst_seq,
        smallest_key: smallest_key.clone(),
        largest_key: largest_key.clone(),
        smallest_seq,
        largest_seq,
        sublevel: 0, // Will be assigned when added to manifest
        cloud_location: None,
        cloud_checksum: None,
        cloud_uploaded_at: None,
        cloud_state: None,
        point_tombstone_count: stats.point_tombstone_count,
        range_tombstone_count: stats.range_tombstone_count,
        total_entries: stats.total_entries,
    };

    // Upload to cloud if manager is provided
    if let Some(mgr) = config.cloud_sst_mgr {
        let sequence_range = (smallest_seq.unwrap_or(0), largest_seq.unwrap_or(0));

        // Queue async upload (no callback needed - manifest updated by worker)
        mgr.upload_sst_async(
            sst_name.clone(),
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
/// 1. Allocates a global WAL sequence number
/// 2. Closes the current WAL and creates a new one
/// 3. Drains the memtable
/// 4. Sends a flush job to the background worker
///
/// Returns the sequence number of the rotated WAL segment.
pub fn rollover_and_queue_flush<F>(
    cf_id: crate::api::column_family::ColumnFamilyId,
    wal: &parking_lot::RwLock<Box<dyn crate::wal::WalWriter>>,
    wal_factory: &Arc<dyn crate::wal::WalFactory>,
    wal_dir: &Path,
    memtable_drain: F,
    flush_coordinator: &crate::core::FlushCoordinator,
) -> MidgeResult<u64>
where
    F: FnOnce() -> (Vec<crate::core::EntryMeta>, Vec<(Vec<u8>, Vec<u8>, u64)>),
{
    use super::worker::FlushJob;

    // Allocate global WAL sequence
    let seq = crate::core::naming::allocate_wal_seq();

    // Rotate writer (this will rename wal.log -> wal-<seq>.wal for FS)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::EntryMeta;

    #[test]
    fn should_compute_bounds_from_entries() {
        // Arrange
        let entries = vec![
            EntryMeta {
                key: b"key1".to_vec(),
                value: Some(b"val1".to_vec()),
                sequence: 1,
                is_tombstone: false,
                expiration_millis: None,
                op_type: crate::core::skiplist::OpType::Put,
            },
            EntryMeta {
                key: b"key3".to_vec(),
                value: Some(b"val3".to_vec()),
                sequence: 3,
                is_tombstone: false,
                expiration_millis: None,
                op_type: crate::core::skiplist::OpType::Put,
            },
            EntryMeta {
                key: b"key2".to_vec(),
                value: Some(b"val2".to_vec()),
                sequence: 2,
                is_tombstone: false,
                expiration_millis: None,
                op_type: crate::core::skiplist::OpType::Put,
            },
        ];

        // Act
        let (smallest_key, largest_key, smallest_seq, largest_seq) = compute_bounds(&entries, &[]);

        // Assert
        assert_eq!(smallest_key, Some(b"key1".to_vec()));
        assert_eq!(largest_key, Some(b"key3".to_vec()));
        assert_eq!(smallest_seq, Some(1));
        assert_eq!(largest_seq, Some(3));
    }

    #[test]
    fn should_compute_bounds_from_range_tombstones() {
        // Arrange
        let range_tombstones = vec![
            (b"a".to_vec(), b"m".to_vec(), 10),
            (b"n".to_vec(), b"z".to_vec(), 20),
        ];

        // Act
        let (smallest_key, largest_key, smallest_seq, largest_seq) =
            compute_bounds(&[], &range_tombstones);

        // Assert
        assert_eq!(smallest_key, Some(b"a".to_vec()));
        assert_eq!(largest_key, Some(b"z".to_vec()));
        assert_eq!(smallest_seq, Some(10));
        assert_eq!(largest_seq, Some(20));
    }

    #[test]
    fn should_compute_bounds_from_mixed_entries_tombstones() {
        // Arrange
        let entries = vec![EntryMeta {
            key: b"key5".to_vec(),
            value: Some(b"val5".to_vec()),
            sequence: 5,
            is_tombstone: false,
            expiration_millis: None,
            op_type: crate::core::skiplist::OpType::Put,
        }];
        let range_tombstones = vec![(b"key1".to_vec(), b"key9".to_vec(), 3)];

        // Act
        let (smallest_key, largest_key, smallest_seq, largest_seq) =
            compute_bounds(&entries, &range_tombstones);

        // Assert
        assert_eq!(smallest_key, Some(b"key1".to_vec()));
        assert_eq!(largest_key, Some(b"key9".to_vec()));
        assert_eq!(smallest_seq, Some(3));
        assert_eq!(largest_seq, Some(5));
    }

    #[test]
    fn should_return_none_when_no_entries_or_tombstones() {
        // Arrange
        let entries: Vec<crate::core::EntryMeta> = vec![];
        let range_tombstones: Vec<(Vec<u8>, Vec<u8>, u64)> = vec![];

        // Act
        let (smallest_key, largest_key, smallest_seq, largest_seq) =
            compute_bounds(&entries, &range_tombstones);

        // Assert
        assert_eq!(smallest_key, None);
        assert_eq!(largest_key, None);
        assert_eq!(smallest_seq, None);
        assert_eq!(largest_seq, None);
    }
}
