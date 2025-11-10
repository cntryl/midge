///! Writing compacted SST files to disk.

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::MidgeResult;

use super::types::CompactionVersion;

/// Write a compacted SST file from the given versions.
///
/// This creates a new SST file on disk with all the provided versions in sorted order.
/// The SST metadata (smallest/largest key, sequence range) is computed from the input.
///
/// # Arguments
/// * `sst_factory` - Factory for creating SST writers
/// * `compression` - Compression algorithm to use
/// * `block_size` - Target block size in bytes
/// * `sst_dir` - Directory where the SST file should be written
/// * `versions` - Sorted list of key versions to write
/// * `cloud_sst_manager` - Optional cloud SST manager for uploading to cloud storage
/// * `manifest` - Optional manifest for updating cloud metadata
///
/// # Returns
/// * `Ok(Some((path, metadata)))` - Path to the created SST and its metadata
/// * `Ok(None)` - No SST created (empty input)
/// * `Err(_)` - I/O or encoding error
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_compacted_sst(
    sst_factory: &Arc<dyn crate::sst::SstFactory>,
    compression: crate::codec::CompressionType,
    block_size: usize,
    sst_dir: &std::path::Path,
    versions: &[CompactionVersion],
    cf_id: u32,
    cloud_sst_manager: Option<&Arc<crate::sst::cloud::CloudSstManager>>,
    _manifest: Option<&mut crate::manifest::Manifest>,
) -> MidgeResult<Option<(PathBuf, crate::manifest::FileMeta)>> {
    if versions.is_empty() {
        return Ok(None);
    }

    // Enforce precondition: versions must be deduplicated (one entry per user_key)
    // This prevents writing SSTs with duplicate keys which violates the SST invariant
    let mut last_user_key: Option<&[u8]> = None;
    for entry in versions {
        if let Some(last_key) = last_user_key {
            if last_key == entry.user_key.as_slice() {
                return Err(crate::error::MidgeError::InvalidData(format!(
                    "write_compacted_sst called with duplicate user_key: {}. \
                     Versions must be deduplicated before writing.",
                    hex::encode(entry.user_key.as_slice())
                )));
            }
        }
        last_user_key = Some(entry.user_key.as_slice());
    }

    let mut writer = sst_factory.create(compression, block_size, true);
    let mut smallest_key: Option<Vec<u8>> = None;
    let mut largest_key: Option<Vec<u8>> = None;
    let mut smallest_seq: Option<u64> = None;
    let mut largest_seq: Option<u64> = None;

    for entry in versions {
        if smallest_key.is_none() {
            smallest_key = Some(entry.user_key.clone());
        }
        largest_key = Some(entry.user_key.clone());
        smallest_seq = Some(match smallest_seq {
            Some(s) => s.min(entry.seq),
            None => entry.seq,
        });
        largest_seq = Some(match largest_seq {
            Some(s) => s.max(entry.seq),
            None => entry.seq,
        });

        if entry.tombstone {
            writer.add_with_meta(entry.user_key.as_slice(), None, entry.seq, true, None)?;
        } else if let Some(value) = &entry.value {
            writer.add_with_meta(
                entry.user_key.as_slice(),
                Some(value.as_ref()),
                entry.seq,
                false,
                entry.expiration,
            )?;
        }
    }

    let raw = writer.finish_bytes()?;
    let id = uuid::Uuid::new_v4().to_string();
    let file_path = sst_dir.join(format!("{}.sst", id));
    std::fs::write(&file_path, &raw)?;

    // Calculate tombstone counts
    let point_tombstone_count = versions.iter().filter(|v| v.tombstone).count() as u64;
    let range_tombstone_count = 0; // Range tombstones handled separately in versions
    let total_entries = versions.len() as u64;

    let size_bytes = std::fs::metadata(&file_path)
        .map(|md| md.len())
        .unwrap_or(0);
    let name = file_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_string();
    let meta = crate::manifest::FileMeta {
        name: name.clone(),
        level: 0,
        size_bytes,
        cf_id,
        smallest_key,
        largest_key,
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

    // Upload SST to cloud if cloud manager is configured
    if let Some(cloud_manager) = cloud_sst_manager {
        let sst_id = id.clone();
        let sequence_range = (smallest_seq.unwrap_or(0), largest_seq.unwrap_or(0));
        let key_range = (
            meta.smallest_key.clone().map(|k| k.to_vec()),
            meta.largest_key.clone().map(|k| k.to_vec()),
        );

        // Spawn async upload (non-blocking)
        let cloud_manager_clone = cloud_manager.clone();
        let file_path_clone = file_path.clone();
        let sst_id_clone = sst_id.clone();

        std::thread::spawn(move || {
            if let Err(e) = cloud_manager_clone.upload_sst_async(
                sst_id_clone,
                file_path_clone,
                sequence_range,
                key_range,
                None,
            ) {
                tracing::error!("Failed to upload compacted SST to cloud: {}", e);
            }
        });
    }

    Ok(Some((file_path, meta)))
}
