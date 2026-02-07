//! Read snapshot - immutable view for parallel read execution
//!
//! Captures immutable references to memtables and SST metadata
//! at a specific sequence number, allowing safe parallel reads.

use crate::metadata::FileMeta;
use crate::sst::traits::SstReader;
use crate::sst::{Memtable, SkipListMemtable};
use std::sync::Arc;

/// Type alias for memtable range scan results
type MemtableRangeResult = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Immutable snapshot of readable state for a column family
///
/// This struct captures all necessary state to perform read operations
/// without holding references to mutable runtime state.
#[derive(Clone)]
pub struct ReadSnapshot {
    /// CF ID
    pub cf_id: crate::engine::ColumnFamilyId,
    /// Active memtable snapshot
    pub memtable: Arc<SkipListMemtable>,
    /// Immutable memtables (newest to oldest)
    pub immutable_memtables: Vec<Arc<SkipListMemtable>>,
    /// SST file metadata for this CF
    pub sst_files: Vec<FileMeta>,
    /// SST directory path
    pub sst_dir: std::path::PathBuf,
}

impl ReadSnapshot {
    /// Create a new read snapshot
    pub fn new(
        memtable: Arc<SkipListMemtable>,
        immutable_memtables: Vec<Arc<SkipListMemtable>>,
        sst_files: Vec<FileMeta>,
        sst_dir: std::path::PathBuf,
    ) -> Self {
        // Extract cf_id from first SST file or default to DEFAULT
        let cf_id = sst_files.first().map(|f| f.cf_id).unwrap_or(0);
        Self {
            cf_id,
            memtable,
            immutable_memtables,
            sst_files,
            sst_dir,
        }
    }

    /// Perform a point read on this snapshot
    pub fn get(&self, key: &[u8], seq: u64) -> Option<Vec<u8>> {
        // Active memtable
        if seq == u64::MAX {
            if let Ok(Some(v)) = self.memtable.get(key) {
                return Some(v);
            }
        } else if let Ok(Some(v)) = self.memtable.get_at_seq(key, seq) {
            return Some(v);
        }

        // Immutable memtables (newest → oldest)
        for imm in self.immutable_memtables.iter().rev() {
            if seq == u64::MAX {
                if let Ok(Some(v)) = imm.get(key) {
                    return Some(v);
                }
            } else if let Ok(Some(v)) = imm.get_at_seq(key, seq) {
                return Some(v);
            }
        }

        // SST lookup: check files from newest to oldest across all levels
        let mut files_by_level: std::collections::BTreeMap<u32, Vec<_>> =
            std::collections::BTreeMap::new();
        for file in &self.sst_files {
            files_by_level
                .entry(file.level)
                .or_default()
                .push(file.clone());
        }

        // Search L0 first (newest to oldest), then L1, L2, etc.
        if let Some(l0_files) = files_by_level.get(&0) {
            for file_meta in l0_files.iter().rev() {
                file_meta.record_read();
                let sst_path = self.sst_dir.join(&file_meta.name);
                if let Ok(reader) = crate::sst::fs::SstFileIo::open_with_real_fs(&sst_path) {
                    if let Ok(Some(value)) = reader.get(key) {
                        return Some(value.to_vec());
                    }
                }
            }
        }

        // Search higher levels (L1+)
        for level in 1..=6 {
            if let Some(files) = files_by_level.get(&level) {
                for file_meta in files {
                    // Check key range before opening SST
                    if let (Some(smallest), Some(largest)) =
                        (&file_meta.smallest_key, &file_meta.largest_key)
                    {
                        if key < smallest.as_slice() || key > largest.as_slice() {
                            continue;
                        }
                    }

                    file_meta.record_read();
                    let sst_path = self.sst_dir.join(&file_meta.name);
                    if let Ok(reader) = crate::sst::fs::SstFileIo::open_with_real_fs(&sst_path) {
                        if let Ok(Some(value)) = reader.get(key) {
                            return Some(value.to_vec());
                        }
                    }
                }
            }
        }

        None
    }

    /// Perform a range scan on this snapshot
    pub fn range_scan(&self, start: &[u8], end: &[u8], seq: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        use std::collections::BTreeMap;

        let mut merged: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Collect from memtables
        // Active memtable
        if let Ok(entries) = self.collect_memtable_range(&self.memtable, start, end, seq) {
            for (k, v) in entries {
                merged.insert(k, v);
            }
        }

        // Immutable memtables
        for imm in self.immutable_memtables.iter().rev() {
            if let Ok(entries) = self.collect_memtable_range(imm, start, end, seq) {
                for (k, v) in entries {
                    merged.entry(k).or_insert(v);
                }
            }
        }

        // SST files (simplified - would need full merge iterator in production)
        // For now, just return memtable results

        merged
            .into_iter()
            .filter_map(|(k, v_opt)| v_opt.map(|v| (k, v)))
            .collect()
    }

    fn collect_memtable_range(
        &self,
        memtable: &Arc<SkipListMemtable>,
        start: &[u8],
        end: &[u8],
        seq: u64,
    ) -> Result<MemtableRangeResult, ()> {
        use std::collections::BTreeMap;

        // De-duplicate versions within this memtable (keep first = newest)
        let all_entries = memtable.iter_all(seq);
        let mut by_key: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        for (key, value_opt, entry_seq) in all_entries {
            if entry_seq < seq {
                by_key.entry(key).or_insert(value_opt);
            }
        }

        // Filter by range
        let mut results = Vec::new();
        for (key, value_opt) in by_key {
            if &key[..] >= start && (end.is_empty() || &key[..] < end) {
                results.push((key, value_opt));
            }
        }

        Ok(results)
    }
}

impl std::fmt::Debug for ReadSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadSnapshot")
            .field("cf_id", &self.cf_id)
            .field("immutable_memtables_len", &self.immutable_memtables.len())
            .field("sst_files_len", &self.sst_files.len())
            .field("sst_dir", &self.sst_dir)
            .finish()
    }
}
