//! Read snapshot - immutable view for parallel read execution
//!
//! Captures immutable references to memtables and SST metadata
//! at a specific sequence number, allowing safe parallel reads.

use crate::io::Fs;
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
    /// SST filesystem handle (rooted at db_path)
    pub sst_fs: Arc<dyn Fs>,
    /// SST path prefix relative to the fs root (typically "sst")
    pub sst_path_prefix: std::path::PathBuf,
    /// In-memory mode flag (skip SST reads when true)
    pub memory_mode: bool,
}

impl ReadSnapshot {
    /// Create a new read snapshot
    pub fn new(
        memtable: Arc<SkipListMemtable>,
        immutable_memtables: Vec<Arc<SkipListMemtable>>,
        sst_files: Vec<FileMeta>,
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: std::path::PathBuf,
        memory_mode: bool,
    ) -> Self {
        // Extract cf_id from first SST file or default to DEFAULT
        let cf_id = sst_files.first().map(|f| f.cf_id).unwrap_or(0);
        Self {
            cf_id,
            memtable,
            immutable_memtables,
            sst_files,
            sst_fs,
            sst_path_prefix,
            memory_mode,
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

        if !self.memory_mode {
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
                    let sst_path = self.sst_path_prefix.join(&file_meta.name);
                    let path_str = sst_path.to_string_lossy().to_string();
                    if let Ok(reader) =
                        crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))
                    {
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
                        let sst_path = self.sst_path_prefix.join(&file_meta.name);
                        let path_str = sst_path.to_string_lossy().to_string();
                        if let Ok(reader) =
                            crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))
                        {
                            if let Ok(Some(value)) = reader.get(key) {
                                return Some(value.to_vec());
                            }
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

        let mut results: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // Treat empty bounds as unbounded
        let start_opt = if start.is_empty() { None } else { Some(start) };
        let end_opt = if end.is_empty() { None } else { Some(end) };

        if !self.memory_mode {
            // === Phase 1: SST files (oldest -> newest so newer overrides older) ===
            let mut files_by_level: BTreeMap<u32, Vec<_>> = BTreeMap::new();
            for file in &self.sst_files {
                files_by_level
                    .entry(file.level)
                    .or_default()
                    .push(file.clone());
            }

            // L0: newest -> oldest (may overlap)
            if let Some(l0_files) = files_by_level.get(&0) {
                for file_meta in l0_files.iter().rev() {
                    file_meta.record_read();
                    let sst_path = self.sst_path_prefix.join(&file_meta.name);
                    let path_str = sst_path.to_string_lossy().to_string();
                    if let Ok(reader) =
                        crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))
                    {
                        if let Ok(pairs) = reader.scan_range(start_opt, end_opt) {
                            for (k, v) in pairs {
                                // First occurrence from newest file wins (or_insert)
                                results.entry(k.to_vec()).or_insert(v.to_vec());
                            }
                        }
                    }
                }
            }

            // Higher levels (L1+): each level is non-overlapping, sorted by key
            for (&level, files) in files_by_level.iter() {
                if level == 0 {
                    continue;
                }

                for file_meta in files.iter() {
                    // Skip if key range doesn't overlap
                    if let (Some(ref smallest), Some(ref largest)) =
                        (&file_meta.smallest_key, &file_meta.largest_key)
                    {
                        // Check if [start, end) overlaps with [smallest, largest]
                        if let Some(s) = start_opt {
                            if s >= largest.as_slice() {
                                continue; // start is beyond this file
                            }
                        }
                        if let Some(e) = end_opt {
                            if e <= smallest.as_slice() {
                                continue; // end is before this file
                            }
                        }
                    }

                    file_meta.record_read();
                    let sst_path = self.sst_path_prefix.join(&file_meta.name);
                    let path_str = sst_path.to_string_lossy().to_string();
                    if let Ok(reader) =
                        crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))
                    {
                        if let Ok(pairs) = reader.scan_range(start_opt, end_opt) {
                            for (k, v) in pairs {
                                results.entry(k.to_vec()).or_insert(v.to_vec());
                            }
                        }
                    }
                }
            }
        }

        // === Phase 2: Immutable memtables (oldest -> newest, newer overrides older) ===
        for imm in self.immutable_memtables.iter() {
            if let Ok(entries) = self.collect_memtable_range(imm, start, end, seq) {
                for (k, v_opt) in entries {
                    // Insert or update with newer memtable value, but only if not a tombstone
                    if let Some(v) = v_opt {
                        results.insert(k, v);
                    }
                }
            }
        }

        // === Phase 3: Active memtable (newest, overrides everything) ===
        if let Ok(entries) = self.collect_memtable_range(&self.memtable, start, end, seq) {
            for (k, v_opt) in entries {
                if let Some(v) = v_opt {
                    results.insert(k, v);
                }
            }
        }

        // Convert BTreeMap to Vec, keeping only non-tombstone entries
        results.into_iter().collect()
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
            .field("sst_path_prefix", &self.sst_path_prefix)
            .field("memory_mode", &self.memory_mode)
            .finish()
    }
}
