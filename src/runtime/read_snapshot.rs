//! Read snapshot - immutable view for parallel read execution
//!
//! Captures immutable references to memtables and SST metadata
//! at a specific sequence number, allowing safe parallel reads.

use crate::io::Fs;
use crate::metadata::FileMeta;
use crate::runtime::read_resources::ReadResources;
use crate::sst::fs::SstFileIo;
use crate::sst::traits::SstStateReader;
use crate::sst::types::{KeyState, RangeTombstone};
use crate::sst::SkipListMemtable;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Immutable snapshot of readable state for a column family
///
/// This struct captures all necessary state to perform read operations
/// without holding references to mutable runtime state.
#[derive(Clone)]
pub struct ReadSnapshot {
    /// CF ID
    pub cf_id: crate::types::ColumnFamilyId,
    /// Active memtable snapshot
    pub memtable: Arc<SkipListMemtable>,
    /// Immutable memtables (newest to oldest)
    pub immutable_memtables: Vec<Arc<SkipListMemtable>>,
    /// SST file metadata for this CF
    pub sst_files: Vec<FileMeta>,
    /// SST filesystem handle (rooted at `db_path`)
    pub sst_fs: Arc<dyn Fs>,
    /// SST path prefix relative to the fs root (typically "sst")
    pub sst_path_prefix: std::path::PathBuf,
    /// In-memory mode flag (skip SST reads when true)
    pub memory_mode: bool,
    read_resources: Option<Arc<ReadResources>>,
    sst_readers: HashMap<String, Arc<SstFileIo>>,
}

impl ReadSnapshot {
    fn current_time_millis() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_millis()),
        )
        .unwrap_or(u64::MAX)
    }

    fn is_expired(expiration: Option<u64>) -> bool {
        expiration.is_some_and(|expiration| expiration <= Self::current_time_millis())
    }

    fn state_sequence(state: &KeyState) -> Option<u64> {
        match state {
            KeyState::Absent => None,
            KeyState::Tombstone(seq) | KeyState::Value(_, seq, _, _) => Some(*seq),
        }
    }

    fn normalize_state(state: KeyState) -> KeyState {
        match state {
            KeyState::Value(_, seq, exp, _) if Self::is_expired(exp) => KeyState::Tombstone(seq),
            _ => state,
        }
    }

    fn candidate_wins(existing: &KeyState, candidate: &KeyState) -> bool {
        let existing_seq = Self::state_sequence(existing).unwrap_or(0);
        let candidate_seq = Self::state_sequence(candidate).unwrap_or(0);

        candidate_seq > existing_seq
            || (candidate_seq == existing_seq
                && matches!(candidate, KeyState::Tombstone(_))
                && !matches!(existing, KeyState::Tombstone(_)))
    }

    fn merge_state(states: &mut BTreeMap<Vec<u8>, KeyState>, key: Vec<u8>, state: KeyState) {
        let normalized = Self::normalize_state(state);
        if matches!(normalized, KeyState::Absent) {
            return;
        }

        match states.get(&key) {
            Some(existing) if !Self::candidate_wins(existing, &normalized) => {}
            _ => {
                states.insert(key, normalized);
            }
        }
    }

    fn merge_best_state(best_state: &mut Option<KeyState>, state: KeyState) {
        let normalized = Self::normalize_state(state);
        if matches!(normalized, KeyState::Absent) {
            return;
        }

        if best_state
            .as_ref()
            .is_none_or(|existing| Self::candidate_wins(existing, &normalized))
        {
            *best_state = Some(normalized);
        }
    }

    fn is_visible_state(state: &KeyState, snapshot_seq: u64) -> bool {
        snapshot_seq == u64::MAX
            || Self::state_sequence(state).is_some_and(|state_seq| state_seq <= snapshot_seq)
    }

    fn range_tombstone_overlaps_query(
        tombstone: &RangeTombstone,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> bool {
        if let Some(scan_end) = end {
            if tombstone.start.as_slice() >= scan_end {
                return false;
            }
        }
        if let Some(scan_start) = start {
            if tombstone.end.as_slice() <= scan_start {
                return false;
            }
        }
        true
    }

    fn range_tombstone_covers_state(
        tombstones: &[RangeTombstone],
        key: &[u8],
        state: &KeyState,
    ) -> bool {
        let state_seq = Self::state_sequence(state).unwrap_or(0);
        tombstones
            .iter()
            .any(|tombstone| tombstone.covers(key) && tombstone.seq >= state_seq)
    }

    /// Create a new read snapshot
    pub fn new(
        memtable: Arc<SkipListMemtable>,
        immutable_memtables: Vec<Arc<SkipListMemtable>>,
        sst_files: Vec<FileMeta>,
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: std::path::PathBuf,
        memory_mode: bool,
    ) -> Self {
        Self::new_with_resources(
            memtable,
            immutable_memtables,
            sst_files,
            sst_fs,
            sst_path_prefix,
            memory_mode,
            None,
        )
    }

    pub(crate) fn new_with_resources(
        memtable: Arc<SkipListMemtable>,
        immutable_memtables: Vec<Arc<SkipListMemtable>>,
        sst_files: Vec<FileMeta>,
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: std::path::PathBuf,
        memory_mode: bool,
        read_resources: Option<Arc<ReadResources>>,
    ) -> Self {
        // Extract cf_id from first SST file or default to DEFAULT
        let cf_id = sst_files.first().map_or(0, |f| f.cf_id);
        let sst_readers = if memory_mode {
            HashMap::new()
        } else {
            read_resources
                .as_ref()
                .map_or_else(HashMap::new, |resources| {
                    resources.capture_readers(&sst_files)
                })
        };
        Self {
            cf_id,
            memtable,
            immutable_memtables,
            sst_files,
            sst_fs,
            sst_path_prefix,
            memory_mode,
            read_resources,
            sst_readers,
        }
    }

    fn sst_reader(&self, file_meta: &FileMeta) -> crate::common::MidgeResult<Arc<SstFileIo>> {
        if let Some(reader) = self.sst_readers.get(&file_meta.name) {
            crate::sst::read_path_metrics::global_sst_read_metrics().record_reader_cache_hit();
            return Ok(Arc::clone(reader));
        }
        if let Some(resources) = &self.read_resources {
            return resources.reader_for(file_meta);
        }

        let sst_path = self.sst_path_prefix.join(&file_meta.name);
        let path_str = sst_path.to_string_lossy().to_string();
        crate::sst::read_path_metrics::global_sst_read_metrics().record_reader_cache_miss();
        Ok(Arc::new(crate::sst::fs::SstFileIo::open(
            &path_str,
            Arc::clone(&self.sst_fs),
        )?))
    }

    /// Perform a point read on this snapshot
    pub fn get(&self, key: &[u8], seq: u64) -> Option<Vec<u8>> {
        let mut best_state = None;
        let mut range_tombstones = Vec::new();

        if let Ok(state) = self.memtable.get_key_state_at(key, seq) {
            Self::merge_best_state(&mut best_state, state);
        }

        for imm in &self.immutable_memtables {
            if let Ok(state) = imm.get_key_state_at(key, seq) {
                Self::merge_best_state(&mut best_state, state);
            }
        }

        if !self.memory_mode {
            for file_meta in &self.sst_files {
                if let (Some(smallest), Some(largest)) =
                    (&file_meta.smallest_key, &file_meta.largest_key)
                {
                    if key < smallest.as_slice() || key > largest.as_slice() {
                        continue;
                    }
                }

                file_meta.record_read();
                crate::sst::read_path_metrics::global_sst_read_metrics()
                    .record_candidate_sst_file_checked();
                if let Ok(reader) = self.sst_reader(file_meta) {
                    if let Ok(state) = reader.get_state_at(key, seq) {
                        Self::merge_best_state(&mut best_state, state);
                    }

                    crate::sst::read_path_metrics::global_sst_read_metrics()
                        .record_range_tombstone_scan();
                    range_tombstones.extend(reader.range_tombstones().into_iter().filter(
                        |tombstone| {
                            (seq == u64::MAX || tombstone.seq <= seq) && tombstone.covers(key)
                        },
                    ));
                }
            }
        }

        let state = best_state?;
        if Self::range_tombstone_covers_state(&range_tombstones, key, &state) {
            return None;
        }

        match state {
            KeyState::Value(value, _, exp, _) if !Self::is_expired(exp) => Some(value.to_vec()),
            _ => None,
        }
    }

    /// Return the latest sequence touching `key` across memtables + SSTs.
    ///
    /// Includes tombstones and range tombstones so conflict detection can
    /// identify any write after a transaction start snapshot.
    pub fn latest_state_sequence(&self, key: &[u8]) -> Option<u64> {
        let mut best_state = None;
        let mut range_tombstones = Vec::new();

        if let Ok(state) = self.memtable.get_key_state_at(key, u64::MAX) {
            Self::merge_best_state(&mut best_state, state);
        }

        for imm in &self.immutable_memtables {
            if let Ok(state) = imm.get_key_state_at(key, u64::MAX) {
                Self::merge_best_state(&mut best_state, state);
            }
        }

        if !self.memory_mode {
            for file_meta in &self.sst_files {
                if let (Some(smallest), Some(largest)) =
                    (&file_meta.smallest_key, &file_meta.largest_key)
                {
                    if key < smallest.as_slice() || key > largest.as_slice() {
                        continue;
                    }
                }

                if let Ok(reader) = self.sst_reader(file_meta) {
                    if let Ok(state) = reader.get_state_at(key, u64::MAX) {
                        Self::merge_best_state(&mut best_state, state);
                    }

                    range_tombstones.extend(
                        reader
                            .range_tombstones()
                            .into_iter()
                            .filter(|tombstone| tombstone.covers(key)),
                    );
                }
            }
        }

        let state_seq = best_state
            .as_ref()
            .and_then(Self::state_sequence)
            .unwrap_or(0);
        let range_tombstone_seq = range_tombstones.iter().map(|t| t.seq).max().unwrap_or(0);
        let max_seq = state_seq.max(range_tombstone_seq);

        if max_seq == 0 {
            None
        } else {
            Some(max_seq)
        }
    }

    /// Return the latest sequence touching any key in [start, end).
    ///
    /// Includes value/tombstone state and overlapping range tombstones.
    pub fn latest_sequence_in_range(&self, start: &[u8], end: &[u8]) -> Option<u64> {
        let start_opt = if start.is_empty() { None } else { Some(start) };
        let end_opt = if end.is_empty() { None } else { Some(end) };

        let mut max_seq = 0u64;

        for (_key, state) in self.memtable.range_state_at(start_opt, end_opt, u64::MAX) {
            if let Some(seq) = Self::state_sequence(&state) {
                max_seq = max_seq.max(seq);
            }
        }

        for imm in &self.immutable_memtables {
            for (_key, state) in imm.range_state_at(start_opt, end_opt, u64::MAX) {
                if let Some(seq) = Self::state_sequence(&state) {
                    max_seq = max_seq.max(seq);
                }
            }
        }

        if !self.memory_mode {
            for file_meta in &self.sst_files {
                if let (Some(smallest), Some(largest)) =
                    (&file_meta.smallest_key, &file_meta.largest_key)
                {
                    if start_opt.is_some_and(|s| s > largest.as_slice())
                        || end_opt.is_some_and(|e| e <= smallest.as_slice())
                    {
                        continue;
                    }
                }

                if let Ok(reader) = self.sst_reader(file_meta) {
                    if let Ok(entries) = reader.scan_range_state(start_opt, end_opt) {
                        for (_key, state) in entries {
                            if let Some(seq) = Self::state_sequence(&state) {
                                max_seq = max_seq.max(seq);
                            }
                        }
                    }

                    for tombstone in reader.range_tombstones() {
                        if Self::range_tombstone_overlaps_query(&tombstone, start_opt, end_opt) {
                            max_seq = max_seq.max(tombstone.seq);
                        }
                    }
                }
            }
        }

        if max_seq == 0 {
            None
        } else {
            Some(max_seq)
        }
    }

    /// Perform a range scan on this snapshot
    pub fn range_scan(&self, start: &[u8], end: &[u8], seq: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut states: BTreeMap<Vec<u8>, KeyState> = BTreeMap::new();
        let mut range_tombstones = Vec::new();

        // Treat empty bounds as unbounded
        let start_opt = if start.is_empty() { None } else { Some(start) };
        let end_opt = if end.is_empty() { None } else { Some(end) };

        for (key, state) in self.memtable.range_state_at(start_opt, end_opt, seq) {
            Self::merge_state(&mut states, key, state);
        }

        for imm in &self.immutable_memtables {
            for (key, state) in imm.range_state_at(start_opt, end_opt, seq) {
                Self::merge_state(&mut states, key, state);
            }
        }

        if !self.memory_mode {
            for file_meta in &self.sst_files {
                if let (Some(smallest), Some(largest)) =
                    (&file_meta.smallest_key, &file_meta.largest_key)
                {
                    if start_opt.is_some_and(|s| s > largest.as_slice())
                        || end_opt.is_some_and(|e| e <= smallest.as_slice())
                    {
                        continue;
                    }
                }

                file_meta.record_read();
                if let Ok(reader) = self.sst_reader(file_meta) {
                    if let Ok(entries) = reader.scan_range_state(start_opt, end_opt) {
                        for (key, state) in entries {
                            if Self::is_visible_state(&state, seq) {
                                Self::merge_state(&mut states, key.to_vec(), state);
                            }
                        }
                    }

                    range_tombstones.extend(reader.range_tombstones().into_iter().filter(
                        |tombstone| {
                            (seq == u64::MAX || tombstone.seq <= seq)
                                && Self::range_tombstone_overlaps_query(
                                    tombstone, start_opt, end_opt,
                                )
                        },
                    ));
                }
            }
        }

        states
            .into_iter()
            .filter_map(|(key, state)| {
                if Self::range_tombstone_covers_state(&range_tombstones, &key, &state) {
                    return None;
                }

                match state {
                    KeyState::Value(value, _, exp, _) if !Self::is_expired(exp) => {
                        Some((key, value.to_vec()))
                    }
                    _ => None,
                }
            })
            .collect()
    }
}

impl std::fmt::Debug for ReadSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadSnapshot")
            .field("cf_id", &self.cf_id)
            .field("memtable", &"<memtable>")
            .field("immutable_memtables_len", &self.immutable_memtables.len())
            .field("sst_files_len", &self.sst_files.len())
            .field("sst_fs", &"<dyn Fs>")
            .field("sst_path_prefix", &self.sst_path_prefix)
            .field("memory_mode", &self.memory_mode)
            .field("read_resources", &self.read_resources.is_some())
            .field("sst_readers", &self.sst_readers.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::SstFactory;

    fn wait_for_cache_entry(cache: &crate::sst::cache::BlockCache) {
        for _ in 0..50 {
            if !cache.is_empty() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn should_use_shared_block_cache_for_snapshot_sst_reads() -> crate::common::MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"cache-key", Some(b"cache-value"), 10, 0, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join("cache.sst"))?;

        let file_meta = FileMeta {
            name: "cache.sst".to_string(),
            level: 0,
            size_bytes: std::fs::metadata(temp_dir.path().join("cache.sst"))?.len(),
            cf_id: 0,
            smallest_key: Some(b"cache-key".to_vec()),
            largest_key: Some(b"cache-key".to_vec()),
            smallest_seq: Some(10),
            largest_seq: Some(10),
            ..Default::default()
        };
        let read_resources = Arc::new(ReadResources::new(
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
            std::path::PathBuf::new(),
            1024 * 1024,
            crate::sst::cache::CachePolicyType::Lru,
        ));
        let snapshot = ReadSnapshot::new_with_resources(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            vec![file_meta],
            Arc::new(crate::io::RealFs::new(temp_dir.path())?),
            std::path::PathBuf::new(),
            false,
            Some(Arc::clone(&read_resources)),
        );
        let block_cache = read_resources.block_cache();

        // Act
        let first = snapshot.get(b"cache-key", u64::MAX);
        wait_for_cache_entry(&block_cache);
        let hits_before = block_cache.metrics().hit_count();
        let second = snapshot.get(b"cache-key", u64::MAX);
        let hits_after = block_cache.metrics().hit_count();

        // Assert
        assert_eq!(first, Some(b"cache-value".to_vec()));
        assert_eq!(second, Some(b"cache-value".to_vec()));
        assert!(
            hits_after > hits_before,
            "second snapshot read should hit shared block cache"
        );
        Ok(())
    }
}
