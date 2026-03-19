//! Read snapshot - immutable view for parallel read execution
//!
//! Captures immutable references to memtables and SST metadata
//! at a specific sequence number, allowing safe parallel reads.

use crate::io::Fs;
use crate::metadata::FileMeta;
use crate::sst::traits::SstStateReader;
use crate::sst::types::{KeyState, RangeTombstone};
use crate::sst::SkipListMemtable;
use std::collections::BTreeMap;
use std::sync::Arc;

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
    fn current_time_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn is_expired(expiration: Option<u64>) -> bool {
        expiration.is_some_and(|expiration| expiration <= Self::current_time_millis())
    }

    fn state_sequence(state: &KeyState) -> Option<u64> {
        match state {
            KeyState::Absent => None,
            KeyState::Tombstone(seq) => Some(*seq),
            KeyState::Value(_, seq, _, _) => Some(*seq),
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
        let mut states = BTreeMap::new();
        let mut range_tombstones = Vec::new();

        if let Ok(state) = self.memtable.get_key_state_at(key, seq) {
            Self::merge_state(&mut states, key.to_vec(), state);
        }

        for imm in &self.immutable_memtables {
            if let Ok(state) = imm.get_key_state_at(key, seq) {
                Self::merge_state(&mut states, key.to_vec(), state);
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
                let sst_path = self.sst_path_prefix.join(&file_meta.name);
                let path_str = sst_path.to_string_lossy().to_string();
                if let Ok(reader) =
                    crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))
                {
                    if let Ok(state) = reader.get_state_at(key, seq) {
                        Self::merge_state(&mut states, key.to_vec(), state);
                    }

                    range_tombstones.extend(reader.range_tombstones().into_iter().filter(
                        |tombstone| {
                            (seq == u64::MAX || tombstone.seq <= seq) && tombstone.covers(key)
                        },
                    ));
                }
            }
        }

        let state = states.remove(key)?;
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
        let mut states = BTreeMap::new();
        let mut range_tombstones = Vec::new();

        if let Ok(state) = self.memtable.get_key_state_at(key, u64::MAX) {
            Self::merge_state(&mut states, key.to_vec(), state);
        }

        for imm in &self.immutable_memtables {
            if let Ok(state) = imm.get_key_state_at(key, u64::MAX) {
                Self::merge_state(&mut states, key.to_vec(), state);
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

                let sst_path = self.sst_path_prefix.join(&file_meta.name);
                let path_str = sst_path.to_string_lossy().to_string();
                if let Ok(reader) =
                    crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))
                {
                    if let Ok(state) = reader.get_state_at(key, u64::MAX) {
                        Self::merge_state(&mut states, key.to_vec(), state);
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

        let state_seq = states
            .get(key)
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

                let sst_path = self.sst_path_prefix.join(&file_meta.name);
                let path_str = sst_path.to_string_lossy().to_string();
                if let Ok(reader) =
                    crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))
                {
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
                let sst_path = self.sst_path_prefix.join(&file_meta.name);
                let path_str = sst_path.to_string_lossy().to_string();
                if let Ok(reader) =
                    crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))
                {
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
            .field("immutable_memtables_len", &self.immutable_memtables.len())
            .field("sst_files_len", &self.sst_files.len())
            .field("sst_path_prefix", &self.sst_path_prefix)
            .field("memory_mode", &self.memory_mode)
            .finish()
    }
}
