//! Read snapshot - immutable view for parallel read execution
//!
//! Captures immutable references to memtables and SST metadata
//! at a specific sequence number, allowing safe parallel reads.

use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;
use crate::metadata::FileMeta;
use crate::runtime::read_resources::ReadResources;
use crate::sst::fs::reader_io::SstStateScan;
use crate::sst::fs::SstFileIo;
use crate::sst::traits::SstStateReader;
use crate::sst::types::{KeyState, RangeTombstone};
use crate::sst::SkipListMemtable;
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::HashMap;
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
    /// Wall-clock captured when this snapshot was created. TTL visibility is
    /// evaluated against this value for the whole snapshot.
    pub read_time_millis: u64,
    read_resources: Option<Arc<ReadResources>>,
    diagnostics: Arc<crate::diagnostics::RuntimeDiagnostics>,
    sst_readers: HashMap<String, Arc<SstFileIo>>,
}

enum SnapshotStateIterator {
    Memory(std::vec::IntoIter<(bytes::Bytes, KeyState)>),
    Sst(Box<SstStateScan>),
}

impl SnapshotStateIterator {
    fn next(&mut self) -> Option<MidgeResult<(bytes::Bytes, KeyState)>> {
        match self {
            Self::Memory(entries) => entries.next().map(Ok),
            Self::Sst(entries) => entries.next(),
        }
    }
}

struct SnapshotStateSource {
    iterator: SnapshotStateIterator,
    head: Option<MidgeResult<(bytes::Bytes, KeyState)>>,
    needs_advance: bool,
}

impl SnapshotStateSource {
    fn new(mut iterator: SnapshotStateIterator) -> Self {
        let head = iterator.next();
        Self {
            iterator,
            head,
            needs_advance: false,
        }
    }

    fn advance_if_needed(&mut self) {
        if self.needs_advance {
            self.head = self.iterator.next();
            self.needs_advance = false;
        }
    }

    fn take_head(&mut self) -> Option<MidgeResult<(bytes::Bytes, KeyState)>> {
        let head = self.head.take();
        self.needs_advance = head.is_some();
        head
    }
}

/// Lazy merge cursor over every state source pinned by a read snapshot.
pub(crate) struct SnapshotScan {
    snapshot: Arc<ReadSnapshot>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    reverse: bool,
    sequence: u64,
    initialized: bool,
    exhausted: bool,
    sources: Vec<SnapshotStateSource>,
    range_tombstones: Vec<RangeTombstone>,
}

impl SnapshotScan {
    fn new(
        snapshot: Arc<ReadSnapshot>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        sequence: u64,
    ) -> Self {
        Self {
            snapshot,
            start,
            end,
            reverse,
            sequence,
            initialized: false,
            exhausted: false,
            sources: Vec::new(),
            range_tombstones: Vec::new(),
        }
    }

    fn memory_iterator(
        mut states: Vec<(Vec<u8>, KeyState)>,
        reverse: bool,
    ) -> SnapshotStateIterator {
        if reverse {
            states.reverse();
        }
        SnapshotStateIterator::Memory(
            states
                .into_iter()
                .map(|(key, state)| (bytes::Bytes::from(key), state))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn initialize(&mut self) -> MidgeResult<()> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        let start = self.start.as_deref();
        let end = self.end.as_deref();
        let snapshot = &self.snapshot;

        self.sources
            .push(SnapshotStateSource::new(Self::memory_iterator(
                snapshot.memtable.range_state_at_with_time(
                    start,
                    end,
                    self.sequence,
                    snapshot.read_time_millis,
                ),
                self.reverse,
            )));
        self.range_tombstones.extend(
            snapshot
                .memtable
                .range_tombstones_at(self.sequence)
                .into_iter()
                .filter(|tombstone| {
                    ReadSnapshot::range_tombstone_overlaps_query(tombstone, start, end)
                }),
        );

        for immutable in &snapshot.immutable_memtables {
            self.sources
                .push(SnapshotStateSource::new(Self::memory_iterator(
                    immutable.range_state_at_with_time(
                        start,
                        end,
                        self.sequence,
                        snapshot.read_time_millis,
                    ),
                    self.reverse,
                )));
            self.range_tombstones.extend(
                immutable
                    .range_tombstones_at(self.sequence)
                    .into_iter()
                    .filter(|tombstone| {
                        ReadSnapshot::range_tombstone_overlaps_query(tombstone, start, end)
                    }),
            );
        }

        if !snapshot.memory_mode {
            for file_meta in &snapshot.sst_files {
                file_meta.record_read();
                snapshot
                    .diagnostics
                    .sst_metrics()
                    .record_candidate_sst_file_checked();
                let reader = snapshot.sst_reader(file_meta)?;
                self.range_tombstones
                    .extend(reader.range_tombstones().into_iter().filter(|tombstone| {
                        snapshot
                            .diagnostics
                            .sst_metrics()
                            .record_range_tombstone_scan();
                        (self.sequence == u64::MAX || tombstone.seq <= self.sequence)
                            && ReadSnapshot::range_tombstone_overlaps_query(tombstone, start, end)
                    }));
                self.sources
                    .push(SnapshotStateSource::new(SnapshotStateIterator::Sst(
                        Box::new(reader.state_scan(
                            self.start.clone(),
                            self.end.clone(),
                            self.reverse,
                            self.sequence,
                            snapshot.read_time_millis,
                        )),
                    )));
            }
        }
        Ok(())
    }

    fn take_source_error(&mut self) -> Option<MidgeError> {
        self.sources.iter_mut().find_map(|source| {
            if source.head.as_ref().is_some_and(Result::is_err) {
                source.take_head().and_then(Result::err)
            } else {
                None
            }
        })
    }

    fn next_key(&self) -> Option<bytes::Bytes> {
        self.sources
            .iter()
            .filter_map(|source| source.head.as_ref()?.as_ref().ok().map(|(key, _)| key))
            .cloned()
            .reduce(|selected, candidate| {
                let candidate_wins = if self.reverse {
                    candidate > selected
                } else {
                    candidate < selected
                };
                if candidate_wins {
                    candidate
                } else {
                    selected
                }
            })
    }

    fn next_visible(&mut self) -> MidgeResult<Option<(bytes::Bytes, bytes::Bytes)>> {
        self.initialize()?;
        loop {
            for source in &mut self.sources {
                source.advance_if_needed();
            }
            if let Some(error) = self.take_source_error() {
                return Err(error);
            }

            let Some(key) = self.next_key() else {
                return Ok(None);
            };
            let mut best = None;

            for source in &mut self.sources {
                let matches_key = source
                    .head
                    .as_ref()
                    .and_then(|head| head.as_ref().ok())
                    .is_some_and(|(candidate, _)| candidate == &key);
                if !matches_key {
                    continue;
                }

                let state = source
                    .take_head()
                    .and_then(Result::ok)
                    .map_or(KeyState::Absent, |(_, state)| state);
                ReadSnapshot::merge_best_state(&mut best, state, self.snapshot.read_time_millis);
            }

            let Some(best) = best else {
                continue;
            };

            if ReadSnapshot::range_tombstone_covers_state(
                &self.range_tombstones,
                key.as_ref(),
                &best,
            ) {
                continue;
            }

            if let KeyState::Value(value, _, expiration, _) = best {
                if !crate::common::time::is_expired_at(expiration, self.snapshot.read_time_millis) {
                    return Ok(Some((key, value)));
                }
            }
        }
    }
}

impl std::iter::Iterator for SnapshotScan {
    type Item = MidgeResult<(bytes::Bytes, bytes::Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.next_visible() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.exhausted = true;
                None
            }
            Err(error) => {
                self.exhausted = true;
                Some(Err(error))
            }
        }
    }
}

impl ReadSnapshot {
    pub(crate) fn current_time_millis() -> u64 {
        crate::common::time::unix_time_millis()
    }

    fn state_sequence(state: &KeyState) -> Option<u64> {
        match state {
            KeyState::Absent => None,
            KeyState::Tombstone(seq) | KeyState::Value(_, seq, _, _) => Some(*seq),
        }
    }

    fn normalize_state(state: KeyState, now_millis: u64) -> KeyState {
        match state {
            KeyState::Value(_, seq, exp, _)
                if crate::common::time::is_expired_at(exp, now_millis) =>
            {
                KeyState::Tombstone(seq)
            }
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

    #[cfg(test)]
    fn merge_state(
        states: &mut BTreeMap<Vec<u8>, KeyState>,
        key: Vec<u8>,
        state: KeyState,
        now_millis: u64,
    ) {
        let normalized = Self::normalize_state(state, now_millis);
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

    fn merge_best_state(best_state: &mut Option<KeyState>, state: KeyState, now_millis: u64) {
        let normalized = Self::normalize_state(state, now_millis);
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

    #[cfg(test)]
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
        let diagnostics = read_resources.as_ref().map_or_else(
            crate::diagnostics::legacy_runtime_diagnostics,
            |resources| resources.diagnostics(),
        );
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
            read_time_millis: Self::current_time_millis(),
            read_resources,
            diagnostics,
            sst_readers,
        }
    }

    pub(crate) fn with_read_time_millis(mut self, read_time_millis: u64) -> Self {
        self.read_time_millis = read_time_millis;
        self
    }

    pub(crate) fn state_scan(
        self: &Arc<Self>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        sequence: u64,
    ) -> SnapshotScan {
        SnapshotScan::new(Arc::clone(self), start, end, reverse, sequence)
    }

    fn sst_reader(&self, file_meta: &FileMeta) -> crate::common::MidgeResult<Arc<SstFileIo>> {
        if let Some(reader) = self.sst_readers.get(&file_meta.name) {
            self.diagnostics.sst_metrics().record_reader_cache_hit();
            return Ok(Arc::clone(reader));
        }
        if let Some(resources) = &self.read_resources {
            return resources.reader_for(file_meta);
        }

        let sst_path = self.sst_path_prefix.join(&file_meta.name);
        let path_str = sst_path.to_string_lossy().to_string();
        self.diagnostics.sst_metrics().record_reader_cache_miss();
        Ok(Arc::new(
            crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))?
                .with_read_path_diagnostics(Arc::clone(&self.diagnostics)),
        ))
    }

    /// Perform a point read on this snapshot
    pub fn get(&self, key: &[u8], seq: u64) -> MidgeResult<Option<Vec<u8>>> {
        let mut best_state = None;
        let mut range_tombstones = Vec::new();

        let state = self
            .memtable
            .get_key_state_at_with_time(key, seq, self.read_time_millis)?;
        Self::merge_best_state(&mut best_state, state, self.read_time_millis);

        for imm in &self.immutable_memtables {
            let state = imm.get_key_state_at_with_time(key, seq, self.read_time_millis)?;
            Self::merge_best_state(&mut best_state, state, self.read_time_millis);
            range_tombstones.extend(
                imm.range_tombstones_at(seq)
                    .into_iter()
                    .filter(|tombstone| tombstone.covers(key)),
            );
        }
        range_tombstones.extend(
            self.memtable
                .range_tombstones_at(seq)
                .into_iter()
                .filter(|tombstone| tombstone.covers(key)),
        );

        // Manifest key bounds are advisory and older manifests may not
        // include range-tombstone endpoints. Let each opened SST's persisted
        // metadata make the exclusion decision instead of risking a skipped
        // masking tombstone.
        if !self.memory_mode {
            for file_meta in &self.sst_files {
                file_meta.record_read();
                self.diagnostics
                    .sst_metrics()
                    .record_candidate_sst_file_checked();
                let reader = self.sst_reader(file_meta)?;
                let state = reader.get_state_at_with_time(key, seq, self.read_time_millis)?;
                Self::merge_best_state(&mut best_state, state, self.read_time_millis);

                self.diagnostics.sst_metrics().record_range_tombstone_scan();
                range_tombstones.extend(reader.range_tombstones().into_iter().filter(
                    |tombstone| (seq == u64::MAX || tombstone.seq <= seq) && tombstone.covers(key),
                ));
            }
        }

        let Some(state) = best_state else {
            return Ok(None);
        };
        if Self::range_tombstone_covers_state(&range_tombstones, key, &state) {
            return Ok(None);
        }

        Ok(match state {
            KeyState::Value(value, _, exp, _)
                if !crate::common::time::is_expired_at(exp, self.read_time_millis) =>
            {
                Some(value.to_vec())
            }
            _ => None,
        })
    }

    /// Return the latest sequence touching `key` across memtables + SSTs.
    ///
    /// Includes tombstones and range tombstones so conflict detection can
    /// identify any write after a transaction start snapshot.
    pub fn latest_state_sequence(&self, key: &[u8]) -> MidgeResult<Option<u64>> {
        let mut best_state = None;
        let mut range_tombstones = Vec::new();

        let state =
            self.memtable
                .get_key_state_at_with_time(key, u64::MAX, self.read_time_millis)?;
        Self::merge_best_state(&mut best_state, state, self.read_time_millis);

        for imm in &self.immutable_memtables {
            let state = imm.get_key_state_at_with_time(key, u64::MAX, self.read_time_millis)?;
            Self::merge_best_state(&mut best_state, state, self.read_time_millis);
            range_tombstones.extend(
                imm.range_tombstones_at(u64::MAX)
                    .into_iter()
                    .filter(|tombstone| tombstone.covers(key)),
            );
        }
        range_tombstones.extend(
            self.memtable
                .range_tombstones_at(u64::MAX)
                .into_iter()
                .filter(|tombstone| tombstone.covers(key)),
        );

        if !self.memory_mode {
            for file_meta in &self.sst_files {
                let reader = self.sst_reader(file_meta)?;
                let state = reader.get_state_at_with_time(key, u64::MAX, self.read_time_millis)?;
                Self::merge_best_state(&mut best_state, state, self.read_time_millis);

                range_tombstones.extend(
                    reader
                        .range_tombstones()
                        .into_iter()
                        .filter(|tombstone| tombstone.covers(key)),
                );
            }
        }

        let state_seq = best_state
            .as_ref()
            .and_then(Self::state_sequence)
            .unwrap_or(0);
        let range_tombstone_seq = range_tombstones.iter().map(|t| t.seq).max().unwrap_or(0);
        let max_seq = state_seq.max(range_tombstone_seq);

        if max_seq == 0 {
            Ok(None)
        } else {
            Ok(Some(max_seq))
        }
    }

    /// Return the latest sequence touching any key in [start, end).
    ///
    /// Includes value/tombstone state and overlapping range tombstones.
    pub fn latest_sequence_in_range(&self, start: &[u8], end: &[u8]) -> MidgeResult<Option<u64>> {
        let start_opt = if start.is_empty() { None } else { Some(start) };
        let end_opt = if end.is_empty() { None } else { Some(end) };

        let mut max_seq = 0u64;

        for (_key, state) in self.memtable.range_state_at_with_time(
            start_opt,
            end_opt,
            u64::MAX,
            self.read_time_millis,
        ) {
            if let Some(seq) = Self::state_sequence(&state) {
                max_seq = max_seq.max(seq);
            }
        }

        for imm in &self.immutable_memtables {
            for (_key, state) in
                imm.range_state_at_with_time(start_opt, end_opt, u64::MAX, self.read_time_millis)
            {
                if let Some(seq) = Self::state_sequence(&state) {
                    max_seq = max_seq.max(seq);
                }
            }
            for tombstone in imm.range_tombstones_at(u64::MAX) {
                if Self::range_tombstone_overlaps_query(&tombstone, start_opt, end_opt) {
                    max_seq = max_seq.max(tombstone.seq);
                }
            }
        }

        for tombstone in self.memtable.range_tombstones_at(u64::MAX) {
            if Self::range_tombstone_overlaps_query(&tombstone, start_opt, end_opt) {
                max_seq = max_seq.max(tombstone.seq);
            }
        }

        if !self.memory_mode {
            for file_meta in &self.sst_files {
                let reader = self.sst_reader(file_meta)?;
                let entries =
                    reader.scan_range_state_with_time(start_opt, end_opt, self.read_time_millis)?;
                for (_key, state) in entries {
                    if let Some(seq) = Self::state_sequence(&state) {
                        max_seq = max_seq.max(seq);
                    }
                }

                for tombstone in reader.range_tombstones() {
                    if Self::range_tombstone_overlaps_query(&tombstone, start_opt, end_opt) {
                        max_seq = max_seq.max(tombstone.seq);
                    }
                }
            }
        }

        if max_seq == 0 {
            Ok(None)
        } else {
            Ok(Some(max_seq))
        }
    }

    /// Perform a range scan on this snapshot
    #[cfg(test)]
    pub fn range_scan(
        &self,
        start: &[u8],
        end: &[u8],
        seq: u64,
    ) -> MidgeResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut states: BTreeMap<Vec<u8>, KeyState> = BTreeMap::new();
        let mut range_tombstones = Vec::new();

        // Treat empty bounds as unbounded
        let start_opt = if start.is_empty() { None } else { Some(start) };
        let end_opt = if end.is_empty() { None } else { Some(end) };

        for (key, state) in
            self.memtable
                .range_state_at_with_time(start_opt, end_opt, seq, self.read_time_millis)
        {
            Self::merge_state(&mut states, key, state, self.read_time_millis);
        }

        for imm in &self.immutable_memtables {
            for (key, state) in
                imm.range_state_at_with_time(start_opt, end_opt, seq, self.read_time_millis)
            {
                Self::merge_state(&mut states, key, state, self.read_time_millis);
            }
        }

        range_tombstones.extend(self.memtable.range_tombstones_at(seq).into_iter().filter(
            |tombstone| Self::range_tombstone_overlaps_query(tombstone, start_opt, end_opt),
        ));
        for imm in &self.immutable_memtables {
            range_tombstones.extend(
                imm.range_tombstones_at(seq)
                    .into_iter()
                    .filter(|tombstone| {
                        Self::range_tombstone_overlaps_query(tombstone, start_opt, end_opt)
                    }),
            );
        }

        if !self.memory_mode {
            for file_meta in &self.sst_files {
                file_meta.record_read();
                let reader = self.sst_reader(file_meta)?;
                self.diagnostics
                    .sst_metrics()
                    .record_candidate_sst_file_checked();
                let entries =
                    reader.scan_range_state_with_time(start_opt, end_opt, self.read_time_millis)?;
                for (key, state) in entries {
                    if Self::is_visible_state(&state, seq) {
                        Self::merge_state(&mut states, key.to_vec(), state, self.read_time_millis);
                    }
                }

                range_tombstones.extend(reader.range_tombstones().into_iter().filter(
                    |tombstone| {
                        self.diagnostics.sst_metrics().record_range_tombstone_scan();
                        (seq == u64::MAX || tombstone.seq <= seq)
                            && Self::range_tombstone_overlaps_query(tombstone, start_opt, end_opt)
                    },
                ));
            }
        }

        Ok(states
            .into_iter()
            .filter_map(|(key, state)| {
                if Self::range_tombstone_covers_state(&range_tombstones, &key, &state) {
                    return None;
                }

                match state {
                    KeyState::Value(value, _, exp, _)
                        if !crate::common::time::is_expired_at(exp, self.read_time_millis) =>
                    {
                        Some((key, value.to_vec()))
                    }
                    _ => None,
                }
            })
            .collect())
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
            .field("read_time_millis", &self.read_time_millis)
            .field("read_resources", &self.read_resources.is_some())
            .field("sst_readers", &self.sst_readers.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::SstFactory;

    #[test]
    fn should_use_shared_block_cache_for_snapshot_sst_reads() -> crate::common::MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
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
        assert!(
            !block_cache.is_empty(),
            "first point read should synchronously populate shared block cache"
        );
        let hits_before = block_cache.metrics().hit_count();
        let second = snapshot.get(b"cache-key", u64::MAX);
        let hits_after = block_cache.metrics().hit_count();

        // Assert
        assert_eq!(first?, Some(b"cache-value".to_vec()));
        assert_eq!(second?, Some(b"cache-value".to_vec()));
        assert!(
            hits_after > hits_before,
            "second snapshot read should hit shared block cache"
        );
        Ok(())
    }

    #[test]
    fn should_not_skip_range_tombstone_when_manifest_bounds_are_narrow(
    ) -> crate::common::MidgeResult<()> {
        // Arrange: model an older manifest whose second SST recorded only its
        // point key `m`, even though the SST itself contains [a, c) tombstone.
        let temp_dir = tempfile::tempdir()?;
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);

        let mut old_writer = factory.create()?;
        old_writer.add_with_meta(b"b", Some(b"old"), 1, 0, None)?;
        crate::sst::fs::finish_writer_to_path(old_writer, &temp_dir.path().join("old.sst"))?;

        let mut tombstone_writer = factory.create()?;
        tombstone_writer.add_with_meta(b"m", Some(b"new"), 3, 0, None)?;
        tombstone_writer.add_range_tombstone(b"a", b"c", 2)?;
        crate::sst::fs::finish_writer_to_path(
            tombstone_writer,
            &temp_dir.path().join("tombstone.sst"),
        )?;

        let snapshot = ReadSnapshot::new(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            vec![
                FileMeta {
                    name: "old.sst".to_string(),
                    level: 0,
                    size_bytes: std::fs::metadata(temp_dir.path().join("old.sst"))?.len(),
                    cf_id: 0,
                    smallest_key: Some(b"b".to_vec()),
                    largest_key: Some(b"b".to_vec()),
                    smallest_seq: Some(1),
                    largest_seq: Some(1),
                    ..Default::default()
                },
                FileMeta {
                    name: "tombstone.sst".to_string(),
                    level: 0,
                    size_bytes: std::fs::metadata(temp_dir.path().join("tombstone.sst"))?.len(),
                    cf_id: 0,
                    // Intentionally stale/narrow manifest metadata.
                    smallest_key: Some(b"m".to_vec()),
                    largest_key: Some(b"m".to_vec()),
                    smallest_seq: Some(2),
                    largest_seq: Some(3),
                    ..Default::default()
                },
            ],
            fs,
            std::path::PathBuf::new(),
            false,
        );

        // Act
        let value = snapshot.get(b"b", u64::MAX)?;

        // Assert
        assert_eq!(value, None);
        Ok(())
    }
}
