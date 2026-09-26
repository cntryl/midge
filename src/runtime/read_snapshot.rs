//! Read snapshot - immutable view for parallel read execution
//!
//! Captures immutable references to memtables and SST metadata
//! at a specific sequence number, allowing safe parallel reads.

use crate::common::{MidgeError, MidgeResult};
use crate::io::Fs;
use crate::memtable::SkipListMemtable;
use crate::metadata::FileMeta;
use crate::runtime::read_resources::ReadResources;
use crate::runtime::sst_read_view::{LevelRangeCandidates, RangeCandidates, SstReadView};
use crate::sst::fs::reader_io::SstStateScan;
use crate::sst::fs::SstFileIo;
use crate::sst::read_path_metrics::SstReadObserver as _;
use crate::sst::traits::SstStateReader;
#[cfg(test)]
use crate::types::EntryType;
use crate::types::{KeyState, RangeTombstone};
#[cfg(test)]
use std::collections::BTreeMap;
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
    /// Immutable indexed SST catalog for this column family.
    pub(crate) sst_view: Arc<SstReadView>,
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
}

enum SnapshotStateIterator {
    Memory(std::vec::IntoIter<(bytes::Bytes, KeyState)>),
    Sst(Box<SstStateScan>),
    SstLevel(Box<SstLevelStateIterator>),
}

impl SnapshotStateIterator {
    fn next(&mut self) -> Option<MidgeResult<(bytes::Bytes, KeyState)>> {
        match self {
            Self::Memory(entries) => entries.next().map(Ok),
            Self::Sst(entries) => entries.next(),
            Self::SstLevel(entries) => entries.next(),
        }
    }
}

struct SstLevelStateIterator {
    snapshot: Arc<ReadSnapshot>,
    files: std::vec::IntoIter<Arc<FileMeta>>,
    current: Option<SstStateScan>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    reverse: bool,
    sequence: u64,
    pending: Option<MidgeResult<(bytes::Bytes, KeyState)>>,
    /// Range tombstones of files opened since the scan last collected them.
    /// A file's tombstones join the scan when the cursor opens it, not up
    /// front: bounds of ordered files include their tombstone extents, and
    /// the cursor opens files in key order, so every file whose tombstone can
    /// cover a key has been opened before that key is merged (#390).
    opened_tombstones: Vec<RangeTombstone>,
    /// False for fallback files, whose untrusted bounds force their
    /// tombstones to be collected up front instead.
    collect_tombstones: bool,
}

impl SstLevelStateIterator {
    fn new(
        snapshot: Arc<ReadSnapshot>,
        mut files: Vec<Arc<FileMeta>>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        sequence: u64,
    ) -> Self {
        if reverse {
            files.reverse();
        }
        Self {
            snapshot,
            files: files.into_iter(),
            current: None,
            start,
            end,
            reverse,
            sequence,
            pending: None,
            opened_tombstones: Vec::new(),
            collect_tombstones: true,
        }
    }

    /// For a file whose tombstones the scan already collected up front.
    fn without_tombstone_collection(mut self) -> Self {
        self.collect_tombstones = false;
        self
    }

    fn next_raw(&mut self) -> Option<MidgeResult<(bytes::Bytes, KeyState)>> {
        loop {
            if let Some(current) = &mut self.current {
                if let Some(next) = current.next() {
                    return Some(next);
                }
                self.current = None;
            }

            let file_meta = self.files.next()?;
            file_meta.record_read();
            self.snapshot
                .diagnostics
                .sst_metrics()
                .record_candidate_sst_file_checked();
            let reader = match self.snapshot.sst_reader(&file_meta) {
                Ok(reader) => reader,
                Err(error) => return Some(Err(error)),
            };
            let tombstones = if self.collect_tombstones {
                reader.range_tombstones()
            } else {
                Vec::new()
            };
            for tombstone in tombstones {
                self.snapshot
                    .diagnostics
                    .sst_metrics()
                    .record_range_tombstone_scan();
                if tombstone.visible_at(self.sequence)
                    && ReadSnapshot::range_tombstone_overlaps_query(
                        &tombstone,
                        self.start.as_deref(),
                        self.end.as_deref(),
                    )
                {
                    self.opened_tombstones.push(tombstone);
                }
            }
            self.current = Some(reader.raw_state_scan(
                self.start.clone(),
                self.end.clone(),
                self.reverse,
                self.sequence,
            ));
        }
    }

    fn next(&mut self) -> Option<MidgeResult<(bytes::Bytes, KeyState)>> {
        let first = self.pending.take().or_else(|| self.next_raw())?;
        let (key, first_state) = match first {
            Ok(entry) => entry,
            Err(error) => return Some(Err(error)),
        };
        let mut best = None;
        if let Err(error) = ReadSnapshot::merge_best_state(&mut best, first_state) {
            return Some(Err(error));
        }

        loop {
            let Some(next) = self.next_raw() else {
                return Some(Ok((key, best.unwrap_or(KeyState::Absent))));
            };
            match next {
                Ok((candidate_key, candidate_state)) if candidate_key == key => {
                    if let Err(error) = ReadSnapshot::merge_best_state(&mut best, candidate_state) {
                        return Some(Err(error));
                    }
                }
                // The read-ahead may have failed opening the next file, whose
                // tombstones could cover `key`. Fail closed: surface the error
                // instead of returning a key that may be deleted (#554).
                Err(error) => return Some(Err(error)),
                other => {
                    self.pending = Some(other);
                    return Some(Ok((key, best.unwrap_or(KeyState::Absent))));
                }
            }
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

    /// Moves tombstones of level files opened while computing heads into
    /// `into`; see `SstLevelStateIterator::opened_tombstones`.
    fn drain_opened_tombstones(&mut self, into: &mut Vec<RangeTombstone>) {
        if let SnapshotStateIterator::SstLevel(level) = &mut self.iterator {
            into.append(&mut level.opened_tombstones);
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
    lifecycle: SnapshotScanLifecycle,
    sources: Vec<SnapshotStateSource>,
    range_tombstones: Vec<RangeTombstone>,
}

enum SnapshotScanLifecycle {
    Active,
    Exhausted,
    Failed(MidgeError),
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
            lifecycle: SnapshotScanLifecycle::Active,
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

    fn append_reader_tombstones(
        &mut self,
        file_meta: &FileMeta,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Arc<SstFileIo>> {
        let reader = self.snapshot.sst_reader(file_meta)?;
        self.range_tombstones
            .extend(reader.range_tombstones().into_iter().filter(|tombstone| {
                self.snapshot
                    .diagnostics
                    .sst_metrics()
                    .record_range_tombstone_scan();
                (self.sequence == u64::MAX || tombstone.seq <= self.sequence)
                    && ReadSnapshot::range_tombstone_overlaps_query(tombstone, start, end)
            }));
        Ok(reader)
    }

    fn add_l0_sources(
        &mut self,
        runs: Vec<Vec<Arc<FileMeta>>>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<()> {
        for mut files in runs {
            if files.len() > 1 {
                self.add_level_source(
                    LevelRangeCandidates {
                        ordered: files,
                        fallback: Vec::new(),
                    },
                    start,
                    end,
                )?;
                continue;
            }
            let Some(file_meta) = files.pop() else {
                continue;
            };
            file_meta.record_read();
            self.snapshot
                .diagnostics
                .sst_metrics()
                .record_candidate_sst_file_checked();
            let reader = self.append_reader_tombstones(&file_meta, start, end)?;
            self.sources
                .push(SnapshotStateSource::new(SnapshotStateIterator::Sst(
                    Box::new(reader.raw_state_scan(
                        self.start.clone(),
                        self.end.clone(),
                        self.reverse,
                        self.sequence,
                    )),
                )));
        }
        Ok(())
    }

    fn add_level_source(
        &mut self,
        candidates: LevelRangeCandidates,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<()> {
        // Ordered files' tombstones are collected as the level cursor opens
        // them (see `SstLevelStateIterator::opened_tombstones`), so a limited
        // scan opens only the files it reaches (#390).
        if !candidates.ordered.is_empty() {
            self.sources
                .push(SnapshotStateSource::new(SnapshotStateIterator::SstLevel(
                    Box::new(SstLevelStateIterator::new(
                        Arc::clone(&self.snapshot),
                        candidates.ordered,
                        self.start.clone(),
                        self.end.clone(),
                        self.reverse,
                        self.sequence,
                    )),
                )));
        }

        if candidates.fallback.is_empty() {
            return Ok(());
        }

        // Unknown or quarantined files cannot safely be chained by advisory
        // bounds, so each gets its own streaming source. Materializing them
        // into one map would copy every key in range from the whole level
        // (for example every L1+ file right after a legacy upgrade) into
        // memory with no budget; one cursor per fallback file is bounded by
        // the file count instead. The merge across sources already resolves
        // the same key appearing in several of them.
        for file_meta in candidates.fallback {
            file_meta.record_read();
            self.snapshot
                .diagnostics
                .sst_metrics()
                .record_candidate_sst_file_checked();
            let _reader = self.append_reader_tombstones(&file_meta, start, end)?;
            self.sources
                .push(SnapshotStateSource::new(SnapshotStateIterator::SstLevel(
                    Box::new(
                        SstLevelStateIterator::new(
                            Arc::clone(&self.snapshot),
                            vec![file_meta],
                            self.start.clone(),
                            self.end.clone(),
                            self.reverse,
                            self.sequence,
                        )
                        .without_tombstone_collection(),
                    ),
                )));
        }
        Ok(())
    }

    fn initialize(&mut self) -> MidgeResult<()> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        let start_bound = self.start.clone();
        let end_bound = self.end.clone();
        let start = start_bound.as_deref();
        let end = end_bound.as_deref();
        let snapshot = Arc::clone(&self.snapshot);

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
            let RangeCandidates { l0, levels } = snapshot.sst_view.range_candidates(start, end);
            self.add_l0_sources(l0, start, end)?;
            for level in levels {
                self.add_level_source(level, start, end)?;
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
                // Every head is now computed, so every level file that can
                // cover the next key has been opened; collect its tombstones
                // before any key is checked against them. (The level cursor
                // also reads one entry ahead, which opens files a round early;
                // draining here does not rely on that.)
                source.drain_opened_tombstones(&mut self.range_tombstones);
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
                ReadSnapshot::merge_best_state(&mut best, state)?;
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
        match &self.lifecycle {
            SnapshotScanLifecycle::Failed(error) => return Some(Err(error.replay())),
            SnapshotScanLifecycle::Exhausted => return None,
            SnapshotScanLifecycle::Active => {}
        }
        match self.next_visible() {
            Ok(Some(row)) => Some(Ok(row)),
            Ok(None) => {
                self.lifecycle = SnapshotScanLifecycle::Exhausted;
                None
            }
            Err(error) => {
                self.lifecycle = SnapshotScanLifecycle::Failed(error);
                match &self.lifecycle {
                    SnapshotScanLifecycle::Failed(error) => Some(Err(error.replay())),
                    SnapshotScanLifecycle::Active | SnapshotScanLifecycle::Exhausted => {
                        unreachable!()
                    }
                }
            }
        }
    }
}

impl ReadSnapshot {
    fn state_sequence(state: &KeyState) -> Option<u64> {
        match state {
            KeyState::Absent => None,
            KeyState::Tombstone(seq) | KeyState::Value(_, seq, _, _) => Some(*seq),
        }
    }

    fn candidate_wins(existing: &KeyState, candidate: &KeyState) -> MidgeResult<bool> {
        let existing_seq = Self::state_sequence(existing).unwrap_or(0);
        let candidate_seq = Self::state_sequence(candidate).unwrap_or(0);
        if !matches!(existing, KeyState::Absent)
            && !matches!(candidate, KeyState::Absent)
            && candidate_seq == existing_seq
        {
            crate::types::resolve_same_sequence(
                crate::types::VersionContent::from_state(existing).expect("present state"),
                crate::types::VersionContent::from_state(candidate).expect("present state"),
            )
            .map_err(|()| {
                MidgeError::Corruption(format!(
                    "conflicting read versions at sequence {candidate_seq}"
                ))
            })?;
        }
        Ok(candidate_seq > existing_seq || matches!(existing, KeyState::Absent))
    }

    #[cfg(test)]
    fn merge_state(
        states: &mut BTreeMap<Vec<u8>, KeyState>,
        key: Vec<u8>,
        state: KeyState,
    ) -> MidgeResult<()> {
        if matches!(state, KeyState::Absent) {
            return Ok(());
        }

        match states.get(&key) {
            Some(existing) if !Self::candidate_wins(existing, &state)? => {}
            _ => {
                states.insert(key, state);
            }
        }
        Ok(())
    }

    fn merge_best_state(best_state: &mut Option<KeyState>, state: KeyState) -> MidgeResult<()> {
        if matches!(state, KeyState::Absent) {
            return Ok(());
        }

        if best_state
            .as_ref()
            .map_or(Ok(true), |existing| Self::candidate_wins(existing, &state))?
        {
            *best_state = Some(state);
        }
        Ok(())
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

    /// Create a read snapshot that opens SST readers directly, bypassing the
    /// runtime reader and block caches. Production reads go through
    /// [`Self::new_with_resources`] (#492).
    #[cfg(test)]
    pub fn new(
        memtable: Arc<SkipListMemtable>,
        immutable_memtables: Vec<Arc<SkipListMemtable>>,
        sst_files: Vec<FileMeta>,
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: std::path::PathBuf,
        memory_mode: bool,
        read_time_millis: u64,
    ) -> Self {
        Self::new_with_resources(
            memtable,
            immutable_memtables,
            sst_files,
            sst_fs,
            sst_path_prefix,
            memory_mode,
            read_time_millis,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_resources(
        memtable: Arc<SkipListMemtable>,
        immutable_memtables: Vec<Arc<SkipListMemtable>>,
        sst_files: Vec<FileMeta>,
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: std::path::PathBuf,
        memory_mode: bool,
        read_time_millis: u64,
        read_resources: Option<Arc<ReadResources>>,
    ) -> Self {
        let cf_id = sst_files.first().map_or(0, |f| f.cf_id);
        Self::new_with_view_resources(
            cf_id,
            memtable,
            immutable_memtables,
            Arc::new(SstReadView::new(cf_id, sst_files)),
            sst_fs,
            sst_path_prefix,
            memory_mode,
            read_time_millis,
            read_resources,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_view_resources(
        cf_id: crate::types::ColumnFamilyId,
        memtable: Arc<SkipListMemtable>,
        immutable_memtables: Vec<Arc<SkipListMemtable>>,
        sst_view: Arc<SstReadView>,
        sst_fs: Arc<dyn Fs>,
        sst_path_prefix: std::path::PathBuf,
        memory_mode: bool,
        read_time_millis: u64,
        read_resources: Option<Arc<ReadResources>>,
    ) -> Self {
        let diagnostics = read_resources.as_ref().map_or_else(
            || Arc::new(crate::diagnostics::RuntimeDiagnostics::default()),
            |resources| resources.diagnostics(),
        );
        Self {
            cf_id,
            memtable,
            immutable_memtables,
            sst_view,
            sst_fs,
            sst_path_prefix,
            memory_mode,
            read_time_millis,
            read_resources,
            diagnostics,
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
        if let Some(resources) = &self.read_resources {
            return resources.reader_for(file_meta);
        }

        let sst_path = self.sst_path_prefix.join(&file_meta.name);
        let path_str = sst_path.to_string_lossy().to_string();
        self.diagnostics.sst_metrics().record_reader_cache_miss();
        Ok(Arc::new(
            crate::sst::fs::SstFileIo::open(&path_str, Arc::clone(&self.sst_fs))?
                .with_read_path_diagnostics(self.diagnostics.clone()),
        ))
    }

    pub(crate) fn pinned_sst_names(&self) -> Arc<std::collections::HashSet<String>> {
        self.sst_view.pinned_sst_names()
    }

    #[cfg(test)]
    pub(crate) fn shares_sst_view_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.sst_view, &other.sst_view)
    }

    /// Perform a point read on this snapshot without copying the value.
    pub fn get_bytes(&self, key: &[u8], seq: u64) -> MidgeResult<Option<bytes::Bytes>> {
        let mut best_state = None;
        // Only the highest covering tombstone matters, so none is copied.
        let mut covering_tombstone_seq = None;
        let mut ssts_touched = 0u64;
        let mut l0_ssts_touched = 0u64;
        let mut blocks_read = 0u64;

        let state = self
            .memtable
            .get_key_state_at_with_time(key, seq, self.read_time_millis)?;
        Self::merge_best_state(&mut best_state, state)?;

        for imm in &self.immutable_memtables {
            let state = imm.get_key_state_at_with_time(key, seq, self.read_time_millis)?;
            Self::merge_best_state(&mut best_state, state)?;
            covering_tombstone_seq =
                covering_tombstone_seq.max(imm.max_covering_tombstone_seq(key, seq));
        }
        covering_tombstone_seq =
            covering_tombstone_seq.max(self.memtable.max_covering_tombstone_seq(key, seq));

        // Complete manifest bounds drive indexed selection. Legacy bounds stay
        // in the view's fallback bucket and are still opened conservatively.
        if !self.memory_mode {
            for file_meta in self.sst_view.point_candidates(key) {
                file_meta.record_read();
                self.diagnostics
                    .sst_metrics()
                    .record_candidate_sst_file_checked();
                let reader = self.sst_reader(&file_meta)?;
                let (state, read_stats) = reader.get_raw_state_at_with_stats(key, seq)?;
                if read_stats.sst_touched {
                    ssts_touched = ssts_touched.saturating_add(1);
                    if file_meta.level == 0 {
                        l0_ssts_touched = l0_ssts_touched.saturating_add(1);
                    }
                }
                blocks_read = blocks_read.saturating_add(read_stats.blocks_read);
                Self::merge_best_state(&mut best_state, state)?;

                self.diagnostics.sst_metrics().record_range_tombstone_scan();
                covering_tombstone_seq =
                    covering_tombstone_seq.max(reader.max_covering_tombstone_seq(key, seq));
            }
        }

        self.diagnostics
            .read_amp_metrics()
            .record_read(ssts_touched, l0_ssts_touched, blocks_read);

        let Some(state) = best_state else {
            return Ok(None);
        };
        let state_seq = Self::state_sequence(&state).unwrap_or(0);
        if covering_tombstone_seq.is_some_and(|tombstone_seq| tombstone_seq >= state_seq) {
            return Ok(None);
        }

        Ok(match state {
            KeyState::Value(value, _, exp, _)
                if !crate::common::time::is_expired_at(exp, self.read_time_millis) =>
            {
                Some(value)
            }
            _ => None,
        })
    }

    /// Perform a point read on this snapshot.
    pub fn get(&self, key: &[u8], seq: u64) -> MidgeResult<Option<Vec<u8>>> {
        self.get_bytes(key, seq)
            .map(|value| value.map(|bytes| bytes.to_vec()))
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
        Self::merge_best_state(&mut best_state, state)?;

        for imm in &self.immutable_memtables {
            let state = imm.get_key_state_at_with_time(key, u64::MAX, self.read_time_millis)?;
            Self::merge_best_state(&mut best_state, state)?;
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
            for file_meta in self.sst_view.point_candidates(key) {
                let reader = self.sst_reader(&file_meta)?;
                let (state, _) = reader.get_raw_state_at_with_stats(key, u64::MAX)?;
                Self::merge_best_state(&mut best_state, state)?;

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
            let RangeCandidates { l0, levels } = self.sst_view.range_candidates(start_opt, end_opt);
            let files = l0.into_iter().flatten().chain(
                levels
                    .into_iter()
                    .flat_map(|level| level.ordered.into_iter().chain(level.fallback)),
            );
            for file_meta in files {
                let reader = self.sst_reader(&file_meta)?;
                let entries = reader.scan_range_raw_state(start_opt, end_opt)?;
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
            Self::merge_state(&mut states, key, state)?;
        }

        for imm in &self.immutable_memtables {
            for (key, state) in
                imm.range_state_at_with_time(start_opt, end_opt, seq, self.read_time_millis)
            {
                Self::merge_state(&mut states, key, state)?;
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
            let RangeCandidates { l0, levels } = self.sst_view.range_candidates(start_opt, end_opt);
            let files = l0.into_iter().flatten().chain(
                levels
                    .into_iter()
                    .flat_map(|level| level.ordered.into_iter().chain(level.fallback)),
            );
            for file_meta in files {
                file_meta.record_read();
                let reader = self.sst_reader(&file_meta)?;
                self.diagnostics
                    .sst_metrics()
                    .record_candidate_sst_file_checked();
                let entries = reader.scan_range_raw_state(start_opt, end_opt)?;
                for (key, state) in entries {
                    if Self::is_visible_state(&state, seq) {
                        Self::merge_state(&mut states, key.to_vec(), state)?;
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
            .field("sst_files_len", &self.sst_view.file_count())
            .field("sst_fs", &"<dyn Fs>")
            .field("sst_path_prefix", &self.sst_path_prefix)
            .field("memory_mode", &self.memory_mode)
            .field("read_time_millis", &self.read_time_millis)
            .field("read_resources", &self.read_resources.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod l0_scan_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::traits::SstFactory;

    #[test]
    fn should_reject_conflicting_equal_sequence_ssts_for_point_and_scans(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let dir = tempfile::tempdir()?;
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
        let mut files = Vec::new();
        for (name, value, op) in [
            ("put.sst", Some(b"value".as_slice()), EntryType::Put),
            ("delete.sst", None, EntryType::Delete),
        ] {
            let mut writer = factory.create()?;
            writer.add_with_meta(b"key", value, 5, op, None)?;
            crate::sst::fs::finish_writer_to_path(writer, &dir.path().join(name))?;
            files.push(FileMeta {
                name: name.to_string(),
                level: 0,
                cf_id: 0,
                size_bytes: std::fs::metadata(dir.path().join(name))?.len(),
                smallest_key: Some(b"key".to_vec()),
                largest_key: Some(b"key".to_vec()),
                smallest_seq: Some(5),
                largest_seq: Some(5),
                key_bounds_complete: true,
                ..Default::default()
            });
        }
        let snapshot = Arc::new(ReadSnapshot::new(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            files.clone(),
            fs,
            std::path::PathBuf::new(),
            false,
            0,
        ));

        // Act and assert
        assert!(matches!(
            snapshot.get(b"key", u64::MAX),
            Err(MidgeError::Corruption(_))
        ));
        assert!(matches!(
            snapshot.range_scan(b"", b"", u64::MAX),
            Err(MidgeError::Corruption(_))
        ));
        assert!(matches!(
            snapshot.state_scan(None, None, false, u64::MAX).next(),
            Some(Err(MidgeError::Corruption(_)))
        ));

        // Recovery can legitimately replay a delete at its original sequence.
        let mut writer = factory.create()?;
        writer.add_with_meta(b"key", None, 5, EntryType::Delete, None)?;
        crate::sst::fs::finish_writer_to_path(writer, &dir.path().join("duplicate.sst"))?;
        let mut duplicate = files[1].clone();
        duplicate.name = "duplicate.sst".to_string();
        duplicate.size_bytes = std::fs::metadata(dir.path().join("duplicate.sst"))?.len();
        let duplicate_snapshot = Arc::new(ReadSnapshot::new(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            vec![files[1].clone(), duplicate],
            Arc::new(crate::io::RealFs::new(dir.path())?),
            std::path::PathBuf::new(),
            false,
            0,
        ));
        assert_eq!(duplicate_snapshot.get(b"key", u64::MAX)?, None);
        assert!(duplicate_snapshot
            .range_scan(b"", b"", u64::MAX)?
            .is_empty());
        assert!(duplicate_snapshot
            .state_scan(None, None, false, u64::MAX)
            .next()
            .is_none());

        // TTL interpretation must not hide differing persisted metadata.
        let mut expired_files = Vec::new();
        for (name, expiration) in [("expired-a.sst", 1), ("expired-b.sst", 2)] {
            let mut writer = factory.create()?;
            writer.add_with_meta(b"key", Some(b"value"), 5, EntryType::Put, Some(expiration))?;
            crate::sst::fs::finish_writer_to_path(writer, &dir.path().join(name))?;
            let mut file = files[0].clone();
            file.name = name.to_string();
            file.size_bytes = std::fs::metadata(dir.path().join(name))?.len();
            expired_files.push(file);
        }
        let expired_snapshot = Arc::new(ReadSnapshot::new(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            expired_files,
            Arc::new(crate::io::RealFs::new(dir.path())?),
            std::path::PathBuf::new(),
            false,
            0,
        ));
        assert!(matches!(
            expired_snapshot.get(b"key", u64::MAX),
            Err(MidgeError::Corruption(_))
        ));
        assert!(matches!(
            expired_snapshot
                .state_scan(None, None, false, u64::MAX)
                .next(),
            Some(Err(MidgeError::Corruption(_)))
        ));
        Ok(())
    }

    #[test]
    fn should_use_shared_block_cache_for_snapshot_sst_reads() -> crate::common::MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(fs, 4096);
        let mut writer = factory.create()?;
        writer.add_with_meta(b"cache-key", Some(b"cache-value"), 10, EntryType::Put, None)?;
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
            0,
            Some(Arc::clone(&read_resources)),
        );
        let block_cache = read_resources.block_cache();
        assert_eq!(
            read_resources.cached_reader_count(),
            0,
            "snapshot construction must not eagerly open every SST reader"
        );

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
    fn should_share_indexed_sst_view_across_consecutive_snapshots() {
        // Arrange
        let sst_view = Arc::new(SstReadView::new(0, Vec::new()));
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::MockFs::new());

        // Act
        let first = ReadSnapshot::new_with_view_resources(
            0,
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            Arc::clone(&sst_view),
            Arc::clone(&fs),
            std::path::PathBuf::new(),
            true,
            1,
            None,
        );
        let second = ReadSnapshot::new_with_view_resources(
            0,
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            sst_view,
            fs,
            std::path::PathBuf::new(),
            true,
            2,
            None,
        );

        // Assert
        assert!(first.shares_sst_view_with(&second));
    }

    #[test]
    fn should_open_only_adjacent_lower_level_readers_for_point_lookup(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
        let mut files = Vec::new();
        for index in 0..128_u64 {
            let name = format!("point-{index:04}.sst");
            let key = index.to_be_bytes();
            let mut writer = factory.create()?;
            writer.add_with_meta(&key, Some(key.as_slice()), index + 1, EntryType::Put, None)?;
            crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(&name))?;
            let size_bytes = std::fs::metadata(temp_dir.path().join(&name))?.len();
            files.push(FileMeta {
                name,
                level: 1,
                size_bytes,
                cf_id: 0,
                sst_seq: index + 1,
                smallest_key: Some(key.to_vec()),
                largest_key: Some((index + 1).to_be_bytes().to_vec()),
                smallest_seq: Some(index + 1),
                largest_seq: Some(index + 1),
                key_bounds_complete: true,
                ..Default::default()
            });
        }
        let read_resources = Arc::new(ReadResources::new(
            Arc::clone(&fs),
            std::path::PathBuf::new(),
            1024 * 1024,
            crate::sst::cache::CachePolicyType::Lru,
        ));
        let snapshot = ReadSnapshot::new_with_resources(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            files,
            fs,
            std::path::PathBuf::new(),
            false,
            0,
            Some(Arc::clone(&read_resources)),
        );
        let key = 64_u64.to_be_bytes();

        // Act
        let value = snapshot.get(&key, u64::MAX)?;

        // Assert
        assert_eq!(value.as_deref(), Some(key.as_slice()));
        assert_eq!(
            read_resources.cached_reader_count(),
            2,
            "an equality boundary may open both adjacent files, never the full level"
        );
        Ok(())
    }

    #[test]
    fn should_use_one_sequential_cursor_per_complete_lower_level_in_both_directions(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
        let mut files = Vec::new();
        for index in 0..64_u64 {
            let name = format!("level-{index:04}.sst");
            let key = format!("key-{index:04}").into_bytes();
            let mut writer = factory.create()?;
            writer.add_with_meta(&key, Some(key.as_slice()), index + 1, EntryType::Put, None)?;
            crate::sst::fs::finish_writer_to_path(writer, &temp_dir.path().join(&name))?;
            files.push(FileMeta {
                name,
                level: 1,
                size_bytes: 1,
                cf_id: 0,
                sst_seq: index + 1,
                smallest_key: Some(key.clone()),
                largest_key: Some(key),
                smallest_seq: Some(index + 1),
                largest_seq: Some(index + 1),
                key_bounds_complete: true,
                ..Default::default()
            });
        }
        let snapshot = Arc::new(ReadSnapshot::new(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            files,
            fs,
            std::path::PathBuf::new(),
            false,
            0,
        ));

        // Act
        let mut forward = snapshot.state_scan(None, None, false, u64::MAX);
        forward.initialize()?;
        let forward_cursor_slots = forward
            .sources
            .iter()
            .filter(|source| {
                matches!(
                    &source.iterator,
                    SnapshotStateIterator::Sst(_) | SnapshotStateIterator::SstLevel(_)
                )
            })
            .count();
        let forward_rows = forward.collect::<MidgeResult<Vec<_>>>()?;

        let mut reverse = snapshot.state_scan(None, None, true, u64::MAX);
        reverse.initialize()?;
        let reverse_cursor_slots = reverse
            .sources
            .iter()
            .filter(|source| {
                matches!(
                    &source.iterator,
                    SnapshotStateIterator::Sst(_) | SnapshotStateIterator::SstLevel(_)
                )
            })
            .count();
        let reverse_rows = reverse.collect::<MidgeResult<Vec<_>>>()?;

        // Assert
        assert_eq!(forward_cursor_slots, 1);
        assert_eq!(reverse_cursor_slots, 1);
        assert_eq!(forward_rows.len(), 64);
        assert_eq!(reverse_rows.len(), 64);
        assert_eq!(
            forward_rows.first().map(|row| row.0.as_ref()),
            Some(b"key-0000".as_slice())
        );
        assert_eq!(
            forward_rows.last().map(|row| row.0.as_ref()),
            Some(b"key-0063".as_slice())
        );
        assert_eq!(
            reverse_rows.first().map(|row| row.0.as_ref()),
            Some(b"key-0063".as_slice())
        );
        assert_eq!(
            reverse_rows.last().map(|row| row.0.as_ref()),
            Some(b"key-0000".as_slice())
        );
        Ok(())
    }

    #[test]
    fn should_merge_equal_boundary_key_once_in_each_scan_direction(
    ) -> crate::common::MidgeResult<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let fs: Arc<dyn crate::io::Fs> = Arc::new(crate::io::RealFs::new(temp_dir.path())?);
        let factory = crate::sst::FsSstFactoryIo::new(Arc::clone(&fs), 4096);
        let mut first_writer = factory.create()?;
        first_writer.add_with_meta(b"a", Some(b"first-a"), 1, EntryType::Put, None)?;
        first_writer.add_with_meta(b"b", Some(b"old-b"), 2, EntryType::Put, None)?;
        crate::sst::fs::finish_writer_to_path(first_writer, &temp_dir.path().join("first.sst"))?;
        let mut second_writer = factory.create()?;
        second_writer.add_with_meta(b"b", Some(b"new-b"), 3, EntryType::Put, None)?;
        second_writer.add_with_meta(b"c", Some(b"second-c"), 4, EntryType::Put, None)?;
        crate::sst::fs::finish_writer_to_path(second_writer, &temp_dir.path().join("second.sst"))?;
        let files = vec![
            FileMeta {
                name: "first.sst".to_string(),
                level: 1,
                cf_id: 0,
                smallest_key: Some(b"a".to_vec()),
                largest_key: Some(b"b".to_vec()),
                smallest_seq: Some(1),
                largest_seq: Some(2),
                key_bounds_complete: true,
                ..Default::default()
            },
            FileMeta {
                name: "second.sst".to_string(),
                level: 1,
                cf_id: 0,
                smallest_key: Some(b"b".to_vec()),
                largest_key: Some(b"c".to_vec()),
                smallest_seq: Some(3),
                largest_seq: Some(4),
                key_bounds_complete: true,
                ..Default::default()
            },
        ];
        let snapshot = Arc::new(ReadSnapshot::new(
            Arc::new(SkipListMemtable::new()),
            Vec::new(),
            files,
            fs,
            std::path::PathBuf::new(),
            false,
            0,
        ));

        // Act
        let point = snapshot.get(b"b", u64::MAX)?;
        let forward = snapshot
            .state_scan(None, None, false, u64::MAX)
            .collect::<MidgeResult<Vec<_>>>()?;
        let reverse = snapshot
            .state_scan(None, None, true, u64::MAX)
            .collect::<MidgeResult<Vec<_>>>()?;

        // Assert
        assert_eq!(point.as_deref(), Some(b"new-b".as_slice()));
        assert_eq!(forward.len(), 3);
        assert_eq!(reverse.len(), 3);
        assert_eq!(forward[1].0.as_ref(), b"b");
        assert_eq!(forward[1].1.as_ref(), b"new-b");
        assert_eq!(reverse[1].0.as_ref(), b"b");
        assert_eq!(reverse[1].1.as_ref(), b"new-b");
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
        old_writer.add_with_meta(b"b", Some(b"old"), 1, EntryType::Put, None)?;
        crate::sst::fs::finish_writer_to_path(old_writer, &temp_dir.path().join("old.sst"))?;

        let mut tombstone_writer = factory.create()?;
        tombstone_writer.add_with_meta(b"m", Some(b"new"), 3, EntryType::Put, None)?;
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
            0,
        );

        // Act
        let value = snapshot.get(b"b", u64::MAX)?;

        // Assert
        assert_eq!(value, None);
        Ok(())
    }
}
