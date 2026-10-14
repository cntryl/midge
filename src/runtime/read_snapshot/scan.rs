use super::{
    Arc, FileMeta, KeyState, LevelRangeCandidates, MidgeError, MidgeResult, RangeCandidates,
    RangeCoverSweep, RangeTombstone, ReadSnapshot, SstFileIo, SstStateReader, SstStateScan,
};
use crate::sst::read_path_metrics::SstReadObserver as _;

#[cfg(test)]
#[path = "l0_scan_tests.rs"]
mod l0_scan_tests;
#[cfg(test)]
#[path = "tests.rs"]
mod tests;

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
    fn drain_opened_tombstones(&mut self, into: &mut RangeCoverSweep) {
        if let SnapshotStateIterator::SstLevel(level) = &mut self.iterator {
            for tombstone in level.opened_tombstones.drain(..) {
                into.insert(tombstone.start, tombstone.end, tombstone.seq);
            }
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
    range_cover: RangeCoverSweep,
}

enum SnapshotScanLifecycle {
    Active,
    Exhausted,
    Failed(MidgeError),
}

impl SnapshotScan {
    pub(super) fn new(
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
            range_cover: RangeCoverSweep::new(reverse),
        }
    }

    /// Deterministic count of range-tombstone coverage steps performed.
    #[cfg(test)]
    pub(super) fn tombstone_work(&self) -> u64 {
        self.range_cover.work()
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
        for tombstone in reader.range_tombstones() {
            self.snapshot
                .diagnostics
                .sst_metrics()
                .record_range_tombstone_scan();
            if tombstone.visible_at(self.sequence)
                && ReadSnapshot::range_tombstone_overlaps_query(&tombstone, start, end)
            {
                self.range_cover
                    .insert(tombstone.start, tombstone.end, tombstone.seq);
            }
        }
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
                snapshot
                    .memtable
                    .range_raw_state_at(start, end, self.sequence),
                self.reverse,
            )));
        for tombstone in snapshot.memtable.range_tombstones_at(self.sequence) {
            if ReadSnapshot::range_tombstone_overlaps_query(&tombstone, start, end) {
                self.range_cover
                    .insert(tombstone.start, tombstone.end, tombstone.seq);
            }
        }

        for immutable in &snapshot.immutable_memtables {
            self.sources
                .push(SnapshotStateSource::new(Self::memory_iterator(
                    immutable.range_raw_state_at(start, end, self.sequence),
                    self.reverse,
                )));
            for tombstone in immutable.range_tombstones_at(self.sequence) {
                if ReadSnapshot::range_tombstone_overlaps_query(&tombstone, start, end) {
                    self.range_cover
                        .insert(tombstone.start, tombstone.end, tombstone.seq);
                }
            }
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
                source.drain_opened_tombstones(&mut self.range_cover);
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

            let state_seq = ReadSnapshot::state_sequence(&best).unwrap_or(0);
            if self
                .range_cover
                .max_cover_seq(key.as_ref())
                .is_some_and(|cover_seq| cover_seq >= state_seq)
            {
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
