use super::point_read::{Extent, Observe};
#[cfg(test)]
use super::BTreeMap;
use super::{
    Arc, FileMeta, Fs, MidgeResult, RangeCandidates, ReadResources, ReadSnapshot, SkipListMemtable,
    SnapshotScan, SstFileIo, SstReadView,
};
#[cfg(test)]
use super::{KeyState, SstStateReader};
use crate::sst::cache::CacheFill;
use crate::sst::read_path_metrics::SstReadObserver as _;

impl ReadSnapshot {
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

    #[cfg(test)]
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
        #[cfg(any(test, feature = "internal-testing"))]
        diagnostics.record_read_snapshot_build();
        Self {
            read_time_millis,
            sources: Arc::new(super::ReadSnapshotSources {
                cf_id,
                memtable,
                immutable_memtables,
                sst_view,
                sst_fs,
                sst_path_prefix,
                memory_mode,
                read_resources,
                diagnostics,
            }),
        }
    }

    pub(crate) fn has_sources(
        &self,
        memtable: &Arc<SkipListMemtable>,
        immutable_memtables: &[Arc<SkipListMemtable>],
        sst_view: &Arc<SstReadView>,
    ) -> bool {
        Arc::ptr_eq(&self.memtable, memtable)
            && Arc::ptr_eq(&self.sst_view, sst_view)
            && self.immutable_memtables.len() == immutable_memtables.len()
            && self
                .immutable_memtables
                .iter()
                .zip(immutable_memtables)
                .all(|(left, right)| Arc::ptr_eq(left, right))
    }

    pub(crate) fn with_read_time_millis(mut self, read_time_millis: u64) -> Self {
        self.read_time_millis = read_time_millis;
        self
    }

    /// A scan whose SST block reads bypass block-cache insertion (#944).
    #[cfg(test)]
    pub(crate) fn state_scan(
        self: &Arc<Self>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        sequence: u64,
    ) -> SnapshotScan {
        self.state_scan_with_cache_fill(start, end, reverse, sequence, CacheFill::Bypass)
    }

    pub(crate) fn state_scan_with_cache_fill(
        self: &Arc<Self>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        reverse: bool,
        sequence: u64,
        cache_fill: CacheFill,
    ) -> SnapshotScan {
        SnapshotScan::new(Arc::clone(self), start, end, reverse, sequence, cache_fill)
    }

    pub(super) fn sst_reader(
        &self,
        file_meta: &FileMeta,
    ) -> crate::common::MidgeResult<Arc<SstFileIo>> {
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
    ///
    /// Sources are visited newest-first and the walk stops at the first one
    /// that decides the key (#903); level order (SST-10) guarantees the rest
    /// hold only older versions. `read_amp_metrics` counts the SSTs touched.
    pub fn get_bytes(&self, key: &[u8], seq: u64) -> MidgeResult<Option<bytes::Bytes>> {
        let walk = self.walk_point(key, seq, Observe::Reads, Extent::FirstHit)?;
        self.diagnostics.read_amp_metrics().record_read(
            walk.ssts_touched,
            walk.l0_ssts_touched,
            walk.blocks_read,
        );
        #[cfg(any(test, feature = "internal-testing"))]
        self.audit_point_walk(key, seq, &walk)?;
        Ok(walk.resolve(self.read_time_millis))
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
    ///
    /// The walk stops at the first deciding source like [`Self::get_bytes`];
    /// it records neither read heat nor read-amplification metrics.
    pub fn latest_state_sequence(&self, key: &[u8]) -> MidgeResult<Option<u64>> {
        let walk = self.walk_point(key, u64::MAX, Observe::Nothing, Extent::FirstHit)?;
        #[cfg(any(test, feature = "internal-testing"))]
        self.audit_point_walk(key, u64::MAX, &walk)?;
        Ok(walk.decided_sequence().filter(|sequence| *sequence > 0))
    }

    /// Find one sequence newer than `threshold` touching [start, end).
    /// Includes point state and overlapping range tombstones. A complete
    /// manifest sequence bound can rule out a file before it is opened.
    pub fn any_sequence_after_in_range(
        &self,
        start: &[u8],
        end: &[u8],
        threshold: u64,
    ) -> MidgeResult<Option<u64>> {
        let start_opt = if start.is_empty() { None } else { Some(start) };
        let end_opt = if end.is_empty() { None } else { Some(end) };

        for (_key, state) in self.memtable.range_state_at_with_time(
            start_opt,
            end_opt,
            u64::MAX,
            self.read_time_millis,
        ) {
            if let Some(seq) = Self::state_sequence(&state).filter(|seq| *seq > threshold) {
                return Ok(Some(seq));
            }
        }

        for imm in &self.immutable_memtables {
            for (_key, state) in
                imm.range_state_at_with_time(start_opt, end_opt, u64::MAX, self.read_time_millis)
            {
                if let Some(seq) = Self::state_sequence(&state).filter(|seq| *seq > threshold) {
                    return Ok(Some(seq));
                }
            }
            for tombstone in imm.range_tombstones_at(u64::MAX) {
                if Self::range_tombstone_overlaps_query(&tombstone, start_opt, end_opt)
                    && tombstone.seq > threshold
                {
                    return Ok(Some(tombstone.seq));
                }
            }
        }

        for tombstone in self.memtable.range_tombstones_at(u64::MAX) {
            if Self::range_tombstone_overlaps_query(&tombstone, start_opt, end_opt)
                && tombstone.seq > threshold
            {
                return Ok(Some(tombstone.seq));
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
                if file_meta.key_bounds_complete
                    && file_meta.largest_seq.is_some_and(|seq| seq <= threshold)
                {
                    continue;
                }
                let reader = self.sst_reader(&file_meta)?;
                if let Some(seq) =
                    reader.newer_overlapping_range_tombstone_seq(start_opt, end_opt, threshold)
                {
                    return Ok(Some(seq));
                }
                for entry in reader.raw_state_scan(
                    start_opt.map(<[u8]>::to_vec),
                    end_opt.map(<[u8]>::to_vec),
                    false,
                    u64::MAX,
                    CacheFill::Normal,
                ) {
                    let (_, state) = entry?;
                    if let Some(seq) = Self::state_sequence(&state).filter(|seq| *seq > threshold) {
                        return Ok(Some(seq));
                    }
                }
            }
        }
        Ok(None)
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

        for (key, state) in self.memtable.range_raw_state_at(start_opt, end_opt, seq) {
            Self::merge_state(&mut states, key, state)?;
        }

        for imm in &self.immutable_memtables {
            for (key, state) in imm.range_raw_state_at(start_opt, end_opt, seq) {
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

impl Clone for ReadSnapshot {
    fn clone(&self) -> Self {
        #[cfg(any(test, feature = "internal-testing"))]
        self.diagnostics.record_read_snapshot_build();
        Self {
            sources: Arc::clone(&self.sources),
            read_time_millis: self.read_time_millis,
        }
    }
}
