use super::{KeyState, SstFileIo, SstPointReadStats};
use crate::common::MidgeResult;
use bytes::Bytes;

impl SstFileIo {
    pub(crate) fn get_state_at_with_time_and_stats(
        &self,
        key: &[u8],
        snapshot_seq: u64,
        now_millis: u64,
    ) -> MidgeResult<(crate::types::KeyState, SstPointReadStats)> {
        let (raw, stats) = self.get_raw_state_at_with_stats(key, snapshot_seq)?;
        let state = match raw {
            KeyState::Value(_, sequence, expiration, _)
                if crate::common::time::is_expired_at(expiration, now_millis) =>
            {
                KeyState::Tombstone(sequence)
            }
            state => state,
        };
        Ok((state, stats))
    }

    pub(crate) fn get_raw_state_at_with_stats(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<(crate::types::KeyState, SstPointReadStats)> {
        if self.key_outside_persisted_range(key) {
            return Ok((KeyState::Absent, SstPointReadStats::default()));
        }

        let mut best_state = KeyState::Absent;
        let index = self.index_entries()?;
        let mut blocks_read = 1u64;

        let candidate_blocks = self.candidate_data_blocks(index.as_ref(), key);
        self.diagnostics
            .sst_metrics()
            .record_candidate_blocks_checked(candidate_blocks.len());

        for (idx, handle) in candidate_blocks {
            if !self.check_block_bloom(idx, key) {
                continue;
            }

            blocks_read = blocks_read.saturating_add(1);
            let block_data = self.read_cached_data_block(&handle)?;
            let candidate = self.key_state_from_encoded_block(&block_data, key, snapshot_seq)?;
            Self::merge_newer_state(&mut best_state, candidate)?;
        }

        Ok((
            best_state,
            SstPointReadStats {
                sst_touched: true,
                blocks_read,
            },
        ))
    }
}

impl crate::sst::SstStateReader for SstFileIo {
    fn raw_version_cursor_with_budget(
        self: Box<Self>,
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        budget: Option<crate::common::resource_budget::ResourceBudget>,
    ) -> MidgeResult<crate::sst::traits::RawSstVersionCursor> {
        let reader: std::sync::Arc<Self> = self.into();
        Ok(Box::new(super::SstRawVersionScan::new(
            reader, start, end, budget,
        )?))
    }

    fn scan_range_raw_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, crate::types::KeyState)>> {
        if self.range_outside_persisted_bounds(start, end) {
            return Ok(Vec::new());
        }
        let index = self.index_entries()?;
        let mut result = Vec::new();
        let Some(span) = self.block_span(index.as_ref(), start, end) else {
            return Ok(result);
        };
        for (_first_key, handle) in &index[span] {
            let block_data = self.read_cached_data_block(handle)?;
            for entry in self.scan_block_entries_from_bytes(&block_data)? {
                if start.is_some_and(|bound| entry.key.as_slice() < bound)
                    || end.is_some_and(|bound| entry.key.as_slice() >= bound)
                {
                    continue;
                }
                let key = Bytes::from(entry.key.clone());
                let state = if entry.is_tombstone() {
                    KeyState::Tombstone(entry.sequence)
                } else {
                    Self::state_from_entry(entry)
                };
                result.push((key, state));
            }
        }
        Ok(result)
    }

    fn get_state(&self, key: &[u8]) -> MidgeResult<crate::types::KeyState> {
        if self.key_outside_persisted_range(key) {
            return Ok(crate::types::KeyState::Absent);
        }

        let mut best_state = KeyState::Absent;
        let index = self.index_entries()?;

        let candidate_blocks = self.candidate_data_blocks(index.as_ref(), key);
        self.diagnostics
            .sst_metrics()
            .record_candidate_blocks_checked(candidate_blocks.len());

        for (_idx, handle) in candidate_blocks {
            let block_data = self.read_cached_data_block(&handle)?;
            let candidate = self.key_state_from_encoded_block(&block_data, key, u64::MAX)?;
            Self::merge_newer_state(&mut best_state, candidate)?;
        }

        let now_millis = crate::common::time::unix_time_millis();
        Ok(match best_state {
            KeyState::Value(_, sequence, expiration, _)
                if crate::common::time::is_expired_at(expiration, now_millis) =>
            {
                KeyState::Tombstone(sequence)
            }
            state => state,
        })
    }

    fn get_state_at_with_time(
        &self,
        key: &[u8],
        snapshot_seq: u64,
        now_millis: u64,
    ) -> MidgeResult<crate::types::KeyState> {
        self.get_state_at_with_time_and_stats(key, snapshot_seq, now_millis)
            .map(|(state, _stats)| state)
    }

    fn get_state_at(&self, key: &[u8], snapshot_seq: u64) -> MidgeResult<crate::types::KeyState> {
        let now_millis = crate::common::time::unix_time_millis();
        self.get_state_at_with_time(key, snapshot_seq, now_millis)
    }

    fn scan_range_state_with_time(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        now_millis: u64,
    ) -> MidgeResult<Vec<(Bytes, crate::types::KeyState)>> {
        if self.range_outside_persisted_bounds(start, end) {
            return Ok(Vec::new());
        }

        let index = self.index_entries()?;
        let mut result = Vec::new();

        let Some(span) = self.block_span(index.as_ref(), start, end) else {
            return Ok(Vec::new());
        };

        let handles = &index[span];
        self.diagnostics
            .sst_metrics()
            .record_candidate_blocks_checked(handles.len());

        for (_first_key, handle) in handles {
            let block_data = self.read_cached_data_block(handle)?;
            for entry in self.scan_block_entries_from_bytes(&block_data)? {
                if let Some(s) = start {
                    if entry.key.as_slice() < s {
                        continue;
                    }
                }
                if let Some(e) = end {
                    if entry.key.as_slice() >= e {
                        continue;
                    }
                }

                let key = Bytes::from(entry.key.clone());
                let state = if entry.is_tombstone() || entry.is_expired(now_millis) {
                    KeyState::Tombstone(entry.sequence)
                } else {
                    Self::state_from_entry(entry)
                };
                result.push((key, state));
            }
        }

        Ok(result)
    }

    fn scan_range_state(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, crate::types::KeyState)>> {
        let now_millis = crate::common::time::unix_time_millis();
        self.scan_range_state_with_time(start, end, now_millis)
    }

    fn range_tombstones(&self) -> Vec<crate::types::RangeTombstone> {
        self.range_tombstones.clone()
    }

    fn max_covering_tombstone_seq(&self, key: &[u8], snapshot_seq: u64) -> Option<u64> {
        crate::memtable::max_covering_seq(&self.range_tombstones, key, snapshot_seq)
    }

    fn range_tombstone_memory_usage(&self) -> usize {
        self.range_tombstones.iter().fold(
            self.range_tombstones
                .len()
                .saturating_mul(std::mem::size_of::<crate::types::RangeTombstone>()),
            |total, tombstone| {
                total
                    .saturating_add(tombstone.start.capacity())
                    .saturating_add(tombstone.end.capacity())
            },
        )
    }
}
