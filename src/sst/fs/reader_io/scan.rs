use super::{BlockHandle, KeyState, SstEntry, SstFileIo};
use crate::common::MidgeResult;
use bytes::Bytes;

impl crate::sst::SstReader for SstFileIo {
    fn get(&self, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        let now_millis = crate::common::time::unix_time_millis();
        let state = <Self as crate::sst::SstStateReader>::get_state_at_with_time(
            self,
            key,
            u64::MAX,
            now_millis,
        )?;
        Ok(match state {
            KeyState::Value(value, _, _, _) => Some(value),
            KeyState::Absent | KeyState::Tombstone(_) => None,
        })
    }

    fn scan_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        if self.range_outside_persisted_bounds(start, end) {
            return Ok(Vec::new());
        }

        let index = self.index_entries()?;
        let mut latest_by_key: std::collections::BTreeMap<Vec<u8>, SstEntry> =
            std::collections::BTreeMap::new();
        let now_millis = crate::common::time::unix_time_millis();

        let start_block = start
            .and_then(|start_key| self.candidate_block_indices(index.as_ref(), start_key))
            .map_or(0, |range| *range.start());
        let end_block = end
            .and_then(|end_key| self.candidate_block_indices(index.as_ref(), end_key))
            .map_or_else(|| index.len().saturating_sub(1), |range| *range.end());

        if index.is_empty() || start_block >= index.len() || start_block > end_block {
            return Ok(Vec::new());
        }

        let handles: Vec<BlockHandle> = index[start_block..=end_block]
            .iter()
            .map(|(_, handle)| *handle)
            .collect();
        self.diagnostics
            .sst_metrics()
            .record_candidate_blocks_checked(handles.len());

        // Process blocks in readahead windows for cold-cache efficiency
        for window_start in (0..handles.len()).step_by(Self::READAHEAD_WINDOW_BLOCKS) {
            let window_end = (window_start + Self::READAHEAD_WINDOW_BLOCKS).min(handles.len());
            let window_handles = &handles[window_start..window_end];

            // Single contiguous IO for entire window
            let block_data_vec = self.read_blocks_contiguous(window_handles)?;

            // Process each block's data
            for block_data in block_data_vec {
                let entries = self.scan_block_entries_from_bytes(&block_data)?;
                for entry in entries {
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

                    let replace = latest_by_key.get(&entry.key).is_none_or(|current| {
                        entry.sequence > current.sequence
                            || (entry.sequence == current.sequence
                                && entry.is_tombstone()
                                && !current.is_tombstone())
                    });
                    if replace {
                        latest_by_key.insert(entry.key.clone(), entry);
                    }
                }
            }
        }

        Ok(latest_by_key
            .into_iter()
            .filter_map(|(key, entry)| {
                (!entry.is_tombstone() && !entry.is_expired(now_millis))
                    .then(|| entry.value.map(|value| (Bytes::from(key), value)))
                    .flatten()
            })
            .collect())
    }
}

impl crate::sst::SstStateReader for SstFileIo {
    fn get_state(&self, key: &[u8]) -> MidgeResult<crate::sst::types::KeyState> {
        if self.key_outside_persisted_range(key) {
            return Ok(crate::sst::types::KeyState::Absent);
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
            Self::merge_newer_state(&mut best_state, candidate);
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
    ) -> MidgeResult<crate::sst::types::KeyState> {
        if self.key_outside_persisted_range(key) {
            self.read_amp_metrics.record_read(0, 0, 0);
            return Ok(KeyState::Absent);
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

            blocks_read += 1;
            let block_data = self.read_cached_data_block(&handle)?;
            let candidate = self.key_state_from_encoded_block(&block_data, key, snapshot_seq)?;
            Self::merge_newer_state(&mut best_state, candidate);
        }

        self.read_amp_metrics.record_read(1, 0, blocks_read);

        Ok(match best_state {
            KeyState::Value(_, sequence, expiration, _)
                if crate::common::time::is_expired_at(expiration, now_millis) =>
            {
                KeyState::Tombstone(sequence)
            }
            state => state,
        })
    }

    fn get_state_at(
        &self,
        key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<crate::sst::types::KeyState> {
        let now_millis = crate::common::time::unix_time_millis();
        self.get_state_at_with_time(key, snapshot_seq, now_millis)
    }

    fn scan_range_state_with_time(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        now_millis: u64,
    ) -> MidgeResult<Vec<(Bytes, crate::sst::types::KeyState)>> {
        if self.range_outside_persisted_bounds(start, end) {
            return Ok(Vec::new());
        }

        let index = self.index_entries()?;
        let mut result = Vec::new();

        let start_block = start
            .and_then(|start_key| self.candidate_block_indices(index.as_ref(), start_key))
            .map_or(0, |range| *range.start());
        let end_block = end
            .and_then(|end_key| self.candidate_block_indices(index.as_ref(), end_key))
            .map_or_else(|| index.len().saturating_sub(1), |range| *range.end());

        if index.is_empty() || start_block >= index.len() || start_block > end_block {
            return Ok(Vec::new());
        }

        let handles = &index[start_block..=end_block];
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
    ) -> MidgeResult<Vec<(Bytes, crate::sst::types::KeyState)>> {
        let now_millis = crate::common::time::unix_time_millis();
        self.scan_range_state_with_time(start, end, now_millis)
    }

    fn range_tombstones(&self) -> Vec<crate::sst::types::RangeTombstone> {
        self.range_tombstones.clone()
    }
}
