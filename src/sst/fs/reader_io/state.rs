use super::{BlockEntryDecoder, KeyState, SstEntry, SstFileIo};
use crate::common::{MidgeError, MidgeResult};
use crate::sst::bloom::writer::BloomTestResult;
use crate::sst::encoding;
use crate::types::EntryType;
use bytes::Bytes;

impl SstFileIo {
    pub(super) fn scan_block_entries_from_bytes(
        &self,
        block_data: &bytes::Bytes,
    ) -> MidgeResult<Vec<SstEntry>> {
        let mut result = Vec::new();
        let mut decoder = BlockEntryDecoder::default();

        while let Some(entry) =
            decoder.next(block_data, self.format_version, None, "range decoder key")?
        {
            let value_bytes = if let Some(val_off) = entry.value_offset {
                let val_len = match entry.value {
                    Some(v) => v.len(),
                    None => {
                        return Err(MidgeError::Corruption(
                            "value offset present without value".into(),
                        ))
                    }
                };
                Some(block_data.slice(val_off..val_off + val_len))
            } else {
                None
            };

            result.push(SstEntry::new(
                decoder.key().to_vec(),
                value_bytes,
                entry.sequence,
                entry.entry_type,
                entry.expiration,
            ));
        }

        Ok(result)
    }

    pub(super) fn state_from_entry(entry: SstEntry) -> KeyState {
        if entry.is_tombstone() {
            KeyState::Tombstone(entry.sequence)
        } else if let Some(value) = entry.value {
            KeyState::Value(value, entry.sequence, entry.expiration, entry.op_type)
        } else {
            KeyState::Absent
        }
    }

    fn state_from_entry_view(block_data: &Bytes, entry: encoding::EntryView<'_>) -> KeyState {
        if matches!(entry.entry_type, EntryType::Delete) {
            return KeyState::Tombstone(entry.sequence);
        }

        let Some(value_offset) = entry.value_offset else {
            return KeyState::Absent;
        };
        let value_len = entry.value.map_or(0, <[u8]>::len);
        let value_end = value_offset.saturating_add(value_len);
        KeyState::Value(
            block_data.slice(value_offset..value_end),
            entry.sequence,
            entry.expiration,
            entry.entry_type,
        )
    }

    fn state_sequence(state: &KeyState) -> u64 {
        match state {
            KeyState::Absent => 0,
            KeyState::Tombstone(sequence) | KeyState::Value(_, sequence, _, _) => *sequence,
        }
    }

    pub(super) fn merge_newer_state(best_state: &mut KeyState, candidate: KeyState) {
        let candidate_sequence = Self::state_sequence(&candidate);
        let best_sequence = Self::state_sequence(best_state);
        if matches!(best_state, KeyState::Absent)
            || candidate_sequence > best_sequence
            || (candidate_sequence == best_sequence
                && matches!(&candidate, KeyState::Tombstone(_))
                && !matches!(best_state, KeyState::Tombstone(_)))
        {
            *best_state = candidate;
        }
    }

    pub(super) fn key_state_from_encoded_block(
        &self,
        block_data: &Bytes,
        key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<KeyState> {
        let mut decoder = BlockEntryDecoder::default();
        let mut best_state = KeyState::Absent;

        let budget = self
            .recovery_block
            .as_ref()
            .and(self.metadata_budget.as_ref());
        while let Some(entry) = decoder.next(
            block_data,
            self.format_version,
            budget,
            "recovery decoder key",
        )? {
            match decoder.key().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    if snapshot_seq == u64::MAX || entry.sequence <= snapshot_seq {
                        let candidate = Self::state_from_entry_view(block_data, entry);
                        Self::merge_newer_state(&mut best_state, candidate);
                    }
                }
            }
        }

        Ok(best_state)
    }

    /// Check block bloom filter with proper metrics and failure-safe semantics
    pub(super) fn check_block_bloom(&self, block_idx: usize, key: &[u8]) -> bool {
        if let Some(ref block_bloom) = self.block_bloom_filter {
            self.bloom_metrics.record_check();
            self.diagnostics.sst_metrics().record_bloom_check();

            match block_bloom.might_contain_in_block(block_idx, key) {
                BloomTestResult::DefinitelyNotPresent => {
                    self.bloom_metrics.record_negative();
                    self.bloom_metrics.record_block_skipped();
                    self.diagnostics.sst_metrics().record_bloom_reject();
                    false
                }
                BloomTestResult::MightBePresent => true,
            }
        } else {
            // No bloom filter - default to MAYBE (safe)
            true
        }
    }
}
