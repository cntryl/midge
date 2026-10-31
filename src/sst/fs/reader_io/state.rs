use super::{BlockEntryDecoder, KeyState, SstFileIo};
use crate::common::{MidgeError, MidgeResult};
use crate::sst::bloom::writer::BloomTestResult;
use crate::sst::encoding;
use crate::types::EntryType;
use bytes::Bytes;

/// A data block decoded for a lazy scan: one key arena plus per-entry
/// positions into it and into the block's value bytes.
pub(super) struct DecodedScanBlock {
    keys: Bytes,
    entries: Vec<ScanEntryMeta>,
}

struct ScanEntryMeta {
    key_start: usize,
    key_end: usize,
    value: Option<Bytes>,
    sequence: u64,
    entry_type: EntryType,
    expiration: Option<u64>,
}

/// One entry handed to the scan; `key` and `value` share their block's
/// buffers, so cloning either is a reference count.
pub(super) struct ScanEntry {
    pub(super) key: Bytes,
    pub(super) value: Option<Bytes>,
    pub(super) sequence: u64,
    pub(super) entry_type: EntryType,
    pub(super) expiration: Option<u64>,
}

impl DecodedScanBlock {
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn entry(&self, index: usize) -> ScanEntry {
        let meta = &self.entries[index];
        ScanEntry {
            key: self.keys.slice(meta.key_start..meta.key_end),
            value: meta.value.clone(),
            sequence: meta.sequence,
            entry_type: meta.entry_type,
            expiration: meta.expiration,
        }
    }
}

impl SstFileIo {
    /// Decode a data block for a lazy scan. Every reconstructed key is appended
    /// to one arena and the entries index into it, so handing a key to the
    /// scan is a slice of the shared arena rather than a copy per key (#945).
    pub(super) fn decode_scan_block(&self, block_data: &Bytes) -> MidgeResult<DecodedScanBlock> {
        Self::decode_scan_extent(block_data, self.format_version, 0, None)
    }

    pub(super) fn decode_scan_interval(
        block_data: &Bytes,
        format_version: u32,
        restart: usize,
    ) -> MidgeResult<DecodedScanBlock> {
        let layout = crate::sst::data_block::DataBlockLayout::read(block_data)?;
        Self::decode_scan_extent(
            block_data,
            format_version,
            restart,
            Some(layout.interval_end(restart)?),
        )
    }

    fn decode_scan_extent(
        block_data: &Bytes,
        format_version: u32,
        restart: usize,
        stop: Option<usize>,
    ) -> MidgeResult<DecodedScanBlock> {
        // The block is sliced once per value; cloning it first promotes the
        // buffer to a shared one so every later slice is a reference count.
        let block = block_data.clone();
        let mut keys = Vec::new();
        let mut entries = Vec::new();
        let mut decoder = BlockEntryDecoder::at_restart(&block, restart)?;

        while stop.is_none_or(|stop| decoder.offset < stop) {
            let Some(entry) = decoder.next(&block, format_version, None, "range decoder key")?
            else {
                break;
            };
            let value = match (entry.value_offset, entry.value) {
                (Some(offset), Some(value)) => Some(block.slice(offset..offset + value.len())),
                (Some(_), None) => {
                    return Err(MidgeError::Corruption(
                        "value offset present without value".into(),
                    ))
                }
                (None, _) => None,
            };
            let key_start = keys.len();
            keys.extend_from_slice(decoder.key());
            entries.push(ScanEntryMeta {
                key_start,
                key_end: keys.len(),
                value,
                sequence: entry.sequence,
                entry_type: entry.entry_type,
                expiration: entry.expiration,
            });
        }

        Ok(DecodedScanBlock {
            keys: Bytes::from(keys),
            entries,
        })
    }

    pub(super) fn state_from_scan_entry(entry: ScanEntry) -> KeyState {
        if entry.entry_type == EntryType::Delete {
            KeyState::Tombstone(entry.sequence)
        } else if let Some(value) = entry.value {
            KeyState::Value(value, entry.sequence, entry.expiration, entry.entry_type)
        } else {
            KeyState::Absent
        }
    }

    pub(super) fn state_from_entry_view(
        block_data: &Bytes,
        entry: encoding::EntryView<'_>,
    ) -> KeyState {
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

    pub(super) fn merge_newer_state(
        best_state: &mut KeyState,
        candidate: KeyState,
    ) -> MidgeResult<()> {
        let candidate_sequence = Self::state_sequence(&candidate);
        let best_sequence = Self::state_sequence(best_state);
        if !matches!(best_state, KeyState::Absent)
            && !matches!(candidate, KeyState::Absent)
            && candidate_sequence == best_sequence
        {
            crate::types::resolve_same_sequence(
                crate::types::VersionContent::from_state(best_state).expect("present state"),
                crate::types::VersionContent::from_state(&candidate).expect("present state"),
            )
            .map_err(|()| {
                crate::common::MidgeError::Corruption(format!(
                    "conflicting SST versions at sequence {candidate_sequence}"
                ))
            })?;
        }
        if matches!(best_state, KeyState::Absent) || candidate_sequence > best_sequence {
            *best_state = candidate;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn key_state_from_encoded_block(
        &self,
        block_data: &Bytes,
        key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<KeyState> {
        self.key_state_from_encoded_block_with_stats(block_data, key, snapshot_seq)
            .map(|(state, _)| state)
    }

    #[cfg(test)]
    pub(super) fn key_state_from_encoded_block_with_stats(
        &self,
        block_data: &Bytes,
        key: &[u8],
        snapshot_seq: u64,
    ) -> MidgeResult<(KeyState, u64)> {
        let mut best_state = KeyState::Absent;
        let (entries_decoded, _) =
            self.merge_key_run_from_block(block_data, key, snapshot_seq, &mut best_state)?;
        Ok((best_state, entries_decoded))
    }

    /// Merge `key`'s visible versions in one block into `best_state`.
    ///
    /// Versions of a key are written newest first, so once a visible version
    /// is known only equal-sequence duplicates can still matter: they must
    /// agree, or the SST is corrupt. Returns the entries decoded and whether
    /// the key's run is complete, meaning later blocks cannot change the
    /// result: a lower sequence or a different key followed the winner.
    pub(super) fn merge_key_run_from_block(
        &self,
        block_data: &Bytes,
        key: &[u8],
        snapshot_seq: u64,
        best_state: &mut KeyState,
    ) -> MidgeResult<(u64, bool)> {
        let layout = crate::sst::data_block::DataBlockLayout::read(block_data)?;
        let (restart, probes) = layout.seek_interval(key)?;
        let mut decoder = BlockEntryDecoder::at_restart(block_data, restart)?;
        let mut interval = restart;
        let mut entries_decoded = probes as u64;
        loop {
            if decoder.offset == layout.interval_end(interval)? {
                let next_interval = interval + 1;
                if next_interval == layout.restart_count {
                    return Ok((entries_decoded, false));
                }
                let next_key = layout.validate_restart_link(next_interval, decoder.key())?;
                if next_key != key {
                    return Ok((entries_decoded, true));
                }
                interval = next_interval;
            }
            let Some(entry) =
                decoder.next(block_data, self.format_version, None, "point decoder key")?
            else {
                return Ok((entries_decoded, false));
            };
            entries_decoded += 1;
            match decoder.key().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Greater => return Ok((entries_decoded, true)),
                std::cmp::Ordering::Equal => {
                    if !matches!(best_state, KeyState::Absent)
                        && entry.sequence < Self::state_sequence(best_state)
                    {
                        return Ok((entries_decoded, true));
                    }
                    if snapshot_seq == u64::MAX || entry.sequence <= snapshot_seq {
                        let candidate = Self::state_from_entry_view(block_data, entry);
                        Self::merge_newer_state(best_state, candidate)?;
                    }
                }
            }
        }
    }

    /// Check block bloom filter with proper metrics and failure-safe semantics
    pub(super) fn check_block_bloom(&self, block_idx: usize, key: &[u8]) -> bool {
        if let Some(ref block_bloom) = self.block_bloom_filter {
            self.diagnostics.sst_metrics().record_bloom_check();

            match block_bloom.might_contain_in_block(block_idx, key) {
                BloomTestResult::DefinitelyNotPresent => {
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
