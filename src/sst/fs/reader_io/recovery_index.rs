//! Charged seek metadata owned by one retained, validated decoded block.

use super::SstFileIo;
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};
use crate::sst::encoding;
use crate::types::KeyState;
use bytes::Bytes;

struct IndexedKey {
    start: usize,
    len: usize,
    encoded_offset: usize,
}

pub(super) struct RecoveryKeyIndex {
    keys: Vec<u8>,
    entries: Vec<IndexedKey>,
    pub(super) charged_bytes: usize,
    _reservation: ResourceReservation,
}

impl RecoveryKeyIndex {
    pub(super) fn build(
        block: &Bytes,
        version: u32,
        budget: &ResourceBudget,
        available_retention: usize,
        steps: &mut u64,
        allocations: &mut u64,
    ) -> MidgeResult<Option<Self>> {
        // Size the two fixed buffers without constructing any keys. Prefix
        // length is checked against the previous reconstructed length.
        let (mut offset, mut previous_len, mut key_bytes, mut count) = (0, 0, 0_usize, 0_usize);
        while offset < block.len() {
            let (entry, next) = encoding::decode_with_format(block, offset, version)?;
            *steps = steps.saturating_add(1);
            let shared = SstFileIo::shared_prefix_len(usize::from(entry.shared_len), previous_len)?;
            previous_len = shared
                .checked_add(entry.key_delta.len())
                .ok_or_else(index_overflow)?;
            key_bytes = key_bytes
                .checked_add(previous_len)
                .ok_or_else(index_overflow)?;
            count = count.checked_add(1).ok_or_else(index_overflow)?;
            offset = next;
        }
        let charged_bytes = count
            .checked_mul(std::mem::size_of::<IndexedKey>())
            .and_then(|bytes| bytes.checked_add(key_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() + 64))
            .ok_or_else(index_overflow)?;
        if charged_bytes > available_retention {
            return Ok(None);
        }
        let reservation = match budget.reserve(charged_bytes, "recovery block key index") {
            Ok(reservation) => reservation,
            Err(MidgeError::ResourceLimit(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut index = Self {
            keys: Vec::with_capacity(key_bytes),
            entries: Vec::with_capacity(count),
            charged_bytes,
            _reservation: reservation,
        };
        *allocations = allocations.saturating_add(u64::from(key_bytes > 0));
        offset = 0;
        while offset < block.len() {
            let (entry, next) = encoding::decode_with_format(block, offset, version)?;
            *steps = steps.saturating_add(1);
            let start = index.keys.len();
            let shared = usize::from(entry.shared_len);
            if let Some(previous) = index.entries.last() {
                index
                    .keys
                    .extend_from_within(previous.start..previous.start + shared);
            }
            index.keys.extend_from_slice(entry.key_delta);
            let key = IndexedKey {
                start,
                len: index.keys.len() - start,
                encoded_offset: offset,
            };
            if index
                .entries
                .last()
                .is_some_and(|previous| index.key(previous) > index.key(&key))
            {
                return Err(MidgeError::Corruption(
                    "unordered recovery block keys".into(),
                ));
            }
            index.entries.push(key);
            offset = next;
        }
        Ok(Some(index))
    }

    fn key(&self, entry: &IndexedKey) -> &[u8] {
        &self.keys[entry.start..entry.start + entry.len]
    }

    pub(super) fn state(
        &self,
        block: &Bytes,
        version: u32,
        key: &[u8],
        snapshot_seq: u64,
        steps: &mut u64,
    ) -> MidgeResult<KeyState> {
        let first = self.entries.partition_point(|entry| self.key(entry) < key);
        let mut state = KeyState::Absent;
        for indexed in &self.entries[first..] {
            if self.key(indexed) != key {
                break;
            }
            let (entry, _) = encoding::decode_with_format(block, indexed.encoded_offset, version)?;
            *steps = steps.saturating_add(1);
            if entry.sequence <= snapshot_seq {
                SstFileIo::merge_newer_state(
                    &mut state,
                    SstFileIo::state_from_entry_view(block, entry),
                )?;
            }
        }
        Ok(state)
    }
}

fn index_overflow() -> MidgeError {
    MidgeError::Corruption("recovery block key index size overflow".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_invalid_prefix_before_allocating_recovery_seek_metadata() {
        // Arrange
        let bytes = Bytes::from(encoding::encode(
            b"delta",
            1,
            Some(b"value"),
            7,
            crate::types::EntryType::Put,
        ));
        let budget = ResourceBudget::new(1024);
        let (mut steps, mut allocations) = (0, 0);

        // Act
        let result = RecoveryKeyIndex::build(
            &bytes,
            crate::sst::types::SST_FORMAT_V4,
            &budget,
            usize::MAX,
            &mut steps,
            &mut allocations,
        );

        // Assert
        assert!(matches!(result, Err(MidgeError::Corruption(_))));
        assert_eq!(allocations, 0);
        assert_eq!(budget.used(), 0);
    }
}
