//! Sequenced writes admitted before their memtable publication.
//!
//! This is the shared validation view for a coalesced group and, when enabled,
//! earlier in-flight groups. It retains Bytes handles, never copies key/value
//! payloads, and records only successfully prepared units in sequence order.

use super::{PreparedTransactionAppend, TransactionApplyOp};
use bytes::Bytes;

#[derive(Default)]
pub(crate) struct StagedWriteOverlay {
    mutations: Vec<StagedMutation>,
}

enum StagedMutation {
    Point {
        cf_id: u32,
        key: Bytes,
        sequence: u64,
        exists: bool,
        expiration: Option<u64>,
    },
    Range {
        cf_id: u32,
        start: Bytes,
        end: Bytes,
        sequence: u64,
    },
}

impl StagedWriteOverlay {
    pub(crate) fn record(&mut self, prepared: &PreparedTransactionAppend) {
        self.record_ops(&prepared.apply_ops);
    }

    fn record_ops(&mut self, apply_ops: &[TransactionApplyOp]) {
        self.mutations.extend(apply_ops.iter().map(|op| match op {
            TransactionApplyOp::Put {
                cf_id,
                key,
                sequence,
                expiration,
                ..
            } => StagedMutation::Point {
                cf_id: *cf_id,
                key: key.clone(),
                sequence: *sequence,
                exists: true,
                expiration: *expiration,
            },
            TransactionApplyOp::Delete {
                cf_id,
                key,
                sequence,
            } => StagedMutation::Point {
                cf_id: *cf_id,
                key: key.clone(),
                sequence: *sequence,
                exists: false,
                expiration: None,
            },
            TransactionApplyOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
                sequence,
            } => StagedMutation::Range {
                cf_id: *cf_id,
                start: start_key.clone(),
                end: end_key.clone(),
                sequence: *sequence,
            },
        }));
    }

    pub(super) fn latest_key_sequence(&self, cf_id: u32, key: &[u8]) -> Option<u64> {
        self.mutations
            .iter()
            .rev()
            .find_map(|mutation| match mutation {
                StagedMutation::Point {
                    cf_id: family,
                    key: written,
                    sequence,
                    ..
                } if *family == cf_id && written.as_ref() == key => Some(*sequence),
                StagedMutation::Range {
                    cf_id: family,
                    start,
                    end,
                    sequence,
                } if *family == cf_id && start.as_ref() <= key && key < end.as_ref() => {
                    Some(*sequence)
                }
                _ => None,
            })
    }

    pub(super) fn latest_range_sequence(
        &self,
        cf_id: u32,
        start: &[u8],
        end: &[u8],
    ) -> Option<u64> {
        self.mutations
            .iter()
            .rev()
            .find_map(|mutation| match mutation {
                StagedMutation::Point {
                    cf_id: family,
                    key,
                    sequence,
                    ..
                } if *family == cf_id && start <= key.as_ref() && key.as_ref() < end => {
                    Some(*sequence)
                }
                StagedMutation::Range {
                    cf_id: family,
                    start: written_start,
                    end: written_end,
                    sequence,
                } if *family == cf_id
                    && start < written_end.as_ref()
                    && written_start.as_ref() < end =>
                {
                    Some(*sequence)
                }
                _ => None,
            })
    }

    pub(super) fn key_exists(&self, cf_id: u32, key: &[u8], read_time_millis: u64) -> Option<bool> {
        self.mutations
            .iter()
            .rev()
            .find_map(|mutation| match mutation {
                StagedMutation::Point {
                    cf_id: family,
                    key: written,
                    exists,
                    expiration,
                    ..
                } if *family == cf_id && written.as_ref() == key => Some(
                    *exists && !crate::common::time::is_expired_at(*expiration, read_time_millis),
                ),
                StagedMutation::Range {
                    cf_id: family,
                    start,
                    end,
                    ..
                } if *family == cf_id && start.as_ref() <= key && key < end.as_ref() => Some(false),
                _ => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::MidgeResult;
    use bytes::Bytes;
    use proptest::prelude::*;

    fn generated_ops(encoded: &[(u8, u32, u8, u8, u8)]) -> Vec<TransactionApplyOp> {
        encoded
            .iter()
            .enumerate()
            .map(|(index, (kind, cf_id, first, second, ttl))| {
                let sequence = u64::try_from(index).unwrap() + 2;
                let key = Bytes::copy_from_slice(&[*first]);
                match kind {
                    0 => TransactionApplyOp::Put {
                        op: crate::wal::WalOpKind::Put,
                        cf_id: *cf_id,
                        key,
                        value: Bytes::copy_from_slice(&[u8::try_from(index).unwrap_or(u8::MAX)]),
                        expiration: match ttl {
                            0 => None,
                            1 => Some(10),
                            2 => Some(20),
                            _ => Some(30),
                        },
                        sequence,
                    },
                    1 => TransactionApplyOp::Delete {
                        cf_id: *cf_id,
                        key,
                        sequence,
                    },
                    _ => {
                        let start = (*first).min(*second);
                        let end = (*first).max(*second).max(start.saturating_add(1));
                        TransactionApplyOp::DeleteRange {
                            cf_id: *cf_id,
                            start_key: Bytes::copy_from_slice(&[start]),
                            end_key: Bytes::copy_from_slice(&[end]),
                            sequence,
                        }
                    }
                }
            })
            .collect()
    }

    fn apply_to_memtables(
        memtables: &[crate::memtable::SkipListMemtable; 2],
        ops: &[TransactionApplyOp],
    ) -> MidgeResult<()> {
        for op in ops {
            match op {
                TransactionApplyOp::Put {
                    cf_id,
                    key,
                    value,
                    expiration,
                    sequence,
                    ..
                } => memtables[*cf_id as usize].put_bytes_with_seq(
                    key.clone(),
                    value.clone(),
                    *sequence,
                    *expiration,
                )?,
                TransactionApplyOp::Delete {
                    cf_id,
                    key,
                    sequence,
                } => memtables[*cf_id as usize].delete_bytes_with_seq(key.clone(), *sequence)?,
                TransactionApplyOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    sequence,
                } => memtables[*cf_id as usize]
                    .delete_range_with_seq(start_key, end_key, *sequence)?,
            }
        }
        Ok(())
    }

    fn state_sequence(state: &crate::types::KeyState) -> Option<u64> {
        match state {
            crate::types::KeyState::Absent => None,
            crate::types::KeyState::Tombstone(sequence)
            | crate::types::KeyState::Value(_, sequence, _, _) => Some(*sequence),
        }
    }

    fn assert_overlay_matches_memtables(
        overlay: &StagedWriteOverlay,
        memtables: &[crate::memtable::SkipListMemtable; 2],
    ) -> MidgeResult<()> {
        for (cf_id, memtable) in (0u32..).zip(memtables.iter()) {
            let ranges = memtable.range_tombstones_at(u64::MAX);
            for key in 0u8..=8 {
                let key = [key];
                let point = memtable.get_key_state_at_with_time(&key, u64::MAX, 25)?;
                let range_sequence = ranges
                    .iter()
                    .filter(|range| range.covers(&key))
                    .map(|range| range.seq)
                    .max();
                let expected_sequence = state_sequence(&point)
                    .into_iter()
                    .chain(range_sequence)
                    .max();
                assert_eq!(overlay.latest_key_sequence(cf_id, &key), expected_sequence);
                for read_time in [0, 10, 25, 100] {
                    let point = memtable.get_key_state_at_with_time(&key, u64::MAX, read_time)?;
                    let covered = ranges
                        .iter()
                        .filter(|range| range.covers(&key))
                        .map(|range| range.seq)
                        .max();
                    let exists = match (point, covered) {
                        (crate::types::KeyState::Value(_, sequence, _, _), Some(range_seq)) => {
                            sequence > range_seq
                        }
                        (crate::types::KeyState::Value(_, _, _, _), None) => true,
                        _ => false,
                    };
                    assert_eq!(
                        overlay.key_exists(cf_id, &key, read_time).unwrap_or(false),
                        exists
                    );
                }
            }
            for start in 0u8..8 {
                for end in start + 1..=8 {
                    let point_sequence = memtable
                        .range_state_at_with_time(Some(&[start]), Some(&[end]), u64::MAX, 25)
                        .into_iter()
                        .filter_map(|(_, state)| state_sequence(&state))
                        .max();
                    let range_sequence = ranges
                        .iter()
                        .filter(|range| {
                            [start].as_slice() < range.end.as_slice()
                                && range.start.as_slice() < [end].as_slice()
                        })
                        .map(|range| range.seq)
                        .max();
                    assert_eq!(
                        overlay.latest_range_sequence(cf_id, &[start], &[end]),
                        point_sequence.into_iter().chain(range_sequence).max()
                    );
                }
            }
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

        #[test]
        fn should_match_memtable_after_apply_given_generated_point_and_range_mutations(
            encoded in prop::collection::vec((0u8..3, 0u32..2, 0u8..8, 0u8..8, 0u8..4), 1..32)
        ) {
            // Arrange
            let ops = generated_ops(&encoded);
            let mut overlay = StagedWriteOverlay::default();
            overlay.record_ops(&ops);
            let memtables = [
                crate::memtable::SkipListMemtable::new(),
                crate::memtable::SkipListMemtable::new(),
            ];

            // Act
            apply_to_memtables(&memtables, &ops).unwrap();

            // Assert
            assert_overlay_matches_memtables(&overlay, &memtables).unwrap();
        }
    }
}
