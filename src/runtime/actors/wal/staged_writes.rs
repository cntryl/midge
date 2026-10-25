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
        self.mutations
            .extend(prepared.apply_ops.iter().map(|op| match op {
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
