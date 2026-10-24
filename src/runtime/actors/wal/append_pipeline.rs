//! Loop-owned accounting for writer-owned sequenced encoding and append.

use super::{
    PendingStrictAppend, PendingStrictSync, PreparedTransactionAppend, WalActor,
    WalTransitionOperation,
};
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::runtime::state::RuntimeState;
use crate::runtime::wal_transition::WalSyncTicket;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use crate::wal::append_completion::{AppendResult, WalAppendPrefix};
use crate::wal::sequenced::SequencedWalUnit;
use std::time::Instant;

impl WalActor {
    pub(super) fn begin_sequenced_group_append(
        &mut self,
        state: &mut RuntimeState,
        prepared: &[PreparedTransactionAppend],
        ticket: &WalSyncTicket,
        root: Option<crate::lease::WriterEffectGroup>,
        authority_prevalidated: bool,
    ) -> MidgeResult<PendingStrictAppend> {
        Self::prepared_append_failpoints(prepared)?;
        let units = prepared
            .iter()
            .filter_map(|unit| unit.wal_batch.unit.clone())
            .collect::<Vec<_>>();
        let total_wal_bytes = prepared
            .iter()
            .map(|unit| unit.wal_batch.total_wal_bytes)
            .sum();
        let pending_wal_records = prepared
            .iter()
            .map(|unit| unit.wal_batch.pending_wal_records)
            .sum();
        let expected = WalAppendPrefix {
            barrier_id: ticket.generation(),
            writer_epoch: self.current_epoch,
            segment_id: state.wal.open_segment(),
            first_sequence: prepared
                .first()
                .expect("nonempty group")
                .sequence_plan
                .begin_seq,
            last_sequence: prepared
                .last()
                .expect("nonempty group")
                .sequence_plan
                .commit_seq,
        };
        let admitted = self.admit_sequenced_wal(&units)?;
        let previous_position = self.writer().map_or(0, crate::wal::WalWriter::current_pos);
        let started = Instant::now();
        if let Err(error) = self
            .ensure_lease_validity()
            .and_then(|()| self.begin_io_transition(WalTransitionOperation::Append))
        {
            self.settle_wal_append(admitted, previous_position);
            return Err(error);
        }
        let submitted = self
            .writer()
            .and_then(|writer| {
                writer.submit_sequenced_batch(
                    expected,
                    units,
                    self.storage_io_timeout,
                    root.as_ref(),
                )
            })
            .unwrap_or_else(|| {
                Err(crate::wal::traits::WalAppendError::unchanged(
                    MidgeError::Internal("WAL writer lost sequenced submission capability".into()),
                ))
            });
        match submitted {
            Ok(physical) => Ok(PendingStrictAppend {
                root,
                physical,
                expected,
                admitted,
                previous_position,
                total_wal_bytes,
                pending_wal_records,
                authority_prevalidated,
                started,
                deadline: OperationDeadline::from_start(started, self.storage_io_timeout),
            }),
            Err(failure) => {
                let error = self.settle_failed_wal_append(admitted, failure);
                self.finish_io_transition()?;
                Err(error)
            }
        }
    }

    fn admit_sequenced_wal(&self, units: &[SequencedWalUnit]) -> MidgeResult<u64> {
        let Some(storage) = self
            .storage_budget
            .as_ref()
            .filter(|storage| storage.ephemeral_sst_cache_enabled())
        else {
            return Ok(0);
        };
        let bytes = units.iter().try_fold(0u64, |bytes, unit| {
            bytes
                .checked_add(u64::try_from(unit.frame_size_bound()?).unwrap_or(u64::MAX))
                .ok_or_else(|| MidgeError::ResourceLimit("WAL append admission overflow".into()))
        })?;
        storage.admit_local_wal_bytes(bytes)?;
        Ok(bytes)
    }

    pub(crate) fn finish_sequenced_group_append(
        &mut self,
        state: &mut RuntimeState,
        mut pending: PendingStrictAppend,
        ticket: &WalSyncTicket,
        completion: AppendResult,
    ) -> MidgeResult<PendingStrictSync> {
        if let Err(error) = self.commit_sequenced_append(state, &pending, ticket, completion) {
            self.fence_transition(state, format!("sequenced WAL append failed: {error}"));
            return Err(error);
        }
        self.sync_authority_prevalidated = pending.authority_prevalidated;
        let submitted = self.submit_strict_sync(state, ticket, pending.root.as_ref());
        self.sync_authority_prevalidated = false;
        match submitted {
            Ok(mut sync) => {
                sync.root = pending.root.take();
                Ok(sync)
            }
            Err(error) => {
                self.fence_transition(
                    state,
                    format!("strict sync submission failed after append: {error}"),
                );
                Err(Self::appended_unit_error(error))
            }
        }
    }

    fn commit_sequenced_append(
        &mut self,
        state: &mut RuntimeState,
        pending: &PendingStrictAppend,
        ticket: &WalSyncTicket,
        completion: AppendResult,
    ) -> MidgeResult<()> {
        let receipt = match completion {
            Ok(receipt) => receipt,
            Err(failure) => return Err(self.settle_failed_wal_append(pending.admitted, failure)),
        };
        if receipt.prefix != pending.expected
            || receipt.prefix.barrier_id != ticket.generation()
            || receipt.start_offset != pending.previous_position
            || receipt.end_offset != self.writer().map_or(0, crate::wal::WalWriter::current_pos)
        {
            return Err(MidgeError::OutcomeUnknown(
                "WAL append completion does not match its admitted group".into(),
            ));
        }
        self.settle_wal_append(pending.admitted, pending.previous_position);
        self.finish_append_instrumentation(
            pending.total_wal_bytes as u64,
            pending.started.elapsed(),
        );
        WalTransitionBoundary::AfterAppendBeforeAccounting
            .check()
            .map_err(Self::appended_unit_error)?;
        crate::failpoints::fail_point!("midge::wal::after_append_batch_before_sync");
        state.wal.pending_writes = state
            .wal
            .pending_writes
            .saturating_add(pending.pending_wal_records);
        state.wal.appended_bytes = state
            .wal
            .appended_bytes
            .saturating_add(pending.total_wal_bytes as u64);
        self.pending_sync_count = self
            .pending_sync_count
            .saturating_add(pending.pending_wal_records);
        self.bytes_since_sync = self
            .bytes_since_sync
            .saturating_add(pending.total_wal_bytes);
        self.finish_io_transition()
    }
}
