//! Split strict append, physical sync completion, and loop-owned publication.

use super::{
    PendingStrictIo, PendingStrictSync, PreparedTransactionAppend, WalActor, WalTransitionOperation,
};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::runtime::wal_transition::WalSyncTicket;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use crate::wal::traits::{WalSyncCompletion, WalSyncPrefix};
use crate::wal::DurabilityPolicy;

impl WalActor {
    /// Called only after the pending physical callback has reached terminal
    /// completion, so joining this retained worker cannot wait on loop work.
    pub(crate) fn release_fenced_writer_after_completion(&mut self) {
        self.fenced_writer.take();
    }

    pub(crate) fn supports_sync_submission(&self) -> bool {
        self.writer()
            .is_some_and(crate::wal::WalWriter::supports_sync_submission)
    }

    pub(crate) fn prepared_retained_bytes(prepared: &[PreparedTransactionAppend]) -> usize {
        prepared.iter().fold(0usize, |total, unit| {
            let operations = unit.apply_ops.iter().fold(0usize, |bytes, op| {
                let payload = match op {
                    super::TransactionApplyOp::Put { key, value, .. } => {
                        key.len().saturating_add(value.len())
                    }
                    super::TransactionApplyOp::Delete { key, .. } => key.len(),
                    super::TransactionApplyOp::DeleteRange {
                        start_key, end_key, ..
                    } => start_key.len().saturating_add(end_key.len()),
                };
                bytes
                    .saturating_add(std::mem::size_of::<super::TransactionApplyOp>())
                    .saturating_add(payload)
            });
            total
                .saturating_add(std::mem::size_of::<PreparedTransactionAppend>())
                .saturating_add(operations)
                .saturating_add(
                    unit.wal_batch
                        .unit
                        .as_ref()
                        .map_or(0, |wal| wal.retained_memory_bound().unwrap_or(usize::MAX)),
                )
        })
    }

    pub(crate) fn begin_strict_group_submission(
        &mut self,
        state: &mut RuntimeState,
        prepared: &[PreparedTransactionAppend],
        ticket: &WalSyncTicket,
    ) -> MidgeResult<PendingStrictIo> {
        if prepared.is_empty()
            || prepared
                .iter()
                .any(|unit| unit.effective_durability != DurabilityPolicy::Strict)
        {
            return Err(MidgeError::InvalidArgument(
                "a strict pipeline group requires strict transaction units".into(),
            ));
        }
        self.preflight_prepared_group(state, prepared)?;
        let root = self.begin_strict_writer_group(state)?;
        let prevalidated = root.is_some() || self.validate_writer_admission(state)?;
        if self
            .writer()
            .is_some_and(crate::wal::WalWriter::supports_sequenced_submission)
        {
            return self
                .begin_sequenced_group_append(state, prepared, ticket, root, prevalidated)
                .map(PendingStrictIo::Append);
        }
        self.append_prepared_transaction_batches(state, prepared, root.as_ref())?;
        self.sync_authority_prevalidated = prevalidated;
        let submitted = self.submit_strict_sync(state, ticket, root.as_ref());
        self.sync_authority_prevalidated = false;
        let mut pending = submitted.map_err(|error| {
            if !self.is_fenced() {
                self.fence_transition(
                    state,
                    format!("strict sync submission failed after append: {error}"),
                );
            }
            Self::appended_unit_error(error)
        })?;
        pending.root = root;
        Ok(PendingStrictIo::Sync(pending))
    }

    pub(crate) fn begin_spilled_strict_group_submission(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        params: &super::SpilledTransactionAppendParams,
        proof: Option<&super::SpilledValidationProof>,
        ticket: &WalSyncTicket,
        memory: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<(PendingStrictSync, super::PreparedSpilledAppend)> {
        let root = self.begin_strict_writer_group(state)?;
        let authority_prevalidated = root.is_some() || self.validate_writer_admission(state)?;
        let prepared = self.append_spilled_strict_for_pipeline(
            state,
            source,
            params,
            proof,
            root.as_ref(),
            memory,
        )?;
        self.sync_authority_prevalidated = authority_prevalidated;
        let submitted = self.submit_strict_sync(state, ticket, root.as_ref());
        self.sync_authority_prevalidated = false;
        let mut pending = submitted.map_err(|error| {
            if !self.is_fenced() {
                self.fence_transition(
                    state,
                    format!("strict spilled sync submission failed after append: {error}"),
                );
            }
            Self::appended_unit_error(error)
        })?;
        pending.root = root;
        Ok((pending, prepared))
    }

    pub(super) fn submit_strict_sync(
        &mut self,
        state: &mut RuntimeState,
        ticket: &WalSyncTicket,
        root: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<PendingStrictSync> {
        crate::failpoints::fail_point!("midge::wal::txn_after_commit_append_before_sync");
        self.validate_sync_authority(state)?;
        self.begin_io_transition(WalTransitionOperation::Sync)?;
        WalTransitionBoundary::BeforeFsync.check()?;
        Self::sync_failure_boundary()?;
        let mut receipt = self.capture_sync_receipt(state);
        receipt.generation = Some(ticket.generation());
        let expected = WalSyncPrefix {
            barrier_id: ticket.generation(),
            writer_epoch: self.current_epoch,
            segment_id: state.wal.open_segment(),
            first_sequence: state
                .wal
                .frontiers
                .last_synced()
                .saturating_add(1)
                .min(receipt.durable_sequence),
            last_sequence: receipt.durable_sequence,
            end_offset: receipt.end_offset.ok_or_else(|| {
                MidgeError::Internal("strict pipeline has no filesystem prefix".into())
            })?,
        };
        let started = std::time::Instant::now();
        let physical = self
            .writer()
            .and_then(|writer| writer.submit_sync_prefix(expected, self.storage_io_timeout, root))
            .ok_or_else(|| {
                MidgeError::Internal("WAL writer lost nonblocking sync capability".into())
            })??;
        Ok(PendingStrictSync {
            root: None,
            physical,
            receipt,
            expected,
            started,
            deadline: crate::common::OperationDeadline::from_start(
                started,
                self.storage_io_timeout,
            ),
        })
    }

    pub(crate) fn finish_strict_group_submission(
        &mut self,
        state: &mut RuntimeState,
        pending: &PendingStrictSync,
        ticket: &WalSyncTicket,
        completion: MidgeResult<WalSyncCompletion>,
    ) -> MidgeResult<super::WalSyncReceipt> {
        let receipt_observed_at = std::time::Instant::now();
        if let Ok(completion) = &completion {
            let before_physical_sync = completion
                .completed_at
                .saturating_duration_since(pending.started)
                .saturating_sub(completion.elapsed);
            let receipt_delivery =
                receipt_observed_at.saturating_duration_since(completion.completed_at);
            self.counters.record(|metrics| {
                metrics.record_wal_sync_writer_queue_latency(before_physical_sync);
                metrics.record_wal_sync_receipt_delivery_latency(receipt_delivery);
            });
        }
        let completed = completion.and_then(|receipt| {
            if receipt.prefix != pending.expected
                || pending.receipt.generation != Some(ticket.generation())
            {
                return Err(MidgeError::Internal(
                    "strict sync completion does not match its admitted prefix".into(),
                ));
            }
            self.ensure_lease_validity()?;
            Self::after_fsync_boundary()?;
            WalTransitionBoundary::AfterFsync.check()?;
            let proof_started = std::time::Instant::now();
            let proof = self.persist_sync_boundary(
                state,
                pending.root.as_ref(),
                pending.receipt.end_offset,
            );
            self.counters.record(|metrics| {
                metrics.record_wal_sync_ledger_proof_latency(proof_started.elapsed());
            });
            proof?;
            Ok(receipt)
        });
        if let Err(error) = completed {
            if matches!(error, MidgeError::NoSpace(_)) {
                Self::record_no_space_event(&self.counters);
            }
            self.fence_with_cause(
                state,
                format!("strict pipeline sync failed: {error}"),
                Self::is_authority_failure(&error),
            );
            return Err(Self::appended_unit_error(error));
        }
        self.sync_calls = self.sync_calls.saturating_add(1);
        let elapsed = pending.started.elapsed();
        self.sync_total += elapsed;
        self.last_sync_instant = std::time::Instant::now();
        self.counters.record(|metrics| {
            metrics.record_wal_sync();
            metrics.record_wal_fsync_latency(elapsed);
        });
        crate::failpoints::fail_point!("midge::wal::txn_after_sync_before_ack");
        Ok(pending.receipt)
    }
}
