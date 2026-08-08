// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::super::cloud_write_queue::TransactionApplyOp;
use super::{TxnSequencePlan, WalActor};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::wal::DurabilityPolicy;
use crate::wal::{WalOpKind, WalRecord};
use bytes::Bytes;
use std::time::Instant;

impl WalActor {
    /// Stream a spilled transaction through split WAL markers without
    /// reconstructing its complete write set in memory.
    pub(crate) fn append_spilled_transaction(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        params: super::SpilledTransactionAppendParams,
    ) -> MidgeResult<(u64, usize, bool)> {
        let super::SpilledTransactionAppendParams {
            request_id,
            assertions,
            durability_policy,
            start_sequence,
            conflict_policy,
        } = params;

        if source.is_empty() {
            // An assertion-only commit is validated here without ever
            // reaching sequence allocation, WAL append, or memtable apply.
            if !assertions.is_empty() {
                Self::ensure_no_assertion_conflicts(state, &assertions, start_sequence)?;
            }
            return Ok((state.sequence, 0, false));
        }
        Self::validate_spilled_transaction(
            state,
            source,
            &assertions,
            start_sequence,
            conflict_policy,
        )?;

        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        let sequence_plan = Self::allocate_transaction_sequences(state, source.len());
        let commit_time_millis = crate::common::time::unix_time_millis();
        let (wal_bytes, wal_records) = self.append_spilled_wal_records(
            request_id,
            source,
            &sequence_plan,
            effective_durability,
            commit_time_millis,
        )?;
        state.wal.pending_writes = state.wal.pending_writes.saturating_add(wal_records);
        self.pending_sync_count = self.pending_sync_count.saturating_add(wal_records);
        self.bytes_since_sync = self.bytes_since_sync.saturating_add(wal_bytes);

        self.apply_transaction_durability(
            state,
            effective_durability,
            sequence_plan.commit_seq,
            sequence_plan.begin_seq,
        )?;
        self.apply_spilled_transaction_ops(
            state,
            source,
            &sequence_plan,
            effective_durability,
            commit_time_millis,
        )?;

        let deferred = matches!(
            effective_durability,
            DurabilityPolicy::Batched | DurabilityPolicy::CloudAsync
        );
        tracing::trace!(
            txn_id = sequence_plan.txn_id,
            last_sequence = sequence_plan.commit_seq,
            apply_op_count = source.len(),
            "streamed spilled WAL transaction apply"
        );
        Ok((sequence_plan.commit_seq, source.len(), deferred))
    }

    fn validate_spilled_transaction(
        state: &RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        assertions: &[crate::runtime::KeyAssertion],
        start_sequence: u64,
        conflict_policy: crate::runtime::ConflictPolicy,
    ) -> MidgeResult<()> {
        // Assertions are enforced regardless of ConflictPolicy — see the
        // in-memory counterpart in validate_transaction_preconditions.
        if !assertions.is_empty() {
            Self::ensure_no_assertion_conflicts(state, assertions, start_sequence)?;
        }

        let mut expected_ordinal = 0_u64;
        source.for_each(|ordinal, op| {
            if ordinal != expected_ordinal {
                return Err(MidgeError::Corruption(format!(
                    "transaction spill ordinal {ordinal} is out of order; expected {expected_ordinal}"
                )));
            }
            expected_ordinal = expected_ordinal.saturating_add(1);
            let cf_id = crate::runtime::transaction_spill::op_cf_id(&op);
            if !state.column_families.contains_key(&cf_id) {
                return Err(MidgeError::InvalidArgument(format!(
                    "column family {cf_id} does not exist"
                )));
            }

            if matches!(
                conflict_policy,
                crate::runtime::ConflictPolicy::AbortOnWriteConflict
            ) {
                Self::ensure_no_write_conflict_for_op(state, &op, start_sequence)?;
            }

            if let crate::runtime::TransactionOp::Put {
                cf_id,
                key,
                insert_only: true,
                ..
            } = &op
            {
                let exists = match source.latest_before(ordinal, key)? {
                    Some(crate::runtime::transaction_spill::IntentLookup::Present(_)) => true,
                    Some(crate::runtime::transaction_spill::IntentLookup::Deleted) => false,
                    None => Self::key_exists(state, *cf_id, key),
                };
                if exists {
                    return Err(MidgeError::InvalidArgument(
                        "key already exists".to_string(),
                    ));
                }
            }
            Ok(())
        })?;
        if usize::try_from(expected_ordinal).unwrap_or(usize::MAX) != source.len() {
            return Err(MidgeError::Corruption(
                "transaction spill operation count does not match its header".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_no_write_conflict_for_op(
        state: &RuntimeState,
        op: &crate::runtime::TransactionOp,
        start_sequence: u64,
    ) -> MidgeResult<()> {
        match op {
            crate::runtime::TransactionOp::Put { cf_id, key, .. }
            | crate::runtime::TransactionOp::Delete { cf_id, key } => {
                if Self::latest_key_sequence(state, *cf_id, key)?
                    .is_some_and(|sequence| sequence > start_sequence)
                    || state
                        .latest_covering_delete_range_sequence(*cf_id, key)
                        .is_some_and(|sequence| sequence > start_sequence)
                {
                    Self::record_write_conflict_point();
                    return Err(MidgeError::WriteConflict(format!(
                        "key conflict on cf {cf_id} for key {key:?} after tx start {start_sequence}"
                    )));
                }
            }
            crate::runtime::TransactionOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
            } => {
                if Self::latest_range_sequence(state, *cf_id, start_key, end_key)?
                    .is_some_and(|sequence| sequence > start_sequence)
                    || state
                        .latest_overlapping_delete_range_sequence(*cf_id, start_key, end_key)
                        .is_some_and(|sequence| sequence > start_sequence)
                {
                    Self::record_write_conflict_range();
                    return Err(MidgeError::WriteConflict(format!(
                        "range conflict on cf {cf_id} for [{start_key:?}, {end_key:?}) after tx start {start_sequence}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn append_spilled_wal_records(
        &mut self,
        request_id: u64,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
        commit_time_millis: u64,
    ) -> MidgeResult<(usize, usize)> {
        if matches!(effective_durability, DurabilityPolicy::BestEffort) || self.writer.is_none() {
            return Ok((0, 0));
        }

        let mut begin = WalRecord::new_cf(
            0,
            WalOpKind::TxnBegin,
            Bytes::from_static(b"txn"),
            None,
            sequence_plan.begin_seq,
            self.current_epoch,
        );
        begin.txn_id = Some(sequence_plan.txn_id);
        let mut total_bytes = begin.estimated_size();
        let append_started_at = Instant::now();
        self.append_spilled_record(&begin)?;

        let epoch = self.current_epoch;
        source.for_each(|ordinal, op| {
            let sequence = sequence_plan
                .first_op_seq
                .checked_add(ordinal)
                .ok_or_else(|| {
                    MidgeError::ResourceLimit(
                        "transaction spill sequence allocation overflow".to_string(),
                    )
                })?;
            let record = Self::wal_record_for_transaction_op(
                op,
                sequence,
                sequence_plan.txn_id,
                epoch,
                commit_time_millis,
            );
            total_bytes = total_bytes.saturating_add(record.estimated_size());
            self.append_spilled_record(&record)
        })?;

        crate::failpoints::fail_point!("midge::wal::spilled_txn_after_ops_append_before_commit");
        crate::failpoints::fail_point!(
            "midge::wal::inject_no_space_on_txn_commit_append",
            |_| Err(MidgeError::NoSpace(
                "failpoint: no space on transaction commit append".to_string()
            ))
        );
        let mut commit = WalRecord::new_cf(
            0,
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn"),
            None,
            sequence_plan.commit_seq,
            self.current_epoch,
        );
        commit.txn_id = Some(sequence_plan.txn_id);
        total_bytes = total_bytes.saturating_add(commit.estimated_size());
        self.append_spilled_record(&commit)?;
        self.finish_append_instrumentation(total_bytes as u64, append_started_at.elapsed());
        self.record_segment_sequence(sequence_plan.commit_seq);
        crate::failpoints::fail_point!("midge::wal::spilled_after_append_batch_before_sync");
        tracing::trace!(request_id, "appended split transaction WAL frames");
        Ok((total_bytes, source.len().saturating_add(2)))
    }

    fn append_spilled_record(&mut self, record: &WalRecord) -> MidgeResult<()> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            MidgeError::Internal("transaction WAL writer disappeared during append".to_string())
        })?;
        if let Err(error) = writer.append_record(record) {
            if matches!(error, MidgeError::NoSpace(_)) {
                Self::record_no_space_event();
            }
            return Err(error);
        }
        Ok(())
    }

    fn wal_record_for_transaction_op(
        op: crate::runtime::TransactionOp,
        sequence: u64,
        txn_id: u64,
        writer_epoch: u64,
        commit_time_millis: u64,
    ) -> WalRecord {
        let mut record = match op {
            crate::runtime::TransactionOp::Put {
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            } => {
                let mut record = WalRecord::new_cf(
                    cf_id,
                    if insert_only {
                        WalOpKind::Insert
                    } else {
                        WalOpKind::Put
                    },
                    key,
                    Some(value),
                    sequence,
                    writer_epoch,
                );
                record.expiration = ttl_seconds
                    .filter(|ttl| *ttl > 0)
                    .map(|ttl| commit_time_millis.saturating_add(ttl.saturating_mul(1_000)));
                record
            }
            crate::runtime::TransactionOp::Delete { cf_id, key } => {
                WalRecord::new_cf(cf_id, WalOpKind::Delete, key, None, sequence, writer_epoch)
            }
            crate::runtime::TransactionOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
            } => {
                let mut record = WalRecord::new_cf(
                    cf_id,
                    WalOpKind::DeleteRange,
                    start_key,
                    None,
                    sequence,
                    writer_epoch,
                );
                record.range_end = Some(end_key);
                record
            }
        };
        record.txn_id = Some(txn_id);
        record
    }

    fn apply_spilled_transaction_ops(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
        commit_time_millis: u64,
    ) -> MidgeResult<()> {
        let epoch = self.current_epoch;
        source.for_each(|ordinal, op| {
            let sequence = sequence_plan.first_op_seq.saturating_add(ordinal);
            let record = Self::wal_record_for_transaction_op(
                op,
                sequence,
                sequence_plan.txn_id,
                epoch,
                commit_time_millis,
            );
            let apply_op = match record.op {
                WalOpKind::Put | WalOpKind::Insert => TransactionApplyOp::Put {
                    op: record.op,
                    cf_id: record.cf_id,
                    key: record.key,
                    value: record.value.ok_or_else(|| {
                        MidgeError::Corruption("spilled transaction put lost its value".to_string())
                    })?,
                    expiration: record.expiration,
                    sequence,
                },
                WalOpKind::Delete => TransactionApplyOp::Delete {
                    cf_id: record.cf_id,
                    key: record.key,
                    sequence,
                },
                WalOpKind::DeleteRange => TransactionApplyOp::DeleteRange {
                    cf_id: record.cf_id,
                    start_key: record.key,
                    end_key: record.range_end.ok_or_else(|| {
                        MidgeError::Corruption(
                            "spilled transaction range delete lost its end key".to_string(),
                        )
                    })?,
                    sequence,
                },
                WalOpKind::TxnBegin | WalOpKind::TxnCommit | WalOpKind::TxnBatch => {
                    return Err(MidgeError::Internal(
                        "transaction source produced a marker operation".to_string(),
                    ));
                }
            };
            Self::apply_transaction_op_to_memtable(state, apply_op)
        })?;
        tracing::trace!(
            commit_sequence = sequence_plan.commit_seq,
            apply_op_count = source.len(),
            ?effective_durability,
            "applied streamed transaction to memtables"
        );
        Ok(())
    }
}
