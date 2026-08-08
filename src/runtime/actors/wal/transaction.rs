// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::TransactionIntent;
use super::{
    PreparedTransactionAppend, TransactionAppendParams, TransactionAppendResult,
    TransactionApplyOp, TxnSequencePlan, TxnWalBatch, WalActor,
};
#[cfg(all(test, feature = "failpoints"))]
use super::{
    TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_DISABLED_REQUEST_ID,
    TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_REQUEST_ID,
};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::wal::{DurabilityPolicy, WalOpKind, WalRecord};
use bytes::Bytes;
#[cfg(all(test, feature = "failpoints"))]
use std::sync::atomic::Ordering;
use std::time::Instant;

impl WalActor {
    /// Apply a transaction's operations to the WAL as a single atomic unit.
    ///
    /// This method:
    /// - allocates a single transaction id
    /// - writes one `TxnBatch` frame containing the logical transaction (unless `BestEffort`)
    /// - applies all operations to memtables (in order)
    ///
    /// The `durability_policy` parameter allows per-request durability control.
    /// If `Some(DurabilityPolicy::BestEffort)`, WAL writes are skipped entirely
    /// (only memtable updates happen) for maximum bulk-load performance.
    /// If None, uses the actor's configured durability policy.
    ///
    /// Returns the last allocated sequence number for the batch.
    pub fn append_transaction(
        &mut self,
        state: &mut RuntimeState,
        params: TransactionAppendParams,
    ) -> MidgeResult<(u64, usize, bool)> {
        if params.ops.is_empty() {
            // An assertion-only commit is validated here without ever
            // reaching sequence allocation, WAL append, or memtable apply —
            // it has nothing to sequence.
            if !params.assertions.is_empty() {
                let start_sequence = params.start_sequence.ok_or_else(|| {
                    MidgeError::InvalidArgument(
                        "assert_value requires transaction start_sequence".to_string(),
                    )
                })?;
                Self::ensure_no_assertion_conflicts(state, &params.assertions, start_sequence)?;
            }
            return Ok((state.sequence, 0, false));
        }

        for op in &params.ops {
            let cf_id = match op {
                crate::runtime::TransactionOp::Put { cf_id, .. }
                | crate::runtime::TransactionOp::Delete { cf_id, .. }
                | crate::runtime::TransactionOp::DeleteRange { cf_id, .. } => *cf_id,
            };
            if !state.column_families.contains_key(&cf_id) {
                return Err(MidgeError::InvalidArgument(format!(
                    "column family {cf_id} does not exist"
                )));
            }
        }

        let prepared = self.prepare_transaction_append(state, params)?;
        let last_sequence = prepared.sequence_plan.commit_seq;
        let apply_op_count = prepared.apply_ops.len();
        let txn_id = prepared.sequence_plan.txn_id;

        let results = self.append_prepared_transactions(state, vec![prepared])?;
        let deferred = results
            .into_iter()
            .next()
            .is_some_and(|result| result.deferred);

        tracing::trace!(
            txn_id,
            last_sequence,
            apply_op_count,
            "WAL transaction apply"
        );

        Ok((last_sequence, apply_op_count, deferred))
    }

    pub(crate) fn prepare_transaction_append(
        &mut self,
        state: &mut RuntimeState,
        params: TransactionAppendParams,
    ) -> MidgeResult<PreparedTransactionAppend> {
        let TransactionAppendParams {
            request_id,
            ops,
            assertions,
            durability_policy,
            start_sequence,
            conflict_policy,
        } = params;

        if ops.is_empty() {
            return Err(MidgeError::InvalidArgument(
                "transaction append requires at least one operation".to_string(),
            ));
        }

        for op in &ops {
            let cf_id = match op {
                crate::runtime::TransactionOp::Put { cf_id, .. }
                | crate::runtime::TransactionOp::Delete { cf_id, .. }
                | crate::runtime::TransactionOp::DeleteRange { cf_id, .. } => *cf_id,
            };
            if !state.column_families.contains_key(&cf_id) {
                return Err(MidgeError::InvalidArgument(format!(
                    "column family {cf_id} does not exist"
                )));
            }
        }

        Self::validate_transaction_preconditions(
            state,
            &ops,
            &assertions,
            start_sequence,
            conflict_policy,
        )?;
        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        let sequence_plan = Self::allocate_transaction_sequences(state, ops.len());
        let (apply_ops, wal_batch) =
            self.build_transaction_wal_batch(state, ops, &sequence_plan, effective_durability)?;

        Ok(PreparedTransactionAppend {
            request_id,
            sequence_plan,
            apply_ops,
            wal_batch,
            effective_durability,
        })
    }

    pub(crate) fn append_prepared_transactions(
        &mut self,
        state: &mut RuntimeState,
        prepared_transactions: Vec<PreparedTransactionAppend>,
    ) -> MidgeResult<Vec<TransactionAppendResult>> {
        if prepared_transactions.is_empty() {
            return Ok(Vec::new());
        }

        let apply_ops = prepared_transactions
            .iter()
            .flat_map(|prepared| prepared.apply_ops.iter())
            .collect::<Vec<_>>();
        Self::preflight_prepared_transaction_ops(state, &apply_ops)?;

        self.append_prepared_transaction_batches(state, &prepared_transactions)?;
        let strict_group = prepared_transactions
            .iter()
            .all(|prepared| matches!(prepared.effective_durability, DurabilityPolicy::Strict));
        if strict_group {
            let last_sequence = prepared_transactions
                .last()
                .map_or(state.sequence, |prepared| prepared.sequence_plan.commit_seq);
            self.apply_strict_transaction_group_durability(state, last_sequence)?;
        }

        let mut results = Vec::with_capacity(prepared_transactions.len());
        for prepared in prepared_transactions {
            let PreparedTransactionAppend {
                request_id,
                sequence_plan,
                apply_ops,
                effective_durability,
                ..
            } = prepared;
            let last_sequence = sequence_plan.commit_seq;
            let apply_op_count = apply_ops.len();

            if !strict_group {
                self.apply_transaction_durability(
                    state,
                    effective_durability,
                    last_sequence,
                    sequence_plan.begin_seq,
                )?;
            }
            Self::apply_prevalidated_transaction_ops(
                state,
                apply_ops,
                effective_durability,
                last_sequence,
                apply_op_count,
            );

            tracing::trace!(
                txn_id = sequence_plan.txn_id,
                last_sequence,
                apply_op_count,
                "WAL transaction apply"
            );

            let deferred = matches!(
                effective_durability,
                DurabilityPolicy::Batched | DurabilityPolicy::CloudAsync
            );
            results.push(TransactionAppendResult {
                request_id,
                last_sequence,
                op_count: apply_op_count,
                deferred,
            });
        }

        Ok(results)
    }

    fn validate_transaction_preconditions(
        state: &RuntimeState,
        ops: &[crate::runtime::TransactionOp],
        assertions: &[crate::runtime::KeyAssertion],
        start_sequence: Option<u64>,
        conflict_policy: crate::runtime::ConflictPolicy,
    ) -> MidgeResult<()> {
        if matches!(
            conflict_policy,
            crate::runtime::ConflictPolicy::AbortOnWriteConflict
        ) {
            let start_sequence = start_sequence.ok_or_else(|| {
                MidgeError::InvalidArgument(
                    "AbortOnWriteConflict requires transaction start_sequence".to_string(),
                )
            })?;
            Self::ensure_no_write_conflicts(state, ops, start_sequence)?;
        }

        // Assertions are enforced regardless of ConflictPolicy: an explicit
        // assertion is a stronger guarantee than the ambient policy, so even
        // LastWriteWins transactions must not silently pass a stale assert.
        if !assertions.is_empty() {
            let start_sequence = start_sequence.ok_or_else(|| {
                MidgeError::InvalidArgument(
                    "assert_value requires transaction start_sequence".to_string(),
                )
            })?;
            Self::ensure_no_assertion_conflicts(state, assertions, start_sequence)?;
        }

        let mut intents = Vec::with_capacity(ops.len());
        let key_exists_after_intents =
            |cf_id: crate::types::ColumnFamilyId, key: &[u8], intents: &[TransactionIntent<'_>]| {
                for intent in intents.iter().rev() {
                    match intent {
                        TransactionIntent::Point {
                            cf_id: intent_cf,
                            key: intent_key,
                            exists,
                        } if *intent_cf == cf_id && *intent_key == key => return *exists,
                        TransactionIntent::Range {
                            cf_id: intent_cf,
                            start,
                            end,
                        } if *intent_cf == cf_id && *start <= key && key < *end => return false,
                        _ => {}
                    }
                }
                Self::key_exists(state, cf_id, key)
            };

        for op in ops {
            match op {
                crate::runtime::TransactionOp::Put {
                    cf_id,
                    key,
                    insert_only: false,
                    ..
                } => intents.push(TransactionIntent::Point {
                    cf_id: *cf_id,
                    key: key.as_ref(),
                    exists: true,
                }),
                crate::runtime::TransactionOp::Delete { cf_id, key } => {
                    intents.push(TransactionIntent::Point {
                        cf_id: *cf_id,
                        key: key.as_ref(),
                        exists: false,
                    });
                }
                crate::runtime::TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } => intents.push(TransactionIntent::Range {
                    cf_id: *cf_id,
                    start: start_key.as_ref(),
                    end: end_key.as_ref(),
                }),
                crate::runtime::TransactionOp::Put {
                    cf_id,
                    key,
                    insert_only: true,
                    ..
                } => {
                    if key_exists_after_intents(*cf_id, key, &intents) {
                        return Err(MidgeError::InvalidArgument(
                            "key already exists".to_string(),
                        ));
                    }
                    intents.push(TransactionIntent::Point {
                        cf_id: *cf_id,
                        key: key.as_ref(),
                        exists: true,
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) fn allocate_transaction_sequences(
        state: &mut RuntimeState,
        ops_count: usize,
    ) -> TxnSequencePlan {
        debug_assert!(
            ops_count > 0,
            "append_transaction requires at least one operation"
        );
        let ops_count_u64 = u64::try_from(ops_count).unwrap_or(u64::MAX);
        let begin_seq = state.sequence + 1;
        let first_op_seq = begin_seq + 1;
        let commit_seq = begin_seq + 1 + ops_count_u64;
        state.sequence = commit_seq;
        TxnSequencePlan {
            txn_id: state.next_txn_id(),
            begin_seq,
            first_op_seq,
            commit_seq,
        }
    }

    fn build_transaction_wal_batch(
        &mut self,
        state: &RuntimeState,
        ops: Vec<crate::runtime::TransactionOp>,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
    ) -> MidgeResult<(Vec<TransactionApplyOp>, TxnWalBatch)> {
        let skip_wal = matches!(effective_durability, DurabilityPolicy::BestEffort);
        let encode_wal = !skip_wal && self.writer.is_some();
        let apply_ops = Self::build_apply_ops(
            ops,
            sequence_plan.first_op_seq,
            sequence_plan.txn_id,
            state.observed_time_millis(),
        );
        let wal_record =
            self.build_transaction_batch_record(sequence_plan, &apply_ops, encode_wal)?;
        let total_wal_bytes = wal_record.as_ref().map_or(0, WalRecord::estimated_size);
        let pending_wal_records = usize::from(!skip_wal);
        Ok((
            apply_ops,
            TxnWalBatch {
                wal_record,
                total_wal_bytes,
                pending_wal_records,
            },
        ))
    }

    fn build_transaction_batch_record(
        &self,
        sequence_plan: &TxnSequencePlan,
        apply_ops: &[TransactionApplyOp],
        encode_wal: bool,
    ) -> MidgeResult<Option<WalRecord>> {
        if !encode_wal {
            return Ok(None);
        }
        let mut nested_records = Vec::with_capacity(apply_ops.len());
        for apply_op in apply_ops {
            let record = match apply_op {
                TransactionApplyOp::Put {
                    op,
                    cf_id,
                    key,
                    value,
                    expiration,
                    sequence,
                } => crate::wal::encoding::TxnBatchEncodeRecord {
                    cf_id: *cf_id,
                    op: *op,
                    key: key.as_ref(),
                    value: Some(value.as_ref()),
                    seq: *sequence,
                    expiration: *expiration,
                    range_end: None,
                    txn_id: Some(sequence_plan.txn_id),
                    writer_epoch: self.current_epoch,
                },
                TransactionApplyOp::Delete {
                    cf_id,
                    key,
                    sequence,
                } => crate::wal::encoding::TxnBatchEncodeRecord {
                    cf_id: *cf_id,
                    op: WalOpKind::Delete,
                    key: key.as_ref(),
                    value: None,
                    seq: *sequence,
                    expiration: None,
                    range_end: None,
                    txn_id: Some(sequence_plan.txn_id),
                    writer_epoch: self.current_epoch,
                },
                TransactionApplyOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    sequence,
                } => crate::wal::encoding::TxnBatchEncodeRecord {
                    cf_id: *cf_id,
                    op: WalOpKind::DeleteRange,
                    key: start_key.as_ref(),
                    value: None,
                    seq: *sequence,
                    expiration: None,
                    range_end: Some(end_key.as_ref()),
                    txn_id: Some(sequence_plan.txn_id),
                    writer_epoch: self.current_epoch,
                },
            };
            nested_records.push(record);
        }

        let payload = crate::wal::encoding::encode_txn_batch_payload_records(
            sequence_plan.txn_id,
            sequence_plan.begin_seq,
            sequence_plan.commit_seq,
            self.current_epoch,
            &nested_records,
        )
        .map_err(|error| {
            MidgeError::Corruption(format!(
                "failed to encode transaction batch {}: {error}",
                sequence_plan.txn_id
            ))
        })?;

        let mut batch_record = WalRecord::new_cf(
            0,
            WalOpKind::TxnBatch,
            Bytes::from_static(b"txn"),
            Some(payload),
            sequence_plan.commit_seq,
            self.current_epoch,
        );
        batch_record.txn_id = Some(sequence_plan.txn_id);
        Ok(Some(batch_record))
    }

    fn append_prepared_transaction_batches(
        &mut self,
        state: &mut RuntimeState,
        prepared_transactions: &[PreparedTransactionAppend],
    ) -> MidgeResult<()> {
        let mut records = Vec::with_capacity(prepared_transactions.len());
        let mut total_wal_bytes = 0usize;
        let mut pending_wal_records = 0usize;

        for prepared in prepared_transactions {
            if let Some(record) = prepared.wal_batch.wal_record.as_ref() {
                records.push(record.clone());
            }
            total_wal_bytes += prepared.wal_batch.total_wal_bytes;
            pending_wal_records += prepared.wal_batch.pending_wal_records;
        }

        if records.is_empty() {
            state.wal.pending_writes += pending_wal_records;
            self.pending_sync_count += pending_wal_records;
            return Ok(());
        }

        if let Some(writer) = &mut self.writer {
            crate::failpoints::fail_point!(
                "midge::wal::inject_no_space_on_txn_append_batch",
                Self::should_inject_txn_append_batch_no_space(prepared_transactions),
                |_| Err(MidgeError::NoSpace(
                    "failpoint: no space on transaction batch append".to_string()
                ))
            );
            crate::failpoints::fail_point!(
                "midge::wal::inject_no_space_on_txn_commit_append",
                |_| Err(MidgeError::NoSpace(
                    "failpoint: no space on transaction batch append".to_string()
                ))
            );
            crate::failpoints::fail_point!("midge::wal::txn_after_ops_append_before_commit");
            let append_start = Instant::now();
            if let Err(error) = writer.append_batch(&records) {
                if matches!(error, MidgeError::NoSpace(_)) {
                    Self::record_no_space_event();
                }
                return Err(error);
            }
            self.finish_append_instrumentation(total_wal_bytes as u64, append_start.elapsed());
            if let Some(max_sequence) = records.iter().map(|record| record.seq).max() {
                self.record_segment_sequence(max_sequence);
            }
            crate::failpoints::fail_point!("midge::wal::after_append_batch_before_sync");
        }

        state.wal.pending_writes += pending_wal_records;
        self.pending_sync_count += pending_wal_records;
        self.bytes_since_sync += total_wal_bytes;
        Ok(())
    }

    #[cfg(all(test, feature = "failpoints"))]
    fn should_inject_txn_append_batch_no_space(
        prepared_transactions: &[PreparedTransactionAppend],
    ) -> bool {
        let target_request_id =
            TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_REQUEST_ID.load(Ordering::SeqCst);
        target_request_id != TXN_APPEND_BATCH_NO_SPACE_FAILPOINT_DISABLED_REQUEST_ID
            && prepared_transactions
                .iter()
                .any(|prepared| prepared.request_id == target_request_id)
    }

    #[cfg(all(not(test), feature = "failpoints"))]
    fn should_inject_txn_append_batch_no_space(
        _prepared_transactions: &[PreparedTransactionAppend],
    ) -> bool {
        true
    }
}
