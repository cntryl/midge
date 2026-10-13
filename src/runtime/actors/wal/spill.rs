// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::TransactionApplyOp;
use super::{TxnSequencePlan, WalActor};
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::runtime::wal_transition_boundary::WalTransitionBoundary;
use crate::wal::DurabilityPolicy;
use crate::wal::{WalOpKind, WalRecord};
use bytes::Bytes;
use std::time::Instant;

impl WalActor {
    /// Replay buffer bytes a split-marker transaction needs, charged with the
    /// same accounting streaming replay uses.
    fn spilled_replay_bytes(
        source: &crate::runtime::transaction_spill::TransactionOpSource,
    ) -> MidgeResult<usize> {
        let mut replay_bytes = crate::wal::recovery::streaming::pending_txn_overhead_bytes();
        source.for_each(|_, op| {
            let (key_len, value_len, range_end_len) = match &op {
                crate::runtime::TransactionOp::Put { key, value, .. } => {
                    (key.len(), value.len(), 0)
                }
                crate::runtime::TransactionOp::Delete { key, .. } => (key.len(), 0, 0),
                crate::runtime::TransactionOp::DeleteRange {
                    start_key, end_key, ..
                } => (start_key.len(), 0, end_key.len()),
            };
            replay_bytes =
                replay_bytes.saturating_add(crate::wal::recovery::streaming::pending_record_bytes(
                    key_len,
                    value_len,
                    range_end_len,
                ));
            Ok(())
        })?;
        Ok(replay_bytes)
    }

    /// Stream a spilled transaction through split WAL markers without
    /// reconstructing its complete write set in memory.
    pub(crate) fn append_spilled_transaction(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        params: super::SpilledTransactionAppendParams,
    ) -> MidgeResult<(u64, usize, bool)> {
        self.ensure_lease_validity()?;
        let super::SpilledTransactionAppendParams {
            request_id,
            assertions,
            durability_policy,
            start_sequence,
            conflict_policy,
        } = params;

        if source.is_empty() {
            self.validate_assertion_only_spill(state, &assertions, start_sequence)?;
            return Ok((state.sequence, 0, false));
        }
        let cf_id = Self::validate_spilled_transaction(
            state,
            self.read_resources.clone(),
            source,
            &assertions,
            start_sequence,
            conflict_policy,
        )?;
        Self::ensure_family_accepts_writes(state, cf_id)?;

        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        self.ensure_write_durability_available(state, effective_durability)?;
        if self.max_replayable_txn_bytes.is_some() {
            self.ensure_replayable_transaction(Self::spilled_replay_bytes(source)?)?;
        }
        let sequence_plan = Self::allocate_transaction_sequences(state, source.len(), cf_id)?;
        let commit_time_millis = state.observed_time_millis();
        source.for_each(|ordinal, op| {
            let sequence = sequence_plan
                .first_op_seq
                .checked_add(ordinal)
                .ok_or_else(|| {
                    MidgeError::ResourceLimit(
                        "transaction spill sequence allocation overflow".into(),
                    )
                })?;
            let apply_op = TransactionApplyOp::from_source(op, sequence, commit_time_millis);
            Self::preflight_point(state, &apply_op).map(|_| ())
        })?;
        crate::failpoints::fail_point!("midge::wal::after_spilled_preparation");
        self.ensure_lease_validity()?;
        let admitted = self.admit_spilled_wal(
            source,
            &sequence_plan,
            effective_durability,
            commit_time_millis,
        )?;
        let previous_position = self.writer().map_or(0, crate::wal::WalWriter::current_pos);
        let mut wal_may_have_changed = false;
        let append_result = self.append_spilled_wal_records(
            request_id,
            source,
            &sequence_plan,
            effective_durability,
            commit_time_millis,
            &mut wal_may_have_changed,
        );
        // An ambiguous writer failure may still land bytes after it returns.
        // Keep the full reservation until restart reconciles the poisoned WAL;
        // its current position is not proof of the final physical footprint.
        if append_result.is_ok() || !wal_may_have_changed {
            self.settle_wal_append(admitted, previous_position);
        }
        let (wal_bytes, wal_records) = match append_result {
            Ok(result) => result,
            Err(error) => {
                if wal_may_have_changed {
                    self.fence_transition(
                        state,
                        format!("spilled transaction WAL append failed: {error}"),
                    );
                }
                return Err(error);
            }
        };
        state.wal.pending_writes = state.wal.pending_writes.saturating_add(wal_records);
        state.wal.appended_bytes = state.wal.appended_bytes.saturating_add(wal_bytes as u64);
        self.pending_sync_count = self.pending_sync_count.saturating_add(wal_records);
        self.bytes_since_sync = self.bytes_since_sync.saturating_add(wal_bytes);

        self.make_durable_and_apply_spill(
            state,
            source,
            &sequence_plan,
            (effective_durability, commit_time_millis),
            wal_records > 0,
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

    /// Make an appended spilled unit durable under its policy, then apply it.
    /// `logged` says whether its WAL frames were appended.
    fn make_durable_and_apply_spill(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        sequence_plan: &TxnSequencePlan,
        (effective_durability, commit_time_millis): (DurabilityPolicy, u64),
        logged: bool,
    ) -> MidgeResult<()> {
        // A failed sync fences the WAL. None of the unit is in the memtables,
        // so replay keeps or drops it whole whatever frontier a later flush
        // publishes; an appended unit's outcome is unknown.
        self.apply_transaction_durability(state, effective_durability, sequence_plan.commit_seq)
            .map_err(|error| {
                if logged {
                    Self::appended_unit_error(error)
                } else {
                    error
                }
            })?;
        // The WAL now holds the unit and `state.sequence` is past its commit.
        // A lease that lapsed since is a failed apply.
        let mut applied_ops = 0_usize;
        if let Err(error) = self.ensure_lease_validity().and_then(|()| {
            self.apply_spilled_transaction_ops(
                state,
                source,
                sequence_plan,
                effective_durability,
                commit_time_millis,
                &mut applied_ops,
            )
        }) {
            return Err(self.fail_unapplied_transactions(
                state,
                &[sequence_plan],
                super::UnappliedOutcome {
                    logged,
                    partly_applied: applied_ops > 0,
                },
                error,
            ));
        }
        Ok(())
    }

    fn validate_assertion_only_spill(
        &self,
        state: &RuntimeState,
        assertions: &[crate::runtime::KeyAssertion],
        start_sequence: u64,
    ) -> MidgeResult<()> {
        // An assertion-only commit does not allocate sequences or append to the WAL.
        if assertions.is_empty() {
            return Ok(());
        }
        let mut snapshots =
            super::transaction_state::ValidationSnapshots::new(state, self.read_resources.clone());
        Self::ensure_no_assertion_conflicts(state, &mut snapshots, assertions, start_sequence)
    }

    fn validate_spilled_transaction(
        state: &RuntimeState,
        resources: Option<std::sync::Arc<crate::runtime::read_resources::ReadResources>>,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        assertions: &[crate::runtime::KeyAssertion],
        start_sequence: u64,
        conflict_policy: crate::runtime::ConflictPolicy,
    ) -> MidgeResult<crate::types::ColumnFamilyId> {
        // A transaction writes one family; check before anything is read.
        let mut families = Vec::new();
        source.for_each(|_, op| {
            let cf_id = crate::runtime::transaction_spill::op_cf_id(&op);
            if !families.contains(&cf_id) {
                families.push(cf_id);
            }
            Ok(())
        })?;
        let family = Self::transaction_family(families.into_iter())?;
        // Assertions are enforced regardless of ConflictPolicy — see the
        // in-memory counterpart in validate_transaction_preconditions.
        // One snapshot per family serves assertions and conflict checks.
        let mut snapshots = super::transaction_state::ValidationSnapshots::new(state, resources);
        if !assertions.is_empty() {
            Self::ensure_no_assertion_conflicts(state, &mut snapshots, assertions, start_sequence)?;
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
                Self::ensure_no_write_conflict_for_op(state, &mut snapshots, &op, start_sequence)?;
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
                    None => Self::key_exists(&mut snapshots, *cf_id, key)?,
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
        Ok(family)
    }

    fn ensure_no_write_conflict_for_op(
        state: &RuntimeState,
        snapshots: &mut super::transaction_state::ValidationSnapshots<'_>,
        op: &crate::runtime::TransactionOp,
        start_sequence: u64,
    ) -> MidgeResult<()> {
        match op {
            crate::runtime::TransactionOp::Put { cf_id, key, .. }
            | crate::runtime::TransactionOp::Delete { cf_id, key } => {
                if Self::latest_key_sequence(snapshots, *cf_id, key)?
                    .is_some_and(|sequence| sequence > start_sequence)
                    || state
                        .latest_covering_delete_range_sequence(*cf_id, key)
                        .is_some_and(|sequence| sequence > start_sequence)
                {
                    state
                        .diagnostics
                        .record(crate::telemetry::Metrics::record_write_conflict_point);
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
                if Self::any_range_sequence_after(
                    snapshots,
                    *cf_id,
                    start_key,
                    end_key,
                    start_sequence,
                )?
                .is_some()
                    || state
                        .latest_overlapping_delete_range_sequence(*cf_id, start_key, end_key)
                        .is_some_and(|sequence| sequence > start_sequence)
                {
                    state
                        .diagnostics
                        .record(crate::telemetry::Metrics::record_write_conflict_range);
                    return Err(MidgeError::WriteConflict(format!(
                        "range conflict on cf {cf_id} for [{start_key:?}, {end_key:?}) after tx start {start_sequence}"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Reserve the entire streamed unit before writing its first frame. The
    /// sizing pass stays bounded to one operation, just like the append pass.
    fn admit_spilled_wal(
        &self,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        plan: &TxnSequencePlan,
        durability: DurabilityPolicy,
        commit_time_millis: u64,
    ) -> MidgeResult<u64> {
        let Some(storage) = self.storage_budget.as_ref().filter(|storage| {
            self.writer().is_some()
                && storage.ephemeral_sst_cache_enabled()
                && durability != DurabilityPolicy::BestEffort
        }) else {
            return Ok(0);
        };
        let mut bytes = 0_u64;
        let mut add = |record: &WalRecord| -> MidgeResult<()> {
            let frame = crate::wal::encoding::record_frame_size_bound(record)?;
            bytes = bytes
                .checked_add(u64::try_from(frame).unwrap_or(u64::MAX))
                .ok_or_else(|| MidgeError::ResourceLimit("WAL transaction size overflow".into()))?;
            Ok(())
        };
        for (kind, sequence) in [
            (WalOpKind::TxnBegin, plan.begin_seq),
            (WalOpKind::TxnCommit, plan.commit_seq),
        ] {
            let mut record =
                WalRecord::new_cf(plan.cf_id, kind, Bytes::from_static(b"txn"), None, sequence);
            record.txn_id = Some(plan.txn_id);
            add(&record)?;
        }
        source.for_each(|ordinal, op| {
            let sequence = plan.first_op_seq.checked_add(ordinal).ok_or_else(|| {
                MidgeError::ResourceLimit("transaction spill sequence allocation overflow".into())
            })?;
            let op = TransactionApplyOp::from_source(op, sequence, commit_time_millis);
            add(&op.wal_record(plan.txn_id))
        })?;
        storage.admit_local_wal_bytes(bytes)?;
        Ok(bytes)
    }

    fn append_spilled_wal_records(
        &mut self,
        request_id: u64,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
        commit_time_millis: u64,
        wal_may_have_changed: &mut bool,
    ) -> MidgeResult<(usize, usize)> {
        if matches!(effective_durability, DurabilityPolicy::BestEffort) {
            return Ok((0, 0));
        }

        let mut begin = WalRecord::new_cf(
            sequence_plan.cf_id,
            WalOpKind::TxnBegin,
            Bytes::from_static(b"txn"),
            None,
            sequence_plan.begin_seq,
        );
        begin.txn_id = Some(sequence_plan.txn_id);
        let mut total_bytes = begin.estimated_size();
        let append_started_at = Instant::now();
        self.append_spilled_record(&begin, wal_may_have_changed)?;

        source.for_each(|ordinal, op| {
            let sequence = sequence_plan
                .first_op_seq
                .checked_add(ordinal)
                .ok_or_else(|| {
                    MidgeError::ResourceLimit(
                        "transaction spill sequence allocation overflow".to_string(),
                    )
                })?;
            let apply_op = TransactionApplyOp::from_source(op, sequence, commit_time_millis);
            let record = apply_op.wal_record(sequence_plan.txn_id);
            total_bytes = total_bytes.saturating_add(record.estimated_size());
            self.append_spilled_record(&record, wal_may_have_changed)
        })?;

        crate::failpoints::fail_point!("midge::wal::spilled_txn_after_ops_append_before_commit");
        crate::failpoints::fail_point!(
            "midge::wal::inject_no_space_on_txn_commit_append",
            |_| Err(MidgeError::NoSpace(
                "failpoint: no space on transaction commit append".to_string()
            ))
        );
        let mut commit = WalRecord::new_cf(
            sequence_plan.cf_id,
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn"),
            None,
            sequence_plan.commit_seq,
        );
        commit.txn_id = Some(sequence_plan.txn_id);
        total_bytes = total_bytes.saturating_add(commit.estimated_size());
        self.append_spilled_record(&commit, wal_may_have_changed)?;
        self.finish_append_instrumentation(total_bytes as u64, append_started_at.elapsed());
        WalTransitionBoundary::AfterAppendBeforeAccounting.check()?;
        crate::failpoints::fail_point!("midge::wal::spilled_after_append_batch_before_sync");
        tracing::trace!(request_id, "appended split transaction WAL frames");
        Ok((total_bytes, source.len().saturating_add(2)))
    }

    fn append_spilled_record(
        &mut self,
        record: &WalRecord,
        wal_may_have_changed: &mut bool,
    ) -> MidgeResult<()> {
        self.ensure_lease_validity()?;
        let Some(writer) = self.writer_mut() else {
            return Err(MidgeError::Internal(
                "transaction WAL writer disappeared during append".to_string(),
            ));
        };
        if let Err(failure) = writer.append_record_accounted(record) {
            *wal_may_have_changed |= !failure.unchanged;
            return Err(self.settle_failed_wal_append(0, failure));
        }
        *wal_may_have_changed = true;
        Ok(())
    }

    fn apply_spilled_transaction_ops(
        &self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        sequence_plan: &TxnSequencePlan,
        effective_durability: DurabilityPolicy,
        commit_time_millis: u64,
        applied_ops: &mut usize,
    ) -> MidgeResult<()> {
        source.for_each(|ordinal, op| {
            crate::failpoints::fail_point!("midge::wal::spilled_apply_op", |_| Err(
                MidgeError::Io(std::io::Error::other(
                    "failpoint: spilled transaction apply read failed"
                ))
            ));
            let sequence = sequence_plan.first_op_seq.saturating_add(ordinal);
            let apply_op = TransactionApplyOp::from_source(op, sequence, commit_time_millis);
            self.ensure_lease_validity()?;
            Self::apply_transaction_op_to_memtable(state, apply_op)?;
            *applied_ops += 1;
            Ok(())
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
