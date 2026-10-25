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

struct SpilledTransactionContext<'a> {
    source: &'a crate::runtime::transaction_spill::TransactionOpSource,
    sequence_plan: &'a TxnSequencePlan,
    effective_durability: DurabilityPolicy,
    commit_time_millis: u64,
    memory: Option<&'a crate::common::resource_budget::ResourceBudget>,
}

impl WalActor {
    /// Replay buffer bytes a split-marker transaction needs, charged with the
    /// same accounting streaming replay uses.
    fn spilled_replay_bytes(
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        memory: Option<&crate::common::resource_budget::ResourceBudget>,
    ) -> MidgeResult<usize> {
        let mut replay_bytes = crate::wal::recovery::streaming::pending_txn_overhead_bytes();
        source.for_each_bounded(memory, |_, op| {
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
    #[cfg(test)]
    pub(crate) fn append_spilled_transaction(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        params: &super::SpilledTransactionAppendParams,
    ) -> MidgeResult<(u64, usize, bool)> {
        self.append_spilled_transaction_with_proof(state, source, params, None)
    }

    pub(crate) fn append_spilled_transaction_with_proof(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        params: &super::SpilledTransactionAppendParams,
        proof: Option<&super::SpilledValidationProof>,
    ) -> MidgeResult<(u64, usize, bool)> {
        self.ensure_lease_validity()?;
        let Some(cf_id) =
            self.validate_spilled_preconditions(state, source, params, proof, None)?
        else {
            return Ok((state.applied_sequence, 0, false));
        };
        let request_id = params.request_id;
        let durability_policy = params.durability_policy;
        Self::ensure_family_accepts_writes(state, cf_id)?;

        let effective_durability = durability_policy.unwrap_or(self.durability_policy);
        self.ensure_write_durability_available(state, effective_durability)?;
        if self.max_replayable_txn_bytes.is_some() {
            self.ensure_replayable_transaction(Self::spilled_replay_bytes(source, None)?)?;
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
            None,
        )?;
        let previous_position = self.writer().map_or(0, crate::wal::WalWriter::current_pos);
        let mut wal_may_have_changed = false;
        let append_result = self.append_spilled_wal_records(
            request_id,
            &SpilledTransactionContext {
                source,
                sequence_plan: &sequence_plan,
                effective_durability,
                commit_time_millis,
                memory: None,
            },
            &mut wal_may_have_changed,
            None,
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
                return Err(crate::common::error::classify_commit_error(
                    error,
                    !wal_may_have_changed,
                ));
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

    /// Append a Strict spill without waiting for its physical sync or applying
    /// it. The caller retains the source and returned metadata until the
    /// ticketed sync receipt commits.
    pub(crate) fn append_spilled_strict_for_pipeline(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        params: &super::SpilledTransactionAppendParams,
        proof: Option<&super::SpilledValidationProof>,
        group: Option<&crate::lease::WriterEffectGroup>,
        memory: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<super::PreparedSpilledAppend> {
        self.ensure_lease_validity()?;
        let Some(cf_id) =
            self.validate_spilled_preconditions(state, source, params, proof, Some(memory))?
        else {
            return Err(MidgeError::InvalidArgument(
                "an empty strict spill has no asynchronous WAL work".into(),
            ));
        };
        let request_id = params.request_id;
        let effective_durability = params.durability_policy.unwrap_or(self.durability_policy);
        if effective_durability != DurabilityPolicy::Strict {
            return Err(MidgeError::InvalidArgument(
                "the spilled WAL pipeline accepts Strict transactions only".into(),
            ));
        }
        Self::ensure_family_accepts_writes(state, cf_id)?;
        self.ensure_write_durability_available(state, effective_durability)?;
        if self.max_replayable_txn_bytes.is_some() {
            self.ensure_replayable_transaction(Self::spilled_replay_bytes(source, Some(memory))?)?;
        }
        let sequence_plan = Self::allocate_transaction_sequences(state, source.len(), cf_id)?;
        let commit_time_millis = state.observed_time_millis();
        source.for_each_bounded(Some(memory), |ordinal, op| {
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
            Some(memory),
        )?;
        let previous_position = self.writer().map_or(0, crate::wal::WalWriter::current_pos);
        let mut wal_may_have_changed = false;
        let append_result = self.append_spilled_wal_records(
            request_id,
            &SpilledTransactionContext {
                source,
                sequence_plan: &sequence_plan,
                effective_durability,
                commit_time_millis,
                memory: Some(memory),
            },
            &mut wal_may_have_changed,
            group,
        );
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
                return Err(crate::common::error::classify_commit_error(
                    error,
                    !wal_may_have_changed,
                ));
            }
        };
        state.wal.pending_writes = state.wal.pending_writes.saturating_add(wal_records);
        state.wal.appended_bytes = state.wal.appended_bytes.saturating_add(wal_bytes as u64);
        self.pending_sync_count = self.pending_sync_count.saturating_add(wal_records);
        self.bytes_since_sync = self.bytes_since_sync.saturating_add(wal_bytes);
        Ok(super::PreparedSpilledAppend {
            request_id,
            last_sequence: sequence_plan.commit_seq,
            op_count: source.len(),
            touched_cfs: vec![cf_id],
            effective_durability,
            sequence_plan,
            commit_time_millis,
            logged: wal_records > 0,
        })
    }

    /// Apply a previously synchronized spilled Strict transaction as one
    /// publication unit and advance the shared visibility cut only on success.
    pub(crate) fn apply_spilled_strict_after_pipeline_sync(
        &mut self,
        state: &mut RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        prepared: super::PreparedSpilledAppend,
        memory: &crate::common::resource_budget::ResourceBudget,
    ) -> MidgeResult<(u64, usize, Vec<crate::types::ColumnFamilyId>)> {
        let mut applied_ops = 0_usize;
        let context = SpilledTransactionContext {
            source,
            sequence_plan: &prepared.sequence_plan,
            effective_durability: prepared.effective_durability,
            commit_time_millis: prepared.commit_time_millis,
            memory: Some(memory),
        };
        if let Err(error) = self
            .ensure_lease_validity()
            .and_then(|()| self.apply_spilled_transaction_ops(state, &context, &mut applied_ops))
        {
            return Err(self.fail_unapplied_transactions(
                state,
                &[&prepared.sequence_plan],
                super::UnappliedOutcome {
                    logged: prepared.logged,
                    partly_applied: applied_ops > 0,
                },
                error,
            ));
        }
        state.applied_sequence = prepared.sequence_plan.commit_seq;
        Ok((
            prepared.last_sequence,
            prepared.op_count,
            prepared.touched_cfs,
        ))
    }

    fn validate_spilled_preconditions(
        &self,
        state: &RuntimeState,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
        params: &super::SpilledTransactionAppendParams,
        proof: Option<&super::SpilledValidationProof>,
        memory: Option<&crate::common::resource_budget::ResourceBudget>,
    ) -> MidgeResult<Option<u32>> {
        if let Some(proof) = proof {
            let cf_id = proof.validate(state)?;
            return Ok((!source.is_empty()).then_some(cf_id));
        }
        if source.is_empty() {
            self.validate_assertion_only_spill(state, &params.assertions, params.start_sequence)?;
            return Ok(None);
        }
        Self::validate_spilled_transaction(
            state,
            self.read_resources.clone(),
            source,
            &params.assertions,
            params.start_sequence,
            params.conflict_policy,
            memory,
        )
        .map(Some)
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
            .map_err(|error| crate::common::error::classify_commit_error(error, !logged))?;
        // The WAL now holds the unit and `state.sequence` is past its commit.
        // A lease that lapsed since is a failed apply.
        let mut applied_ops = 0_usize;
        let context = SpilledTransactionContext {
            source,
            sequence_plan,
            effective_durability,
            commit_time_millis,
            memory: None,
        };
        if let Err(error) = self
            .ensure_lease_validity()
            .and_then(|()| self.apply_spilled_transaction_ops(state, &context, &mut applied_ops))
        {
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
        state.applied_sequence = sequence_plan.commit_seq;
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
        memory: Option<&crate::common::resource_budget::ResourceBudget>,
    ) -> MidgeResult<crate::types::ColumnFamilyId> {
        // A transaction writes one family; check before anything is read.
        let mut families = Vec::new();
        source.for_each_bounded(memory, |_, op| {
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
        source.for_each_bounded(memory, |ordinal, op| {
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
                let exists = match source.latest_before_bounded(ordinal, key, memory)? {
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
        memory: Option<&crate::common::resource_budget::ResourceBudget>,
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
        source.for_each_bounded(memory, |ordinal, op| {
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
        context: &SpilledTransactionContext<'_>,
        wal_may_have_changed: &mut bool,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<(usize, usize)> {
        if matches!(context.effective_durability, DurabilityPolicy::BestEffort) {
            return Ok((0, 0));
        }

        let mut begin = WalRecord::new_cf(
            context.sequence_plan.cf_id,
            WalOpKind::TxnBegin,
            Bytes::from_static(b"txn"),
            None,
            context.sequence_plan.begin_seq,
        );
        begin.txn_id = Some(context.sequence_plan.txn_id);
        let mut total_bytes = begin.estimated_size();
        let append_started_at = Instant::now();
        self.append_spilled_record(&begin, wal_may_have_changed, group)?;

        context
            .source
            .for_each_bounded(context.memory, |ordinal, op| {
                let sequence = context
                    .sequence_plan
                    .first_op_seq
                    .checked_add(ordinal)
                    .ok_or_else(|| {
                        MidgeError::ResourceLimit(
                            "transaction spill sequence allocation overflow".to_string(),
                        )
                    })?;
                let apply_op =
                    TransactionApplyOp::from_source(op, sequence, context.commit_time_millis);
                let record = apply_op.wal_record(context.sequence_plan.txn_id);
                total_bytes = total_bytes.saturating_add(record.estimated_size());
                self.append_spilled_record(&record, wal_may_have_changed, group)
            })?;

        crate::failpoints::fail_point!("midge::wal::spilled_txn_after_ops_append_before_commit");
        crate::failpoints::fail_point!(
            "midge::wal::inject_no_space_on_txn_commit_append",
            |_| Err(MidgeError::NoSpace(
                "failpoint: no space on transaction commit append".to_string()
            ))
        );
        let mut commit = WalRecord::new_cf(
            context.sequence_plan.cf_id,
            WalOpKind::TxnCommit,
            Bytes::from_static(b"txn"),
            None,
            context.sequence_plan.commit_seq,
        );
        commit.txn_id = Some(context.sequence_plan.txn_id);
        total_bytes = total_bytes.saturating_add(commit.estimated_size());
        self.append_spilled_record(&commit, wal_may_have_changed, group)?;
        self.finish_append_instrumentation(total_bytes as u64, append_started_at.elapsed());
        if let Err(error) = WalTransitionBoundary::AfterAppendBeforeAccounting.check() {
            return Err(Self::appended_unit_error(error));
        }
        crate::failpoints::fail_point!("midge::wal::spilled_after_append_batch_before_sync");
        tracing::trace!(request_id, "appended split transaction WAL frames");
        Ok((total_bytes, context.source.len().saturating_add(2)))
    }

    fn append_spilled_record(
        &mut self,
        record: &WalRecord,
        wal_may_have_changed: &mut bool,
        group: Option<&crate::lease::WriterEffectGroup>,
    ) -> MidgeResult<()> {
        self.ensure_lease_validity()?;
        let Some(writer) = self.writer_mut() else {
            return Err(MidgeError::Internal(
                "transaction WAL writer disappeared during append".to_string(),
            ));
        };
        let append = match group {
            Some(group) => match writer.writer_group_capability() {
                Some(capability) => {
                    capability.append_batch_in_group(std::slice::from_ref(record), group)
                }
                None => Err(crate::wal::traits::WalAppendError::unchanged(
                    MidgeError::Internal("strict spill lost its writer group capability".into()),
                )),
            },
            None => writer.append_record_accounted(record),
        };
        if let Err(failure) = append {
            *wal_may_have_changed |= !failure.unchanged;
            return Err(self.settle_failed_wal_append(0, failure));
        }
        *wal_may_have_changed = true;
        Ok(())
    }

    fn apply_spilled_transaction_ops(
        &self,
        state: &mut RuntimeState,
        context: &SpilledTransactionContext<'_>,
        applied_ops: &mut usize,
    ) -> MidgeResult<()> {
        context
            .source
            .for_each_bounded(context.memory, |ordinal, op| {
                crate::failpoints::fail_point!("midge::wal::spilled_apply_op", |_| Err(
                    MidgeError::Io(std::io::Error::other(
                        "failpoint: spilled transaction apply read failed"
                    ))
                ));
                let sequence = context.sequence_plan.first_op_seq.saturating_add(ordinal);
                let apply_op =
                    TransactionApplyOp::from_source(op, sequence, context.commit_time_millis);
                self.ensure_lease_validity()?;
                Self::apply_transaction_op_to_memtable(state, apply_op)?;
                *applied_ops += 1;
                Ok(())
            })?;
        tracing::trace!(
            commit_sequence = context.sequence_plan.commit_seq,
            apply_op_count = context.source.len(),
            ?context.effective_durability,
            "applied streamed transaction to memtables"
        );
        Ok(())
    }
}
