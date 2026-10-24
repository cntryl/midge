//! Captured concurrent memtable effects and loop-owned group bookkeeping.

use super::{
    PreparedGroupOutcome, PreparedTransactionAppend, TransactionAppendResult, TransactionApplyOp,
    TxnSequencePlan, WalActor,
};
use crate::common::{MidgeError, MidgeResult};
use crate::memtable::SkipListMemtable;
use crate::runtime::state::RuntimeState;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

pub(crate) type MemtableApplyAuthority = Arc<dyn Fn() -> MidgeResult<()> + Send + Sync>;

pub(crate) struct MemtableApplyJob {
    pub(crate) ordinal: usize,
    target: Arc<SkipListMemtable>,
    operations: Vec<TransactionApplyOp>,
}

struct CapturedFamily {
    target: Arc<SkipListMemtable>,
    previous_size: usize,
}

struct UnitMetadata {
    request_id: u64,
    sequence: TxnSequencePlan,
    operation_count: usize,
}

pub(crate) struct MemtableApplyPlan {
    pub(crate) jobs: VecDeque<MemtableApplyJob>,
    families: HashMap<u32, CapturedFamily>,
    units: Vec<UnitMetadata>,
    ranges: Vec<(u32, Bytes, Bytes, u64)>,
    open_segment: u64,
    wal_bytes: u64,
}

impl MemtableApplyJob {
    pub(crate) fn apply(self, authority: &MemtableApplyAuthority) -> MidgeResult<()> {
        crate::failpoints::fail_point!("midge::wal::before_prepared_transaction_apply");
        if self.ordinal == 0 {
            crate::failpoints::fail_point!("midge::apply::before_first_unit");
        }
        for operation in self.operations {
            authority()?;
            match operation {
                TransactionApplyOp::Put {
                    key,
                    value,
                    sequence,
                    expiration,
                    ..
                } => {
                    self.target
                        .put_bytes_with_seq(key, value, sequence, expiration)?;
                }
                TransactionApplyOp::Delete { key, sequence, .. } => {
                    self.target.delete_bytes_with_seq(key, sequence)?;
                }
                TransactionApplyOp::DeleteRange {
                    start_key,
                    end_key,
                    sequence,
                    ..
                } => {
                    self.target
                        .delete_range_with_seq(&start_key, &end_key, sequence)?;
                }
            }
        }
        authority()?;
        if self.ordinal == 1 {
            crate::failpoints::fail_point!("midge::apply::after_second_unit");
        }
        Ok(())
    }
}

impl WalActor {
    /// Capture sources before any worker starts. Freeze, layout and later
    /// writes remain behind the group until all of these effects settle.
    pub(crate) fn prepare_detached_memtable_apply(
        &self,
        state: &RuntimeState,
        prepared: Vec<PreparedTransactionAppend>,
    ) -> MidgeResult<MemtableApplyPlan> {
        self.ensure_lease_validity()?;
        let mut plan = MemtableApplyPlan {
            jobs: VecDeque::new(),
            families: HashMap::new(),
            units: Vec::new(),
            ranges: Vec::new(),
            open_segment: state.wal.open_segment(),
            wal_bytes: state.wal.appended_bytes,
        };
        for (ordinal, unit) in prepared.into_iter().enumerate() {
            let cf_id = unit.sequence_plan.cf_id;
            let family = state.get_cf(cf_id).ok_or_else(|| {
                MidgeError::Internal("memtable family disappeared before apply admission".into())
            })?;
            Self::ensure_family_accepts_writes(state, cf_id)?;
            plan.families
                .entry(cf_id)
                .or_insert_with(|| CapturedFamily {
                    target: Arc::clone(&family.memtable),
                    previous_size: family.memtable.size_bytes(),
                });
            for operation in &unit.apply_ops {
                if let TransactionApplyOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    sequence,
                } = operation
                {
                    plan.ranges
                        .push((*cf_id, start_key.clone(), end_key.clone(), *sequence));
                }
            }
            plan.units.push(UnitMetadata {
                request_id: unit.request_id,
                operation_count: unit.apply_ops.len(),
                sequence: unit.sequence_plan,
            });
            plan.jobs.push_back(MemtableApplyJob {
                ordinal,
                target: Arc::clone(&family.memtable),
                operations: unit.apply_ops,
            });
        }
        Ok(plan)
    }

    /// Called only after every admitted worker is terminal. Absolute source
    /// sizes are settled once, so concurrent inserts cannot double-count a
    /// before/after delta. Visibility advances once for the complete group.
    pub(crate) fn finish_detached_memtable_apply(
        &mut self,
        state: &mut RuntimeState,
        plan: MemtableApplyPlan,
        failure: Option<MidgeError>,
    ) -> MidgeResult<PreparedGroupOutcome> {
        let result = Self::settle_detached_memtable_sizes(state, &plan)
            .and_then(|()| self.ensure_lease_validity())
            .and_then(|()| failure.map_or(Ok(()), Err));
        if let Err(error) = result {
            let sequences = plan
                .units
                .iter()
                .map(|unit| &unit.sequence)
                .collect::<Vec<_>>();
            return Err(self.fail_unapplied_transactions(
                state,
                &sequences,
                super::UnappliedOutcome {
                    logged: true,
                    partly_applied: true,
                },
                error,
            ));
        }
        for (cf_id, start, end, sequence) in plan.ranges {
            state.record_delete_range(cf_id, &start, &end, sequence);
        }
        let applied = plan
            .units
            .into_iter()
            .map(|unit| TransactionAppendResult {
                request_id: unit.request_id,
                last_sequence: unit.sequence.commit_seq,
                op_count: unit.operation_count,
                deferred: false,
            })
            .collect::<Vec<_>>();
        if let Some(last) = applied.last() {
            state.applied_sequence = last.last_sequence;
        }
        Ok(PreparedGroupOutcome {
            applied,
            failure: None,
        })
    }

    fn settle_detached_memtable_sizes(
        state: &mut RuntimeState,
        plan: &MemtableApplyPlan,
    ) -> MidgeResult<()> {
        for (&cf_id, captured) in &plan.families {
            let family = state.get_cf_mut(cf_id).ok_or_else(|| {
                MidgeError::Internal("memtable family disappeared during admitted apply".into())
            })?;
            if !Arc::ptr_eq(&family.memtable, &captured.target) {
                return Err(MidgeError::Internal(
                    "memtable source changed during admitted apply".into(),
                ));
            }
            let current = captured.target.size_bytes();
            if captured.previous_size == 0 && current > 0 {
                family.active_memtable_started_in_segment = plan.open_segment;
                family.active_memtable_started_at_wal_bytes = plan.wal_bytes;
            }
            state.total_memtable_bytes = state
                .total_memtable_bytes
                .saturating_sub(captured.previous_size)
                .saturating_add(current);
        }
        Ok(())
    }
}
