// Responsibilities for this WAL actor slice stay within the actor namespace.
use super::super::cloud_write_queue::TransactionApplyOp;
use super::WalActor;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::state::RuntimeState;
use crate::sst::Memtable;
use crate::wal::{DurabilityPolicy, WalOpKind, WalRecord};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(test)]
thread_local! {
    static ASSERTION_SNAPSHOT_BUILD_COUNT: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

impl WalActor {
    pub(super) fn preflight_prepared_transaction_ops(
        state: &RuntimeState,
        apply_ops: &[&TransactionApplyOp],
    ) -> MidgeResult<()> {
        let mut batch_pairs = HashSet::new();
        for apply_op in apply_ops {
            let (cf_id, point) = match apply_op {
                TransactionApplyOp::Put {
                    cf_id,
                    key,
                    sequence,
                    ..
                }
                | TransactionApplyOp::Delete {
                    cf_id,
                    key,
                    sequence,
                } => (*cf_id, Some((key.as_ref(), *sequence))),
                TransactionApplyOp::DeleteRange { cf_id, .. } => (*cf_id, None),
            };
            let cf_state = state.column_families.get(&cf_id).ok_or_else(|| {
                MidgeError::InvalidArgument(format!("column family {cf_id} does not exist"))
            })?;
            let Some((key, sequence)) = point else {
                continue;
            };
            if !batch_pairs.insert((cf_id, key.to_vec(), sequence))
                || cf_state.memtable.contains_key_sequence(key, sequence)
                || cf_state
                    .immutable_memtables
                    .iter()
                    .any(|memtable| memtable.contains_key_sequence(key, sequence))
            {
                return Err(MidgeError::Corruption(format!(
                    "duplicate memtable key/sequence pair for column family {cf_id} at sequence {sequence}"
                )));
            }
        }
        Ok(())
    }

    /// Apply an operation set that was completely validated before the WAL
    /// append. Runtime event-loop ownership prevents another memtable writer
    /// from invalidating that validation before this phase completes.
    pub(super) fn apply_prevalidated_transaction_ops(
        state: &mut RuntimeState,
        apply_ops: Vec<TransactionApplyOp>,
        effective_durability: DurabilityPolicy,
        last_sequence: u64,
        apply_op_count: usize,
    ) {
        Self::apply_transaction_ops(
            state,
            apply_ops,
            effective_durability,
            last_sequence,
            apply_op_count,
        )
        .expect("prevalidated memtable mutation must remain infallible");
    }

    pub(super) fn apply_transaction_ops(
        state: &mut RuntimeState,
        apply_ops: Vec<TransactionApplyOp>,
        effective_durability: DurabilityPolicy,
        last_sequence: u64,
        apply_op_count: usize,
    ) -> MidgeResult<()> {
        if !matches!(effective_durability, DurabilityPolicy::CloudAsync) {
            return Self::apply_ops_to_memtables(state, apply_ops);
        }
        for apply_op in apply_ops {
            Self::apply_transaction_op_to_memtable(state, apply_op)?;
        }
        tracing::trace!(
            commit_sequence = last_sequence,
            apply_op_count,
            "Applied batch to memtable (CloudAsync)"
        );
        Ok(())
    }

    pub(super) fn apply_transaction_op_to_memtable(
        state: &mut RuntimeState,
        apply_op: TransactionApplyOp,
    ) -> MidgeResult<()> {
        match apply_op {
            TransactionApplyOp::Put {
                cf_id,
                key,
                value,
                expiration,
                sequence,
                ..
            } => Self::apply_to_memtable(state, sequence, cf_id, key, Some(value), expiration),
            TransactionApplyOp::Delete {
                cf_id,
                key,
                sequence,
            } => Self::apply_to_memtable(state, sequence, cf_id, key, None, None),
            TransactionApplyOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
                sequence,
            } => Self::apply_delete_range_to_memtable(state, sequence, cf_id, &start_key, &end_key),
        }
    }

    pub(super) fn ensure_no_write_conflicts(
        state: &RuntimeState,
        ops: &[crate::runtime::TransactionOp],
        start_sequence: u64,
    ) -> MidgeResult<()> {
        let mut checked_keys: HashSet<(crate::types::ColumnFamilyId, Vec<u8>)> = HashSet::new();

        for op in ops {
            match op {
                crate::runtime::TransactionOp::Put { cf_id, key, .. }
                | crate::runtime::TransactionOp::Delete { cf_id, key } => {
                    let dedupe_key = (*cf_id, key.to_vec());
                    if !checked_keys.insert(dedupe_key) {
                        continue;
                    }

                    if let Some(latest_seq) = Self::latest_key_sequence(state, *cf_id, key)? {
                        if latest_seq > start_sequence {
                            Self::record_write_conflict_point();
                            return Err(MidgeError::WriteConflict(format!(
                                "key conflict on cf {cf_id} for key {key:?}: latest seq {latest_seq} > tx start {start_sequence}"
                            )));
                        }
                    }

                    if let Some(range_seq) =
                        state.latest_covering_delete_range_sequence(*cf_id, key)
                    {
                        if range_seq > start_sequence {
                            Self::record_write_conflict_point();
                            return Err(MidgeError::WriteConflict(format!(
                                "key conflict on cf {cf_id} for key {key:?}: covered by delete-range seq {range_seq} > tx start {start_sequence}"
                            )));
                        }
                    }
                }
                crate::runtime::TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } => {
                    if let Some(latest_seq) = Self::latest_range_sequence(
                        state,
                        *cf_id,
                        start_key.as_ref(),
                        end_key.as_ref(),
                    )? {
                        if latest_seq > start_sequence {
                            Self::record_write_conflict_range();
                            return Err(MidgeError::WriteConflict(format!(
                                "range conflict on cf {cf_id} for [{start_key:?}, {end_key:?}): latest seq {latest_seq} > tx start {start_sequence}"
                            )));
                        }
                    }

                    if let Some(range_seq) = state.latest_overlapping_delete_range_sequence(
                        *cf_id,
                        start_key.as_ref(),
                        end_key.as_ref(),
                    ) {
                        if range_seq > start_sequence {
                            Self::record_write_conflict_range();
                            return Err(MidgeError::WriteConflict(format!(
                                "range conflict on cf {cf_id} for [{start_key:?}, {end_key:?}): overlaps delete-range seq {range_seq} > tx start {start_sequence}"
                            )));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Reject the commit if any asserted key received a point mutation or a
    /// covering range deletion after `start_sequence`. Enforced
    /// unconditionally (independent of `ConflictPolicy`) — an explicit
    /// assertion is a stronger guarantee than the ambient conflict policy.
    ///
    /// This is a pure sequence comparison, deliberately not a value re-read:
    /// value equality against the transaction's start snapshot is already
    /// checked client-side before submission. Comparing sequences instead of
    /// values makes this ABA-safe — a write that restores the original
    /// value still bumps the sequence and is still correctly rejected.
    pub(super) fn ensure_no_assertion_conflicts(
        state: &RuntimeState,
        assertions: &[crate::runtime::KeyAssertion],
        start_sequence: u64,
    ) -> MidgeResult<()> {
        let mut checked_keys: HashSet<(crate::types::ColumnFamilyId, Vec<u8>)> = HashSet::new();
        let mut snapshots: HashMap<
            crate::types::ColumnFamilyId,
            Option<crate::runtime::ReadSnapshot>,
        > = HashMap::new();

        for assertion in assertions {
            let dedupe_key = (assertion.cf_id, assertion.key.to_vec());
            if !checked_keys.insert(dedupe_key) {
                continue;
            }

            if !state.column_families.contains_key(&assertion.cf_id) {
                return Err(MidgeError::InvalidArgument(format!(
                    "column family {} does not exist",
                    assertion.cf_id
                )));
            }

            let snapshot = snapshots
                .entry(assertion.cf_id)
                .or_insert_with(|| Self::latest_sequence_snapshot(state, assertion.cf_id));
            if let Some(snapshot) = snapshot {
                if let Some(latest_seq) = snapshot.latest_state_sequence(&assertion.key)? {
                    if latest_seq > start_sequence {
                        Self::record_write_conflict_point();
                        return Err(MidgeError::WriteConflict(format!(
                            "assertion conflict on cf {} for key {:?}: latest seq {latest_seq} > tx start {start_sequence}",
                            assertion.cf_id, assertion.key
                        )));
                    }
                }
            }

            if let Some(range_seq) =
                state.latest_covering_delete_range_sequence(assertion.cf_id, &assertion.key)
            {
                if range_seq > start_sequence {
                    Self::record_write_conflict_point();
                    return Err(MidgeError::WriteConflict(format!(
                        "assertion conflict on cf {} for key {:?}: covered by delete-range seq {range_seq} > tx start {start_sequence}",
                        assertion.cf_id, assertion.key
                    )));
                }
            }
        }

        Ok(())
    }

    pub(super) fn latest_key_sequence(
        state: &RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
    ) -> MidgeResult<Option<u64>> {
        let Some(snapshot) = Self::latest_sequence_snapshot(state, cf_id) else {
            return Ok(None);
        };
        snapshot.latest_state_sequence(key)
    }

    fn latest_sequence_snapshot(
        state: &RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
    ) -> Option<crate::runtime::ReadSnapshot> {
        let cf_state = state.column_families.get(&cf_id)?;
        let sst_files: Vec<_> = state
            .manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect();

        let sst_path_prefix = state
            .sst_dir
            .strip_prefix(&state.db_path)
            .unwrap_or_else(|_| std::path::Path::new("sst"))
            .to_path_buf();

        let mut snapshot = crate::runtime::ReadSnapshot::new(
            cf_state.memtable.clone(),
            cf_state.immutable_memtables.clone(),
            sst_files,
            Arc::clone(&state.fs),
            sst_path_prefix,
            state.is_memory_mode(),
        );
        snapshot.cf_id = cf_id;
        #[cfg(test)]
        ASSERTION_SNAPSHOT_BUILD_COUNT.with(|count| count.set(count.get().saturating_add(1)));
        Some(snapshot)
    }

    #[cfg(test)]
    pub(super) fn reset_assertion_snapshot_build_count() {
        ASSERTION_SNAPSHOT_BUILD_COUNT.with(|count| count.set(0));
    }

    #[cfg(test)]
    pub(super) fn assertion_snapshot_build_count() -> usize {
        ASSERTION_SNAPSHOT_BUILD_COUNT.with(std::cell::Cell::get)
    }

    pub(super) fn latest_range_sequence(
        state: &RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        start_key: &[u8],
        end_key: &[u8],
    ) -> MidgeResult<Option<u64>> {
        let Some(snapshot) = Self::latest_sequence_snapshot(state, cf_id) else {
            return Ok(None);
        };
        snapshot.latest_sequence_in_range(start_key, end_key)
    }

    pub(super) fn build_apply_ops(
        ops: Vec<crate::runtime::TransactionOp>,
        first_op_seq: u64,
        _txn_id: u64,
        current_epoch: u64,
    ) -> Vec<TransactionApplyOp> {
        let mut apply_ops = Vec::with_capacity(ops.len());

        for (i, op) in ops.into_iter().enumerate() {
            let seq = first_op_seq + i as u64;
            match op {
                crate::runtime::TransactionOp::Put {
                    cf_id,
                    key,
                    value,
                    ttl_seconds,
                    insert_only,
                } => {
                    let op_kind = if insert_only {
                        WalOpKind::Insert
                    } else {
                        WalOpKind::Put
                    };

                    let expiration = match ttl_seconds {
                        Some(ttl) if ttl > 0 => {
                            WalRecord::new_with_ttl(
                                cf_id,
                                op_kind,
                                key.clone(),
                                Some(value.clone()),
                                seq,
                                ttl,
                                current_epoch,
                            )
                            .expiration
                        }
                        _ => None,
                    };

                    apply_ops.push(TransactionApplyOp::Put {
                        op: op_kind,
                        cf_id,
                        key,
                        value,
                        expiration,
                        sequence: seq,
                    });
                }
                crate::runtime::TransactionOp::Delete { cf_id, key } => {
                    apply_ops.push(TransactionApplyOp::Delete {
                        cf_id,
                        key,
                        sequence: seq,
                    });
                }
                crate::runtime::TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } => {
                    apply_ops.push(TransactionApplyOp::DeleteRange {
                        cf_id,
                        start_key,
                        end_key,
                        sequence: seq,
                    });
                }
            }
        }

        apply_ops
    }

    fn apply_ops_to_memtables(
        state: &mut RuntimeState,
        apply_ops: Vec<TransactionApplyOp>,
    ) -> MidgeResult<()> {
        for apply_op in apply_ops {
            match apply_op {
                TransactionApplyOp::Put {
                    cf_id,
                    key,
                    value,
                    expiration,
                    sequence,
                    ..
                } => {
                    Self::apply_to_memtable(state, sequence, cf_id, key, Some(value), expiration)?;
                }
                TransactionApplyOp::Delete {
                    cf_id,
                    key,
                    sequence,
                } => {
                    Self::apply_to_memtable(state, sequence, cf_id, key, None, None)?;
                }
                TransactionApplyOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                    sequence,
                } => {
                    Self::apply_delete_range_to_memtable(
                        state, sequence, cf_id, &start_key, &end_key,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Checks current in-memory view (active + immutable memtables) for existence
    pub(super) fn key_exists(
        state: &RuntimeState,
        cf_id: crate::types::ColumnFamilyId,
        key: &[u8],
    ) -> bool {
        let Some(cf_state) = state.column_families.get(&cf_id) else {
            return false;
        };
        let sst_files = state
            .manifest
            .files
            .iter()
            .filter(|file| file.cf_id == cf_id)
            .cloned()
            .collect();
        let sst_path_prefix = state
            .sst_dir
            .strip_prefix(&state.db_path)
            .unwrap_or_else(|_| std::path::Path::new("sst"))
            .to_path_buf();
        let snapshot = crate::runtime::ReadSnapshot::new(
            cf_state.memtable.clone(),
            cf_state.immutable_memtables.clone(),
            sst_files,
            Arc::clone(&state.fs),
            sst_path_prefix,
            state.is_memory_mode(),
        );
        matches!(snapshot.get(key, u64::MAX), Ok(Some(_)))
    }

    /// Apply a write to the memtable
    pub(super) fn apply_to_memtable(
        state: &mut RuntimeState,
        sequence: u64,
        cf_id: crate::types::ColumnFamilyId,
        key: Bytes,
        value: Option<Bytes>,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        let current_segment_id = state.wal.current_segment_id;
        let cf_state = state.column_families.get_mut(&cf_id).ok_or_else(|| {
            MidgeError::Internal(format!(
                "column family {cf_id} disappeared between validation and memtable apply"
            ))
        })?;
        let prev = cf_state.memtable.size_bytes();
        if let Some(val) = value {
            cf_state
                .memtable
                .as_ref()
                .put_bytes_with_seq(key, val, sequence, expiration)?;
        } else {
            cf_state
                .memtable
                .as_ref()
                .delete_bytes_with_seq(key, sequence)?;
        }
        let new = cf_state.memtable.size_bytes();
        if prev == 0 && new > 0 {
            cf_state.active_memtable_started_in_segment = current_segment_id;
        }
        state.recompute_total_memtable_bytes();
        Ok(())
    }

    /// Apply a `delete_range` operation to the memtable
    pub(super) fn apply_delete_range_to_memtable(
        state: &mut RuntimeState,
        sequence: u64,
        cf_id: u32,
        start_key: &[u8],
        end_key: &[u8],
    ) -> MidgeResult<()> {
        let current_segment_id = state.wal.current_segment_id;
        let cf_state = state.column_families.get_mut(&cf_id).ok_or_else(|| {
            MidgeError::Internal(format!(
                "column family {cf_id} disappeared between validation and range apply"
            ))
        })?;
        let memtable = Arc::clone(&cf_state.memtable);
        let prev = memtable.size_bytes();
        memtable
            .as_ref()
            .delete_range_with_seq(start_key, end_key, sequence)?;
        let new = memtable.size_bytes();
        if prev == 0 && new > 0 {
            cf_state.active_memtable_started_in_segment = current_segment_id;
        }
        state.recompute_total_memtable_bytes();
        state.record_delete_range(cf_id, start_key, end_key, sequence);
        Ok(())
    }
}
