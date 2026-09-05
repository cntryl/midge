//! Keep accepted cloud transactions inside a flushable memtable generation.

use super::EventLoop;
use crate::common::{MidgeError, MidgeResult};
use crate::runtime::TransactionOp;
use crate::sst::size_bound::{flush_staging_bytes, point_bytes, range_bytes, FIXED_SST_BYTES};
use crate::sst::Memtable;
use std::collections::HashMap;

impl EventLoop {
    pub(super) fn cloud_flush_staging_window(&self) -> Option<u64> {
        self.hybrid_storage
            .as_ref()
            .filter(|storage| storage.ephemeral_sst_cache_enabled())
            // The other half remains available to compaction staging.
            .map(|storage| storage.budget_snapshot().max_local_bytes / 2)
    }

    pub(super) fn prepare_cloud_transaction_memtables(
        &mut self,
        ops: &[TransactionOp],
    ) -> MidgeResult<()> {
        let Some(window) = self.cloud_flush_staging_window() else {
            return Ok(());
        };
        let mut growth = HashMap::new();
        for op in ops {
            add_operation_growth(&mut growth, op);
        }
        self.admit_cloud_memtable_growth(&growth, window)
    }

    pub(super) fn prepare_cloud_spilled_transaction_memtables(
        &mut self,
        source: &crate::runtime::transaction_spill::TransactionOpSource,
    ) -> MidgeResult<()> {
        let Some(window) = self.cloud_flush_staging_window() else {
            return Ok(());
        };
        let mut growth = HashMap::new();
        source.for_each(|_, op| {
            add_operation_growth(&mut growth, &op);
            Ok(())
        })?;
        self.admit_cloud_memtable_growth(&growth, window)
    }

    fn admit_cloud_memtable_growth(
        &mut self,
        growth: &HashMap<u32, usize>,
        window: u64,
    ) -> MidgeResult<()> {
        let mut freeze = Vec::new();
        // Validate every family before changing any generation. In particular,
        // an indivisible transaction must fail before its first WAL frame.
        for (&cf_id, &additional) in growth {
            let standalone = flush_staging_bytes(FIXED_SST_BYTES.saturating_add(additional));
            if standalone > window {
                return Err(MidgeError::NoSpace(format!(
                    "column family {cf_id} transaction requires {standalone} flush staging bytes, exceeding its {window}-byte local window"
                )));
            }
            let cf = self.state.get_cf(cf_id).ok_or_else(|| {
                MidgeError::InvalidArgument(format!("column family {cf_id} does not exist"))
            })?;
            let projected = flush_staging_bytes(
                cf.memtable
                    .encoded_size_upper_bound()
                    .saturating_add(additional),
            );
            if projected > window && cf.memtable.size_bytes() > 0 {
                if self.state.is_immutable_memtable_queue_full(cf_id)
                    || self.state.l0_slot_usage(cf_id) >= self.state.l0_hard_ceiling()
                {
                    return Err(MidgeError::WriteStall(format!(
                        "column family {cf_id} must publish an immutable before admitting another cloud generation"
                    )));
                }
                freeze.push(cf_id);
            }
        }
        for cf_id in freeze {
            self.freeze_active_memtable(cf_id)?;
        }
        self.schedule_next_flush_worker();
        Ok(())
    }

    pub(super) fn freeze_cloud_memtables_near_staging_limit(&mut self) -> usize {
        let Some(window) = self.cloud_flush_staging_window() else {
            return 0;
        };
        let candidates: Vec<_> = self
            .state
            .column_families
            .iter()
            .filter(|(_, cf)| cf.memtable.size_bytes() > 0)
            .filter(|(_, cf)| {
                flush_staging_bytes(cf.memtable.encoded_size_upper_bound()) >= window / 2
            })
            .map(|(&cf_id, _)| cf_id)
            .collect();
        candidates
            .into_iter()
            .filter(|cf_id| matches!(self.freeze_active_memtable(*cf_id), Ok(Some(_))))
            .count()
    }
}

fn add_operation_growth(growth: &mut HashMap<u32, usize>, op: &TransactionOp) {
    let (cf_id, bytes) = match op {
        TransactionOp::Put {
            cf_id, key, value, ..
        } => (*cf_id, point_bytes(key.len(), value.len())),
        TransactionOp::Delete { cf_id, key } => (*cf_id, point_bytes(key.len(), 0)),
        TransactionOp::DeleteRange {
            cf_id,
            start_key,
            end_key,
        } => {
            if start_key >= end_key {
                return;
            }
            (*cf_id, range_bytes(start_key.len(), end_key.len()))
        }
    };
    let current = growth.entry(cf_id).or_default();
    *current = current.saturating_add(bytes);
}
