//! Single-operation appends for tests, through the production transaction
//! path (#504).
//!
//! Tests used to reach a separate test-only append that had drifted from
//! production: it skipped WAL budget settlement on failure, wrote untagged
//! records and bypassed transaction framing. These helpers build a one-op
//! transaction instead, so every test exercises the code release builds run.

use super::{TransactionAppendParams, WalActor};
use crate::common::MidgeResult;
use crate::runtime::state::RuntimeState;
use crate::runtime::TransactionOp;
use crate::wal::DurabilityPolicy;
use bytes::Bytes;

/// One put (or, with `value: None`, one delete) to append.
pub struct AppendParams {
    pub request_id: u64,
    pub cf_id: crate::types::ColumnFamilyId,
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub insert_only: bool,
    pub ttl_seconds: Option<u64>,
}

impl WalActor {
    /// Append one put or delete as a single-operation transaction. Returns
    /// its sequence and whether durability is deferred to a later sync.
    pub(crate) fn append_single_op(
        &mut self,
        state: &mut RuntimeState,
        params: AppendParams,
    ) -> MidgeResult<(u64, bool)> {
        let AppendParams {
            request_id,
            cf_id,
            key,
            value,
            insert_only,
            ttl_seconds,
        } = params;
        let op = match value {
            Some(value) => TransactionOp::Put {
                cf_id,
                key,
                value,
                ttl_seconds,
                insert_only,
            },
            None => TransactionOp::Delete { cf_id, key },
        };
        self.append_one(state, request_id, op, None)
    }

    /// Append one range delete as a single-operation transaction.
    pub(crate) fn append_single_range_delete(
        &mut self,
        state: &mut RuntimeState,
        request_id: u64,
        cf_id: crate::types::ColumnFamilyId,
        start_key: Bytes,
        end_key: Bytes,
        durability_policy: Option<DurabilityPolicy>,
    ) -> MidgeResult<(u64, bool)> {
        let op = TransactionOp::DeleteRange {
            cf_id,
            start_key,
            end_key,
        };
        self.append_one(state, request_id, op, durability_policy)
    }

    fn append_one(
        &mut self,
        state: &mut RuntimeState,
        request_id: u64,
        op: TransactionOp,
        durability_policy: Option<DurabilityPolicy>,
    ) -> MidgeResult<(u64, bool)> {
        let (sequence, _, deferred) = self.append_transaction(
            state,
            TransactionAppendParams {
                request_id,
                ops: vec![op],
                assertions: Vec::new(),
                durability_policy,
                start_sequence: None,
                conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            },
        )?;
        Ok((sequence, deferred))
    }
}
