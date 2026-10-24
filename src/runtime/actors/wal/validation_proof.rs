//! Detached SST reads tied to one fully applied runtime cut.

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::read_snapshot::point_read::ValidationPointState;
use crate::runtime::{ConflictPolicy, KeyAssertion, ReadSnapshot, TransactionOp};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

type PointProofs = HashMap<u32, HashMap<Bytes, ValidationPointState>>;
type RangeProofs = HashMap<u32, HashMap<Bytes, HashMap<Bytes, Option<u64>>>>;

pub(crate) struct CommitValidationProof {
    pub(crate) applied_sequence: u64,
    points: PointProofs,
    ranges: RangeProofs,
}

impl CommitValidationProof {
    pub(crate) fn read(
        applied_sequence: u64,
        snapshots: &HashMap<u32, Arc<ReadSnapshot>>,
        ops: &[TransactionOp],
        assertions: &[KeyAssertion],
        conflict_policy: ConflictPolicy,
        start_sequence: Option<u64>,
        deadline: crate::common::OperationDeadline,
    ) -> MidgeResult<Self> {
        let mut proof = Self {
            applied_sequence,
            points: HashMap::new(),
            ranges: HashMap::new(),
        };
        for op in ops {
            if deadline.is_expired() {
                return Err(MidgeError::Timeout(
                    "transaction validation expired between reads".into(),
                ));
            }
            match op {
                TransactionOp::Put {
                    cf_id,
                    key,
                    insert_only,
                    ..
                } if *insert_only || conflict_policy == ConflictPolicy::AbortOnWriteConflict => {
                    proof.read_point(snapshots, *cf_id, key)?;
                }
                TransactionOp::Delete { cf_id, key }
                    if conflict_policy == ConflictPolicy::AbortOnWriteConflict =>
                {
                    proof.read_point(snapshots, *cf_id, key)?;
                }
                TransactionOp::DeleteRange {
                    cf_id,
                    start_key,
                    end_key,
                } if conflict_policy == ConflictPolicy::AbortOnWriteConflict => {
                    let threshold = start_sequence.ok_or_else(|| {
                        MidgeError::InvalidArgument(
                            "AbortOnWriteConflict requires transaction start_sequence".into(),
                        )
                    })?;
                    let sequence = Self::snapshot(snapshots, *cf_id)?
                        .any_sequence_after_in_range(start_key, end_key, threshold)?;
                    proof
                        .ranges
                        .entry(*cf_id)
                        .or_default()
                        .entry(start_key.clone())
                        .or_default()
                        .insert(end_key.clone(), sequence);
                }
                _ => {}
            }
        }
        for assertion in assertions {
            if deadline.is_expired() {
                return Err(MidgeError::Timeout(
                    "transaction assertion validation expired between reads".into(),
                ));
            }
            proof.read_point(snapshots, assertion.cf_id, &assertion.key)?;
        }
        Ok(proof)
    }

    fn snapshot(
        snapshots: &HashMap<u32, Arc<ReadSnapshot>>,
        cf_id: u32,
    ) -> MidgeResult<&ReadSnapshot> {
        snapshots.get(&cf_id).map(Arc::as_ref).ok_or_else(|| {
            MidgeError::InvalidArgument(format!("column family {cf_id} does not exist"))
        })
    }

    fn read_point(
        &mut self,
        snapshots: &HashMap<u32, Arc<ReadSnapshot>>,
        cf_id: u32,
        key: &Bytes,
    ) -> MidgeResult<()> {
        let points = self.points.entry(cf_id).or_default();
        if !points.contains_key(key) {
            let state = Self::snapshot(snapshots, cf_id)?
                .validation_point_state(key, self.applied_sequence)?;
            points.insert(key.clone(), state);
        }
        Ok(())
    }

    pub(super) fn point(&self, cf_id: u32, key: &[u8]) -> MidgeResult<ValidationPointState> {
        self.points
            .get(&cf_id)
            .and_then(|points| points.get(key))
            .copied()
            .ok_or_else(|| MidgeError::Internal("detached validation omitted a point read".into()))
    }

    pub(super) fn range(&self, cf_id: u32, start: &[u8], end: &[u8]) -> MidgeResult<Option<u64>> {
        self.ranges
            .get(&cf_id)
            .and_then(|ranges| ranges.get(start))
            .and_then(|ranges| ranges.get(end))
            .copied()
            .ok_or_else(|| MidgeError::Internal("detached validation omitted a range read".into()))
    }
}
