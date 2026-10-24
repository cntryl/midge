//! Constant-size streamed validation, retaining no per-key result cache.

use super::WalActor;
use crate::common::resource_budget::ResourceBudget;
use crate::common::{MidgeError, MidgeResult, OperationDeadline};
use crate::runtime::read_snapshot::point_read::ValidationValue;
use crate::runtime::state::{RecentDeleteRange, RuntimeState};
use crate::runtime::transaction_spill::{IntentLookup, TransactionOpSource};
use crate::runtime::{ConflictPolicy, KeyAssertion, ReadSnapshot, TransactionOp};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) struct SpilledValidationInput<'a> {
    pub source: &'a TransactionOpSource,
    pub snapshots: &'a HashMap<u32, Arc<ReadSnapshot>>,
    pub recent_ranges: &'a [RecentDeleteRange],
    pub applied_sequence: u64,
    pub assertions: &'a [KeyAssertion],
    pub start_sequence: u64,
    pub conflict_policy: ConflictPolicy,
    pub deadline: OperationDeadline,
    pub cancelled: &'a AtomicBool,
    pub memory: &'a ResourceBudget,
}

enum ValidationFailure {
    Point {
        cf_id: u32,
        key: Bytes,
    },
    Range {
        cf_id: u32,
        start: Bytes,
        end: Bytes,
    },
    Assertion {
        cf_id: u32,
        key: Bytes,
        sequence: u64,
    },
    Duplicate,
    Read(MidgeError),
}

pub(crate) struct SpilledValidationProof {
    pub(crate) applied_sequence: u64,
    snapshots: HashMap<u32, Arc<ReadSnapshot>>,
    family: u32,
    start_sequence: u64,
    // Only finite insertion conflicts before the first static error matter.
    // If all expire by admission, that later error (or success) takes effect.
    insertion_expires_at: Option<u64>,
    failure: Option<ValidationFailure>,
}

impl SpilledValidationProof {
    pub(crate) fn read(input: SpilledValidationInput<'_>) -> MidgeResult<Self> {
        let mut families = Vec::new();
        input.source.for_each_bounded(Some(input.memory), |_, op| {
            input.check_deadline()?;
            let family = crate::runtime::transaction_spill::op_cf_id(&op);
            if !families.contains(&family) {
                families.push(family);
            }
            Ok(())
        })?;
        let family = WalActor::transaction_family(families.into_iter())?;
        let mut proof = Self {
            applied_sequence: input.applied_sequence,
            snapshots: input.snapshots.clone(),
            family,
            start_sequence: input.start_sequence,
            insertion_expires_at: None,
            failure: None,
        };
        let mut reader = StreamValidation {
            input,
            proof: &mut proof,
        };
        let result = reader.assertions().and_then(|()| reader.operations());
        if let Err(error) = result {
            // A classified conflict has its exact position above. An I/O or
            // structural failure is the first later static failure otherwise.
            if proof.failure.is_none() {
                proof.failure = Some(ValidationFailure::Read(error));
            }
        }
        Ok(proof)
    }

    pub(crate) fn matches_sources(&self, state: &RuntimeState) -> bool {
        self.applied_sequence == state.applied_sequence
            && self.snapshots.iter().all(|(cf_id, snapshot)| {
                state.column_families.get(cf_id).is_some_and(|family| {
                    snapshot.has_sources(
                        &family.memtable,
                        &family.immutable_memtables,
                        &state.manifest.read_view_for(*cf_id),
                    )
                })
            })
    }

    pub(crate) fn validate(&self, state: &RuntimeState) -> MidgeResult<u32> {
        if !self.matches_sources(state) {
            return Err(MidgeError::Internal(
                "stale streamed validation reached WAL admission".into(),
            ));
        }
        if self
            .insertion_expires_at
            .is_some_and(|expiration| expiration > state.observed_time_millis())
        {
            return Err(MidgeError::InvalidArgument("key already exists".into()));
        }
        match &self.failure {
            None => Ok(self.family),
            Some(ValidationFailure::Duplicate) => {
                Err(MidgeError::InvalidArgument("key already exists".into()))
            }
            Some(ValidationFailure::Read(error)) => Err(error.replay()),
            Some(failure) => Err(self.conflict_error(state, failure)),
        }
    }

    fn conflict_error(&self, state: &RuntimeState, failure: &ValidationFailure) -> MidgeError {
        let start_sequence = self.start_sequence;
        match failure {
            ValidationFailure::Point { cf_id, key } => {
                state
                    .diagnostics
                    .record(crate::telemetry::Metrics::record_write_conflict_point);
                MidgeError::WriteConflict(format!(
                    "key conflict on cf {cf_id} for key {key:?} after tx start {start_sequence}"
                ))
            }
            ValidationFailure::Range { cf_id, start, end } => {
                state
                    .diagnostics
                    .record(crate::telemetry::Metrics::record_write_conflict_range);
                MidgeError::WriteConflict(format!("range conflict on cf {cf_id} for [{start:?}, {end:?}) after tx start {start_sequence}"))
            }
            ValidationFailure::Assertion {
                cf_id,
                key,
                sequence,
            } => {
                state
                    .diagnostics
                    .record(crate::telemetry::Metrics::record_write_conflict_point);
                MidgeError::WriteConflict(format!("assertion conflict on cf {cf_id} for key {key:?}: latest seq {sequence} > tx start {start_sequence}"))
            }
            ValidationFailure::Duplicate | ValidationFailure::Read(_) => {
                unreachable!("handled before conflict classification")
            }
        }
    }
}

impl SpilledValidationInput<'_> {
    fn check_deadline(&self) -> MidgeResult<()> {
        if self.deadline.is_expired() || self.cancelled.load(Ordering::Acquire) {
            Err(MidgeError::Timeout(
                "spilled validation expired between reads".into(),
            ))
        } else {
            Ok(())
        }
    }
}

struct StreamValidation<'a, 'b> {
    input: SpilledValidationInput<'a>,
    proof: &'b mut SpilledValidationProof,
}

impl StreamValidation<'_, '_> {
    fn check_deadline(&self) -> MidgeResult<()> {
        self.input.check_deadline()
    }

    fn snapshot(&self, cf_id: u32) -> MidgeResult<&ReadSnapshot> {
        self.input
            .snapshots
            .get(&cf_id)
            .map(Arc::as_ref)
            .ok_or_else(|| {
                MidgeError::InvalidArgument(format!("column family {cf_id} does not exist"))
            })
    }

    fn point(
        &self,
        cf_id: u32,
        key: &[u8],
    ) -> MidgeResult<crate::runtime::read_snapshot::point_read::ValidationPointState> {
        self.snapshot(cf_id)?
            .validation_point_state(key, self.input.applied_sequence)
    }

    fn latest_point(&self, cf_id: u32, key: &[u8]) -> MidgeResult<Option<u64>> {
        Ok(self
            .point(cf_id, key)?
            .sequence
            .into_iter()
            .chain(
                self.input
                    .recent_ranges
                    .iter()
                    .filter(|range| {
                        range.cf_id == cf_id
                            && key >= range.start_key.as_slice()
                            && key < range.end_key.as_slice()
                    })
                    .map(|range| range.sequence),
            )
            .max())
    }

    fn stop(&mut self, failure: ValidationFailure) -> MidgeResult<()> {
        self.proof.failure = Some(failure);
        // The concrete error is formatted only on the loop at final admission.
        Err(MidgeError::Internal(
            "streamed validation stopped at static failure".into(),
        ))
    }

    fn assertions(&mut self) -> MidgeResult<()> {
        for assertion in self.input.assertions {
            self.check_deadline()?;
            if let Some(sequence) = self
                .latest_point(assertion.cf_id, &assertion.key)?
                .filter(|sequence| *sequence > self.input.start_sequence)
            {
                return self.stop(ValidationFailure::Assertion {
                    cf_id: assertion.cf_id,
                    key: assertion.key.clone(),
                    sequence,
                });
            }
        }
        Ok(())
    }

    fn operations(&mut self) -> MidgeResult<()> {
        let mut expected = 0_u64;
        self.input
            .source
            .for_each_bounded(Some(self.input.memory), |ordinal, op| {
                self.check_deadline()?;
                if ordinal != expected {
                    return Err(MidgeError::Corruption(format!(
                        "transaction spill ordinal {ordinal} is out of order; expected {expected}"
                    )));
                }
                expected = expected.checked_add(1).ok_or_else(|| {
                    MidgeError::Corruption("transaction spill ordinal overflow".into())
                })?;
                let cf_id = crate::runtime::transaction_spill::op_cf_id(&op);
                self.snapshot(cf_id)?;
                if self.input.conflict_policy == ConflictPolicy::AbortOnWriteConflict {
                    self.write_conflict(&op)?;
                }
                if let TransactionOp::Put {
                    cf_id,
                    key,
                    insert_only: true,
                    ..
                } = &op
                {
                    self.insert_only(*cf_id, key, ordinal)?;
                }
                Ok(())
            })?;
        if usize::try_from(expected).ok() != Some(self.input.source.len()) {
            return Err(MidgeError::Corruption(
                "transaction spill operation count does not match its header".into(),
            ));
        }
        Ok(())
    }

    fn write_conflict(&mut self, op: &TransactionOp) -> MidgeResult<()> {
        match op {
            TransactionOp::Put { cf_id, key, .. } | TransactionOp::Delete { cf_id, key } => {
                if self
                    .latest_point(*cf_id, key)?
                    .is_some_and(|sequence| sequence > self.input.start_sequence)
                {
                    return self.stop(ValidationFailure::Point {
                        cf_id: *cf_id,
                        key: key.clone(),
                    });
                }
            }
            TransactionOp::DeleteRange {
                cf_id,
                start_key,
                end_key,
            } => {
                let snapshot = self.snapshot(*cf_id)?;
                let conflict = snapshot
                    .any_sequence_after_in_range(start_key, end_key, self.input.start_sequence)?
                    .is_some()
                    || self.input.recent_ranges.iter().any(|range| {
                        range.cf_id == *cf_id
                            && range.sequence > self.input.start_sequence
                            && range.start_key.as_slice() < end_key.as_ref()
                            && range.end_key.as_slice() > start_key.as_ref()
                    });
                if conflict {
                    return self.stop(ValidationFailure::Range {
                        cf_id: *cf_id,
                        start: start_key.clone(),
                        end: end_key.clone(),
                    });
                }
            }
        }
        Ok(())
    }

    fn insert_only(&mut self, cf_id: u32, key: &Bytes, ordinal: u64) -> MidgeResult<()> {
        match self
            .input
            .source
            .latest_before_bounded(ordinal, key, Some(self.input.memory))?
        {
            Some(IntentLookup::Present(_)) => self.stop(ValidationFailure::Duplicate),
            Some(IntentLookup::Deleted) => Ok(()),
            None => match self.point(cf_id, key)?.value {
                ValidationValue::Present { expiration: None } => {
                    self.stop(ValidationFailure::Duplicate)
                }
                ValidationValue::Present {
                    expiration: Some(expiration),
                } => {
                    self.proof.insertion_expires_at = Some(
                        self.proof
                            .insertion_expires_at
                            .map_or(expiration, |previous| previous.max(expiration)),
                    );
                    Ok(())
                }
                ValidationValue::Absent => Ok(()),
            },
        }
    }
}
