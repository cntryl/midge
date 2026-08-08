//! Direct transaction submission and write-stall backpressure.
//!
//! Runtime-side write draining owns transaction coalescing. Keeping each
//! caller submission distinct here preserves its snapshot sequence,
//! assertions, error, and timing attribution.

use crate::common::{MidgeError, MidgeResult};
use crate::runtime::{next_request_id, RuntimeHandle, RuntimeResponse, TransactionSubmission};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

fn submit_timing_phase_start(enabled: bool) -> Option<Instant> {
    enabled.then(Instant::now)
}

fn record_submit_runtime_apply(started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        crate::diagnostics::record_transaction_submit_runtime_apply(started_at.elapsed());
    }
}

/// Per-column-family submission coordinator.
pub(crate) struct IngestCoordinator {
    cf_id: crate::engine::ColumnFamilyId,
    stall_flag: AtomicBool,
}

impl IngestCoordinator {
    pub fn new(cf_id: crate::engine::ColumnFamilyId) -> Self {
        Self {
            cf_id,
            stall_flag: AtomicBool::new(false),
        }
    }

    pub fn submit_ops(
        &self,
        runtime: &RuntimeHandle,
        submission: TransactionSubmission,
        collect_submit_timing: bool,
    ) -> MidgeResult<u64> {
        if submission.ops.is_empty() && submission.assertions.is_empty() {
            return Ok(0);
        }
        self.check_write_stall(runtime)?;
        let expected_op_count = submission.ops.len();
        self.send_apply_transaction(
            runtime,
            submission,
            collect_submit_timing,
            Some(expected_op_count),
        )
    }

    pub(crate) fn submit_spilled_ops(
        &self,
        runtime: &RuntimeHandle,
        submission: crate::runtime::SpilledTransactionSubmission,
        collect_submit_timing: bool,
    ) -> MidgeResult<u64> {
        if submission.source.is_empty() && submission.assertions.is_empty() {
            return Ok(0);
        }
        self.check_write_stall(runtime)?;
        let expected_op_count = submission.source.len();
        let request_id = next_request_id()?;
        let started_at = submit_timing_phase_start(collect_submit_timing);
        let response = runtime.send_spilled_transaction_and_wait(request_id, submission);
        record_submit_runtime_apply(started_at);
        Self::decode_apply_transaction_response(
            response?,
            Some(expected_op_count),
            &self.stall_flag,
        )
    }

    fn check_write_stall(&self, runtime: &RuntimeHandle) -> MidgeResult<()> {
        if self.stall_flag.load(Ordering::Acquire) {
            if runtime.check_write_stall(self.cf_id)? {
                return Err(MidgeError::WriteStall(format!(
                    "Memory budget exceeded for CF {}",
                    self.cf_id
                )));
            }
            self.stall_flag.store(false, Ordering::Release);
        }
        Ok(())
    }

    fn send_apply_transaction(
        &self,
        runtime: &RuntimeHandle,
        submission: TransactionSubmission,
        collect_submit_timing: bool,
        expected_op_count: Option<usize>,
    ) -> MidgeResult<u64> {
        let request_id = next_request_id()?;
        let started_at = submit_timing_phase_start(collect_submit_timing);
        let response = runtime.send_apply_transaction_and_wait(request_id, submission);
        record_submit_runtime_apply(started_at);
        Self::decode_apply_transaction_response(response?, expected_op_count, &self.stall_flag)
    }

    fn decode_apply_transaction_response(
        response: RuntimeResponse,
        expected_op_count: Option<usize>,
        stall_flag: &AtomicBool,
    ) -> MidgeResult<u64> {
        match response {
            RuntimeResponse::TransactionApplied {
                last_sequence,
                op_count,
                write_stall_hint,
                ..
            } => {
                stall_flag.store(write_stall_hint, Ordering::Release);
                if let Some(expected) = expected_op_count {
                    if op_count != expected {
                        return Err(MidgeError::Internal(format!(
                            "Batch op count mismatch: expected {expected}, got {op_count}"
                        )));
                    }
                }
                Ok(last_sequence)
            }
            RuntimeResponse::Error { error, .. } => Err(error),
            _ => Err(MidgeError::Internal(
                "Unexpected response to ApplyTransaction".to_string(),
            )),
        }
    }
}
