//! Internal diagnostics hooks used by tests and benchmarks.

use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;

static TRANSACTION_COMMIT_TIMING_ENABLED: AtomicBool = AtomicBool::new(false);
static TRANSACTION_COMMIT_TIMINGS: OnceLock<SegQueue<TransactionCommitTimingSample>> =
    OnceLock::new();

/// One internal timing sample for `Transaction::commit`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransactionCommitTimingSample {
    pub commit_total_ns: u64,
    pub submit_apply_transaction_ns: u64,
    pub durability_finalize_ns: u64,
    pub unregister_snapshot_ns: u64,
    pub succeeded: bool,
}

/// Guard that enables collection of internal transaction commit timing samples.
#[derive(Debug)]
pub struct TransactionCommitTimingGuard {
    active: bool,
}

impl TransactionCommitTimingGuard {
    /// Enable transaction commit timing collection until the guard is dropped.
    #[must_use]
    pub fn start() -> Self {
        clear_transaction_commit_timings();
        TRANSACTION_COMMIT_TIMING_ENABLED.store(true, Ordering::Release);
        Self { active: true }
    }

    /// Drain all collected commit timing samples.
    #[must_use]
    pub fn drain(&self) -> Vec<TransactionCommitTimingSample> {
        if !self.active {
            return Vec::new();
        }

        drain_transaction_commit_timings()
    }
}

impl Drop for TransactionCommitTimingGuard {
    fn drop(&mut self) {
        if self.active {
            TRANSACTION_COMMIT_TIMING_ENABLED.store(false, Ordering::Release);
            self.active = false;
        }
    }
}

#[must_use]
pub(crate) fn transaction_commit_timing_enabled() -> bool {
    TRANSACTION_COMMIT_TIMING_ENABLED.load(Ordering::Acquire)
}

pub(crate) fn record_transaction_commit_timing(sample: TransactionCommitTimingSample) {
    transaction_commit_timing_queue().push(sample);
}

fn transaction_commit_timing_queue() -> &'static SegQueue<TransactionCommitTimingSample> {
    TRANSACTION_COMMIT_TIMINGS.get_or_init(SegQueue::new)
}

fn clear_transaction_commit_timings() {
    let _ = drain_transaction_commit_timings();
}

fn drain_transaction_commit_timings() -> Vec<TransactionCommitTimingSample> {
    let queue = transaction_commit_timing_queue();
    let mut samples = Vec::new();

    while let Some(sample) = queue.pop() {
        samples.push(sample);
    }

    samples
}
