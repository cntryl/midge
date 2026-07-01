//! Internal diagnostics hooks used by tests and benchmarks.

use crossbeam::queue::SegQueue;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

static TRANSACTION_COMMIT_TIMING_ENABLED: AtomicBool = AtomicBool::new(false);
static TRANSACTION_COMMIT_TIMINGS: OnceLock<SegQueue<TransactionCommitTimingSample>> =
    OnceLock::new();

/// One internal timing sample for `Transaction::commit`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransactionCommitTimingSample {
    pub commit_total_ns: u64,
    pub submit_apply_transaction_ns: u64,
    pub write_group_leader_collect_ns: u64,
    pub write_group_runtime_apply_ns: u64,
    pub write_group_follower_wait_ns: u64,
    pub durability_finalize_ns: u64,
    pub unregister_snapshot_ns: u64,
    pub succeeded: bool,
}

/// Internal per-thread submit timing accumulated while one transaction commits.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TransactionSubmitTimingSample {
    pub(crate) leader_collect: u64,
    pub(crate) runtime_apply: u64,
    pub(crate) follower_wait: u64,
}

impl TransactionSubmitTimingSample {
    const ZERO: Self = Self {
        leader_collect: 0,
        runtime_apply: 0,
        follower_wait: 0,
    };
}

thread_local! {
    static CURRENT_TRANSACTION_SUBMIT_TIMING: Cell<TransactionSubmitTimingSample> =
        const { Cell::new(TransactionSubmitTimingSample::ZERO) };
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

pub(crate) fn clear_current_transaction_submit_timing() {
    if transaction_commit_timing_enabled() {
        CURRENT_TRANSACTION_SUBMIT_TIMING
            .with(|timing| timing.set(TransactionSubmitTimingSample::ZERO));
    }
}

#[must_use]
pub(crate) fn take_current_transaction_submit_timing() -> TransactionSubmitTimingSample {
    if !transaction_commit_timing_enabled() {
        return TransactionSubmitTimingSample::ZERO;
    }

    CURRENT_TRANSACTION_SUBMIT_TIMING.with(|timing| {
        let sample = timing.get();
        timing.set(TransactionSubmitTimingSample::ZERO);
        sample
    })
}

pub(crate) fn record_transaction_submit_leader_collect(duration: Duration) {
    record_current_transaction_submit_timing(|sample| {
        sample.leader_collect = sample
            .leader_collect
            .saturating_add(duration_as_nanos(duration));
    });
}

pub(crate) fn record_transaction_submit_runtime_apply(duration: Duration) {
    record_current_transaction_submit_timing(|sample| {
        sample.runtime_apply = sample
            .runtime_apply
            .saturating_add(duration_as_nanos(duration));
    });
}

pub(crate) fn record_transaction_submit_follower_wait(duration: Duration) {
    record_current_transaction_submit_timing(|sample| {
        sample.follower_wait = sample
            .follower_wait
            .saturating_add(duration_as_nanos(duration));
    });
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

fn record_current_transaction_submit_timing(
    update: impl FnOnce(&mut TransactionSubmitTimingSample),
) {
    if !transaction_commit_timing_enabled() {
        return;
    }

    CURRENT_TRANSACTION_SUBMIT_TIMING.with(|timing| {
        let mut sample = timing.get();
        update(&mut sample);
        timing.set(sample);
    });
}

fn duration_as_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
