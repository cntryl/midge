//! Internal diagnostics hooks used by tests and benchmarks.

use crossbeam::queue::SegQueue;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

static TRANSACTION_COMMIT_TIMING_ENABLED: AtomicBool = AtomicBool::new(false);
static TRANSACTION_COMMIT_TIMINGS: OnceLock<SegQueue<TransactionCommitTimingSample>> =
    OnceLock::new();
static READ_ONLY_BEGIN_TX_COUNT: AtomicU64 = AtomicU64::new(0);
static READ_ONLY_SNAPSHOT_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static READ_ONLY_SNAPSHOT_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_REGISTER_COUNT: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_UNREGISTER_COUNT: AtomicU64 = AtomicU64::new(0);

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

pub(crate) fn record_read_only_begin_tx() {
    READ_ONLY_BEGIN_TX_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_read_only_snapshot_cache_hit() {
    READ_ONLY_SNAPSHOT_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_read_only_snapshot_cache_miss() {
    READ_ONLY_SNAPSHOT_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_snapshot_register() {
    SNAPSHOT_REGISTER_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_snapshot_unregister() {
    SNAPSHOT_UNREGISTER_COUNT.fetch_add(1, Ordering::Relaxed);
}

fn transaction_commit_timing_queue() -> &'static SegQueue<TransactionCommitTimingSample> {
    TRANSACTION_COMMIT_TIMINGS.get_or_init(SegQueue::new)
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
