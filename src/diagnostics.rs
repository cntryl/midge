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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReadTransactionDiagnosticsSnapshot {
    pub(crate) read_only_begin_tx_count: u64,
    pub(crate) read_only_snapshot_cache_hits: u64,
    pub(crate) read_only_snapshot_cache_misses: u64,
    pub(crate) snapshot_register_count: u64,
    pub(crate) snapshot_unregister_count: u64,
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

#[must_use]
pub(crate) fn read_transaction_diagnostics_snapshot() -> ReadTransactionDiagnosticsSnapshot {
    ReadTransactionDiagnosticsSnapshot {
        read_only_begin_tx_count: READ_ONLY_BEGIN_TX_COUNT.load(Ordering::Relaxed),
        read_only_snapshot_cache_hits: READ_ONLY_SNAPSHOT_CACHE_HITS.load(Ordering::Relaxed),
        read_only_snapshot_cache_misses: READ_ONLY_SNAPSHOT_CACHE_MISSES.load(Ordering::Relaxed),
        snapshot_register_count: SNAPSHOT_REGISTER_COUNT.load(Ordering::Relaxed),
        snapshot_unregister_count: SNAPSHOT_UNREGISTER_COUNT.load(Ordering::Relaxed),
    }
}

impl ReadTransactionDiagnosticsSnapshot {
    pub(crate) fn delta_since(self, start: Self) -> Self {
        Self {
            read_only_begin_tx_count: self
                .read_only_begin_tx_count
                .saturating_sub(start.read_only_begin_tx_count),
            read_only_snapshot_cache_hits: self
                .read_only_snapshot_cache_hits
                .saturating_sub(start.read_only_snapshot_cache_hits),
            read_only_snapshot_cache_misses: self
                .read_only_snapshot_cache_misses
                .saturating_sub(start.read_only_snapshot_cache_misses),
            snapshot_register_count: self
                .snapshot_register_count
                .saturating_sub(start.snapshot_register_count),
            snapshot_unregister_count: self
                .snapshot_unregister_count
                .saturating_sub(start.snapshot_unregister_count),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_report_saturating_read_transaction_diagnostic_deltas() {
        // Arrange
        let start = ReadTransactionDiagnosticsSnapshot {
            read_only_begin_tx_count: 10,
            read_only_snapshot_cache_hits: 8,
            read_only_snapshot_cache_misses: 2,
            snapshot_register_count: 10,
            snapshot_unregister_count: 9,
        };
        let end = ReadTransactionDiagnosticsSnapshot {
            read_only_begin_tx_count: 14,
            read_only_snapshot_cache_hits: 9,
            read_only_snapshot_cache_misses: 1,
            snapshot_register_count: 15,
            snapshot_unregister_count: 12,
        };

        // Act
        let delta = end.delta_since(start);

        // Assert
        assert_eq!(delta.read_only_begin_tx_count, 4);
        assert_eq!(delta.read_only_snapshot_cache_hits, 1);
        assert_eq!(delta.read_only_snapshot_cache_misses, 0);
        assert_eq!(delta.snapshot_register_count, 5);
        assert_eq!(delta.snapshot_unregister_count, 3);
    }
}
