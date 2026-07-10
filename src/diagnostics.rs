//! Internal diagnostics hooks used by tests and benchmarks.

use crossbeam::queue::SegQueue;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

static TRANSACTION_COMMIT_TIMING_ENABLED: AtomicBool = AtomicBool::new(false);
static TRANSACTION_COMMIT_TIMINGS: OnceLock<SegQueue<TransactionCommitTimingSample>> =
    OnceLock::new();

/// Read-path counters captured for benchmark diagnostics.
///
/// This is an internal, doc-hidden API. Consumers should capture two snapshots
/// and subtract them to report one measured window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[doc(hidden)]
pub struct ReadPathDiagnosticsSnapshot {
    pub read_only_begin_tx_count: u64,
    pub read_only_snapshot_cache_hits: u64,
    pub read_only_snapshot_cache_misses: u64,
    pub snapshot_register_count: u64,
    pub snapshot_unregister_count: u64,
    pub sst_reader_cache_hits: u64,
    pub sst_reader_cache_misses: u64,
    pub sst_block_cache_hits: u64,
    pub sst_block_cache_misses: u64,
    pub candidate_sst_files_checked: u64,
    pub candidate_blocks_checked: u64,
    pub data_blocks_read: u64,
    pub bloom_rejects: u64,
    pub range_tombstone_scans: u64,
}

/// Read-path counters owned by one runtime.
///
/// A runtime owns its cache and snapshot state, so the counters must follow
/// that same ownership boundary. This keeps concurrent engines in one process
/// from leaking work into each other's benchmark windows.
#[derive(Debug, Default)]
pub(crate) struct RuntimeDiagnostics {
    read_only_begin_tx_count: AtomicU64,
    read_only_snapshot_cache_hits: AtomicU64,
    read_only_snapshot_cache_misses: AtomicU64,
    snapshot_register_count: AtomicU64,
    snapshot_unregister_count: AtomicU64,
    sst: crate::sst::read_path_metrics::SstReadMetrics,
}

impl RuntimeDiagnostics {
    pub(crate) fn snapshot(&self) -> ReadPathDiagnosticsSnapshot {
        ReadPathDiagnosticsSnapshot {
            read_only_begin_tx_count: self.read_only_begin_tx_count.load(Ordering::Relaxed),
            read_only_snapshot_cache_hits: self
                .read_only_snapshot_cache_hits
                .load(Ordering::Relaxed),
            read_only_snapshot_cache_misses: self
                .read_only_snapshot_cache_misses
                .load(Ordering::Relaxed),
            snapshot_register_count: self.snapshot_register_count.load(Ordering::Relaxed),
            snapshot_unregister_count: self.snapshot_unregister_count.load(Ordering::Relaxed),
            sst_reader_cache_hits: self.sst.reader_cache_hits(),
            sst_reader_cache_misses: self.sst.reader_cache_misses(),
            sst_block_cache_hits: self.sst.block_cache_hits(),
            sst_block_cache_misses: self.sst.block_cache_misses(),
            candidate_sst_files_checked: self.sst.candidate_sst_files_checked(),
            candidate_blocks_checked: self.sst.candidate_blocks_checked(),
            data_blocks_read: self.sst.data_blocks_read(),
            bloom_rejects: self.sst.bloom_rejects(),
            range_tombstone_scans: self.sst.range_tombstone_scans(),
        }
    }

    pub(crate) fn sst_metrics(&self) -> &crate::sst::read_path_metrics::SstReadMetrics {
        &self.sst
    }

    pub(crate) fn record_read_only_begin_tx(&self) {
        self.read_only_begin_tx_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_read_only_snapshot_cache_hit(&self) {
        self.read_only_snapshot_cache_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_read_only_snapshot_cache_miss(&self) {
        self.read_only_snapshot_cache_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_snapshot_register(&self) {
        self.snapshot_register_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_snapshot_unregister(&self) {
        self.snapshot_unregister_count
            .fetch_add(1, Ordering::Relaxed);
    }
}

static LEGACY_RUNTIME_DIAGNOSTICS: OnceLock<Arc<RuntimeDiagnostics>> = OnceLock::new();

/// Diagnostics for direct standalone readers and the legacy global snapshot
/// helper. Engines always receive an independent `RuntimeDiagnostics` value.
pub(crate) fn legacy_runtime_diagnostics() -> Arc<RuntimeDiagnostics> {
    Arc::clone(LEGACY_RUNTIME_DIAGNOSTICS.get_or_init(|| Arc::new(RuntimeDiagnostics::default())))
}

/// Capture legacy process-wide read-path counters for benchmark diagnostics.
///
/// New benchmark code should call `Engine::read_path_diagnostics_snapshot_for_benchmarks`
/// so a concurrent engine cannot contaminate its measurement window. This
/// function is retained for existing doc-hidden consumers and standalone SST
/// readers that have no runtime owner.
#[must_use]
#[doc(hidden)]
pub fn read_path_diagnostics_snapshot_for_benchmarks() -> ReadPathDiagnosticsSnapshot {
    legacy_runtime_diagnostics().snapshot()
}

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

/// Enable internal transaction commit timing collection for benchmark crates.
///
/// This is not part of the stable user API; benchmarks use it to keep latency
/// breakdown rows tied to real commit phase timings instead of synthetic totals.
#[doc(hidden)]
pub fn enable_transaction_commit_timing_for_benchmarks() {
    let _ = drain_transaction_commit_timings_for_benchmarks();
    clear_current_transaction_submit_timing();
    TRANSACTION_COMMIT_TIMING_ENABLED.store(true, Ordering::Release);
}

/// Disable internal transaction commit timing collection for benchmark crates.
#[doc(hidden)]
pub fn disable_transaction_commit_timing_for_benchmarks() {
    TRANSACTION_COMMIT_TIMING_ENABLED.store(false, Ordering::Release);
    clear_current_transaction_submit_timing();
}

/// Drain collected transaction commit timing samples for benchmark crates.
#[must_use]
#[doc(hidden)]
pub fn drain_transaction_commit_timings_for_benchmarks() -> Vec<TransactionCommitTimingSample> {
    let queue = transaction_commit_timing_queue();
    let mut samples = Vec::new();
    while let Some(sample) = queue.pop() {
        samples.push(sample);
    }
    samples
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
