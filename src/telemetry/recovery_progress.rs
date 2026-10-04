//! Completed startup work, independent of benchmark or runtime policy.

use std::time::{Duration, Instant};

const BYTE_QUANTUM: u64 = 64 * 1024;
const FRAME_QUANTUM: u64 = 64;
const PARTIAL_REPORT_INTERVAL: Duration = Duration::from_millis(500);

/// Batch fast cached work; a slow successful completion can report a partial
/// batch. This has no timer, destructor callback, or shared mutable state.
pub(crate) struct WorkProgress {
    stage: &'static str,
    completed_bytes: u64,
    completed_frames: u64,
    completed_operations: u64,
    reported_at: Instant,
}

impl WorkProgress {
    pub(crate) fn new(stage: &'static str) -> Self {
        Self {
            stage,
            completed_bytes: 0,
            completed_frames: 0,
            completed_operations: 0,
            reported_at: Instant::now(),
        }
    }

    pub(crate) fn completed(&mut self, bytes: u64, frames: u64) {
        self.record(bytes, frames, 0);
    }

    pub(crate) fn completed_operation(&mut self) {
        self.record(0, 0, 1);
    }

    fn record(&mut self, bytes: u64, frames: u64, operations: u64) {
        if bytes == 0 && frames == 0 && operations == 0 {
            return;
        }
        if !tracing::enabled!(target: "midge::recovery::work", tracing::Level::DEBUG) {
            return;
        }
        self.completed_bytes = self.completed_bytes.saturating_add(bytes);
        self.completed_frames = self.completed_frames.saturating_add(frames);
        self.completed_operations = self.completed_operations.saturating_add(operations);
        if self.completed_bytes >= BYTE_QUANTUM
            || self.completed_frames >= FRAME_QUANTUM
            || self.completed_operations >= FRAME_QUANTUM
            || self.reported_at.elapsed() >= PARTIAL_REPORT_INTERVAL
        {
            self.finish();
        }
    }

    /// Call only after a successful bounded walk, never during error cleanup.
    pub(crate) fn finish(&mut self) {
        if self.completed_bytes > 0 || self.completed_frames > 0 || self.completed_operations > 0 {
            emit(
                self.stage,
                self.completed_bytes,
                self.completed_frames,
                self.completed_operations,
            );
            self.completed_bytes = 0;
            self.completed_frames = 0;
            self.completed_operations = 0;
            self.reported_at = Instant::now();
        }
    }
}

fn emit(
    stage: &'static str,
    completed_bytes: u64,
    completed_frames: u64,
    completed_operations: u64,
) {
    tracing::debug!(
        target: "midge::recovery::work",
        recovery_work_completed = true,
        stage,
        completed_bytes,
        completed_frames,
        completed_operations,
        failed = false,
        "recovery work completed"
    );
}

/// A transport callback is already one bounded work unit. Report even a small
/// successful range; requests, failures, and empty responses are not progress.
pub(crate) struct RecoveryReadObserver;

impl crate::io::traits::ReadObserver for RecoveryReadObserver {
    fn remote_range_started(&self) {}

    fn remote_range_completed(&self, returned_bytes: u64, _elapsed: Duration, failed: bool) {
        if !failed && returned_bytes > 0 {
            emit("remote_read", returned_bytes, 0, 0);
        }
    }
}

pub(crate) fn observe_reads(
    fs: std::sync::Arc<dyn crate::io::Fs>,
) -> std::sync::Arc<dyn crate::io::Fs> {
    fs.with_read_observer(std::sync::Arc::new(RecoveryReadObserver))
        .unwrap_or(fs)
}
