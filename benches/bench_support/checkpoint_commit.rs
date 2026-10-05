//! Fixed checkpoint commit policy; counters do not advance benchmark progress.

use cntryl_midge::{MidgeError, MidgeResult};
use serde::Serialize;
use std::time::{Duration, Instant};

const MAX_STALL_BUDGET: Duration = Duration::from_secs(30);
const WAIT_SLICE: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Default, Serialize)]
pub struct Observations {
    pub attempts: u64,
    pub successful_commits: u64,
    pub write_stalls: u64,
    pub wait_calls: u64,
    pub wait_timeouts: u64,
    pub wait_write_stalls: u64,
    pub wait_elapsed_ns: u64,
    pub last_stall: Option<String>,
}

impl Observations {
    fn record_stall(&mut self, detail: String) {
        self.write_stalls += 1;
        self.last_stall = Some(detail);
    }

    fn exhausted(&self) -> MidgeError {
        MidgeError::Timeout(format!(
            "checkpoint commit backpressure budget expired; last stall: {}",
            self.last_stall.as_deref().unwrap_or("none")
        ))
    }
}

/// Retry only rejected writes, rebuilding the exact logical transaction.
///
/// Capture one allowance before the first attempt. A wait may admit another
/// attempt but never constitutes a commit. Accepted public commit I/O retains
/// its existing caller timeout and ownership; this loop does not preempt it.
///
/// # Errors
/// Returns original non-stall commit/wait errors, or `Timeout` when the captured
/// allowance is exhausted before another commit or wait can be admitted.
pub fn commit_with_backpressure(
    cell_deadline: Instant,
    stall_budget: Duration,
    observations: &mut Observations,
    mut now: impl FnMut() -> Instant,
    mut commit: impl FnMut() -> MidgeResult<()>,
    mut wait: impl FnMut(Duration) -> MidgeResult<bool>,
) -> MidgeResult<()> {
    let started = now();
    let deadline = started
        .checked_add(stall_budget.min(MAX_STALL_BUDGET))
        .unwrap_or(cell_deadline)
        .min(cell_deadline);
    loop {
        if now() >= deadline {
            return Err(observations.exhausted());
        }
        observations.attempts += 1;
        match commit() {
            Ok(()) => {
                observations.successful_commits += 1;
                return Ok(());
            }
            Err(MidgeError::WriteStall(detail)) => observations.record_stall(detail),
            Err(error) => return Err(error),
        }
        loop {
            let waiting_at = now();
            let remaining = deadline.saturating_duration_since(waiting_at);
            if remaining.is_zero() {
                return Err(observations.exhausted());
            }
            observations.wait_calls += 1;
            let result = wait(remaining.min(WAIT_SLICE));
            let elapsed = now().saturating_duration_since(waiting_at).as_nanos();
            observations.wait_elapsed_ns = observations
                .wait_elapsed_ns
                .saturating_add(u64::try_from(elapsed).unwrap_or(u64::MAX));
            match result {
                Ok(true) => break,
                Ok(false) => observations.wait_timeouts += 1,
                Err(MidgeError::WriteStall(detail)) => {
                    observations.wait_write_stalls += 1;
                    observations.last_stall = Some(detail);
                }
                Err(error) => return Err(error),
            }
        }
    }
}
