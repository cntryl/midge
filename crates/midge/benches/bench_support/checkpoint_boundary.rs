//! Sampling boundary only; metadata quiescence is not global runtime idleness.

use cntryl_midge::__internal::checkpoint::{Medium, Snapshot};
use cntryl_midge::{MidgeError, MidgeResult};
use serde::Serialize;
use std::time::{Duration, Instant};

const MAX_BOUNDARY_BUDGET: Duration = Duration::from_secs(30);
const PAUSE_SLICE: Duration = Duration::from_millis(1);

#[derive(Clone, Debug, Default, Serialize)]
pub struct Observation {
    pub samples: u64,
    pub active_samples: u64,
    pub pause_calls: u64,
    pub pause_requested_ns: u64,
    pub max_pause_requested_ns: u64,
    pub paused_ns: u64,
    pub elapsed_ns: u64,
    pub final_persistent_active: u64,
    pub complete: bool,
}

pub struct Boundary<T> {
    pub runtime: T,
    pub snapshot: Snapshot,
    /// Final sample query begins the measured clock at the warmup boundary.
    pub query_started: Instant,
}

fn persistent_active(snapshot: &Snapshot) -> u64 {
    snapshot
        .buckets
        .iter()
        .filter(|bucket| bucket.medium == Medium::Persistent)
        .fold(0_u64, |total, bucket| {
            total.saturating_add(bucket.active_operations)
        })
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn expired() -> MidgeError {
    MidgeError::Timeout("metadata boundary allowance expired before an idle capture".into())
}

/// Capture one idle Persistent metadata sample within one original allowance.
/// The callback queries runtime metrics first and reads that owner's metadata
/// snapshot second. Earlier active candidates do not refresh either deadline.
/// Sampling and pauses never advance benchmark progress. Accepted runtime
/// work keeps its existing ownership; this is no global runtime barrier.
///
/// # Errors
/// Returns the original query error, or `Timeout` before admitting new work or
/// accepting an idle candidate at or after the captured deadline.
pub fn capture_metadata_boundary<T>(
    cell_deadline: Instant,
    boundary_budget: Duration,
    observation: &mut Observation,
    mut now: impl FnMut() -> Instant,
    mut query_and_snapshot: impl FnMut(Duration) -> MidgeResult<(T, Snapshot)>,
    mut pause: impl FnMut(Duration),
) -> MidgeResult<Boundary<T>> {
    let started = now();
    let deadline = cell_deadline.min(started + boundary_budget.min(MAX_BOUNDARY_BUDGET));
    loop {
        let query_started = now();
        let remaining = deadline.saturating_duration_since(query_started);
        observation.elapsed_ns = nanos(query_started.saturating_duration_since(started));
        if remaining.is_zero() {
            return Err(expired());
        }
        observation.samples += 1;
        let candidate = query_and_snapshot(remaining);
        let captured_at = now();
        observation.elapsed_ns = nanos(captured_at.saturating_duration_since(started));
        let (runtime, snapshot) = candidate?;
        observation.final_persistent_active = persistent_active(&snapshot);
        if observation.final_persistent_active > 0 {
            observation.active_samples += 1;
        }
        if captured_at >= deadline {
            return Err(expired());
        }
        if observation.final_persistent_active == 0 {
            observation.complete = true;
            return Ok(Boundary {
                runtime,
                snapshot,
                query_started,
            });
        }
        let pause_started = now();
        let remaining = deadline.saturating_duration_since(pause_started);
        if remaining.is_zero() {
            return Err(expired());
        }
        let allowance = remaining.min(PAUSE_SLICE);
        observation.pause_calls += 1;
        observation.pause_requested_ns += nanos(allowance);
        observation.max_pause_requested_ns =
            observation.max_pause_requested_ns.max(nanos(allowance));
        pause(allowance);
        observation.paused_ns += nanos(now().saturating_duration_since(pause_started));
    }
}
