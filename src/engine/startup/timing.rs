//! Inclusive phase timings; nested phases must not be summed into open time.

use crate::common::MidgeResult;

pub(super) fn measure<T>(
    phase: &'static str,
    action: impl FnOnce() -> MidgeResult<T>,
) -> MidgeResult<T> {
    let started = std::time::Instant::now();
    let result = action();
    tracing::info!(
        target: "midge::recovery",
        phase,
        elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        failed = result.is_err(),
        "recovery phase completed"
    );
    result
}
