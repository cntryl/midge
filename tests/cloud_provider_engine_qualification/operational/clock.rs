//! One deterministic TTL clock per qualification child; lease clocks stay real.

use cntryl_midge::common::time::Clock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

const START: u64 = 4_000_000_000_000;
static CLOCK: LazyLock<Arc<QualificationClock>> = LazyLock::new(|| {
    let expired = std::env::var("MIDGE_OPERATIONAL_CHILD_PHASE")
        .is_ok_and(|phase| matches!(phase.as_str(), "verified" | "restored" | "disk-exhausted"));
    Arc::new(QualificationClock(AtomicU64::new(
        START + if expired { 10_000 } else { 0 },
    )))
});

struct QualificationClock(AtomicU64);
impl Clock for QualificationClock {
    fn now_millis(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

pub(super) fn source() -> Arc<dyn Clock> {
    CLOCK.clone()
}

pub(super) fn expire_values() {
    CLOCK.0.store(START + 10_000, Ordering::Release);
}
