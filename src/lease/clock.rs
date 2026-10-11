//! The wall clock a lease stamps its records with and judges another
//! holder's expiry by (#835).
//!
//! Production reads the system clock directly, at full precision. Tests may
//! inject a clock to take a lease over without waiting out its TTL: advancing
//! the clock a new process reads makes the old holder's record stale to it,
//! and it acquires the next epoch through the normal path. The old holder's
//! local validity is monotonic and does not follow the injected clock, but a
//! provider lease judges its own document's expiry by this clock when it
//! renews or publishes.

use crate::common::time::Clock;
use std::sync::Arc;

#[derive(Clone, Default)]
pub(crate) struct LeaseClock(Option<Arc<dyn Clock>>);

impl std::fmt::Debug for LeaseClock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(if self.0.is_some() {
            "LeaseClock(injected)"
        } else {
            "LeaseClock(system)"
        })
    }
}

impl LeaseClock {
    /// A lease clock that reads `clock`, in milliseconds.
    #[cfg(feature = "internal-testing")]
    pub(crate) fn injected(clock: Arc<dyn Clock>) -> Self {
        Self(Some(clock))
    }

    pub(crate) fn now(&self) -> chrono::DateTime<chrono::Utc> {
        match &self.0 {
            None => chrono::Utc::now(),
            Some(clock) => i64::try_from(clock.now_millis())
                .ok()
                .and_then(chrono::DateTime::from_timestamp_millis)
                .expect("an injected lease clock reads a representable time"),
        }
    }
}
