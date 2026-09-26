//! Monotonic retry deadlines shared by runtime owners.

use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) trait RetryClock: Send + Sync {
    fn now(&self) -> Instant;
}

struct SystemRetryClock;

impl RetryClock for SystemRetryClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Clone)]
pub(crate) struct RetrySchedule {
    at: Option<Instant>,
    delay: Duration,
    clock: Arc<dyn RetryClock>,
}

impl RetrySchedule {
    pub(crate) fn new(delay: Duration) -> Self {
        Self::with_clock(delay, Arc::new(SystemRetryClock))
    }

    pub(crate) fn with_clock(delay: Duration, clock: Arc<dyn RetryClock>) -> Self {
        Self {
            at: None,
            delay,
            clock,
        }
    }

    pub(crate) fn defer(&mut self) {
        self.at = Some(self.clock.now() + self.delay);
    }

    pub(crate) fn defer_for(&mut self, delay: Duration) {
        self.delay = delay;
        self.defer();
    }

    pub(crate) fn defer_from(&mut self, now: Instant, delay: Duration) {
        self.delay = delay;
        self.at = Some(now + delay);
    }

    #[cfg(test)]
    pub(crate) fn mark_due(&mut self) {
        self.at = Some(self.clock.now());
    }

    pub(crate) fn clear(&mut self) {
        self.at = None;
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.is_ready_at(self.clock.now())
    }

    pub(crate) fn is_ready_at(&self, now: Instant) -> bool {
        self.at.is_none_or(|at| now >= at)
    }

    pub(crate) fn is_scheduled(&self) -> bool {
        self.at.is_some()
    }

    pub(crate) fn remaining(&self) -> Option<Duration> {
        self.at
            .map(|at| at.saturating_duration_since(self.clock.now()))
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryClock, RetrySchedule};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    struct FakeClock(Mutex<Instant>);

    impl RetryClock for FakeClock {
        fn now(&self) -> Instant {
            *self.0.lock().expect("clock lock")
        }
    }

    #[test]
    fn should_wake_when_retry_deadline_arrives() {
        // Arrange
        let start = Instant::now();
        let clock = Arc::new(FakeClock(Mutex::new(start)));
        let mut schedule = RetrySchedule::with_clock(Duration::from_secs(1), clock.clone());

        // Act
        schedule.defer();
        assert!(!schedule.is_ready());
        assert_eq!(schedule.remaining(), Some(Duration::from_secs(1)));
        *clock.0.lock().expect("clock lock") = start + Duration::from_secs(1);

        // Assert
        assert!(schedule.is_ready());
        assert_eq!(schedule.remaining(), Some(Duration::ZERO));
    }

    #[test]
    fn should_clear_retry_deadline_after_attempt() {
        // Arrange
        let clock = Arc::new(FakeClock(Mutex::new(Instant::now())));
        let mut schedule = RetrySchedule::with_clock(Duration::from_secs(1), clock);
        schedule.defer();

        // Act
        schedule.clear();

        // Assert
        assert!(schedule.is_ready());
        assert_eq!(schedule.remaining(), None);
    }
}
