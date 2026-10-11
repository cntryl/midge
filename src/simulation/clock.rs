//! A lease wall clock a test moves forward (#835).
//!
//! It reads the system clock plus an offset the test advances, so a process
//! given this clock sees another holder's lease as expired as soon as the
//! test says so, while time still passes normally. Forks start at the same
//! reading and then move independently, so two processes can disagree about
//! the time as skewed hosts would.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct SimulatedClock {
    offset_millis: Arc<AtomicU64>,
}

impl SimulatedClock {
    /// A clock that reads the system clock until advanced.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Move this clock, and every clone of it, forward by `by`.
    pub fn advance(&self, by: Duration) {
        let by = u64::try_from(by.as_millis()).unwrap_or(u64::MAX);
        self.offset_millis.fetch_add(by, Ordering::SeqCst);
    }

    /// Move the clock forward until it reads at least `millis`; a reading
    /// saved earlier stays in the past.
    pub fn advance_to(&self, millis: u64) {
        let now = crate::Clock::now_millis(self);
        if millis > now {
            self.advance(Duration::from_millis(millis - now));
        }
    }

    /// An independent clock reading the same time now.
    #[must_use]
    pub fn fork(&self) -> Self {
        Self {
            offset_millis: Arc::new(AtomicU64::new(self.offset_millis.load(Ordering::SeqCst))),
        }
    }
}

impl crate::Clock for SimulatedClock {
    fn now_millis(&self) -> u64 {
        crate::common::time::unix_time_millis()
            .saturating_add(self.offset_millis.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Clock;

    #[test]
    fn should_move_clones_together_when_advanced() {
        // Arrange
        let clock = SimulatedClock::new();
        let clone = clock.clone();
        let before = clone.now_millis();

        // Act
        clock.advance(Duration::from_secs(60));

        // Assert
        assert!(clone.now_millis() >= before + 60_000);
    }

    #[test]
    fn should_keep_forks_independent_when_one_advances() {
        // Arrange
        let clock = SimulatedClock::new();
        let fork = clock.fork();

        // Act
        fork.advance(Duration::from_secs(60));

        // Assert
        assert!(fork.now_millis() >= clock.now_millis() + 59_000);
    }

    #[test]
    fn should_not_move_back_when_advanced_to_an_earlier_reading() {
        // Arrange
        let clock = SimulatedClock::new();
        clock.advance(Duration::from_secs(60));
        let ahead = clock.now_millis();

        // Act
        clock.advance_to(ahead - 30_000);

        // Assert
        assert!(clock.now_millis() >= ahead);
    }
}
