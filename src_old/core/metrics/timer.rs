use std::time::{Duration, Instant};

/// Timer for measuring operation latency
///
/// A simple utility for timing database operations and recording
/// performance metrics.
///
/// # Example
///
/// ```rust,ignore
/// use cntryl_midge::core::metrics::Timer;
///
/// let timer = Timer::new();
/// // ... perform operation ...
/// let latency_us = timer.elapsed_micros();
/// ```
#[derive(Debug)]
pub struct Timer {
    start: Instant,
}

impl Timer {
    /// Create a new timer starting now
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed time as a Duration
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Get elapsed time in microseconds
    pub fn elapsed_micros(&self) -> u64 {
        self.elapsed().as_micros() as u64
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_track_operation_duration() {
        // Arrange
        let timer = Timer::new();

        // Act
        std::thread::sleep(Duration::from_millis(10));
        let elapsed = timer.elapsed_micros();

        // Assert
        // Should be at least 10ms = 10,000 microseconds
        assert!(elapsed >= 10_000);
    }
}
