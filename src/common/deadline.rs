//! Shared operation deadline.
//!
//! One runtime request can drive several sequential storage operations, each
//! independently bounded by `storage_io_timeout`. Without a deadline that spans
//! the whole request, those budgets compose to far more than the caller's
//! `runtime_response_timeout`, and the caller's route is torn down while the
//! work is still running. An `OperationDeadline` is the shared budget those
//! operations subtract from.
//!
//! The deadline is a value, not a request handle. Work that has a caller builds
//! one with [`OperationDeadline::from_start`] so the budget starts when the
//! caller began waiting rather than when the event loop reached the message;
//! callerless maintenance work builds one with
//! [`OperationDeadline::from_budget`].

use std::time::{Duration, Instant};

/// A shared budget for a sequence of storage operations.
#[derive(Clone, Copy, Debug)]
pub struct OperationDeadline {
    expires_at: Option<Instant>,
}

impl OperationDeadline {
    /// A deadline that never expires, for call sites that have no budget to
    /// enforce yet.
    ///
    /// This is the compatibility shim for paths not yet threaded; it is not a
    /// substitute for a real budget.
    #[must_use]
    pub fn unbounded() -> Self {
        Self { expires_at: None }
    }

    /// A deadline `budget` from now, for work with no waiting caller.
    #[must_use]
    pub fn from_budget(budget: Duration) -> Self {
        Self {
            expires_at: Instant::now().checked_add(budget),
        }
    }

    /// A deadline `budget` from `start`, for work on behalf of a caller that
    /// began waiting at `start`.
    ///
    /// Time already spent queued is charged against the budget, so a request
    /// that waited in the runtime queue does not get a fresh allowance.
    #[must_use]
    pub fn from_start(start: Instant, budget: Duration) -> Self {
        Self {
            expires_at: start.checked_add(budget),
        }
    }

    /// Budget left before this deadline expires. Zero once expired.
    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.expires_at.map_or(Duration::MAX, |expires_at| {
            expires_at.saturating_duration_since(Instant::now())
        })
    }

    /// Whether the budget is exhausted.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.remaining().is_zero()
    }

    /// Whether this deadline has a finite expiration instant.
    #[must_use]
    pub fn is_bounded(&self) -> bool {
        self.expires_at.is_some()
    }

    /// Clamp a per-operation timeout to what the shared budget still allows.
    #[must_use]
    pub fn clamp(&self, per_operation_timeout: Duration) -> Duration {
        per_operation_timeout.min(self.remaining())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_report_full_budget_given_fresh_deadline_when_no_time_has_passed() {
        // Arrange
        let budget = Duration::from_secs(30);

        // Act
        let deadline = OperationDeadline::from_budget(budget);

        // Assert
        assert!(!deadline.is_expired());
        assert!(deadline.remaining() <= budget);
        assert!(deadline.remaining() > budget / 2);
    }

    #[test]
    fn should_charge_queue_time_against_budget_given_earlier_start_when_derived_from_caller() {
        // Arrange: a caller that has already been waiting for most of its budget.
        let budget = Duration::from_secs(10);
        let start = Instant::now()
            .checked_sub(Duration::from_secs(9))
            .expect("test instant supports nine-second subtraction");

        // Act
        let deadline = OperationDeadline::from_start(start, budget);

        // Assert
        assert!(!deadline.is_expired());
        assert!(
            deadline.remaining() <= Duration::from_secs(1),
            "time already queued must not be refunded"
        );
    }

    #[test]
    fn should_report_expired_given_start_older_than_budget_when_caller_already_timed_out() {
        // Arrange
        let start = Instant::now()
            .checked_sub(Duration::from_secs(120))
            .expect("test instant supports two-minute subtraction");

        // Act
        let deadline = OperationDeadline::from_start(start, Duration::from_secs(60));

        // Assert
        assert!(deadline.is_expired());
        assert_eq!(deadline.remaining(), Duration::ZERO);
    }

    #[test]
    fn should_clamp_operation_timeout_given_smaller_remaining_budget_when_sequence_is_long() {
        // Arrange
        let deadline = OperationDeadline::from_start(
            Instant::now()
                .checked_sub(Duration::from_secs(55))
                .expect("test instant supports deadline offset"),
            Duration::from_secs(60),
        );

        // Act
        let clamped = deadline.clamp(Duration::from_secs(30));

        // Assert
        assert!(
            clamped <= Duration::from_secs(5),
            "a per-op timeout must not outlive the shared budget"
        );
    }

    #[test]
    fn should_keep_operation_timeout_given_ample_budget_when_sequence_is_short() {
        // Arrange
        let deadline = OperationDeadline::from_budget(Duration::from_secs(600));

        // Act
        let clamped = deadline.clamp(Duration::from_secs(30));

        // Assert
        assert_eq!(clamped, Duration::from_secs(30));
    }
}
