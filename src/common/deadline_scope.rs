//! A shared absolute budget which is disarmed only after ownership transfer.

use super::{MidgeError, MidgeResult, OperationDeadline};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Active,
    Cancelled,
    Complete,
}

#[derive(Debug)]
struct State {
    phase: Phase,
    ambiguous_mutation: bool,
}

#[derive(Debug)]
struct Inner {
    deadline: OperationDeadline,
    state: Mutex<State>,
}

/// Private views share this scope without retaining an expired lifetime budget.
#[derive(Clone, Debug)]
pub(crate) struct DeadlineScope(Arc<Inner>);

impl DeadlineScope {
    pub(crate) fn new(deadline: OperationDeadline) -> Self {
        Self(Arc::new(Inner {
            deadline,
            state: Mutex::new(State {
                phase: Phase::Active,
                ambiguous_mutation: false,
            }),
        }))
    }

    pub(crate) fn deadline(&self) -> OperationDeadline {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.phase {
            Phase::Active => self.0.deadline,
            Phase::Cancelled => OperationDeadline::from_budget(Duration::ZERO),
            Phase::Complete => OperationDeadline::unbounded(),
        }
    }

    pub(crate) fn clamp(&self, per_operation_timeout: Duration) -> Duration {
        self.deadline().clamp(per_operation_timeout)
    }

    pub(crate) fn check(&self, context: &str) -> MidgeResult<()> {
        if self.deadline().is_expired() {
            Err(Self::timeout(context))
        } else {
            Ok(())
        }
    }

    /// Cancel and snapshot uncertainty under the same lock as mutation admission.
    pub(crate) fn cancel(&self) -> bool {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != Phase::Complete {
            state.phase = Phase::Cancelled;
        }
        state.ambiguous_mutation
    }

    /// Call only from the successful ownership-admission transition.
    pub(crate) fn complete(&self) -> MidgeResult<()> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match state.phase {
            Phase::Complete => return Ok(()),
            Phase::Cancelled => return Err(Self::timeout("ownership admission")),
            Phase::Active if self.0.deadline.is_expired() => {
                return Err(Self::timeout("ownership admission"));
            }
            Phase::Active => {}
        }
        if state.ambiguous_mutation {
            return Err(MidgeError::Internal(
                "unresolved mutation at ownership admission".into(),
            ));
        }
        state.phase = Phase::Complete;
        Ok(())
    }

    /// Bracket a serialized mutation whose unresolved result changes cancellation typing.
    pub(crate) fn begin_ambiguous_mutation(&self, context: &str) -> MidgeResult<()> {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase == Phase::Cancelled || self.0.deadline.is_expired() {
            return Err(Self::timeout(context));
        }
        if state.phase != Phase::Active || state.ambiguous_mutation {
            return Err(MidgeError::Internal(
                "invalid scoped mutation admission".into(),
            ));
        }
        state.ambiguous_mutation = true;
        Ok(())
    }

    /// Resolve only a definitive rejection or a confirmed, tracked owner.
    pub(crate) fn resolve_ambiguous_mutation(&self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ambiguous_mutation = false;
    }

    fn timeout(context: &str) -> MidgeError {
        MidgeError::Timeout(format!("operation deadline exhausted during {context}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::time::Instant;

    fn active_scope() -> DeadlineScope {
        DeadlineScope::new(OperationDeadline::from_budget(Duration::from_secs(5)))
    }

    #[test]
    fn should_reject_completion_when_shared_owner_has_cancelled() {
        // Arrange
        let scope = active_scope();
        let view = scope.clone();

        // Act
        let ambiguous = scope.cancel();
        let completed = view.complete();

        // Assert: cancellation is absorbing for every retained view.
        assert!(!ambiguous);
        assert!(matches!(completed, Err(MidgeError::Timeout(_))));
        assert!(matches!(
            view.check("retained view"),
            Err(MidgeError::Timeout(_))
        ));
        assert!(view.clamp(Duration::from_secs(30)).is_zero());
    }

    #[test]
    fn should_keep_completed_views_unbounded_when_cancellation_arrives_late() {
        // Arrange: the original deadline is finite and later expires.
        let started = Instant::now();
        let budget = Duration::from_millis(20);
        let scope = DeadlineScope::new(OperationDeadline::from_start(started, budget));
        let view = scope.clone();
        scope.complete().unwrap();

        // Act
        std::thread::sleep(budget.saturating_sub(started.elapsed()));
        let ambiguous = scope.cancel();

        // Assert: successful transfer disarms the startup budget permanently.
        assert!(!ambiguous);
        assert!(!view.deadline().is_bounded());
        assert!(view.check("normal operation").is_ok());
        assert_eq!(view.clamp(Duration::from_secs(30)), Duration::from_secs(30));
    }

    #[test]
    fn should_reject_late_completion_when_original_budget_is_already_exhausted() {
        // Arrange: no timer race is needed to establish an expired deadline.
        let started = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        let scope = DeadlineScope::new(OperationDeadline::from_start(
            started,
            Duration::from_millis(1),
        ));

        // Act
        let completed = scope.complete();

        // Assert
        assert!(matches!(completed, Err(MidgeError::Timeout(_))));
        assert!(scope.deadline().is_expired());
    }

    #[test]
    fn should_preserve_cancelled_state_when_an_uncertain_mutation_resolves_late() {
        // Arrange
        let scope = active_scope();
        scope.begin_ambiguous_mutation("lease CAS").unwrap();

        // Act: cancellation snapshots uncertainty before owner cleanup resolves it.
        let caller_was_indeterminate = scope.cancel();
        scope.resolve_ambiguous_mutation();
        let completed = scope.complete();

        // Assert: later cleanup cannot turn the original timeout into admission.
        assert!(caller_was_indeterminate);
        assert!(!scope.cancel());
        assert!(matches!(completed, Err(MidgeError::Timeout(_))));
        assert!(matches!(
            scope.begin_ambiguous_mutation("second lease CAS"),
            Err(MidgeError::Timeout(_))
        ));
    }

    #[test]
    fn should_withhold_completion_when_an_admitted_mutation_is_unresolved() {
        // Arrange
        let scope = active_scope();
        scope.begin_ambiguous_mutation("lease CAS").unwrap();

        // Act
        let unresolved = scope.complete();
        scope.resolve_ambiguous_mutation();
        let resolved = scope.complete();

        // Assert
        assert!(matches!(unresolved, Err(MidgeError::Internal(_))));
        resolved.unwrap();
        assert!(!scope.deadline().is_bounded());
    }

    #[test]
    fn should_choose_one_scope_transition_when_completion_races_cancellation() {
        // Arrange
        let scope = active_scope();
        let reached = Arc::new(Barrier::new(2));

        // Act: exercise the actual shared state lock, not independent snapshots.
        let (completed, ambiguous) = std::thread::scope(|threads| {
            let completing_scope = scope.clone();
            let completing_barrier = Arc::clone(&reached);
            let completing = threads.spawn(move || {
                completing_barrier.wait();
                completing_scope.complete()
            });
            reached.wait();
            let ambiguous = scope.cancel();
            (completing.join().unwrap(), ambiguous)
        });

        // Assert
        assert!(!ambiguous);
        match completed {
            Ok(()) => assert!(!scope.deadline().is_bounded()),
            Err(MidgeError::Timeout(_)) => assert!(scope.deadline().is_expired()),
            Err(error) => panic!("unexpected scope race result: {error}"),
        }
    }

    #[test]
    fn should_snapshot_admitted_uncertainty_when_mutation_races_cancellation() {
        // Arrange
        let scope = active_scope();
        let reached = Arc::new(Barrier::new(2));

        // Act: the cancellation and CAS admission share one state transition.
        let (admitted, ambiguous) = std::thread::scope(|threads| {
            let mutation_scope = scope.clone();
            let mutation_barrier = Arc::clone(&reached);
            let mutation = threads.spawn(move || {
                mutation_barrier.wait();
                mutation_scope.begin_ambiguous_mutation("lease CAS")
            });
            reached.wait();
            let ambiguous = scope.cancel();
            (mutation.join().unwrap(), ambiguous)
        });

        // Assert: no admitted uncertain mutation can be classified as no owner.
        match admitted {
            Ok(()) => assert!(ambiguous),
            Err(MidgeError::Timeout(_)) => assert!(!ambiguous),
            Err(error) => panic!("unexpected mutation race result: {error}"),
        }
        assert!(matches!(scope.complete(), Err(MidgeError::Timeout(_))));
    }
}
