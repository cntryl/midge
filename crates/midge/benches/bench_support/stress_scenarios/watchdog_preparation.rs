//! Scoped transfer of prepared fixtures to the native stress worker.
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Weak};

type Queue<T> = Mutex<VecDeque<T>>;

/// Keeps prepared inputs owned by the child entry point until it returns.
pub(crate) trait PreparationGuard {}

pub(super) struct PreparedSlot<T> {
    queue: Mutex<Weak<Queue<T>>>,
}

pub(super) struct PreparationScope<T> {
    queue: Arc<Queue<T>>,
    cancel: fn(T),
}

impl<T> PreparedSlot<T> {
    pub(super) const fn new() -> Self {
        Self {
            queue: Mutex::new(Weak::new()),
        }
    }

    pub(super) fn install(&self, fixtures: Vec<T>, cancel: fn(T)) -> PreparationScope<T> {
        let mut slot = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(
            slot.upgrade().is_none(),
            "a preparation scope already owns this slot"
        );
        let queue = Arc::new(Mutex::new(fixtures.into()));
        *slot = Arc::downgrade(&queue);
        PreparationScope { queue, cancel }
    }

    pub(super) fn take(&self) -> T {
        let queue = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upgrade()
            .expect("fixture preparation scope must be active");
        let result = queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .expect("prepare one fixture for every native sample");
        result
    }
}

impl<T> PreparationGuard for PreparationScope<T> {}

impl<T> PreparationScope<T> {
    pub(super) fn push(&self, fixture: T) {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(fixture);
    }
}

impl<T> Drop for PreparationScope<T> {
    fn drop(&mut self) {
        let pending = std::mem::take(
            &mut *self
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for fixture in pending {
            (self.cancel)(fixture);
        }
    }
}
