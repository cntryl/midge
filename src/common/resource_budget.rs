//! Shared bounded-resource accounting used by internal streaming pipelines.

use super::{MidgeError, MidgeResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
struct ResourceBudgetInner {
    limit: usize,
    current: AtomicUsize,
    peak: AtomicUsize,
}

/// Cloneable byte budget with RAII reservations.
#[derive(Debug, Clone)]
pub struct ResourceBudget {
    inner: Arc<ResourceBudgetInner>,
}

impl ResourceBudget {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(ResourceBudgetInner {
                limit,
                current: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            }),
        }
    }

    pub fn reserve(
        &self,
        bytes: usize,
        resource: &'static str,
    ) -> MidgeResult<ResourceReservation> {
        let mut current = self.inner.current.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return Err(MidgeError::ResourceLimit(format!(
                    "{resource} reservation overflowed the byte counter"
                )));
            };
            if next > self.inner.limit {
                return Err(MidgeError::ResourceLimit(format!(
                    "{resource} requires {bytes} bytes with {current} of {} bytes already reserved",
                    self.inner.limit
                )));
            }
            match self.inner.current.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inner.peak.fetch_max(next, Ordering::AcqRel);
                    return Ok(ResourceReservation {
                        budget: self.clone(),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    #[must_use]
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    pub(crate) fn used(&self) -> usize {
        self.inner.current.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn peak(&self) -> usize {
        self.inner.peak.load(Ordering::Acquire)
    }
}

/// Reservation released automatically when its retained buffer is dropped.
#[derive(Debug)]
pub struct ResourceReservation {
    budget: ResourceBudget,
    bytes: usize,
}

impl Drop for ResourceReservation {
    fn drop(&mut self) {
        self.budget
            .inner
            .current
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_report_current_charge_until_the_last_shared_reservation_is_released() {
        // Arrange
        let budget = ResourceBudget::new(10);
        let first = Arc::new(budget.reserve(7, "shared proof").unwrap());
        let second = Arc::clone(&first);
        assert_eq!(budget.used(), 7);

        // Act
        drop(first);
        let shared_charge = budget.used();
        drop(second);

        // Assert
        assert_eq!(shared_charge, 7);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn should_reject_reservation_when_resource_budget_would_be_exceeded() {
        // Arrange
        let budget = ResourceBudget::new(10);
        let held = budget.reserve(7, "test buffer").expect("reserve bytes");

        // Act
        let result = budget.reserve(4, "test buffer");

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(budget.peak(), 7);
        drop(held);
        assert!(budget.reserve(10, "test buffer").is_ok());
    }
}
