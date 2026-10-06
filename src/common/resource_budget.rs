//! Shared bounded-resource accounting used by internal streaming pipelines.

use super::{MidgeError, MidgeResult};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Source of pool identities. A pool id is never reused within a process, so a
/// failure cannot be attributed to a different pool after the original is
/// dropped.
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

/// Marker appended to retryable admission failures. The cause travels inside
/// the error itself, so it survives every string-level wrap or clone and can
/// never be consumed or erased by an unrelated reservation.
const CONTENTION_MARKER: &str = " [admission-contention pool=";

#[derive(Debug)]
struct ResourceBudgetInner {
    id: u64,
    limit: usize,
    current: AtomicUsize,
    peak: AtomicUsize,
    /// Upward-only link to the enclosing pool. A child's admission also
    /// occupies every ancestor, so a sub-budget can cap one operation without
    /// escaping the total it was carved from. Never cyclic: a child is only
    /// ever created from an existing parent.
    parent: Option<ResourceBudget>,
}

/// Measure serialized output without allocating a staging buffer.
#[derive(Default)]
pub(crate) struct ByteCounter(pub(crate) usize);

impl std::io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("serialized length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Cloneable byte budget with RAII reservations.
#[derive(Debug, Clone)]
pub struct ResourceBudget {
    inner: Arc<ResourceBudgetInner>,
    report_contention: bool,
}

impl ResourceBudget {
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            inner: Arc::new(ResourceBudgetInner {
                id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
                limit,
                current: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                parent: None,
            }),
            report_contention: false,
        }
    }

    /// Carve a caller-scoped sub-budget out of this pool.
    ///
    /// Every admission against the child also charges this pool, so a child
    /// bounds one operation without escaping the shared total. `limit` may
    /// exceed the parent's; the parent still governs.
    ///
    /// Contention reporting is deliberately not inherited — it lives outside
    /// the shared `Arc` because it is a per-holder error contract, and
    /// propagating it would change the errors unrelated callers observe.
    #[must_use]
    pub(crate) fn child(&self, limit: usize) -> Self {
        Self {
            inner: Arc::new(ResourceBudgetInner {
                id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
                limit,
                current: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                parent: Some(self.clone()),
            }),
            report_contention: false,
        }
    }

    /// Whether `ancestor` is this pool or encloses it.
    pub(crate) fn is_within(&self, ancestor: &Self) -> bool {
        self.is_within_pool(ancestor.inner.id)
    }

    fn is_within_pool(&self, pool_id: u64) -> bool {
        let mut cursor = self.inner.as_ref();
        loop {
            if cursor.id == pool_id {
                return true;
            }
            let Some(parent) = &cursor.parent else {
                return false;
            };
            cursor = parent.inner.as_ref();
        }
    }

    /// Preserve the cause of admission failure for an internal retrying caller.
    /// Ordinary consumers retain the public `ResourceLimit` error contract.
    pub(crate) fn with_contention_errors(mut self) -> Self {
        self.report_contention = true;
        self
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
                let message = format!(
                    "{resource} requires {bytes} bytes with {current} of {} bytes already reserved",
                    self.inner.limit
                );
                if bytes <= self.inner.limit && self.report_contention {
                    // Provenance is part of this attempt's own error. There is
                    // no shared slot for a sibling reservation to clear.
                    return Err(MidgeError::ResourceLimit(format!(
                        "{message}{CONTENTION_MARKER}{} release={}]",
                        self.inner.id,
                        next - self.inner.limit
                    )));
                }
                return Err(MidgeError::ResourceLimit(message));
            }
            match self.inner.current.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.inner.peak.fetch_max(next, Ordering::AcqRel);
                    let mut reservation = ResourceReservation {
                        budget: self.clone(),
                        bytes,
                        parent: None,
                    };
                    // Charge the parent only after this level succeeded, so the
                    // level that rejects is the level named in the error. If the
                    // parent rejects, `reservation` is dropped on the early
                    // return and releases this level's charge — no manual
                    // unwind, and no path that leaks a partial admission.
                    if let Some(parent) = &self.inner.parent {
                        reservation.parent = Some(Box::new(parent.reserve(bytes, resource)?));
                    }
                    return Ok(reservation);
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

    #[cfg(any(test, feature = "internal-testing"))]
    pub(crate) fn peak(&self) -> usize {
        self.inner.peak.load(Ordering::Acquire)
    }
}

/// An admission rejected by existing reservations in a specific shared pool.
/// Decoded from the failed attempt's own error; oversized requests remain a
/// plain `MidgeError::ResourceLimit`.
#[derive(Debug, Clone)]
pub(crate) struct ResourceContention {
    pool_id: u64,
    required_release: usize,
    message: String,
}

impl ResourceContention {
    /// Recover the contention that produced `error`, if it was produced by a
    /// contention-reporting budget. Pure: nothing is consumed, so the same
    /// error can be inspected any number of times and no other attempt can
    /// affect the answer.
    pub(crate) fn from_error(error: &MidgeError) -> Option<Self> {
        let MidgeError::ResourceLimit(message) = error else {
            return None;
        };
        let start = message.rfind(CONTENTION_MARKER)?;
        let tail = &message[start + CONTENTION_MARKER.len()..];
        let (pool, tail) = tail.split_once(" release=")?;
        let release = tail.strip_suffix(']')?;
        Some(Self {
            pool_id: pool.parse().ok()?,
            required_release: release.parse().ok()?,
            message: message.clone(),
        })
    }

    pub(crate) fn is_blocked_by(&self, budget: &ResourceBudget) -> bool {
        // Temporary allocations have unwound before the caller decides to retry.
        // The remaining reservations must cover the shortfall; otherwise the
        // operation's own working set cannot fit even if those owners finish.
        //
        // `budget` qualifies when it is the contended pool or sits inside it:
        // draining a descendant decrements the contended level by exactly that
        // amount. A strict ancestor does not qualify — releasing work in some
        // sibling subtree leaves the contended level untouched.
        budget.is_within_pool(self.pool_id) && budget.used() >= self.required_release
    }

    #[cfg(test)]
    pub(crate) fn with_context(mut self, context: &str) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }
}

impl std::fmt::Display for ResourceContention {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

/// Reservation released automatically when its retained buffer is dropped.
#[derive(Debug)]
pub struct ResourceReservation {
    budget: ResourceBudget,
    bytes: usize,
    // Matching charge held against the enclosing pool. Declared after `budget`
    // and `bytes` so this level is released before its parent.
    parent: Option<Box<ResourceReservation>>,
}

impl ResourceReservation {
    pub(crate) fn belongs_to(&self, budget: &ResourceBudget) -> bool {
        Arc::ptr_eq(&self.budget.inner, &budget.inner)
    }

    pub(crate) fn reserve_related(
        &self,
        bytes: usize,
        resource: &'static str,
    ) -> MidgeResult<Self> {
        self.budget.reserve(bytes, resource)
    }
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
mod hierarchy_tests {
    use super::*;

    #[test]
    fn should_charge_parent_when_child_reservation_is_admitted() {
        // Arrange
        let root = ResourceBudget::new(100);
        let child = root.child(40);

        // Act
        let _held = child.reserve(30, "child work").expect("admit child work");

        // Assert
        assert_eq!(child.used(), 30);
        assert_eq!(
            root.used(),
            30,
            "a child charge must also occupy its parent"
        );
    }

    #[test]
    fn should_release_parent_charge_when_child_reservation_is_dropped() {
        // Arrange
        let root = ResourceBudget::new(100);
        let child = root.child(40);
        let held = child.reserve(30, "child work").expect("admit child work");

        // Act
        drop(held);

        // Assert
        assert_eq!(child.used(), 0);
        assert_eq!(root.used(), 0, "parent must not retain a released charge");
    }

    #[test]
    fn should_reject_child_reservation_when_parent_is_exhausted() {
        // Arrange: both children fit individually, but not together.
        let root = ResourceBudget::new(100);
        let first = root.child(80);
        let second = root.child(80);
        let _held = first.reserve(80, "first owner").expect("admit first owner");

        // Act
        let result = second.reserve(40, "second owner");

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(
            root.used(),
            80,
            "a rejected child admission must not leak a parent charge"
        );
        assert_eq!(
            second.used(),
            0,
            "the rejecting child must unwind its own charge"
        );
    }

    #[test]
    fn should_reject_oversized_request_when_child_limit_is_smaller_than_the_root() {
        // Arrange: this is the prune-guard shape — a caller-scoped cap below the
        // shared pool must still reject work that can never fit inside it, as a
        // hard limit rather than as retryable contention.
        let root = ResourceBudget::new(64 * 1024).with_contention_errors();
        let scoped = root.child(128);

        // Act
        let result = scoped.reserve(4096, "wal retirement proof");

        // Assert
        assert!(
            matches!(result, Err(MidgeError::ResourceLimit(_))),
            "{result:?}"
        );
    }

    #[test]
    fn should_block_retry_on_descendant_pool_when_ancestor_admission_failed() {
        // Arrange: the root rejects, so the contention names the root while the
        // bytes that must drain are held through a child.
        let root = ResourceBudget::new(100).with_contention_errors();
        let holder = root.child(100);
        let requester = root.child(100).with_contention_errors();
        let _held = holder.reserve(100, "holder").expect("admit holder");
        let error = requester
            .reserve(1, "request")
            .expect_err("root must reject the request");
        let contention = ResourceContention::from_error(&error)
            .expect("root admission failure must retain internal contention metadata");

        // Act
        let descendant_blocks = contention.is_blocked_by(&holder);
        let root_blocks = contention.is_blocked_by(&root);
        let unrelated_blocks = contention.is_blocked_by(&ResourceBudget::new(100));

        // Assert
        assert!(
            descendant_blocks,
            "a descendant carrying the charge can unblock the retry"
        );
        assert!(root_blocks, "the contended pool itself qualifies");
        assert!(
            !unrelated_blocks,
            "an unrelated pool must never unblock the retry"
        );
    }

    #[test]
    fn should_not_inherit_contention_reporting_when_child_pool_is_derived() {
        // Arrange: `report_contention` is per-clone by design. Propagating it
        // would silently change the error contract for unrelated callers.
        let root = ResourceBudget::new(100).with_contention_errors();
        let child = root.child(10);
        let _held = child.reserve(10, "holder").expect("admit holder");

        // Act
        let result = child.reserve(1, "request");

        // Assert
        assert!(
            matches!(result, Err(MidgeError::ResourceLimit(_))),
            "{result:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_keep_contention_when_sibling_reservation_succeeds_after_failure() {
        // Arrange
        let budget = ResourceBudget::new(10).with_contention_errors();
        let _held = budget.reserve(7, "retained memory").unwrap();
        let error = budget.reserve(4, "first request").unwrap_err();
        let _sibling = budget.reserve(1, "sibling request").unwrap();

        // Act
        let contention = ResourceContention::from_error(&error);

        // Assert
        assert!(contention.is_some_and(|contention| contention.is_blocked_by(&budget)));
    }

    #[test]
    fn should_attribute_each_error_to_its_own_attempt_when_failures_interleave() {
        // Arrange
        let first_pool = ResourceBudget::new(10).with_contention_errors();
        let second_pool = ResourceBudget::new(10).with_contention_errors();
        let _first_held = first_pool.reserve(9, "first holder").unwrap();
        let _second_held = second_pool.reserve(9, "second holder").unwrap();
        let first_error = first_pool.reserve(5, "first request").unwrap_err();
        let second_error = second_pool.reserve(5, "second request").unwrap_err();

        // Act
        let first = ResourceContention::from_error(&first_error).unwrap();
        let second = ResourceContention::from_error(&second_error).unwrap();

        // Assert
        assert!(first.is_blocked_by(&first_pool) && !first.is_blocked_by(&second_pool));
        assert!(second.is_blocked_by(&second_pool) && !second.is_blocked_by(&first_pool));
    }

    #[test]
    fn should_keep_contention_when_error_message_is_wrapped_with_context() {
        // Arrange
        let budget = ResourceBudget::new(10).with_contention_errors();
        let _held = budget.reserve(7, "retained memory").unwrap();
        let MidgeError::ResourceLimit(message) = budget.reserve(4, "request").unwrap_err() else {
            panic!("expected resource limit");
        };
        let wrapped = MidgeError::ResourceLimit(format!("catalog readback: {message}"));

        // Act
        let contention = ResourceContention::from_error(&wrapped);

        // Assert
        assert!(contention.is_some_and(|contention| contention.is_blocked_by(&budget)));
    }

    #[test]
    fn should_keep_contention_when_nested_parent_rejects_after_sibling_success() {
        // Arrange
        let root = ResourceBudget::new(100).with_contention_errors();
        let requester = root.child(100);
        let sibling = root.child(100);
        let holder = root.child(100);
        let _held = holder.reserve(95, "holder").unwrap();
        let error = requester.reserve(10, "request").unwrap_err();
        let _sibling_held = sibling.reserve(1, "sibling").unwrap();

        // Act
        let contention = ResourceContention::from_error(&error);

        // Assert
        let contention = contention.expect("parent rejection must carry contention");
        assert!(contention.is_blocked_by(&holder));
        assert!(contention.is_blocked_by(&root));
    }

    #[test]
    fn should_not_report_contention_when_error_has_no_admission_marker() {
        // Arrange
        let error = MidgeError::ResourceLimit("identity space exhausted".into());

        // Act
        let contention = ResourceContention::from_error(&error);

        // Assert
        assert!(contention.is_none());
    }

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

    #[test]
    fn should_preserve_budget_identity_when_contention_is_contextualized_and_replayed() {
        // Arrange
        let budget = ResourceBudget::new(10).with_contention_errors();
        let _held = budget.reserve(7, "retained memory").unwrap();
        let error = budget.reserve(4, "request").unwrap_err();
        let contention = ResourceContention::from_error(&error).expect("expected contention");

        // Act
        let contention = contention.with_context("catalog readback");

        // Assert
        assert!(contention.is_blocked_by(&budget));
        assert!(!contention.is_blocked_by(&ResourceBudget::new(10)));
        assert!(contention.to_string().contains("catalog readback"));
    }

    #[test]
    fn should_reject_retry_when_remaining_reservations_cannot_cover_shortfall() {
        // Arrange
        let budget = ResourceBudget::new(10).with_contention_errors();
        let _external = budget.reserve(1, "other owner").unwrap();
        let temporary = budget.reserve(8, "own workspace").unwrap();
        let error = budget.reserve(4, "next workspace").unwrap_err();
        let contention = ResourceContention::from_error(&error).expect("expected contention");

        // Act
        drop(temporary);

        // Assert
        assert!(!contention.is_blocked_by(&budget));
    }

    #[test]
    fn should_keep_permanent_admission_errors_when_contention_reporting_is_enabled() {
        // Arrange
        let budget = ResourceBudget::new(10).with_contention_errors();
        let _held = budget.reserve(1, "retained memory").unwrap();

        // Act
        let oversized = budget.reserve(11, "oversized request");
        let overflow = budget.reserve(usize::MAX, "overflowing request");

        // Assert
        assert!(matches!(oversized, Err(MidgeError::ResourceLimit(_))));
        assert!(matches!(overflow, Err(MidgeError::ResourceLimit(_))));
    }
}
