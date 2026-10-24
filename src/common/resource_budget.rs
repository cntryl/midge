//! Shared bounded-resource accounting used by internal streaming pipelines.

use super::{MidgeError, MidgeResult};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
                        shared_credit: None,
                        credit_claimed: AtomicBool::new(false),
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

    /// Include one already charged shared workspace in this reservation.
    /// The credit is exclusive and follows the reservation's final physical
    /// owner, so concurrent callback clones cannot spend the same bytes twice.
    /// `bytes` must include the actual shared workspace; this cannot discount
    /// a separate new allocation. The result's `bytes`, `resize` and `split`
    /// describe its independently charged part, with credit retained separately.
    pub(crate) fn reserve_with_shared_credit(
        &self,
        bytes: usize,
        credit: &Arc<ResourceReservation>,
        resource: &'static str,
    ) -> MidgeResult<ResourceReservation> {
        let reused = credit.bytes.min(bytes / 2);
        if !Arc::ptr_eq(&self.inner, &credit.budget.inner)
            || credit.shared_credit.is_some()
            || reused == 0
            || credit
                .credit_claimed
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return self.reserve(bytes, resource);
        }
        let credit_guard = SharedReservationCredit {
            reservation: Arc::clone(credit),
        };
        // Keep at least half of the new operation's workspace independently
        // charged, including space it may split into a new compact witness.
        let mut reservation = self.reserve(bytes - reused, resource)?;
        reservation.shared_credit = Some(credit_guard);
        Ok(reservation)
    }

    #[must_use]
    pub fn limit(&self) -> usize {
        self.inner.limit
    }

    pub(crate) fn used(&self) -> usize {
        self.inner.current.load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "internal-testing"))]
    #[cfg(test)]
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
    /// Match this failed attempt's originating pool even if the owner that
    /// caused contention has already released its charge.
    pub(crate) fn is_for_pool(&self, budget: &ResourceBudget) -> bool {
        budget.is_within_pool(self.pool_id)
    }
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
    shared_credit: Option<SharedReservationCredit>,
    credit_claimed: AtomicBool,
}

#[derive(Debug)]
struct SharedReservationCredit {
    reservation: Arc<ResourceReservation>,
}

impl Drop for SharedReservationCredit {
    fn drop(&mut self) {
        self.reservation
            .credit_claimed
            .store(false, Ordering::Release);
    }
}

impl ResourceReservation {
    /// Transfer an existing charge without releasing or increasing any level
    /// of its pool hierarchy. The two resulting owners release independently.
    /// Borrowed credit stays with this source; it is never copied to the split.
    pub(crate) fn split(&mut self, bytes: usize) -> MidgeResult<Self> {
        if bytes > self.bytes || self.credit_claimed.load(Ordering::Acquire) {
            return Err(MidgeError::ResourceLimit(
                "resource reservation cannot split its owned charge".into(),
            ));
        }
        let parent = self
            .parent
            .as_mut()
            .map(|parent| parent.split(bytes).map(Box::new))
            .transpose()?;
        self.bytes -= bytes;
        Ok(Self {
            budget: self.budget.clone(),
            bytes,
            parent,
            shared_credit: None,
            credit_claimed: AtomicBool::new(false),
        })
    }
    pub(crate) fn reserve_related(
        &self,
        bytes: usize,
        resource: &'static str,
    ) -> MidgeResult<Self> {
        self.budget.reserve(bytes, resource)
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn limit(&self) -> usize {
        self.budget.limit()
    }

    /// Payload retained by this reservation and its owned parent Boxes.
    /// The caller accounts for its outer Arc header separately. Shared pool
    /// objects and borrowed-credit allocations already have separate owners.
    pub(crate) fn allocation_bytes(&self) -> usize {
        let mut bytes = std::mem::size_of::<Self>();
        let mut parent = self.parent.as_deref();
        while let Some(reservation) = parent {
            bytes = bytes.saturating_add(std::mem::size_of::<Self>());
            parent = reservation.parent.as_deref();
        }
        bytes
    }

    /// Hold `bytes` of independent charge. Growth is admitted like a new
    /// reservation and a refusal leaves the charge unchanged; a smaller total
    /// releases the rest. Any borrowed credit remains separately owned.
    pub(crate) fn resize(&mut self, bytes: usize, resource: &'static str) -> MidgeResult<()> {
        if self.credit_claimed.load(Ordering::Acquire) {
            return Err(MidgeError::ResourceLimit(
                "resource reservation cannot resize its lent charge".into(),
            ));
        }
        if bytes > self.bytes {
            let mut growth = self.budget.reserve(bytes - self.bytes, resource)?;
            self.absorb(&mut growth);
        } else {
            self.release_to(bytes);
        }
        Ok(())
    }

    // Both charges come from one budget, so their parent chains match level
    // for level. The emptied reservation then releases nothing on drop.
    fn absorb(&mut self, other: &mut Self) {
        self.bytes += std::mem::take(&mut other.bytes);
        if let (Some(parent), Some(other)) = (self.parent.as_mut(), other.parent.as_mut()) {
            parent.absorb(other);
        }
    }

    fn release_to(&mut self, bytes: usize) {
        self.budget
            .inner
            .current
            .fetch_sub(self.bytes - bytes, Ordering::AcqRel);
        self.bytes = bytes;
        if let Some(parent) = self.parent.as_mut() {
            parent.release_to(bytes);
        }
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
mod shared_credit_tests {
    use super::*;

    #[test]
    fn should_leave_credit_available_when_workspace_has_no_reusable_bytes() {
        for bytes in [0, 1] {
            // Arrange
            let budget = ResourceBudget::new(128 + bytes);
            let credit = Arc::new(budget.reserve(64, "selected witness").unwrap());

            // Act: neither tiny reservation can reuse a half-workspace byte.
            let small = budget
                .reserve_with_shared_credit(bytes, &credit, "small body")
                .unwrap();
            let body = budget.reserve_with_shared_credit(128, &credit, "next body");

            // Assert: retaining the tiny owner must not consume the claim.
            assert!(body.is_ok(), "workspace={bytes}: {body:?}");
            assert_eq!(budget.used(), 128 + bytes);
            drop(body);
            drop(small);
            drop(credit);
            assert_eq!(budget.used(), 0);
        }
    }

    #[test]
    fn should_restore_shared_credit_when_parent_refuses_admission() {
        // Arrange: child fits, but another subtree fills the shared parent.
        let root = ResourceBudget::new(100);
        let child = root.child(80);
        let credit = Arc::new(child.reserve(40, "selected witness").unwrap());
        let other = root.reserve(60, "other subtree").unwrap();

        // Act
        let refused = child.reserve_with_shared_credit(80, &credit, "selected body");
        let after_refusal = (child.used(), root.used());
        drop(other);
        let admitted = child
            .reserve_with_shared_credit(80, &credit, "retry body")
            .unwrap();

        // Assert: retry could not fit if the failed claim remained borrowed.
        assert!(matches!(refused, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(after_refusal, (40, 100));
        assert_eq!((admitted.bytes(), child.used(), root.used()), (40, 80, 80));
        drop(admitted);
        drop(credit);
        assert_eq!((child.used(), root.used()), (0, 0));
    }

    #[test]
    fn should_keep_credit_exclusive_until_last_physical_callback_drops() {
        // Arrange: double reuse would fit192; a second full body cannot fit.
        let root = ResourceBudget::new(192);
        let child = root.child(192);
        let credit = Arc::new(child.reserve(64, "selected witness").unwrap());
        let caller = Arc::new(
            child
                .reserve_with_shared_credit(128, &credit, "first body")
                .unwrap(),
        );
        let callback = Arc::clone(&caller);
        drop(caller);
        let overlapping_budget = child.clone();
        let overlapping_credit = Arc::clone(&credit);

        // Act: the first caller is gone, but its actual callback still owns work.
        let overlapping = std::thread::spawn(move || {
            overlapping_budget.reserve_with_shared_credit(128, &overlapping_credit, "overlap body")
        })
        .join()
        .unwrap();
        let while_callback_owned = (child.used(), root.used());
        drop(callback);
        let next = child
            .reserve_with_shared_credit(128, &credit, "next body")
            .unwrap();

        // Assert
        assert!(matches!(overlapping, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(while_callback_owned, (128, 128));
        assert_eq!((next.bytes(), child.used(), root.used()), (64, 128, 128));
        drop(next);
        drop(credit);
        assert_eq!((child.used(), root.used()), (0, 0));
        assert!(root.peak() <= root.limit());
    }

    #[test]
    fn should_conserve_every_parent_charge_when_owned_reservation_is_split() {
        // Arrange
        let root = ResourceBudget::new(100);
        let child = root.child(90);
        let leaf = child.child(80);
        let mut source = leaf.reserve(60, "source").unwrap();

        // Act
        let invalid = source.split(61);
        let witness = source.split(20).unwrap();
        let after_split = (leaf.used(), child.used(), root.used());
        source.resize(50, "growth").unwrap();
        source.resize(25, "shrink").unwrap();
        drop(source);

        // Assert: split transfers charge, never re-admits or releases it.
        assert!(matches!(invalid, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(after_split, (60, 60, 60));
        assert_eq!(
            (witness.bytes(), leaf.used(), child.used(), root.used()),
            (20, 20, 20, 20)
        );
        assert_eq!((leaf.peak(), child.peak(), root.peak()), (70, 70, 70));
        drop(witness);
        assert_eq!((leaf.used(), child.used(), root.used()), (0, 0, 0));
    }

    #[test]
    fn should_preserve_borrowed_credit_when_own_charge_resize_fails() {
        // Arrange
        let root = ResourceBudget::new(160);
        let child = root.child(160);
        let credit = Arc::new(child.reserve(40, "selected witness").unwrap());
        let mut body = child
            .reserve_with_shared_credit(120, &credit, "body")
            .unwrap();
        let other = root.reserve(40, "other owner").unwrap();

        // Act: growing fits this child but not the parent.
        let refused = body.resize(81, "growth");
        let after_refusal = (body.bytes(), child.used(), root.used());
        drop(other);
        body.resize(40, "retained body").unwrap();
        let witness = body.split(20).unwrap();
        drop(body);

        // Assert: resizing releases only the independently charged part.
        assert!(matches!(refused, Err(MidgeError::ResourceLimit(_))));
        assert_eq!(after_refusal, (80, 120, 160));
        assert_eq!((witness.bytes(), child.used(), root.used()), (20, 60, 60));
        let retry = child
            .reserve_with_shared_credit(80, &credit, "retry")
            .unwrap();
        assert_eq!((retry.bytes(), child.used(), root.used()), (40, 100, 100));
        drop(retry);
        drop(witness);
        drop(credit);
        assert_eq!((child.used(), root.used()), (0, 0));
    }

    #[test]
    fn should_charge_full_workspace_when_credit_belongs_to_another_pool() {
        // Arrange: sharing a parent does not make sibling pools identical.
        let root = ResourceBudget::new(200);
        let first = root.child(100);
        let second = root.child(100);
        let credit = Arc::new(first.reserve(40, "selected witness").unwrap());

        // Act
        let foreign = second
            .reserve_with_shared_credit(80, &credit, "foreign body")
            .unwrap();
        let local = first
            .reserve_with_shared_credit(80, &credit, "local body")
            .unwrap();

        // Assert: fallback neither discounts nor claims the foreign witness.
        assert_eq!((foreign.bytes(), local.bytes()), (80, 40));
        assert_eq!((first.used(), second.used(), root.used()), (80, 80, 160));
        drop(foreign);
        drop(local);
        drop(credit);
        assert_eq!((first.used(), second.used(), root.used()), (0, 0, 0));
    }

    #[test]
    fn should_keep_half_workspace_independently_charged_when_credit_is_larger() {
        // Arrange: using all80 witness bytes would incorrectly admit100.
        let budget = ResourceBudget::new(100);
        let credit = Arc::new(budget.reserve(80, "selected witness").unwrap());

        // Act
        let oversized = budget.reserve_with_shared_credit(100, &credit, "large body");
        let mut admitted = budget
            .reserve_with_shared_credit(40, &credit, "small body")
            .unwrap();
        let witness = admitted.split(20).unwrap();
        drop(admitted);

        // Assert: the fresh half remains transferable without pool expansion.
        assert!(matches!(oversized, Err(MidgeError::ResourceLimit(_))));
        assert_eq!((witness.bytes(), budget.used()), (20, 100));
        assert!(budget.reserve(1, "over capacity").is_err());
        drop(witness);
        drop(credit);
        assert_eq!(budget.used(), 0);
        assert!(budget.peak() <= budget.limit());
    }

    #[test]
    fn should_keep_split_witness_ownership_independent_of_borrowed_credit() {
        // Arrange
        let root = ResourceBudget::new(160);
        let child = root.child(160);
        let old_witness = Arc::new(child.reserve(40, "old witness").unwrap());
        let mut source = child
            .reserve_with_shared_credit(120, &old_witness, "source body")
            .unwrap();
        let new_witness = Arc::new(source.split(16).unwrap());

        // Act: the next handoff borrows only the independently owned split.
        let next = child
            .reserve_with_shared_credit(32, &new_witness, "next body")
            .unwrap();
        let combined = (child.used(), root.used());
        drop(source);
        let after_source = (child.used(), root.used());
        drop(next);
        drop(new_witness);

        // Assert: no cycle or second use of the old witness's credit.
        assert_eq!(combined, (136, 136));
        assert_eq!(after_source, (72, 72));
        assert_eq!((child.used(), root.used()), (40, 40));
        drop(old_witness);
        assert_eq!((child.used(), root.used()), (0, 0));
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

    #[test]
    fn should_charge_every_level_when_reservation_grows_and_shrinks() {
        // Arrange
        let root = ResourceBudget::new(100);
        let child = root.child(60);
        let mut held = child.reserve(10, "catalog").unwrap();

        // Act
        held.resize(50, "catalog growth").unwrap();
        let grown = (child.used(), root.used());
        held.resize(20, "catalog retained").unwrap();

        // Assert
        assert_eq!(grown, (50, 50));
        assert_eq!((held.bytes(), child.used(), root.used()), (20, 20, 20));
        drop(held);
        assert_eq!((child.used(), root.used()), (0, 0));
    }

    #[test]
    fn should_keep_charge_when_reservation_growth_is_refused() {
        // Arrange
        let budget = ResourceBudget::new(100);
        let mut held = budget.reserve(30, "catalog").unwrap();
        let _other = budget.reserve(61, "compaction").unwrap();

        // Act
        let result = held.resize(40, "catalog growth");

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert_eq!((held.bytes(), budget.used()), (30, 91));
    }
}
