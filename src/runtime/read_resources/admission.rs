//! Bounded ownership of in-flight immutable reader opens.

use super::{ReaderCacheKey, SstFileIo};
use crate::common::resource_budget::{ResourceBudget, ResourceReservation};
use crate::common::{MidgeError, MidgeResult};
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::sync::Arc;

type OpenResult = MidgeResult<Arc<SstFileIo>>;

pub(super) struct OpenAdmission {
    pending: Mutex<HashMap<ReaderCacheKey, Arc<PendingOpen>>>,
    available: Condvar,
    max_owners: usize,
    coordination_budget: ResourceBudget,
}

pub(super) struct PendingOpen {
    result: Mutex<Option<OpenResult>>,
    ready: Condvar,
    _reservation: ResourceReservation,
}

pub(super) enum OpenAttempt<'a> {
    Owner(OpenOwner<'a>),
    Shared(Arc<PendingOpen>),
}

pub(super) struct OpenOwner<'a> {
    admission: &'a OpenAdmission,
    key: ReaderCacheKey,
    pending: Arc<PendingOpen>,
    coordination: Option<ResourceReservation>,
    finished: bool,
}

fn copy_result(result: &OpenResult) -> OpenResult {
    result.as_ref().map(Arc::clone).map_err(MidgeError::replay)
}

impl OpenAdmission {
    pub(super) fn new(metadata_bytes: usize) -> Self {
        // Keep provider fan-out finite and leave at least seven eighths of the
        // pool for actual reader metadata during active opens. Completed shared
        // results retain their actual metadata charge until the last waiter
        // releases them. Share the active coordination allowance:
        // a longer path can reduce fan-out instead of failing because unused
        // owner slots hold equal slices of that allowance.
        let max_owners = (metadata_bytes / (8 * 1024)).clamp(1, 8);
        Self {
            pending: Mutex::new(HashMap::new()),
            available: Condvar::new(),
            max_owners,
            coordination_budget: ResourceBudget::new(metadata_bytes / 8),
        }
    }

    pub(super) fn begin<'a>(
        &'a self,
        key: &ReaderCacheKey,
        budget: &ResourceBudget,
    ) -> MidgeResult<OpenAttempt<'a>> {
        let bytes = std::mem::size_of::<PendingOpen>()
            .saturating_add(std::mem::size_of::<OpenOwner<'_>>())
            .saturating_add(std::mem::size_of::<ReaderCacheKey>())
            .saturating_add(key.name.len().saturating_mul(2))
            .saturating_add(128);
        let mut pending = self.pending.lock();
        let coordination = loop {
            if let Some(shared) = pending.get(key) {
                return Ok(OpenAttempt::Shared(Arc::clone(shared)));
            }
            if bytes > self.coordination_budget.limit() {
                return Err(MidgeError::ResourceLimit(format!(
                    "SST open coordination requires {bytes} bytes but its allowance is {} bytes",
                    self.coordination_budget.limit()
                )));
            }
            if pending.len() < self.max_owners {
                if let Ok(reservation) = self
                    .coordination_budget
                    .reserve(bytes, "SST open coordination allowance")
                {
                    break reservation;
                }
            }
            // The caller already owns its thread. Wait for either an owner
            // slot or enough aggregate coordination space without adding work
            // to an engine queue or allocating per-waiter state.
            self.available.wait(&mut pending);
        };
        let flight = Arc::new(PendingOpen {
            result: Mutex::new(None),
            ready: Condvar::new(),
            _reservation: budget.reserve(bytes, "SST open coordination")?,
        });
        pending.insert(key.clone(), Arc::clone(&flight));
        Ok(OpenAttempt::Owner(OpenOwner {
            admission: self,
            key: key.clone(),
            pending: flight,
            coordination: Some(coordination),
            finished: false,
        }))
    }
}

impl PendingOpen {
    pub(super) fn wait(&self) -> OpenResult {
        let mut result = self.result.lock();
        loop {
            if let Some(result) = result.as_ref() {
                return copy_result(result);
            }
            self.ready.wait(&mut result);
        }
    }
}

impl OpenOwner<'_> {
    pub(super) fn complete(mut self, result: &OpenResult) {
        self.publish(copy_result(result));
    }

    fn publish(&mut self, result: OpenResult) {
        *self.pending.result.lock() = Some(result);
        self.pending.ready.notify_all();
        let mut pending = self.admission.pending.lock();
        pending.remove(&self.key);
        // Release before notifying under the same lock used by begin(), so a
        // waiter cannot observe an occupied allowance after its last owner left.
        drop(self.coordination.take());
        self.finished = true;
        drop(pending);
        self.admission.available.notify_all();
    }
}

impl Drop for OpenOwner<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.publish(Err(MidgeError::Aborted(
                "SST reader open owner was canceled".into(),
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(id: u64) -> ReaderCacheKey {
        ReaderCacheKey {
            name: format!("{id}.sst"),
            sst_id: id,
        }
    }

    #[test]
    fn should_share_coordination_capacity_when_one_open_has_a_long_path() -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let long = ReaderCacheKey {
            name: format!("{}/table.sst", "nested/".repeat(100)),
            sst_id: 1,
        };
        let workspace = budget.reserve(budget.limit() * 7 / 8, "reader metadata")?;

        // Act
        let owner = admission.begin(&long, &budget)?;
        let shared = admission.begin(&long, &budget)?;

        // Assert
        assert!(matches!(owner, OpenAttempt::Owner(_)));
        assert!(matches!(shared, OpenAttempt::Shared(_)));
        assert!(budget.peak() <= budget.limit());
        drop(owner);
        drop(shared);
        drop(workspace);
        assert!(budget.reserve(budget.limit(), "released pool").is_ok());
        Ok(())
    }

    #[test]
    fn should_wait_for_coordination_capacity_when_another_long_path_open_is_active(
    ) -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let first = ReaderCacheKey {
            name: format!("{}/first.sst", "nested/".repeat(100)),
            sst_id: 1,
        };
        let second = ReaderCacheKey {
            name: format!("{}/second.sst", "nested/".repeat(100)),
            sst_id: 2,
        };
        let owner = admission.begin(&first, &budget)?;
        let shared = admission.begin(&first, &budget)?;
        let (started_tx, started_rx) = crossbeam::channel::bounded(1);
        let (finished_tx, finished_rx) = crossbeam::channel::bounded(1);

        // Act
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                started_tx.send(()).expect("second caller started");
                let attempt = admission.begin(&second, &budget);
                finished_tx
                    .send(matches!(attempt, Ok(OpenAttempt::Owner(_))))
                    .expect("second caller completed");
            });
            started_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("second caller entered");
            let early = finished_rx.recv_timeout(std::time::Duration::from_millis(50));
            drop(owner);
            let completed =
                early.or_else(|_| finished_rx.recv_timeout(std::time::Duration::from_secs(2)));
            worker.join().expect("second caller thread");

            // Assert: a completed shared result retains its actual charge but
            // does not keep the next active owner out of coordination capacity.
            assert!(early.is_err(), "temporary contention must wait, not fail");
            assert_eq!(completed, Ok(true));
            assert!(budget
                .reserve(budget.limit(), "held shared result")
                .is_err());
        });
        drop(shared);
        assert!(budget.reserve(budget.limit(), "released pool").is_ok());
        Ok(())
    }

    #[test]
    fn should_reject_oversized_coordination_when_no_owner_can_fit_the_path() {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let oversized = ReaderCacheKey {
            name: "x".repeat(budget.limit()),
            sst_id: 1,
        };

        // Act
        let result = admission.begin(&oversized, &budget);

        // Assert
        assert!(matches!(result, Err(MidgeError::ResourceLimit(_))));
        assert!(admission.pending.lock().is_empty());
        assert!(budget.reserve(budget.limit(), "unmodified pool").is_ok());
    }

    #[test]
    fn should_preserve_shared_budget_when_reader_metadata_leaves_no_coordination_space(
    ) -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let held = budget.reserve(budget.limit(), "active reader metadata")?;

        // Act
        let rejected = admission.begin(&key(1), &budget);
        drop(held);
        let retry = admission.begin(&key(1), &budget)?;

        // Assert
        assert!(matches!(rejected, Err(MidgeError::ResourceLimit(_))));
        assert!(matches!(retry, OpenAttempt::Owner(_)));
        assert!(budget.peak() <= budget.limit());
        drop(retry);
        assert!(budget.reserve(budget.limit(), "released pool").is_ok());
        assert!(admission
            .coordination_budget
            .reserve(budget.limit() / 8, "released coordination")
            .is_ok());
        Ok(())
    }

    #[test]
    fn should_complete_failed_open_when_owner_finishes() -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let OpenAttempt::Owner(owner) = admission.begin(&key(1), &budget)? else {
            panic!("owner")
        };
        let OpenAttempt::Shared(shared) = admission.begin(&key(1), &budget)? else {
            panic!("shared")
        };
        // Act
        owner.complete(&Err(MidgeError::Timeout(
            "one failed provider request".into(),
        )));
        let failure = shared.wait();
        let retry = admission.begin(&key(1), &budget)?;
        // Assert
        assert!(
            matches!(failure, Err(MidgeError::Timeout(message)) if message == "one failed provider request")
        );
        assert!(matches!(retry, OpenAttempt::Owner(_)));
        Ok(())
    }

    #[test]
    fn should_clean_up_pending_open_when_owner_is_dropped() -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let OpenAttempt::Owner(owner) = admission.begin(&key(1), &budget)? else {
            panic!("owner")
        };
        let OpenAttempt::Shared(shared) = admission.begin(&key(1), &budget)? else {
            panic!("shared")
        };
        // Act
        drop(owner);
        let canceled = shared.wait();
        drop(shared);
        // Assert
        assert!(matches!(canceled, Err(MidgeError::Aborted(_))));
        assert!(admission.pending.lock().is_empty());
        assert!(budget
            .reserve(budget.limit(), "all released metadata")
            .is_ok());
        Ok(())
    }

    #[test]
    fn should_leave_metadata_workspace_when_open_coordination_reaches_its_cap() -> MidgeResult<()> {
        // Arrange
        let budget = ResourceBudget::new(16 * 1024);
        let admission = OpenAdmission::new(budget.limit());
        let owners: Vec<_> = (0..admission.max_owners)
            .map(|id| admission.begin(&key(id as u64), &budget))
            .collect::<MidgeResult<_>>()?;
        // Act
        let workspace = budget.reserve(budget.limit() * 7 / 8, "actual reader metadata")?;
        let OpenAttempt::Shared(shared) = admission.begin(&key(0), &budget)? else {
            panic!("same-object waiter must not need another slot")
        };
        // Assert
        assert_eq!(admission.pending.lock().len(), admission.max_owners);
        assert!(budget.peak() <= budget.limit());
        drop(owners);
        assert!(matches!(shared.wait(), Err(MidgeError::Aborted(_))));
        drop(shared);
        drop(workspace);
        assert!(budget.reserve(budget.limit(), "released pool").is_ok());
        Ok(())
    }
}
